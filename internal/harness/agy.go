package harness

// Ported from AgentNet internal/client/agystream.go. Antigravity's native
// stream-json contract, checked by AgentNet against agy 1.2.10:
// https://antigravity.google/docs/cli/headless/
// Each process gets one user event and stdin closes. Only the terminal result
// is an answer; tool output and streamed commentary never become replies.
// AGY has no steering: mid-turn input becomes a follow-up. Changes: results
// map onto answer/failure/needs-human; the conversation's init event is the
// ready signal; activity telemetry is not ported.

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"sync"
)

func agyInput(prompt string) string {
	raw, _ := json.Marshal(struct {
		Event   string `json:"event"`
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}{Event: "user", Message: struct {
		Content string `json:"content"`
	}{Content: prompt}})
	return string(raw) + "\n"
}

type agyBridge struct {
	mu                       sync.Mutex
	ready                    chan struct{}
	readyOnce                sync.Once
	diag                     agyDiagnostics
	line                     []byte
	skipping, damaged        bool
	conversation, expected   string
	complete                 bool
	status, response, detail string
}

func newAgyBridge(_ context.Context, cmd *exec.Cmd, prompt, expected string) *agyBridge {
	b := &agyBridge{ready: make(chan struct{}), expected: expected}
	cmd.Stdin = strings.NewReader(agyInput(prompt))
	if cmd.Stderr != nil {
		cmd.Stderr = io.MultiWriter(cmd.Stderr, &b.diag)
	} else {
		cmd.Stderr = &b.diag
	}
	return b
}

func (b *agyBridge) readyCh() <-chan struct{} { return b.ready }

func (b *agyBridge) steer(context.Context, string, string, []Image) SteerOutcome {
	return SteerUnsupported
}

func (b *agyBridge) settled() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.complete && !b.damaged }

func (b *agyBridge) finish() { b.mu.Lock(); defer b.mu.Unlock(); b.flush() }

func (b *agyBridge) close() {}

func (b *agyBridge) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		chunk := p
		if i >= 0 {
			chunk = p[:i]
		}
		if !b.skipping {
			if len(b.line)+len(chunk) > lineMax {
				b.skipping, b.line = true, nil
			} else {
				b.line = append(b.line, chunk...)
			}
		}
		if i < 0 {
			break
		}
		b.flush()
		p = p[i+1:]
	}
	return n, nil
}

func (b *agyBridge) flush() {
	defer func() { b.line, b.skipping = b.line[:0], false }()
	if b.skipping {
		b.damaged = true
		return
	}
	if len(bytes.TrimSpace(b.line)) == 0 {
		return
	}
	var ev struct {
		Event        string `json:"event"`
		Conversation string `json:"conversation_id"`
		Result       *struct {
			Conversation string `json:"conversation_id"`
			Status       string `json:"status"`
			Response     string `json:"response"`
			Error        string `json:"error"`
		} `json:"result"`
	}
	if json.Unmarshal(b.line, &ev) != nil || ev.Event == "" {
		b.damaged = true
		return
	}
	switch ev.Event {
	case "init":
		b.setConversation(ev.Conversation)
	case "result":
		if b.complete || ev.Result == nil {
			b.damaged = true
			return
		}
		b.complete = true
		b.setConversation(ev.Result.Conversation)
		b.status, b.response, b.detail = ev.Result.Status, ev.Result.Response, ev.Result.Error
	}
}

func (b *agyBridge) setConversation(id string) {
	compact := strings.ReplaceAll(id, "-", "")
	_, err := hex.DecodeString(compact)
	if len(id) != 36 || len(compact) != 32 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' || err != nil ||
		b.conversation != "" && b.conversation != id || b.expected != "" && b.expected != id {
		b.damaged = true
		return
	}
	b.conversation = id
	b.readyOnce.Do(func() { close(b.ready) })
}

// Diagnostics are read on stderr's own goroutine, independently of stdout.
// Keep scanning after the bounded buffer fills: a late soft denial must not
// become success merely because native progress used its first bytes.
type agyDiagnostics struct {
	mu                       sync.Mutex
	tail                     string
	permission, auth, failed bool
}

func (d *agyDiagnostics) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	text := d.tail + strings.ToLower(string(p))
	d.permission = d.permission || strings.Contains(text, "headless mode cannot prompt for") && strings.Contains(text, "auto-denied")
	d.auth = d.auth || strings.Contains(text, "authentication required") || strings.Contains(text, "unauthenticated")
	d.failed = d.failed || strings.Contains(text, "agy_error:")
	d.tail = text[max(0, len(text)-512):]
	return len(p), nil
}

func (b *agyBridge) outcome() outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.diag.mu.Lock()
	permission, auth, failed := b.diag.permission, b.diag.auth, b.diag.failed
	b.diag.mu.Unlock()
	o := outcome{session: b.conversation}
	if b.damaged {
		o.session = ""
	}
	detail := strings.ToLower(b.detail)
	switch {
	case !b.complete && !b.damaged && !permission && !auth:
		// Not settled: the run reports the process outcome.
	case permission:
		o.human = "Antigravity could not obtain a native tool approval. Review the request in its native setup; Heimdall did not change permissions or retry it."
	case auth || strings.Contains(detail, "authentication required") || strings.Contains(detail, "unauthenticated"):
		o.failure = "Antigravity requires sign-in for this OS user; sign in natively, then retry."
	case b.status == "WAITING" && b.complete && !b.damaged:
		o.human = "Antigravity is waiting for native input; this turn was not retried."
	case b.damaged:
		o.failure = "Antigravity's result or conversation identity could not be verified; no partial answer was used."
	case b.status == "CANCELED":
		o.failure = "Antigravity cancelled the run."
	case b.status != "SUCCESS" || failed:
		o.failure = "Antigravity did not complete the run (" + clip([]byte(b.status)) + ")."
	case strings.TrimSpace(b.response) == "":
		o.failure = "Antigravity completed without an answer."
	default:
		o.answer = b.response
	}
	return o
}
