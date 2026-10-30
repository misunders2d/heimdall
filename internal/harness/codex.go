package harness

// Ported from AgentNet internal/client/codexroom.go (codexRoomBridge).
// Changes: always a saved (non-ephemeral) thread, resumed by ID; the
// agentnet_room dynamic tool, approval relay callbacks, diagnostics and
// model/thinking telemetry are not ported. Every native approval or input
// request ends the turn as needs-human, exactly like AgentNet without a relay.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const clientVersion = "0.1.0"

type codexBridge struct {
	ctx                  context.Context
	input                io.WriteCloser
	writeMu              sync.Mutex
	stateMu              sync.Mutex
	readMu               sync.Mutex
	ready                chan struct{}
	readyOnce            sync.Once
	answered             chan struct{} // closed once an answer is published
	steerID              int
	steering             map[string]chan SteerOutcome
	closeOnce            sync.Once
	prompt, cwd          string
	images               []Image
	resume               string
	buf                  []byte
	thread, turn, answer string
	answerPhase          string
	answers              []string // final texts for inputs before the last accepted steer
	done, published      bool
	failure, human       string
}

func newCodexBridge(ctx context.Context, cmd *exec.Cmd, prompt string, images []Image, s Session) (*codexBridge, error) {
	// A real OS stdin pipe lets Cmd.Wait finish on process EOF.
	cmd.Stdin = nil
	out, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	b := &codexBridge{prompt: prompt, images: images, cwd: cmd.Dir, ctx: ctx, input: out, ready: make(chan struct{}), answered: make(chan struct{}), steering: map[string]chan SteerOutcome{}}
	if s.Resume {
		b.resume = s.ID
	}
	go func() {
		b.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{"clientInfo": map[string]any{"name": "heimdall", "version": clientVersion}, "capabilities": map[string]any{"experimentalApi": true}}})
	}()
	// Prompt is retained only for this invocation, never in command arguments.
	go func() { <-ctx.Done(); b.close() }()
	return b, nil
}

func (b *codexBridge) readyCh() <-chan struct{} { return b.ready }

func (b *codexBridge) answeredCh() <-chan struct{} { return b.answered }

func (b *codexBridge) close() { b.closeOnce.Do(func() { b.input.Close() }) }

func (b *codexBridge) send(v any) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	_ = json.NewEncoder(b.input).Encode(v)
}

func (b *codexBridge) failLocked(why string) {
	if !b.done {
		b.done = true
		b.failure = why
	}
	b.close()
}

// humanLocked ends the run with a genuine native permission or input wait.
func (b *codexBridge) humanLocked(why string) {
	if !b.done {
		b.done = true
		b.human = why
	}
	b.close()
}

func (b *codexBridge) fail(why string) { b.stateMu.Lock(); defer b.stateMu.Unlock(); b.failLocked(why) }

func (b *codexBridge) settled() bool { b.stateMu.Lock(); defer b.stateMu.Unlock(); return b.done }

func (b *codexBridge) outcome() outcome {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	o := outcome{failure: b.failure, human: b.human, session: b.thread}
	if b.published {
		o.answer, o.answers = b.answer, slices.Clone(b.answers)
	}
	return o
}

func (b *codexBridge) markReady() { b.readyOnce.Do(func() { close(b.ready) }) }

// codexRPCError words a JSON-RPC error response to one of the bridge's own requests.
func codexRPCError(id, raw json.RawMessage) string {
	method := map[string]string{"1": "initialize", "2": "thread start", "3": "turn start"}[string(id)]
	if method == "" {
		method = "request " + clip(id)
	}
	var e struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &e)
	if e.Message == "" {
		e.Message = "no message"
	}
	return fmt.Sprintf("Codex refused %s: %s (code %d)", method, clip([]byte(e.Message)), e.Code)
}

func (b *codexBridge) finish() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	for id, ch := range b.steering {
		ch <- SteerUnknown
		delete(b.steering, id)
	}
	if !b.done {
		if b.thread == "" {
			b.failLocked("Codex exited before starting a session")
		} else {
			b.failLocked("Codex ended before the turn completed")
		}
	}
	b.close()
}

func (b *codexBridge) Write(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			if len(b.buf)+len(p) > lineMax {
				b.fail("Codex sent a protocol line over the size bound")
				return n, nil
			}
			b.buf = append(b.buf, p...)
			break
		}
		if len(b.buf)+i > lineMax {
			b.fail("Codex sent a protocol line over the size bound")
			return n, nil
		}
		b.buf = append(b.buf, p[:i]...)
		b.event(b.buf)
		b.buf = nil
		p = p[i+1:]
	}
	return n, nil
}

func (b *codexBridge) event(raw []byte) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	var m struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &m) != nil {
		b.failLocked("Codex sent an unreadable protocol line")
		return
	}
	if ch, ok := b.steering[string(m.ID)]; ok && m.Method == "" {
		result := SteerUnknown
		var rpcErr struct {
			Code int `json:"code"`
		}
		var response struct {
			TurnID string `json:"turnId"`
		}
		if json.Unmarshal(m.Error, &rpcErr) == nil && rpcErr.Code == -32601 {
			result = SteerUnsupported
		} else if (len(m.Error) == 0 || string(m.Error) == "null") && json.Unmarshal(m.Result, &response) == nil && response.TurnID == b.turn {
			result = SteerAccepted
			if !b.done {
				// A final answer given before this steer belongs to the
				// earlier inputs; the answer to the steer comes after it.
				// Once the turn completed, the published answer stays.
				b.answers, b.answer, b.answerPhase = keepAnswer(b.answers, b.answer), "", ""
			}
		}
		delete(b.steering, string(m.ID))
		ch <- result
		return
	}
	if b.done || m.Method == "" && strings.HasPrefix(string(m.ID), `"heimdall-steer-`) {
		return
	}
	if len(m.Error) > 0 && string(m.Error) != "null" {
		b.failLocked(codexRPCError(m.ID, m.Error))
		return
	}
	if m.Method == "" {
		switch string(m.ID) {
		case "1":
			b.send(map[string]any{"method": "initialized"})
			method, params := "thread/start", map[string]any{"cwd": b.cwd, "ephemeral": false}
			if b.resume != "" {
				method, params = "thread/resume", map[string]any{"cwd": b.cwd, "threadId": b.resume}
			}
			// Select native automatic review without changing the native sandbox.
			params["approvalPolicy"] = "on-request"
			params["approvalsReviewer"] = "auto_review"
			b.send(map[string]any{"id": 2, "method": method, "params": params})
		case "2":
			var r struct {
				Thread struct {
					ID string `json:"id"`
				} `json:"thread"`
			}
			if json.Unmarshal(m.Result, &r) != nil || r.Thread.ID == "" || b.resume != "" && r.Thread.ID != b.resume {
				b.failLocked("Codex returned no usable session")
				return
			}
			b.thread = r.Thread.ID
			b.send(map[string]any{"id": 3, "method": "turn/start", "params": map[string]any{"threadId": b.thread, "cwd": b.cwd, "input": codexInput(b.prompt, b.images)}})
		case "3":
			var r struct {
				Turn struct {
					ID string `json:"id"`
				} `json:"turn"`
			}
			if json.Unmarshal(m.Result, &r) != nil || r.Turn.ID == "" || b.turn != "" && b.turn != r.Turn.ID {
				b.failLocked("Codex returned an unexpected turn")
				return
			}
			b.turn = r.Turn.ID
			b.markReady()
		}
		return
	}
	var p struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Turn     struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"turn"`
		Item struct {
			ID    string  `json:"id"`
			Type  string  `json:"type"`
			Text  string  `json:"text"`
			Phase *string `json:"phase"`
		} `json:"item"`
		Reason, Message string
	}
	if json.Unmarshal(m.Params, &p) != nil {
		return
	}
	switch m.Method {
	case "turn/started":
		if p.ThreadID == b.thread {
			if b.turn != "" && b.turn != p.Turn.ID {
				b.failLocked("Codex started an unexpected turn")
				return
			}
			b.turn = p.Turn.ID
			b.markReady()
		}
	case "item/completed":
		if p.Item.Type != "agentMessage" || b.thread == "" || b.turn == "" || p.ThreadID != b.thread || p.TurnID != b.turn || strings.TrimSpace(p.Item.Text) == "" {
			return
		}
		phase := "legacy"
		if p.Item.Phase != nil {
			phase = *p.Item.Phase
		}
		switch {
		case p.Item.Phase != nil && phase != "final_answer":
		case phase == "legacy" && b.answerPhase == "final_answer":
		default:
			// Completed items are authoritative. A legacy model may omit
			// phase, but that fallback never replaces an explicit final.
			b.answer, b.answerPhase = p.Item.Text, phase
		}
	case "turn/completed":
		if b.thread == "" || b.turn == "" || p.ThreadID != b.thread || p.Turn.ID != b.turn {
			return
		}
		if p.Turn.Status != "completed" {
			why := "Codex ended the turn as " + strconv.Quote(clip([]byte(p.Turn.Status)))
			if p.Turn.Error != nil && p.Turn.Error.Message != "" {
				why += ": " + clip([]byte(p.Turn.Error.Message))
			}
			b.failLocked(why)
			return
		}
		b.answers = keepAnswer(b.answers, b.answer)
		b.answer = joinAnswers(b.answers)
		if strings.TrimSpace(b.answer) == "" {
			b.failLocked("Codex completed the turn without an answer")
			return
		}
		b.done, b.published = true, true
		b.close()
		close(b.answered)
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "item/tool/requestUserInput", "tool/requestUserInput", "mcpServer/elicitation/request":
		if len(m.ID) > 0 {
			if b.thread == "" || p.ThreadID != b.thread || b.turn == "" || (p.TurnID != b.turn && !(m.Method == "mcpServer/elicitation/request" && p.TurnID == "")) {
				b.failLocked("Codex sent " + m.Method + " for another turn; nothing was approved")
				return
			}
			why := "Codex's native setup requires an approval or input for this action, which this background turn cannot collect. Heimdall did not approve or bypass it."
			for _, text := range []string{p.Reason, p.Message} {
				if text = strings.TrimSpace(text); text != "" {
					why += " " + clip([]byte(text))
				}
			}
			b.humanLocked(why)
		}
	default:
		if len(m.ID) > 0 {
			b.failLocked("Codex sent an unsupported request " + strconv.Quote(clip([]byte(m.Method))) + "; nothing was approved")
		} // new approval or unsupported client action: never approve it
	}
}

// codexInput is a turn's UserInput list: the text, then each image as a
// localImage the app server reads from its path.
func codexInput(text string, images []Image) []any {
	input := []any{map[string]any{"type": "text", "text": text}}
	for _, im := range images {
		if im.Path != "" {
			input = append(input, map[string]any{"type": "localImage", "path": im.Path})
		}
	}
	return input
}

func (b *codexBridge) steer(ctx context.Context, id, text string, images []Image) SteerOutcome {
	b.stateMu.Lock()
	if b.done || b.thread == "" || b.turn == "" || b.ctx.Err() != nil {
		b.stateMu.Unlock()
		return SteerQueued
	}
	b.steerID++
	rpcID := fmt.Sprintf("heimdall-steer-%d", b.steerID)
	keyRaw, _ := json.Marshal(rpcID)
	key := string(keyRaw)
	ch := make(chan SteerOutcome, 1)
	b.steering[key] = ch
	request := map[string]any{"id": rpcID, "method": "turn/steer", "params": map[string]any{"threadId": b.thread, "expectedTurnId": b.turn, "clientUserMessageId": id, "input": codexInput(text, images)}}
	b.stateMu.Unlock()
	// A write failure can follow a partial handover: it proves no rejection.
	go b.send(request)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
	case <-timer.C:
	}
	b.stateMu.Lock()
	delete(b.steering, key)
	b.stateMu.Unlock()
	return SteerUnknown
}
