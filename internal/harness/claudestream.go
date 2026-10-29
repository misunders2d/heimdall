package harness

// Ported from AgentNet internal/client/claudestream.go. Changes: answer,
// failure and needs-human reason are fields instead of an output stream; the
// --include-partial-messages probe, activity and thinking telemetry are not
// ported; launch arguments are built by the caller.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"sync"
)

// claudeStreamBridge owns one fresh native process and one initial request.
type claudeStreamBridge struct {
	writes               sync.WaitGroup
	ctx                  context.Context
	input                io.WriteCloser
	ready                chan struct{}
	readyOnce, closeOnce sync.Once
	mu, writeMu, readMu  sync.Mutex
	buf                  []byte
	active, done, failed bool
	published            bool
	prompt, session      string
	failure, human       string
	closed               chan struct{}
	answer               string
	pending              map[string]*pendingSteer
	seen                 map[string]string
}

// expected is the session ID passed with --session-id or --resume; the
// stream must stay in exactly that session.
func newClaudeStreamBridge(ctx context.Context, cmd *exec.Cmd, prompt, expected string) (*claudeStreamBridge, error) {
	cmd.Stdin = nil
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	b := &claudeStreamBridge{ctx: ctx, input: input, ready: make(chan struct{}), pending: map[string]*pendingSteer{}, seen: map[string]string{}, closed: make(chan struct{}), prompt: prompt, session: expected}
	b.writes.Add(1)
	go func() {
		defer b.writes.Done()
		if b.send(ctx, "", nil, map[string]any{"type": "user", "message": map[string]string{"role": "user", "content": prompt}}) != nil {
			b.fail("Claude did not accept the request input")
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			b.close()
		case <-b.closed:
		}
	}()
	return b, nil
}

func (b *claudeStreamBridge) readyCh() <-chan struct{} { return b.ready }

func (b *claudeStreamBridge) send(ctx context.Context, id string, pending *pendingSteer, v any) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	b.mu.Lock()
	if b.done || b.ctx.Err() != nil || ctx.Err() != nil || id != "" && (!b.active || b.pending[id] == nil || b.pending[id] != pending) {
		b.mu.Unlock()
		return context.Canceled
	}
	b.mu.Unlock()
	return json.NewEncoder(b.input).Encode(v)
}

func (b *claudeStreamBridge) close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.endLocked()
		b.mu.Unlock()
		// CommandContext owns native process cancellation. Close the pipe directly:
		// taking writeMu first could deadlock behind a blocked startup/steer write.
		_ = b.input.Close()
		b.writes.Wait()
		close(b.closed)
	})
}

func (b *claudeStreamBridge) fail(why string) { b.mu.Lock(); defer b.mu.Unlock(); b.failLocked(why) }

func (b *claudeStreamBridge) failLocked(why string) { b.stopLocked(why, false) }

func (b *claudeStreamBridge) humanLocked(why string) { b.stopLocked(why, true) }

func (b *claudeStreamBridge) stopLocked(why string, human bool) {
	if b.done && !b.failed {
		return
	}
	if !b.failed {
		b.failed = true
		if human {
			b.human = why
		} else {
			b.failure = why
		}
	}
	b.endLocked()
	_ = b.input.Close()
}

func (b *claudeStreamBridge) endLocked() {
	b.done = true
	b.active = false
	for id, p := range b.pending {
		p.result <- SteerUnknown
		delete(b.pending, id)
	}
}

// settled reports a terminal native outcome; closing the bridge is not one.
func (b *claudeStreamBridge) settled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.published || b.failed
}

func (b *claudeStreamBridge) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done {
		b.failLocked("Claude ended before its request settled")
	}
}

func (b *claudeStreamBridge) outcome() outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := outcome{failure: b.failure, human: b.human, session: b.session}
	if b.published {
		o.answer = b.answer
	}
	return o
}

func (b *claudeStreamBridge) Write(data []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	for rest := data; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\n')
		n := len(rest)
		if i >= 0 {
			n = i
		}
		if len(b.buf)+n > lineMax {
			b.buf = nil
			b.fail("Claude sent a protocol line over the size bound")
			return len(data), nil
		}
		b.buf = append(b.buf, rest[:n]...)
		if i < 0 {
			break
		}
		b.event(bytes.TrimSuffix(b.buf, []byte{'\r'}))
		b.buf = nil
		rest = rest[i+1:]
	}
	return len(data), nil
}

// The same owned stream must replay the exact correlated user input before
// the original result. A write or assistant echo is never consumption evidence.
func (b *claudeStreamBridge) event(raw []byte) {
	var e struct {
		Type    string          `json:"type"`
		Subtype string          `json:"subtype"`
		Session string          `json:"session_id"`
		UUID    string          `json:"uuid"`
		Replay  bool            `json:"isReplay"`
		Parent  json.RawMessage `json:"parent_tool_use_id"`
		Message struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Result  string            `json:"result"`
		IsError bool              `json:"is_error"`
		Denials []json.RawMessage `json:"permission_denials"`
	}
	if json.Unmarshal(raw, &e) != nil {
		b.fail("Claude sent an unreadable protocol line; no answer was used")
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return
	}
	if b.ctx.Err() != nil {
		b.endLocked()
		_ = b.input.Close()
		return
	}
	// Optional stream events cannot establish or change the owned session.
	if e.Type == "stream_event" {
		return
	}
	if e.Session != "" {
		if b.session != "" && b.session != e.Session {
			b.failLocked("Claude switched to another session; no answer was used")
			return
		}
		b.session = e.Session
	}
	switch e.Type {
	case "user":
		var text string
		if b.session == "" || e.Session != b.session || e.UUID == "" || !e.Replay || string(e.Parent) != "null" || e.Message.Role != "user" || json.Unmarshal(e.Message.Content, &text) != nil {
			return
		}
		if !b.active && text == b.prompt {
			b.active = true
			b.readyOnce.Do(func() { close(b.ready) })
			return
		}
		if !b.active {
			return
		}
		for id, p := range b.pending {
			if text == p.text {
				if p.ctx != nil && p.ctx.Err() != nil {
					p.result <- SteerUnknown
					delete(b.pending, id)
					continue
				}
				p.result <- SteerAccepted
				b.seen[id] = text
				delete(b.pending, id)
			}
		}
	case "result":
		switch {
		case e.Session == "" || e.Session != b.session:
			b.failLocked("Claude reported a result for another session; no answer was used")
			return
		case len(e.Denials) > 0:
			b.humanLocked("Claude's native permission setup denied an action this request needed; review it in a native session")
			return
		case e.IsError:
			why := "Claude reported an error"
			if msg := clip([]byte(e.Result)); msg != "" {
				why += ": " + msg
			} else if e.Subtype != "" {
				why += " (" + clip([]byte(e.Subtype)) + ")"
			}
			b.failLocked(why)
			return
		}
		if len(e.Result) > maxAnswer+1 {
			e.Result = e.Result[:maxAnswer+1]
		}
		b.answer = e.Result
		b.publishLocked()
	case "control_request":
		b.humanLocked("Claude needs native approval or input; no response or permission override was invented")
	case "system":
		if e.Subtype == "permission_denied" {
			b.humanLocked("Claude could not obtain native permission; review it in a native session")
		}
	}
}

func (b *claudeStreamBridge) publishLocked() {
	if strings.TrimSpace(b.answer) == "" {
		b.failLocked("Claude finished without an answer")
		return
	}
	b.published = true
	b.endLocked()
	_ = b.input.Close()
}

func (b *claudeStreamBridge) steer(ctx context.Context, id, text string) SteerOutcome {
	b.mu.Lock()
	if b.done || !b.active || b.ctx.Err() != nil || ctx.Err() != nil || id == "" || text == "" {
		b.mu.Unlock()
		return SteerQueued
	}
	if prior, ok := b.seen[id]; ok {
		b.mu.Unlock()
		if prior == text {
			return SteerAccepted
		}
		return SteerUnknown
	}
	if _, ok := b.pending[id]; ok {
		b.mu.Unlock()
		return SteerUnknown
	}
	p := &pendingSteer{ctx: ctx, text: text, result: make(chan SteerOutcome, 1)}
	b.pending[id] = p
	b.writes.Add(1)
	b.mu.Unlock()
	go func() {
		defer b.writes.Done()
		if b.send(ctx, id, p, map[string]any{"type": "user", "message": map[string]string{"role": "user", "content": text}}) != nil {
			b.mu.Lock()
			if b.pending[id] == p {
				delete(b.pending, id)
				p.result <- SteerUnknown
			}
			b.mu.Unlock()
		}
	}()
	select {
	case result := <-p.result:
		return result
	case <-ctx.Done():
	case <-b.ctx.Done():
	}
	b.mu.Lock()
	delete(b.pending, id)
	b.mu.Unlock()
	return SteerUnknown
}
