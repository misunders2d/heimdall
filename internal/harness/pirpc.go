package harness

// Ported from AgentNet internal/client/pirpc.go. Changes: the answer, failure
// and needs-human reason are kept as fields instead of written to an output
// stream; activity and thinking telemetry are not ported; launch arguments
// are built by the caller (--mode rpc plus session flags).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

const initialID = "heimdall-initial"

// piRPCBridge owns one fresh native process and one initial request. Native
// settings/extensions remain the run-as user's; no interactive session is attached.
type piRPCBridge struct {
	writes               sync.WaitGroup
	ctx                  context.Context
	input                io.WriteCloser
	activityMessageID    string
	ready                chan struct{}
	readyOnce, closeOnce sync.Once
	mu, writeMu, readMu  sync.Mutex
	buf                  []byte
	active, done, failed bool
	omp, ended           bool
	published            bool
	failure, human       string
	session              string // the session directory name
	closed               chan struct{}
	answer               string
	pending              map[string]*pendingSteer
	seen                 map[string]string
}

type pendingSteer struct {
	ctx    context.Context
	text   string
	result chan SteerOutcome
}

func newPiRPCBridge(ctx context.Context, cmd *exec.Cmd, prompt, session string, omp bool) (*piRPCBridge, error) {
	cmd.Stdin = nil
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	b := &piRPCBridge{ctx: ctx, input: input, ready: make(chan struct{}), pending: map[string]*pendingSteer{}, seen: map[string]string{}, closed: make(chan struct{}), omp: omp, session: session}
	b.writes.Add(1)
	go func() {
		defer b.writes.Done()
		if b.send(ctx, "", nil, map[string]any{"id": initialID, "type": "prompt", "message": prompt}) != nil {
			b.fail(b.nativeName() + " did not accept the request input")
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

func (b *piRPCBridge) readyCh() <-chan struct{} { return b.ready }

func (b *piRPCBridge) send(ctx context.Context, id string, pending *pendingSteer, v any) error {
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

func (b *piRPCBridge) close() {
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

func (b *piRPCBridge) fail(why string) { b.mu.Lock(); defer b.mu.Unlock(); b.failLocked(why) }

// failLocked ends the request with a technical failure, never a human decision.
func (b *piRPCBridge) failLocked(why string) { b.stopLocked(why, false) }

// humanLocked ends the request with a genuine native approval or input wait.
func (b *piRPCBridge) humanLocked(why string) { b.stopLocked(why, true) }

func (b *piRPCBridge) stopLocked(why string, human bool) {
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

func (b *piRPCBridge) endLocked() {
	b.done = true
	b.active = false
	b.activityMessageID = ""
	for id, p := range b.pending {
		p.result <- SteerUnknown
		delete(b.pending, id)
	}
}

// settled reports a terminal native outcome; closing the bridge is not one.
func (b *piRPCBridge) settled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.published || b.failed
}

func (b *piRPCBridge) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done {
		b.failLocked(b.nativeName() + " ended before its request settled")
	}
}

func (b *piRPCBridge) outcome() outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := outcome{failure: b.failure, human: b.human, session: b.session}
	if b.published {
		o.answer = b.answer
	}
	return o
}

func (b *piRPCBridge) Write(data []byte) (int, error) {
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
			b.fail(b.nativeName() + " sent a protocol line over the size bound")
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

func piMessageText(raw json.RawMessage) (role, text, stop string, tool bool) {
	var m struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
		Stop    string          `json:"stopReason"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	role, stop = m.Role, m.Stop
	if json.Unmarshal(m.Content, &text) == nil {
		return
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(m.Content, &blocks) != nil {
		return
	}
	for _, v := range blocks {
		if v.Type == "text" {
			text += v.Text
		}
		if v.Type == "toolCall" {
			tool = true
		}
	}
	return
}

func (b *piRPCBridge) event(raw []byte) {
	var e struct {
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		Success bool            `json:"success"`
		Method  string          `json:"method"`
		Message json.RawMessage `json:"message"`
		Data    struct {
			Disposition string `json:"disposition"`
		} `json:"data"`
		Error     string `json:"error"`
		Aborted   bool   `json:"aborted"`
		MessageID string `json:"messageId"`
		Status    string `json:"status"`
		Settled   bool   `json:"sessionSettled"`
	}
	if json.Unmarshal(raw, &e) != nil {
		b.fail(b.nativeName() + " sent an unreadable protocol line; no answer was used")
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
	switch e.Type {
	case "agent_start":
		if b.omp && b.ended {
			return
		}
		b.active = true
		b.activityMessageID = ""
		b.readyOnce.Do(func() { close(b.ready) })
	case "agent_end":
		if b.omp {
			_ = b.input.Close()
		}
		b.ended = true
		b.active = false
		b.activityMessageID = ""
		// A late native queue response never establishes consumption.
		for id, p := range b.pending {
			p.result <- SteerUnknown
			delete(b.pending, id)
		}
	case "response":
		if e.ID == initialID && !e.Success {
			why := b.nativeName() + " refused the request"
			if msg := clip([]byte(e.Error)); msg != "" {
				why += ": " + msg
			}
			b.failLocked(why)
			return
		}
		if p, ok := b.pending[e.ID]; ok {
			if !e.Success {
				p.result <- SteerUnsupported
				delete(b.pending, e.ID)
			} else if !b.omp && e.Data.Disposition != "queued" {
				p.result <- SteerUnknown
				delete(b.pending, e.ID)
			}
			// queued is only transport acceptance. Wait for the user event.
		}
	case "message_start":
		role, text, _, _ := piMessageText(e.Message)
		if role == "assistant" && b.active {
			b.activityMessageID = ""
			if len(e.MessageID) <= 256 {
				b.activityMessageID = e.MessageID
			}
		}
		var native struct {
			Steering    bool   `json:"steering"`
			Attribution string `json:"attribution"`
		}
		_ = json.Unmarshal(e.Message, &native)
		if role == "user" && b.active && (!b.omp || e.MessageID != "" && native.Steering && native.Attribution == "user") {
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
		}
	case "message_end":
		if !b.omp || e.MessageID == b.activityMessageID {
			b.activityMessageID = ""
		}
		role, text, stop, tool := piMessageText(e.Message)
		if role == "assistant" && !tool {
			if stop == "error" || stop == "aborted" {
				why := b.nativeName() + " stopped with " + strconv.Quote(stop)
				var m struct {
					Error string `json:"errorMessage"`
				}
				if json.Unmarshal(e.Message, &m) == nil && m.Error != "" {
					why += ": " + clip([]byte(m.Error))
				}
				b.failLocked(why)
				return
			}
			if len(text) > maxAnswer+1 {
				text = text[:maxAnswer+1]
			}
			b.answer = text
		}
	case "extension_ui_request":
		switch e.Method {
		case "notify", "setStatus", "setWidget", "setTitle", "set_editor_text", "setWorkingMessage", "setFooter", "setHeader":
		default:
			b.humanLocked("A native " + b.nativeName() + " extension needs interactive input; no approval or response was invented")
		}
	case "prompt_result":
		if !b.omp || e.ID != initialID {
			return
		}
		if e.Status != "completed" {
			b.failLocked("OMP ended the prompt as " + strconv.Quote(clip([]byte(e.Status))))
			return
		}
		if !e.Settled {
			b.failLocked("OMP completed the prompt without settling its session")
			return
		}
		b.publishLocked()
	case "agent_settled":
		if b.omp {
			return
		}
		if e.Aborted {
			b.failLocked(b.nativeName() + " was aborted before a completed answer")
			return
		}
		b.publishLocked()
	}
}

func (b *piRPCBridge) publishLocked() {
	if strings.TrimSpace(b.answer) == "" {
		b.failLocked(b.nativeName() + " finished without an answer")
		return
	}
	b.published = true
	b.endLocked()
	_ = b.input.Close()
}

func (b *piRPCBridge) steer(ctx context.Context, id, text string) SteerOutcome {
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
		if b.send(ctx, id, p, map[string]string{"id": id, "type": "steer", "message": text}) != nil {
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

func (b *piRPCBridge) nativeName() string {
	if b.omp {
		return "OMP"
	}
	return "Pi"
}
