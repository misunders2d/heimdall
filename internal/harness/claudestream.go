package harness

// Ported from AgentNet internal/client/claudestream.go. Changes: answer,
// failure and needs-human reason are fields instead of an output stream; input
// written while a result was being produced, which Claude runs as its next
// turn in the same stream, is waited for and its result kept with the first;
// the --include-partial-messages probe, activity and thinking telemetry are
// not ported; launch arguments are built by the caller.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os/exec"
	"slices"
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
	answered             chan struct{} // closed once an answer is published
	answer               string
	answers              []string // each result's text, in order
	draining             bool     // a result came with steers pending: no new input, wait for theirs
	warning              string   // the late turn ended badly: the answers stand, the session is not resumed
	atExit               bool     // published when the process exited during a drain
	pending              map[string]*pendingSteer
	seen                 map[string]string
}

// expected is the session ID passed with --session-id or --resume; the
// stream must stay in exactly that session.
func newClaudeStreamBridge(ctx context.Context, cmd *exec.Cmd, prompt string, images []Image, expected string) (*claudeStreamBridge, error) {
	cmd.Stdin = nil
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	b := &claudeStreamBridge{ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), pending: map[string]*pendingSteer{}, seen: map[string]string{}, closed: make(chan struct{}), prompt: prompt, session: expected}
	b.writes.Add(1)
	go func() {
		defer b.writes.Done()
		if b.send(ctx, "", nil, claudeUser(prompt, images)) != nil {
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

func (b *claudeStreamBridge) answeredCh() <-chan struct{} { return b.answered }

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
	if b.drained() && !b.failed {
		// The turn Claude ran for input written during its result ended
		// badly: the answers it already gave stand, with a note, and the
		// session is not resumed.
		b.warning = "a later message was not answered: " + why
		b.publishLocked()
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

// drained reports a result that came while steers were pending, in a turn
// that was not cancelled: its answers stand whatever Claude does next.
func (b *claudeStreamBridge) drained() bool {
	return b.draining && len(b.answers) > 0 && b.ctx.Err() == nil
}

// settled reports a terminal native outcome; closing the bridge is not one.
// A result that came while steers were pending is one, unless the turn was
// cancelled: a stopped turn is never an answer.
func (b *claudeStreamBridge) settled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.published || b.failed || b.drained()
}

func (b *claudeStreamBridge) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.done:
	case b.drained():
		// Claude exited without answering the pending input: the answers it
		// gave stand; the pending steers are reported unknown. A failed exit
		// makes the session unclean (Run.wait).
		b.atExit = true
		b.publishLocked()
	default:
		b.failLocked("Claude ended before its request settled")
	}
}

func (b *claudeStreamBridge) outcome() outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := outcome{failure: b.failure, human: b.human, session: b.session}
	if b.published {
		o.answer, o.answers, o.warning, o.unclean, o.atExit = b.answer, slices.Clone(b.answers), b.warning, b.warning != "", b.atExit
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
		if b.session == "" || e.Session != b.session || e.UUID == "" || !e.Replay || string(e.Parent) != "null" || e.Message.Role != "user" {
			return
		}
		text, ok := claudeText(e.Message.Content)
		if !ok {
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
		b.answers = keepAnswer(b.answers, e.Result)
		if len(b.pending) > 0 {
			// Input written while this result was produced runs as Claude's
			// next turn in this stream. Send nothing more (Claude exits once
			// its queue is done) and wait for that turn's replay and result.
			b.draining = true
			_ = b.input.Close()
			return
		}
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
	b.answer = joinAnswers(b.answers)
	if strings.TrimSpace(b.answer) == "" {
		b.failLocked("Claude finished without an answer")
		return
	}
	b.published = true
	b.endLocked()
	_ = b.input.Close()
	close(b.answered)
}

// claudeUser is a stream-json user message: text alone, or text and
// base64 image blocks (Claude replays the same blocks).
func claudeUser(text string, images []Image) map[string]any {
	if len(images) == 0 {
		return map[string]any{"type": "user", "message": map[string]string{"role": "user", "content": text}}
	}
	content := []any{map[string]any{"type": "text", "text": text}}
	for _, im := range images {
		content = append(content, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": im.MIME, "data": base64.StdEncoding.EncodeToString(im.Data)}})
	}
	return map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": content}}
}

// claudeText is a replayed user message's text: the string content, or its
// text blocks joined.
func claudeText(raw json.RawMessage) (string, bool) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, true
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return "", false
	}
	for _, b := range blocks {
		if b.Type == "text" {
			text += b.Text
		}
	}
	return text, true
}

func (b *claudeStreamBridge) steer(ctx context.Context, id, text string, images []Image) SteerOutcome {
	b.mu.Lock()
	if b.done || !b.active || b.draining || b.ctx.Err() != nil || ctx.Err() != nil || id == "" || text == "" {
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
		if b.send(ctx, id, p, claudeUser(text, images)) != nil {
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
