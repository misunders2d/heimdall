package harness

import (
	"context"
	"encoding/json"
	"testing"
)

type discardInput struct{}

func (discardInput) Write(b []byte) (int, error) { return len(b), nil }
func (discardInput) Close() error                { return nil }
func asyncFixture(t *testing.T) (*worker, *claudeStreamBridge, *[]Continuation) {
	t.Helper()
	ctx := context.Background()
	input := &warmInput{WriteCloser: discardInput{}}
	b := &claudeStreamBridge{warm: true, ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), closed: make(chan struct{}), session: "owned", prompt: "person", pending: map[string]*pendingSteer{}, seen: map[string]string{}}
	events := []Continuation{}
	r := &Run{bridge: b, ctx: ctx, harness: "claude", continuation: func(e Continuation) bool { events = append(events, e); return true }}
	w := &worker{current: r, root: r, input: input}
	r.worker = w
	return w, b, &events
}
func protocolLine(v any) []byte { b, _ := json.Marshal(v); return append(b, '\n') }
func TestWorkerSeparatesResultsInOneWrite(t *testing.T) {
	w, b, events := asyncFixture(t)
	data := protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "first", "result": "FIRST"})
	data = append(data, protocolLine(map[string]any{"type": "assistant", "session_id": "owned", "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "id": "a", "content": []any{map[string]any{"type": "text", "text": "SECOND"}}}})...)
	data = append(data, protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "second", "result": "SECOND"})...)
	out := &workerOutput{w}
	out.Write(data[:19])
	out.Write(data[19:])
	if b.outcome().answer != "FIRST" || len(*events) != 2 || (*events)[1].Result.Answer != "SECOND" {
		t.Fatalf("mixed result: %+v %+v", b.outcome(), *events)
	}
	out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "second", "result": "SECOND"}))
	if len(*events) != 2 {
		t.Fatal("duplicate native result replayed")
	}
}
func TestBufferedAssistantStillCommitsNativeTurn(t *testing.T) {
	for name, extra := range map[string]map[string]any{"replay": {"isReplay": true}, "subagent": {"parent_tool_use_id": "subagent"}, "foreign": {"session_id": "foreign"}} {
		t.Run(name, func(t *testing.T) {
			w, b, _ := asyncFixture(t)
			e := map[string]any{"type": "assistant", "session_id": "owned", "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "not owned"}}}}
			for k, v := range extra {
				e[k] = v
			}
			(&workerOutput{w}).Write(protocolLine(e))
			select {
			case <-b.ready:
				t.Fatal("unowned output committed turn")
			default:
			}
		})
	}
	w, b, events := asyncFixture(t)
	(&workerOutput{w}).Write(protocolLine(map[string]any{"type": "assistant", "session_id": "owned", "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "owned"}}}}))
	select {
	case <-b.ready:
	default:
		t.Fatal("assistant failed to commit side-effect boundary")
	}
	if len(*events) != 0 || b.outcome().answer != "" {
		t.Fatal("answer published before native result")
	}
}

func TestNativeLossConsumesFramesAndTasksOnce(t *testing.T) {
	w, _, events := asyncFixture(t)
	w.frames = [][]byte{[]byte("buffered assistant")}
	w.frameBytes = len(w.frames[0])
	w.tasks = map[string]*Run{"task": w.current}
	w.lossLocked("retire")
	w.lossLocked("exit")
	notices := 0
	for _, e := range *events {
		if e.Phase == "notice" {
			notices++
		}
	}
	if notices != 1 || w.frameBytes != 0 || len(w.frames) != 0 || len(w.tasks) != 0 {
		t.Fatalf("duplicate/lost loss state: %+v", *events)
	}
}

func TestWarmClaudeEmptyResultUsesOnlyFinalAssistantText(t *testing.T) {
	for _, kind := range []string{"final-text", "tool-only"} {
		t.Run(kind, func(t *testing.T) {
			w, b, _ := asyncFixture(t)
			out := &workerOutput{w}
			blocks := []any{map[string]any{"type": "text", "text": "Legitimate final text"}}
			if kind == "tool-only" {
				blocks = append(blocks, map[string]any{"type": "tool_use", "id": "bash", "name": "Bash", "input": map[string]any{"command": "synthetic"}})
			}
			out.Write(protocolLine(map[string]any{"type": "assistant", "session_id": "owned", "parent_tool_use_id": nil, "message": map[string]any{"id": "m", "role": "assistant", "content": blocks}}))
			out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "result": ""}))
			result := b.outcome()
			if kind == "final-text" {
				if result.answer != "Legitimate final text" || result.failure != "" {
					t.Fatalf("final text lost: %+v", result)
				}
			} else if result.answer != "" || result.failure != "Claude finished without an answer" {
				t.Fatalf("tool-only result invented answer: %+v", result)
			}
		})
	}
}
