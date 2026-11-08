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

func TestClaudeToolSummaryIsNotChatOutput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream []string
	}{
		{"tool description and late summary", []string{
			`{"type":"assistant","session_id":"owned","message":{"id":"tool","role":"assistant","content":[{"type":"tool_use","id":"bash","name":"Bash","input":{"command":"true","description":"Copy candidate binary from Zenbook and verify"}}]}}`,
			`{"type":"system","subtype":"task_started","session_id":"owned","task_id":"bash","run_id":"run"}`,
			`{"type":"tool_use_summary","session_id":"owned","summary":"Copy candidate binary from Zenbook and verify","preceding_tool_use_ids":["bash"]}`,
			`{"type":"assistant","session_id":"owned","message":{"id":"final","role":"assistant","content":[{"type":"text","text":"Real final answer"}]}}`,
			`{"type":"result","subtype":"success","session_id":"owned","uuid":"final-result","result":"Real final answer"}`,
			`{"type":"system","subtype":"task_notification","session_id":"owned","task_id":"bash","run_id":"run","status":"completed","summary":"Copy candidate binary from Zenbook and verify"}`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, b, events := asyncFixture(t)
			out := &workerOutput{w}
			for _, line := range tc.stream {
				_, _ = out.Write([]byte(line + "\n"))
			}
			if result := b.outcome(); result.answer != "Real final answer" || len(result.answers) != 1 || result.failure != "" {
				t.Fatalf("final answer changed: %+v", result)
			}
			for _, event := range *events {
				if event.Phase == "notice" || event.Result.Answer != "" {
					t.Fatalf("metadata created extra chat output: %+v", event)
				}
			}
			if w.tasks["run\x00bash"] != w.current {
				t.Fatal("summary filtering lost task ownership")
			}
		})
	}
}

func TestFinalizationStartWarning(t *testing.T) {
	for _, c := range []struct{ name, existing, start, want string }{
		{"empty", "", "", ""}, {"existing-only", "existing warning", "", "existing warning"},
		{"both", "existing warning", "fresh session", "existing warning; fresh session"}, {"start-only", "", "fresh session", "fresh session"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &Run{done: make(chan struct{}), startWarning: c.start}
			r.finalize(Result{Warning: c.existing})
			if got := r.Result().Warning; got != c.want {
				t.Fatalf("warning got %q want %q", got, c.want)
			}
		})
	}
}

func TestTaskTerminalUpdateSettlesWithoutFollowup(t *testing.T) {
	for _, status := range []string{"completed", "failed", "killed"} {
		t.Run(status, func(t *testing.T) {
			w, _, events := asyncFixture(t)
			out := &workerOutput{w}
			out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_started", "session_id": "owned", "task_id": "task", "run_id": "run"}))
			out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "person", "result": "DONE"}))
			out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_updated", "session_id": "owned", "task_id": "task", "patch": map[string]any{"status": status}}))
			if len(w.tasks) != 0 || len(w.continuing) != 0 {
				t.Fatalf("terminal task retained: tasks=%d passive=%d", len(w.tasks), len(w.continuing))
			}
			w.lossLocked("expiry")
			for _, e := range *events {
				if e.Interrupted {
					t.Fatal("completed work reported interrupted")
				}
			}
		})
	}
}

func TestTerminalTaskFencesAndNotificationOwnership(t *testing.T) {
	w, b, events := asyncFixture(t)
	out := &workerOutput{w}
	event := func(sub, run, status string, extra map[string]any) {
		e := map[string]any{"type": "system", "subtype": sub, "session_id": "owned", "task_id": "task", "run_id": run, "patch": map[string]any{"status": status}}
		for k, v := range extra {
			e[k] = v
		}
		out.Write(protocolLine(e))
	}
	event("task_started", "old", "", nil)
	out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "person", "result": "DONE"}))
	if len(w.tasks) != 1 || len(w.continuing) != 1 {
		t.Fatal("foreground answer settled pending task")
	}
	for _, extra := range []map[string]any{{"session_id": "foreign"}, {"parent_tool_use_id": "sub"}, {"isReplay": true}} {
		event("task_updated", "old", "completed", extra)
		if len(w.tasks) != 1 {
			t.Fatal("unowned terminal update settled task")
		}
	}
	event("task_updated", "wrong-origin", "completed", nil)
	event("task_updated", "old", "running", nil)
	if len(w.tasks) != 1 {
		t.Fatal("nonterminal or wrong origin settled task")
	}
	event("task_updated", "old", "completed", nil)
	if len(w.tasks) != 0 || len(w.continuing) != 0 {
		t.Fatal("terminal state stale")
	}
	// Same native task may restart with a new run ID; replay of old start must not.
	event("task_started", "old", "", nil)
	if len(w.tasks) != 0 {
		t.Fatal("terminal start replay resurrected work")
	}
	old := w.current
	other := &Run{bridge: b, ctx: old.ctx, harness: "claude"}
	w.current = other
	event("task_notification", "old", "", map[string]any{"summary": "completed"})
	if w.tasks["old\x00task"] != old {
		t.Fatal("late notification lost original owner")
	}
	out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "followup", "result": "FOLLOWUP", "origin": map[string]any{"kind": "task-notification", "runId": "old"}}))
	if len(w.tasks) != 0 || len(w.continuing) != 0 {
		t.Fatal("followup retained completed state")
	}
	n := len(*events)
	event("task_notification", "old", "", nil)
	out.Write(protocolLine(map[string]any{"type": "result", "session_id": "owned", "uuid": "followup", "result": "FOLLOWUP", "origin": map[string]any{"kind": "task-notification", "runId": "old"}}))
	if len(*events) != n {
		t.Fatal("terminal notification/result replayed")
	}
	w.lossLocked("expiry")
	if len(*events) != n {
		t.Fatal("delivered followup reported lost")
	}
}

func TestIndependentTasksWithoutRunIDSettleIndependently(t *testing.T) {
	w, _, events := asyncFixture(t)
	out := &workerOutput{w}
	for _, id := range []string{"one", "two"} {
		out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_started", "task_id": id, "session_id": "owned"}))
	}
	for i, id := range []string{"one", "two"} {
		out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_updated", "task_id": id, "session_id": "owned", "patch": map[string]any{"status": "completed"}}))
		if len(w.continuing) != 1-i {
			t.Fatalf("terminal %s left %d passive records", id, len(w.continuing))
		}
	}
	for _, e := range *events {
		if e.Interrupted {
			t.Fatal("completed task reported lost")
		}
	}
}
func TestSharedOriginAggregatesTerminalStatus(t *testing.T) {
	for _, statuses := range [][]string{{"failed", "completed"}, {"completed", "failed"}, {"killed", "completed"}, {"completed", "killed"}} {
		t.Run(statuses[0]+statuses[1], func(t *testing.T) {
			w, _, events := asyncFixture(t)
			out := &workerOutput{w}
			for _, id := range []string{"one", "two"} {
				out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_started", "task_id": id, "run_id": "shared", "session_id": "owned"}))
			}
			for i, id := range []string{"one", "two"} {
				out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_updated", "task_id": id, "run_id": "shared", "session_id": "owned", "patch": map[string]any{"status": statuses[i]}}))
			}
			if len(w.continuing) != 0 || len(*events) != 2 {
				t.Fatalf("shared origin not settled once: %+v", *events)
			}
			want := statuses[0]
			if want == "completed" {
				want = statuses[1]
			}
			if (*events)[1].Notice != want {
				t.Fatalf("status %s, want %s", (*events)[1].Notice, want)
			}
		})
	}
}

func TestSharedOriginRetainsFirstSource(t *testing.T) {
	w, b, _ := asyncFixture(t)
	out := &workerOutput{w}
	first := w.current
	start := func(id string) {
		out.Write(protocolLine(map[string]any{"type": "system", "subtype": "task_started", "session_id": "owned", "task_id": id, "run_id": "shared"}))
	}
	start("one")
	w.current = &Run{bridge: b, ctx: first.ctx, harness: "claude"}
	start("two")
	if w.tasks["shared\x00two"] != first || w.continuing["shared"].source != first {
		t.Fatal("shared origin changed source with foreground run")
	}
}
