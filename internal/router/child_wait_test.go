package router

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/harness"
)

// A remote child check's outcome is only the engine's decoded
// t3_thread_wait call, under any MCP alias, bound to the child's thread,
// environment and run (no runId while the run is unknown). A run-less
// check takes the run the result names; idle or a timed-out wait has not
// ended; only T3's terminal statuses end the run.
func TestNativeChildWaitBinding(t *testing.T) {
	known := &core.Review{Key: "MEL-97/1/child/1/wait/2", Worker: "t3:lap-A@env-L", Run: "run:A:1", Child: true}
	fresh := &core.Review{Key: "MEL-97/1/child/1/wait/1", Worker: "t3:lap-A@env-L", Child: true}
	call := func(tool, a, res string, failed bool) []harness.NativeCall {
		return []harness.NativeCall{{Tool: tool, Args: json.RawMessage(a), Result: json.RawMessage(res), Error: failed}}
	}
	args := `{"threadId":"lap-A","environmentId":"env-L","runId":"run:A:1","timeoutMs":5000}`
	bare := `{"threadId":"lap-A","environmentId":"env-L","timeoutMs":5000}`
	for _, c := range []struct {
		b     *core.Review
		calls []harness.NativeCall
		want  string
	}{
		{known, call("mcp__t3-code__t3_thread_wait", args, `{"structuredContent":{"threadId":"lap-A","runId":"run:A:1","status":"completed","timedOut":false}}`, false), `wait: {"run":"run:A:1","status":"completed","finished":true}`},
		{known, call("mcp__t3__t3_thread_wait", args, `[{"type":"text","text":"{\"runId\":\"run:A:1\",\"status\":\"running\",\"timedOut\":true}"}]`, false), `wait: {"run":"run:A:1","status":"running"}`},
		{known, call("mcp__t3__t3_thread_wait", args, `{"structuredContent":{"runId":"run:A:1","status":"completed","timedOut":true}}`, false), `wait: {"run":"run:A:1","status":"completed"}`},
		{fresh, call("mcp__t3__t3_thread_wait", bare, `{"structuredContent":{"runId":"run:A:7","status":"failed","timedOut":false}}`, false), `wait: {"run":"run:A:7","status":"failed","finished":true}`},
		{fresh, call("mcp__t3__t3_thread_wait", bare, `{"structuredContent":{"runId":null,"status":"idle","timedOut":true}}`, false), `wait: {"status":"idle"}`},
		{known, nil, "held: no native t3_thread_wait call"},
		{known, call("mcp__t3__t3_thread_wait", bare, `{}`, false), "held: the native t3_thread_wait call was not bound"},
		{known, call("mcp__t3__t3_thread_wait", `{"threadId":"lap-A","runId":"run:A:1"}`, `{}`, false), "held: the native t3_thread_wait call was not bound"},
		{fresh, call("mcp__t3__t3_thread_wait", args, `{}`, false), "held: the native t3_thread_wait call was not bound"},
		{known, call("mcp__t3__t3_thread_wait", args, `"denied"`, true), "held: the native t3_thread_wait call failed"},
		{known, call("mcp__t3__t3_thread_wait", args, `{"structuredContent":{"runId":"run:A:9","status":"completed"}}`, false), `wait: {"run":"run:A:9","status":"completed","finished":true}`}, // a restart continuation
		{known, call("mcp__t3__t3_thread_wait", args, `{"structuredContent":{"runId":null,"status":"idle"}}`, false), "held: the native t3_thread_wait result does not name the child's run"},
		{known, call("mcp__t3__t3_thread_wait", args, `{"structuredContent":{"runId":"run:A:1","status":"settling"}}`, false), "held: the native t3_thread_wait result has an unknown status"},
	} {
		if got := nativeChildWait(c.calls, c.b); !strings.HasPrefix(got, c.want) {
			t.Fatalf("want %q, got %q", c.want, got)
		}
	}
	for name, want := range map[string]bool{"mcp__t3__t3_thread_wait": true, "mcp__t3-code__t3_thread_wait": true, "t3_thread_wait": true, "mcp__t3__t3_thread_read": false, "mcp__x__t3_thread_wait_more": false} {
		if isT3Wait(name) != want {
			t.Fatalf("isT3Wait(%q) = %v", name, !want)
		}
	}
}

// A remote child check is one keyed turn of the creator's harness in the
// task's own conversation, asked for one native t3_thread_wait with the
// child's environmentId; a repeated key never starts a second turn. A
// turn without a bound native call is a failed check (an error), never a
// result; a task that is not working its attempt is not checked.
func TestWaitChildTurn(t *testing.T) {
	e, rt := workEnv(t, false, true, true)
	e.send(owner, "k2", "!work add MEL-97 Fix it on the laptop "+say("WAIT 2h\nwait for the laptop child"))
	e.replyIn("k2", "MEL-97")
	phase(t, rt, "MEL-97", durable.PhaseWorking)
	q := durable.ChildCheck{Key: "MEL-97/1/child/1/wait/1", Issue: "MEL-97", Attempt: 1, Owner: core.Principal{Platform: "fake", ID: "owner"},
		Route: core.ConvKey{Interface: "fake", Chat: "k2"}, Thread: "lap-A", Env: "env-L"}
	if _, err := e.r.WaitChild(context.Background(), q); err == nil || !strings.Contains(err.Error(), "no native t3_thread_wait call") {
		t.Fatalf("unbound check: %v", err)
	}
	chat := core.ReviewChat("MEL-97")
	if n := count(t, e.dbPath, `SELECT count(*) FROM inbound WHERE chat=? AND msg_id=? AND text LIKE '%t3_thread_wait exactly once%threadId: lap-A%environmentId: env-L%timeoutMs: 5000%' AND text NOT LIKE '%runId%'`, chat, q.Key); n != 1 {
		t.Fatalf("check prompt not bound: %d", n)
	}
	if _, err := e.r.WaitChild(context.Background(), q); err == nil {
		t.Fatal("repeated key gave a result")
	}
	if n := count(t, e.dbPath, `SELECT count(*) FROM inbound WHERE chat=? AND msg_id=? AND turn_id IS NOT NULL`, chat, q.Key); n != 1 {
		t.Fatalf("check turns %d", n)
	}
	// Another attempt's check is fenced out (the task works attempt 1).
	stale := q
	stale.Key, stale.Attempt = "MEL-97/2/child/1/wait/2", 2
	if _, err := e.r.WaitChild(context.Background(), stale); err == nil {
		t.Fatal("a stale attempt was checked")
	}
	if n := count(t, e.dbPath, `SELECT count(*) FROM inbound WHERE chat=? AND msg_id=? AND turn_id IS NOT NULL`, chat, stale.Key); n != 0 {
		t.Fatalf("stale check ran %d turns", n)
	}
	for _, bad := range []durable.ChildCheck{{Key: "k", Issue: "MEL-97", Owner: q.Owner, Route: q.Route, Thread: "lap-A"}, {Key: "k", Issue: "MEL-97", Owner: q.Owner, Route: q.Route, Thread: "lap A", Env: "env-L"}} {
		if _, err := e.r.WaitChild(context.Background(), bad); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}
