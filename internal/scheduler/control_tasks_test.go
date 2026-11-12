package scheduler

import (
	"bytes"
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
)

// tasksAssistant answers each wake by key: WAIT, then REPORT.
type tasksAssistant struct {
	mu   sync.Mutex
	reqs []durable.WorkRequest
}

func (a *tasksAssistant) Work(_ context.Context, q durable.WorkRequest) (durable.WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reqs = append(a.reqs, q)
	if strings.HasSuffix(q.Key, "/work/1") {
		return durable.WorkResult{Turn: 1, Wait: 50 * time.Millisecond, Next: "read the supplier reply"}, nil
	}
	return durable.WorkResult{Turn: 2, Report: "Supplier confirmed PO 551 by email."}, nil
}

// A bound live owner turn admits a durable task through the existing
// control (no !work typed, no T3): the creator, conversation, privacy and
// OAuth actor are the bound caller's, the same request_id is answered once
// and a changed request under it is refused, and the task is then worked
// by assistant wakes (WAIT, then REPORT, still unaccepted). Scheduled,
// employee, unbound, stale origins and worker cargo are refused.
func TestControlTaskAdmission(t *testing.T) {
	r, c := controlRig(t)
	ctx := context.Background()
	a := &tasksAssistant{}
	work, err := durable.Open(durable.Options{Path: filepath.Join(t.TempDir(), "durable.db"), Outbox: r.st, Assistant: a, Silence: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := work.Launch(); err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	adds := 0
	r.s.Tasks = func(who Caller, action, issue, title, requestID string) (string, bool) {
		if action == "add" {
			adds++
		}
		return work.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, OAuthActor: who.OAuthActor, OAuthInstallation: who.OAuthInstallation}, action, issue, title, requestID)
	}
	live := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true, OAuthActor: "tg_1"}
	add := ControlRequest{Action: "task_add", ID: "MEL-200", Title: "Confirm the supplier PO", RequestID: "task-1"}
	got := c.execute(ctx, 1234, add, live)
	if got.Error != "" || !got.Changed || !strings.HasPrefix(got.Reply, "Tracking MEL-200") {
		t.Fatalf("%+v", got)
	}
	if again := c.execute(ctx, 1234, add, live); again != got || adds != 1 {
		t.Fatalf("same request %+v, admissions %d", again, adds)
	}
	changed := add
	changed.Title = "Something else"
	if got := c.execute(ctx, 1234, changed, live); !strings.Contains(got.Error, "request_id already used for another operation") || adds != 1 {
		t.Fatalf("changed request %+v", got)
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(20 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("timed out: %s", what)
			}
		}
	}
	waitFor("reported", func() bool { st, err := work.State("MEL-200"); return err == nil && st.Phase == durable.PhaseReported })
	st, _ := work.State("MEL-200")
	a.mu.Lock()
	reqs := append([]durable.WorkRequest(nil), a.reqs...)
	a.mu.Unlock()
	if st.Evidence != "Supplier confirmed PO 551 by email." || st.Verdict != "" || len(reqs) != 2 {
		t.Fatalf("%+v %d", st, len(reqs))
	}
	if q := reqs[0]; q.Owner != boss || q.Route != bossDM || !q.Private || q.OAuthActor != "tg_1" || !strings.Contains(reqs[1].Prompt, "Your persisted next action: read the supplier reply") {
		t.Fatalf("wake not bound to the caller: %+v", q)
	}
	if got := c.execute(ctx, 1234, ControlRequest{Action: "task_show", ID: "MEL-200"}, live); !strings.Contains(got.Reply, "MEL-200") {
		t.Fatalf("show %+v", got)
	}

	for name, tc := range map[string]struct {
		req    ControlRequest
		who    Caller
		valid  func() bool
		direct bool // past caller(): the task guard itself
		want   string
	}{
		"scheduled": {req: ControlRequest{Action: "task_add", ID: "MEL-201", Title: "x", RequestID: "s1"}, who: Caller{Principal: boss, Role: core.RoleOwner, Conv: team, Job: "j1", Run: 1}, direct: true, want: "scheduled runs cannot admit"},
		"employee":  {req: ControlRequest{Action: "task_add", ID: "MEL-202", Title: "x", RequestID: "e1"}, who: Caller{Principal: boss, Role: core.RoleEmployee, Conv: bossDM, Private: true}, want: "trusted active request origin required"},
		"unbound":   {req: ControlRequest{Action: "task_add", ID: "MEL-203", Title: "x", RequestID: "u1"}, who: Caller{Principal: boss, Role: core.RoleOwner, Conv: team}, want: "no matching live ingress"},
		"stale":     {req: ControlRequest{Action: "task_add", ID: "MEL-204", Title: "x", RequestID: "t1"}, who: live, valid: func() bool { return false }, want: "expired"},
		"cargo":     {req: ControlRequest{Action: "task_add", ID: "MEL-205", Title: "worker=t3:thread-X Do it", RequestID: "w1"}, who: live, want: ""},
		"control":   {req: ControlRequest{Action: "task_accept", ID: "MEL-200", RequestID: "a1"}, who: live, want: "unsupported task action"},
		"no-id":     {req: ControlRequest{Action: "task_add", ID: "MEL-206", Title: "x"}, who: live, want: "stable request_id"},
	} {
		var res ControlResult
		switch {
		case tc.direct:
			res = c.task(1234, tc.req, tc.who, nil)
		case tc.valid != nil:
			res = c.executeLocked(ctx, 1234, tc.req, []Caller{tc.who}, tc.valid)
		default:
			res = c.execute(ctx, 1234, tc.req, tc.who)
		}
		if name == "cargo" {
			if res.Changed || !strings.Contains(res.Reply, "plain title") {
				t.Fatalf("%s: %+v", name, res)
			}
			continue
		}
		if !strings.Contains(res.Error, tc.want) {
			t.Fatalf("%s: want %q, got %+v", name, tc.want, res)
		}
	}
	if adds != 2 { // MEL-200, and the refused cargo title (refused inside Task)
		t.Fatalf("admissions %d", adds)
	}
	if _, err := work.State("MEL-205"); err == nil {
		t.Fatal("cargo task admitted")
	}
}

// wakeAssistant waits an hour after wake 1 and reports at wake 2.
type wakeAssistant struct {
	mu   sync.Mutex
	keys []string
}

func (a *wakeAssistant) Work(_ context.Context, q durable.WorkRequest) (durable.WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keys = append(a.keys, q.Key)
	if strings.HasSuffix(q.Key, "/work/1") {
		return durable.WorkResult{Turn: 1, Wait: time.Hour, Next: "wait for the worker"}, nil
	}
	return durable.WorkResult{Turn: 2, Report: "worker finished"}, nil
}

func (a *wakeAssistant) count(issue string) (n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, k := range a.keys {
		if strings.HasPrefix(k, issue+"/") {
			n++
		}
	}
	return n
}

// task_wake through the control: the owner's live turn and the trusted
// T3 launcher (no manage) wake their task now; a repeated request_id is
// answered from the record (one signal); it takes no title; scheduled and
// employee callers and a finished task are refused, and each refusal is
// logged without the caller's identity.
func TestControlTaskWake(t *testing.T) {
	r, c := controlRig(t)
	var logs bytes.Buffer
	r.s.log = log.New(&logs, "", 0)
	ctx := context.Background()
	a := &wakeAssistant{}
	work, err := durable.Open(durable.Options{Path: filepath.Join(t.TempDir(), "durable.db"), Outbox: r.st, Assistant: a, Silence: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := work.Launch(); err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	r.s.Tasks = func(who Caller, action, issue, title, requestID string) (string, bool) {
		return work.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, Origin: who.Registered}, action, issue, title, requestID)
	}
	live := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true}
	t3 := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true, External: trustedT3, Registered: trustedT3}
	waiting := func(issue string) {
		t.Helper()
		for end := time.Now().Add(20 * time.Second); a.count(issue) != 1; time.Sleep(20 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("%s never waited", issue)
			}
		}
	}
	reported := func(issue string) {
		t.Helper()
		for end := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			if st, err := work.State(issue); err == nil && st.Phase == durable.PhaseReported {
				return
			}
			if time.Now().After(end) {
				t.Fatalf("%s not woken", issue)
			}
		}
	}
	for i, who := range []Caller{live, t3} {
		issue := []string{"MEL-400", "MEL-401"}[i]
		if got := c.execute(ctx, 1234, ControlRequest{Action: "task_add", ID: issue, Title: "Wait for the worker", RequestID: "add-" + issue}, who); !got.Changed {
			t.Fatalf("%+v", got)
		}
		waiting(issue)
		wake := ControlRequest{Action: "task_wake", ID: issue, RequestID: "wake-" + issue}
		got := c.execute(ctx, 1234, wake, who)
		if got.Error != "" || !got.Changed || got.Reply != "Sent wake to "+issue+"." {
			t.Fatalf("%s: %+v", issue, got)
		}
		reported(issue)
		if again := c.execute(ctx, 1234, wake, who); again != got || a.count(issue) != 2 {
			t.Fatalf("repeated request %+v, wakes %d", again, a.count(issue))
		}
	}
	logs.Reset()
	for name, tc := range map[string]struct {
		req  ControlRequest
		who  Caller
		want string
	}{
		"scheduled": {ControlRequest{Action: "task_wake", ID: "MEL-400", RequestID: "s1"}, Caller{Principal: boss, Role: core.RoleOwner, Conv: team, Job: "j1", Run: 1}, "scheduled origin"},
		"employee":  {ControlRequest{Action: "task_wake", ID: "MEL-400", RequestID: "e1"}, Caller{Principal: boss, Role: core.RoleEmployee, Conv: bossDM, Private: true}, "trusted active request origin required"},
		"text":      {ControlRequest{Action: "task_wake", ID: "MEL-400", Title: "done, see PR", RequestID: "x1"}, live, "takes only an issue id"},
		"no-id":     {ControlRequest{Action: "task_wake", ID: "MEL-400"}, live, "stable request_id"},
		"finished":  {ControlRequest{Action: "task_wake", ID: "MEL-400", RequestID: "f1"}, live, "MEL-400: not waiting."},
	} {
		if res := c.execute(ctx, 1234, tc.req, tc.who); !strings.Contains(res.Error+res.Reply, tc.want) || res.Changed {
			t.Fatalf("%s: want %q, got %+v", name, tc.want, res)
		}
	}
	if n := strings.Count(logs.String(), "action task_wake refused"); n != 5 || strings.Contains(logs.String(), boss.Key()) {
		t.Fatalf("refusal log %q", logs.String())
	}
}
