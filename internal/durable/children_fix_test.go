package durable

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/misunders2d/heimdall/internal/core"
)

// childTask starts MEL-<n> whose first wake records kids and waits an hour;
// later wakes wait an hour too. It returns once the first WAIT is published.
func childTask(t *testing.T, opt Options, n int, kids ...Child) (*env, *childAssistant) {
	t.Helper()
	key := fmt.Sprintf("MEL-%d/1/work/1", n)
	a := &childAssistant{planAssistant: &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Wait: time.Hour, Next: "wait for the children"} }},
		kids: map[string][]Child{key: kids}}
	opt.Assistant, opt.T3Tracking, opt.Silence = a, true, -1
	if opt.Observe == 0 {
		opt.Observe = 20 * time.Millisecond
	}
	e := open(t, t.TempDir(), opt)
	a.rt = e.rt
	e.cmd(fmt.Sprintf("add MEL-%d Work through T3", n))
	waitFor(t, 10*time.Second, "first WAIT", func() bool {
		st, _ := e.rt.State(fmt.Sprintf("MEL-%d", n))
		return strings.HasPrefix(st.Next, "next wake in 1h0m0s")
	})
	return e, a
}

// The watcher's recorded reads hold ids and statuses only, never the
// child thread's text (DBOS stores every step output in durable.db).
func TestChildObserveStoresNoText(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:kid", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1, Reply: "SECRET-REPLY", Replies: map[string]string{"r1": "SECRET-REPLY", "r0": "SECRET-OLD"}})
	e, _ := childTask(t, Options{Observer: obs}, 410, Child{Kind: "send", Thread: "kid", Run: "r1"})
	defer e.close()
	waitFor(t, 10*time.Second, "watched", func() bool { return obs.reads("t3:kid") >= 3 })
	ws, err := dbos.ListWorkflows(e.rt.ctx, dbos.WithFilterName(childName))
	if err != nil || len(ws) != 1 {
		t.Fatalf("watchers %d %v", len(ws), err)
	}
	steps, err := dbos.GetWorkflowSteps(e.rt.ctx, ws[0].ID)
	if err != nil || len(steps) == 0 {
		t.Fatalf("steps %v %v", steps, err)
	}
	for _, s := range steps {
		b, _ := json.Marshal(s.Output)
		if strings.Contains(string(b), "SECRET") {
			t.Fatalf("step %s stored thread text: %s", s.StepName, b)
		}
	}
}

// A child row recorded "active" whose watcher never started (a crash between
// the insert and the workflow start) is watched again at the next start, so
// its end still wakes the task.
func TestChildWatcherReArmedAfterCrash(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:kid", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1})
	e, a := childTask(t, Options{Observer: obs}, 411)
	// The crash: the row exists, no watcher does.
	if _, err := e.rt.db.Exec(`INSERT INTO heimdall_t3_children(issue,thread,env,run,kind,task,wake,status,at) VALUES('MEL-411','kid','','r1','send','','MEL-411/1/work/1','active',1)`); err != nil {
		t.Fatal(err)
	}
	dir := e.dir
	e.close()
	e = open(t, dir, Options{Assistant: a, Observer: obs, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	waitFor(t, 10*time.Second, "re-armed watcher reads", func() bool { return obs.reads("t3:kid") >= 2 })
	obs.set("t3:kid", Observation{Status: "completed", LatestRun: "r1", RunCount: 1, Replies: map[string]string{"r1": "done"}})
	waitFor(t, 10*time.Second, "the child's wake", func() bool { return len(a.seen("MEL-411/")) == 2 })
	time.Sleep(300 * time.Millisecond)
	if got := a.seen("MEL-411/"); len(got) != 2 {
		t.Fatalf("wakes %q", got)
	}
}

// Wake prompts list the newest children, saying how many older ones are
// left out.
func TestChildNoteShowsNewest(t *testing.T) {
	e := open(t, t.TempDir(), Options{Observer: &threadObserver{}, Silence: -1, T3Tracking: true})
	defer e.close()
	for i := range 55 {
		if err := e.rt.RecordChild("MEL-412", "MEL-412/1/work/1", Child{Kind: "send", Thread: "kid", Run: fmt.Sprintf("r%02d", i), Status: "completed"}); err != nil {
			t.Fatal(err)
		}
	}
	note := e.rt.childNote("MEL-412")
	if !strings.Contains(note, "run r54)") || strings.Contains(note, "run r00)") || !strings.Contains(note, "5 older") {
		t.Fatalf("note:\n%s", note)
	}
	if i, j := strings.Index(note, "run r53)"), strings.Index(note, "run r54)"); i < 0 || i > j {
		t.Fatalf("not oldest first:\n%s", note)
	}
}

// A send into a busy thread whose run is still queued behind another run
// has not ended; it ends only on its own run's end. A run that started and
// ended between two reads, with a later run after it, still ends (its own
// reply shows it ran).
func TestChildQueuedRunNotEndedEarly(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:busy", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1})
	obs.set("t3:fast", Observation{Status: "running", LatestRun: "f1", ActiveRun: "f1", RunCount: 1})
	e, a := childTask(t, Options{Observer: obs}, 413, Child{Kind: "send", Thread: "busy", Run: "r2"})
	defer e.close()
	waitFor(t, 10*time.Second, "watched", func() bool { return obs.reads("t3:busy") >= 4 })
	if got := a.seen("MEL-413/"); len(got) != 1 {
		t.Fatalf("a queued run woke the task: %q", got)
	}
	obs.set("t3:busy", Observation{Status: "running", LatestRun: "r2", ActiveRun: "r2", RunCount: 2})
	n := obs.reads("t3:busy")
	waitFor(t, 20*time.Second, "watched r2", func() bool { return obs.reads("t3:busy") >= n+2 })
	if got := a.seen("MEL-413/"); len(got) != 1 {
		t.Fatalf("an active run woke the task: %q", got)
	}
	obs.set("t3:busy", Observation{Status: "completed", LatestRun: "r2", RunCount: 2, Replies: map[string]string{"r2": "done"}})
	waitFor(t, 20*time.Second, "r2's wake", func() bool { return len(a.seen("MEL-413/")) == 2 })

	// f2 ran and ended unseen; f3 runs now.
	if err := e.rt.RecordChild("MEL-413", "MEL-413/1/work/2", Child{Kind: "send", Thread: "fast", Run: "f2"}); err != nil {
		t.Fatal(err)
	}
	obs.set("t3:fast", Observation{Status: "running", LatestRun: "f3", ActiveRun: "f3", RunCount: 3, Replies: map[string]string{"f2": "done"}})
	waitFor(t, 20*time.Second, "f2's wake", func() bool { return len(a.seen("MEL-413/")) == 3 })
}

// A launch whose result named no run and a later send to the same thread
// both finish when that run ends: no row stays "active".
func TestRunlessLaunchAndSendBothFinish(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:T", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1})
	e, a := childTask(t, Options{Observer: obs}, 414, Child{Kind: "launch", Thread: "T"}, Child{Kind: "send", Thread: "T", Run: "r1"})
	defer e.close()
	waitFor(t, 10*time.Second, "watched", func() bool { return obs.reads("t3:T") >= 4 })
	obs.set("t3:T", Observation{Status: "completed", LatestRun: "r1", RunCount: 1, Replies: map[string]string{"r1": "done"}})
	waitFor(t, 10*time.Second, "both finished", func() bool {
		cs, _ := e.rt.Children("MEL-414")
		if len(cs) != 2 {
			return false
		}
		for _, c := range cs {
			if !strings.HasPrefix(c.Status, "finished") {
				return false
			}
		}
		return true
	})
	waitFor(t, 10*time.Second, "one wake", func() bool { return len(a.seen("MEL-414/")) == 2 })
	time.Sleep(300 * time.Millisecond)
	if got := a.seen("MEL-414/"); len(got) != 2 {
		t.Fatalf("wakes %q", got)
	}
}

// A child that ends while its task is paused wakes the task once it is
// resumed, not only when its WAIT runs out.
func TestChildEndedWhilePausedWakesAfterResume(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:kid", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1})
	e, a := childTask(t, Options{Observer: obs}, 415, Child{Kind: "send", Thread: "kid", Run: "r1"})
	defer e.close()
	if r := e.cmd("pause MEL-415"); !strings.Contains(r, "paused") {
		t.Fatal(r)
	}
	obs.set("t3:kid", Observation{Status: "completed", LatestRun: "r1", RunCount: 1, Replies: map[string]string{"r1": "done"}})
	waitFor(t, 10*time.Second, "child finished", func() bool {
		cs, _ := e.rt.Children("MEL-415")
		return len(cs) == 1 && strings.HasPrefix(cs[0].Status, "finished")
	})
	time.Sleep(300 * time.Millisecond)
	if got := a.seen("MEL-415/"); len(got) != 1 {
		t.Fatalf("a paused task was woken: %q", got)
	}
	if r := e.cmd("resume MEL-415"); r != "MEL-415 resumed." {
		t.Fatal(r)
	}
	waitFor(t, 10*time.Second, "wake after resume", func() bool { return len(a.seen("MEL-415/")) == 2 })
}

// A watcher recovered where no T3 CLI is configured any more marks its
// child unwatched and ends cleanly; it never fails.
func TestChildWatcherWithoutObserverAfterRestart(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:kid", Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 1})
	e, a := childTask(t, Options{Observer: obs, Observe: time.Second}, 416, Child{Kind: "send", Thread: "kid", Run: "r1"})
	dir := e.dir
	e.close()
	e = open(t, dir, Options{Assistant: a, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	waitFor(t, 10*time.Second, "unwatched", func() bool {
		cs, _ := e.rt.Children("MEL-416")
		return len(cs) == 1 && cs[0].Status == "not watched: no T3 CLI here"
	})
	ws, _ := dbos.ListWorkflows(e.rt.ctx, dbos.WithFilterName(childName))
	for _, w := range ws {
		if w.Status == dbos.WorkflowStatusError {
			t.Fatalf("watcher failed: %+v", w)
		}
	}
}

// An admitted T3 request whose thread then shows a newer owner request is
// superseded quietly: the newer run's failure never makes the older
// request active work. A later run with no newer owner request (an agent's
// report back) still continues the request when it fails.
func TestFollowNeverChargesANewerRequest(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	f := &fakeIntake{}
	a := &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Wait: time.Hour, Next: "finish"} }}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond, Assistant: a, Silence: -1, T3Tracking: true})
	defer e.close()
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	f.set(IntakeThread{Status: "running", LatestRun: "r1", ActiveRun: "r1", Request: &IntakeMessage{ID: "mA", Run: "r1", Text: "Request A"}})
	e.rt.Task(t3, "intake", "thread-S", "", "prompt_submit")
	e.phase("T3-1", PhaseTracking, 0)
	f.set(IntakeThread{Status: "completed", LatestRun: "r1", Children: 1, Request: &IntakeMessage{ID: "mA", Run: "r1", Text: "Request A"}})
	waitFor(t, 10*time.Second, "children note", func() bool { st, _ := e.rt.State("T3-1"); return strings.Contains(st.Note, "pending T3 child") })
	// B is typed (its hook never reached Heimdall) and its run is interrupted.
	f.set(IntakeThread{Status: "interrupted", LatestRun: "r2", Request: &IntakeMessage{ID: "mB", Run: "r2", Text: "Request B"}})
	e.phase("T3-1", PhaseClosed, 0)
	if st, _ := e.rt.State("T3-1"); !strings.Contains(st.Verdict, "superseded") {
		t.Fatalf("%+v", st)
	}
	if got := a.seen("T3-1/"); len(got) != 0 {
		t.Fatalf("the older request was worked: %q", got)
	}

	// Continuation: an agent's report-back run fails, no newer owner request.
	f.set(IntakeThread{Status: "running", LatestRun: "q1", ActiveRun: "q1", Request: &IntakeMessage{ID: "mC", Run: "q1", Text: "Request C"}})
	e.rt.Task(t3, "intake", "thread-U", "", "prompt_submit")
	e.phase("T3-2", PhaseTracking, 0)
	f.set(IntakeThread{Status: "failed", LatestRun: "q2", Agent: true})
	waitFor(t, 10*time.Second, "continued", func() bool { return len(a.seen("T3-2/")) == 1 })
	if p := a.prompt("T3-2/1/work/1"); !strings.Contains(p, "run q2 ended failed") {
		t.Fatalf("prompt:\n%s", p)
	}
}

// Children with no thread or no run are one row per native call: two
// calls in one wake stay two rows, the same call reported twice is one.
func TestRunlessChildrenOneRowPerCall(t *testing.T) {
	e := open(t, t.TempDir(), Options{Observer: &threadObserver{}, Silence: -1, T3Tracking: true})
	defer e.close()
	for _, c := range []Child{{Kind: "launch", Call: "call-1"}, {Kind: "launch", Call: "call-2"}, {Kind: "launch", Call: "call-1"},
		{Kind: "send", Thread: "T", Call: "call-3"}, {Kind: "send", Thread: "T", Call: "call-4"}, {Kind: "send", Thread: "T", Call: "call-4"}} {
		if err := e.rt.RecordChild("MEL-417", "MEL-417/1/work/1", c); err != nil {
			t.Fatal(err)
		}
	}
	cs, _ := e.rt.Children("MEL-417")
	if len(cs) != 4 {
		t.Fatalf("rows %+v", cs)
	}
	// A run-less send into an existing thread is never matched to whatever
	// run the thread shows (another's).
	if cs[2].Status != "not watched: the send result named no run" {
		t.Fatalf("run-less send %+v", cs[2])
	}
}
