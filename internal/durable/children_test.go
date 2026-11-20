package durable

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// childAssistant records children during a wake, as the router does from
// the wake's native T3 results, then answers from its plan.
type childAssistant struct {
	*planAssistant
	mu   sync.Mutex
	rt   *Runtime
	kids map[string][]Child // wake key → children it starts
}

func (a *childAssistant) Work(ctx context.Context, q WorkRequest) (WorkResult, error) {
	a.mu.Lock()
	rt, kids := a.rt, a.kids[q.Key]
	a.mu.Unlock()
	for _, c := range kids {
		if err := rt.RecordChild(q.Issue, q.Key, c); err != nil {
			return WorkResult{}, err
		}
	}
	return a.planAssistant.Work(ctx, q)
}

// threadObserver is a T3 host whose threads the test sets one by one.
type threadObserver struct {
	mu  sync.Mutex
	obs map[string]Observation
	n   map[string]int
}

func (f *threadObserver) Observable(w string) bool {
	return strings.HasPrefix(w, "t3:") && !strings.Contains(w, "@")
}

func (f *threadObserver) Observe(_ context.Context, w string) (Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == nil {
		f.n = map[string]int{}
	}
	f.n[w]++
	return f.obs[w], nil
}

func (f *threadObserver) set(w string, o Observation) {
	f.mu.Lock()
	if f.obs == nil {
		f.obs = map[string]Observation{}
	}
	f.obs[w] = o
	f.mu.Unlock()
}

func (f *threadObserver) reads(w string) int { f.mu.Lock(); defer f.mu.Unlock(); return f.n[w] }

// A wake's native T3 children are recorded at once (a launch without a run
// yet, a delegation, a send to a thread in another environment, a result
// that named no thread), listed in the next wake and in !work show. The
// local child is watched read-only: while its run is active nothing
// happens; when it ends, the task's hour-long WAIT ends with exactly one
// wake. The remote and unbound children are never watched. With the flag
// off nothing is recorded.
func TestChildrenRecordedAndOneWakeAfterTerminal(t *testing.T) {
	fastWakes(t)
	obs := &threadObserver{}
	obs.set("t3:child-A", Observation{Status: "running", LatestRun: "rA", ActiveRun: "rA", RunCount: 1})
	a := &childAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{
		"MEL-400/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait for child-A"},
		"MEL-400/1/work/2": {Turn: 2, Wait: time.Hour, Next: "read child-A"},
	}}, kids: map[string][]Child{"MEL-400/1/work/1": {
		{Kind: "launch", Thread: "child-A"},
		{Kind: "delegate", Thread: "child-B", Run: "rB", Task: "task-B", Status: "running"},
		{Kind: "send", Thread: "remote-C", Env: "env-L", Run: "rC"},
		{Kind: "launch"},
	}}}
	e := open(t, t.TempDir(), Options{Assistant: a, Observer: obs, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	obs.set("t3:child-B", Observation{Status: "running", LatestRun: "rB", ActiveRun: "rB", RunCount: 1})
	e.cmd("add MEL-400 Fix the build through T3")
	waitFor(t, 10*time.Second, "first wake's WAIT", func() bool {
		st, _ := e.rt.State("MEL-400")
		return strings.HasPrefix(st.Next, "next wake in 1h0m0s")
	})
	waitFor(t, 10*time.Second, "child-A watched", func() bool { return obs.reads("t3:child-A") >= 3 })
	if got := a.seen("MEL-400/"); len(got) != 1 {
		t.Fatalf("a running child woke the task: %q", got)
	}
	show := e.cmd("show MEL-400")
	for _, want := range []string{"T3 children:", "- t3:child-A (launch): active", "- t3:child-B (delegate, run rB, task task-B): active",
		"- t3:remote-C@env-L (send, run rC): in environment env-L: not watched here", "- launch from wake MEL-400/1/work/1: unbound: the launch result named no thread"} {
		if !strings.Contains(show, want) {
			t.Fatalf("show lacks %q:\n%s", want, show)
		}
	}
	if obs.reads("t3:remote-C@env-L") != 0 || obs.reads("t3:remote-C") != 0 {
		t.Fatal("a remote child was read here")
	}

	// child-A's run ends: one wake, whose prompt lists it as finished.
	obs.set("t3:child-A", Observation{Status: "completed", LatestRun: "rA", RunCount: 1})
	waitFor(t, 10*time.Second, "the child's wake", func() bool { return len(a.seen("MEL-400/")) == 2 })
	time.Sleep(400 * time.Millisecond)
	if got := a.seen("MEL-400/"); len(got) != 2 {
		t.Fatalf("wakes after one terminal child: %q", got)
	}
	p := a.prompt("MEL-400/1/work/2")
	if !strings.Contains(p, "T3 work you started for this task") || !strings.Contains(p, "- t3:child-A (launch, run rA): finished (completed)") || !strings.Contains(p, "child-B (delegate, run rB, task task-B): active") {
		t.Fatalf("wake prompt:\n%s", p)
	}
	if strings.Contains(a.prompt("MEL-400/1/work/1"), "T3 work you started") {
		t.Fatal("the first wake listed children it had not started yet")
	}
	// The second child ends while the task waits again: one more wake.
	obs.set("t3:child-B", Observation{Status: "running", LatestRun: "rB2", ActiveRun: "rB2", RunCount: 2})
	waitFor(t, 60*time.Second, "child-B's wake", func() bool { return len(a.seen("MEL-400/")) == 3 })

	// Flag off: nothing is recorded.
	off := open(t, t.TempDir(), Options{Silence: -1})
	defer off.close()
	if off.rt.TracksChildren() {
		t.Fatal("tracking on by default")
	}
	if err := off.rt.RecordChild("MEL-1", "MEL-1/1/work/1", Child{Kind: "launch", Thread: "x"}); err != nil {
		t.Fatal(err)
	}
	if cs, _ := off.rt.Children("MEL-1"); len(cs) != 0 {
		t.Fatalf("recorded with the flag off: %+v", cs)
	}
}

// An intake admitted with T3 tracking follows the thread: a later run (an
// agent's report back) is followed, not a reason to close; a completed
// thread with a pending child task stays tracked; it closes quietly only
// once the thread is idle with no pending children. The choice is frozen
// at admission: a restart with the flag off keeps following.
func TestIntakeWaitsForChildrenAndFollowsRuns(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	dir := t.TempDir()
	f := &fakeIntake{}
	opt := Options{Observer: f, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true}
	e := open(t, dir, opt)
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	f.set(IntakeThread{Status: "running", LatestRun: "r1", ActiveRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	if reply, _ := e.rt.Task(t3, "intake", "thread-F", "", "prompt_submit"); reply != "T3 request noted." {
		t.Fatal(reply)
	}
	e.phase("T3-1", PhaseTracking, 0)
	// r1 ends but a child task is pending, then a later run starts.
	f.set(IntakeThread{Status: "waiting", LatestRun: "r1", Children: 1, Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	waitFor(t, 10*time.Second, "children note", func() bool {
		st, _ := e.rt.State("T3-1")
		return st.Note == "waiting on 1 pending T3 child task(s)"
	})
	f.set(IntakeThread{Status: "completed", LatestRun: "r1", Children: 1, Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	time.Sleep(300 * time.Millisecond)
	if st, _ := e.rt.State("T3-1"); st.Phase != PhaseTracking {
		t.Fatalf("closed with a pending child: %+v", st)
	}
	f.set(IntakeThread{Status: "running", LatestRun: "r2", ActiveRun: "r2", Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	waitFor(t, 10*time.Second, "follows r2", func() bool {
		st, _ := e.rt.State("T3-1")
		return st.Phase == PhaseTracking && strings.Contains(st.Next, "now run r2")
	})
	e.close()
	opt.T3Tracking = false
	e = open(t, dir, opt)
	defer e.close()
	f.set(IntakeThread{Status: "completed", LatestRun: "r2", Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	e.phase("T3-1", PhaseClosed, 0)
	if st, _ := e.rt.State("T3-1"); st.Verdict != "finished in T3 (completed)" {
		t.Fatalf("%+v", st)
	}
	if got := outbound(t, e.dir, "T3-1"); len(got) != 0 {
		t.Fatalf("a tracking close sent %q", got)
	}
}

// An intake without T3 tracking keeps today's path: a later run closes it.
func TestIntakeWithoutTrackingClosesOnLaterRun(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	f := &fakeIntake{}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond, Silence: -1})
	defer e.close()
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	f.set(IntakeThread{Status: "running", LatestRun: "r1", ActiveRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	e.rt.Task(t3, "intake", "thread-G", "", "prompt_submit")
	e.phase("T3-1", PhaseTracking, 0)
	f.set(IntakeThread{Status: "running", LatestRun: "r2", ActiveRun: "r2", Request: &IntakeMessage{Run: "r1", Text: "Fix CI"}})
	e.phase("T3-1", PhaseClosed, 0)
	if st, _ := e.rt.State("T3-1"); st.Verdict != "a later run followed in the T3 thread" {
		t.Fatalf("%+v", st)
	}
}

// An active T3 request (its run was interrupted, so the assistant works
// it) yields to a newer owner request in its own thread: it closes with
// exactly one line to the owner, and its wakes stop. The newer request is
// tracked. Only the same creator's request with T3's own message mark
// counts.
func TestActiveIntakeYieldsToNewerRequest(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	f := &fakeIntake{}
	a := &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Wait: time.Hour, Next: "finish the PO email"} }}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond, Assistant: a, Silence: -1, T3Tracking: true})
	defer e.close()
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	f.set(IntakeThread{Status: "running", LatestRun: "r1", ActiveRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Email the PO"}})
	e.rt.Task(t3, "intake", "thread-Y", "", "prompt_submit")
	e.phase("T3-1", PhaseTracking, 0)
	f.set(IntakeThread{Status: "interrupted", LatestRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Email the PO"}})
	waitFor(t, 10*time.Second, "active wake", func() bool {
		st, _ := e.rt.State("T3-1")
		return strings.HasPrefix(st.Next, "next wake in 1h0m0s")
	})
	// A request known only by the thread's mark (an agent's report back
	// carries it too) never stops the active task.
	f.set(IntakeThread{Status: "running", LatestRun: "r2", ActiveRun: "r2", Request: &IntakeMessage{Run: "r2", Text: "child report", ByThread: true}})
	e.rt.Task(t3, "intake", "thread-Y", "", "prompt_submit")
	e.phase("T3-2", PhaseTracking, 0)
	time.Sleep(300 * time.Millisecond)
	if st, _ := e.rt.State("T3-1"); st.Phase == PhaseClosed {
		t.Fatalf("a thread-mark request stopped the active task: %+v", st)
	}
	f.set(IntakeThread{Status: "running", LatestRun: "r3", ActiveRun: "r3", Request: &IntakeMessage{Run: "r3", Text: "Actually, call them instead"}})
	e.rt.Task(t3, "intake", "thread-Y", "", "prompt_submit")
	e.phase("T3-3", PhaseTracking, 0)
	e.phase("T3-1", PhaseClosed, 0)
	waitFor(t, 5*time.Second, "one line", func() bool { return len(outbound(t, e.dir, "(T3-1) stopped")) == 1 })
	time.Sleep(300 * time.Millisecond)
	if got := outbound(t, e.dir, "T3-1"); len(got) != 1 || !strings.Contains(got[0], `Task "Email the PO" (T3-1) stopped: a newer request in the same T3 thread replaced it. — heimdall`) {
		t.Fatalf("lines %q", got)
	}
	if got := a.seen("T3-1/"); len(got) != 1 {
		t.Fatalf("wakes after yield %q", got)
	}
	if st, _ := e.rt.State("T3-1"); st.Verdict != "superseded by a newer request in the same T3 thread" {
		t.Fatalf("%+v", st)
	}
}

// listIntake is a T3 host that also lists a project's threads.
type listIntake struct {
	fakeIntake
	mu      sync.Mutex
	threads map[string]IntakeThread
	list    []ListedThread
	lists   int
	fail    int // the next lists that fail
}

func (f *listIntake) ReadIntake(_ context.Context, thread string) (IntakeThread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.threads[thread], nil
}

func (f *listIntake) ListThreads(context.Context, string) ([]ListedThread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.fail > 0 {
		f.fail--
		return nil, errors.New("t3 not up yet")
	}
	return f.list, nil
}

// t3Owner is a switchable trusted T3 owner binding.
type t3Owner struct {
	mu    sync.Mutex
	o     *T3Owner
	calls int
	after func(calls int) *T3Owner // when set, decides from the call count
}

func (b *t3Owner) get(context.Context) (T3Owner, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	o := b.o
	if b.after != nil {
		o = b.after(b.calls)
	}
	if o == nil {
		return T3Owner{}, errors.New("not a current owner")
	}
	return *o, nil
}

func (b *t3Owner) set(o *T3Owner) { b.mu.Lock(); b.o = o; b.mu.Unlock() }

// marked is a thread with a T3-marked owner request.
func marked(run, text string) IntakeThread {
	return IntakeThread{Status: "running", LatestRun: run, ActiveRun: run, Project: "proj-1", Request: &IntakeMessage{ID: "m-" + run, Run: run, Text: text}}
}

// Requests typed into T3 while Heimdall was down are reconciled at the
// next start: the projects of earlier requests are listed (a list that
// fails while T3 starts is read again) and each owner thread changed since
// Heimdall was last alive is probed as its hook would have been, bound to
// the trusted T3 owner as it is now, never to a stored request.
// Agent-created, delegated and child threads, threads last changed before
// the downtime, and a request known only by the thread's mark (no hook
// timing to back it) are skipped, each with a valid owner request of its
// own; a second start admits nothing new.
func TestReconcileAfterDowntime(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	dir := t.TempDir()
	f := &listIntake{threads: map[string]IntakeThread{"thread-A": marked("a1", "First")}}
	now := Caller{Principal: core.Principal{Platform: "test", ID: "owner-now"}, Conv: core.ConvKey{Interface: "test", Chat: "c-now"}}
	b := &t3Owner{o: &T3Owner{Principal: now.Principal, Route: now.Conv}}
	opt := Options{Observer: f, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true, T3Owner: b.get}
	e := open(t, dir, opt)
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	e.rt.Task(t3, "intake", "thread-A", "", "prompt_submit")
	e.phase("T3-1", PhaseTracking, 0)
	// Changed while Heimdall was still up, its hook never fired: not missed
	// during the downtime.
	before := time.Now()
	f.mu.Lock()
	f.threads["early-E"] = func() IntakeThread {
		th := marked("e1", "abandoned")
		th.Status, th.ActiveRun = "failed", ""
		return th
	}()
	f.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	defer func(d time.Duration) { aliveEvery = d }(aliveEvery)
	aliveEvery = 10 * time.Millisecond // the margin before the last heartbeat
	e.close()

	// While down: thread-A gets a new request, thread-B is created; an
	// agent thread, a subagent child and a delegated thread also change.
	time.Sleep(20 * time.Millisecond)
	at := time.Now().Add(time.Hour) // later than every restart below
	parent := "thread-A"
	f.mu.Lock()
	f.threads["thread-A"] = marked("a2", "Second")
	f.threads["thread-B"] = IntakeThread{Status: "interrupted", LatestRun: "b1", Project: "proj-1", Request: &IntakeMessage{ID: "m-b1", Run: "b1", Text: "Third"}}
	f.threads["thread-W"] = IntakeThread{Status: "interrupted", LatestRun: "w1", Project: "proj-1", Request: &IntakeMessage{Run: "w1", Text: "thread mark only", ByThread: true}}
	for _, id := range []string{"agent-T", "kid-K", "mcp:launch:z", "old-O", "titled-D"} {
		f.threads[id] = marked("x-"+id, "valid owner request in "+id)
	}
	f.list = []ListedThread{
		{ID: "thread-A", CreatedBy: "user", Updated: at},
		{ID: "thread-B", CreatedBy: "user", Updated: at},
		{ID: "thread-W", CreatedBy: "user", Updated: at},
		{ID: "agent-T", CreatedBy: "agent", Updated: at},
		{ID: "kid-K", CreatedBy: "user", Parent: &parent, Updated: at},
		{ID: "mcp:launch:z", CreatedBy: "user", Updated: at},
		{ID: "titled-D", Title: "mcp:launch:q", CreatedBy: "user", Updated: at},
		{ID: "old-O", CreatedBy: "user", Updated: at.Add(-49 * time.Hour)},
		{ID: "early-E", CreatedBy: "user", Updated: before},
	}
	f.fail = 2
	f.mu.Unlock()
	e = open(t, dir, opt)
	waitFor(t, 10*time.Second, "both admitted", func() bool {
		_, err2 := e.rt.State("T3-2")
		_, err3 := e.rt.State("T3-3")
		return err2 == nil && err3 == nil
	})
	got := map[string]string{}
	for _, n := range []string{"T3-2", "T3-3"} {
		cm, _ := e.rt.input(n)
		if cm.Creator != now.Principal.Key() || cm.Route != now.Conv || !cm.Private || cm.Origin != OriginTrustedT3 || !cm.Intake.Follow || cm.OAuthActor != "" {
			t.Fatalf("%s binding %+v", n, cm)
		}
		got[cm.Intake.Thread] = cm.Intake.Run
	}
	if got["thread-A"] != "a2" || got["thread-B"] != "b1" {
		t.Fatalf("reconciled %v", got)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := e.rt.State("T3-4"); err == nil {
		cm, _ := e.rt.input("T3-4")
		t.Fatalf("a skipped thread was admitted: %s", cm.Intake.Thread)
	}
	e.close()
	e = open(t, dir, opt)
	defer e.close()
	waitFor(t, 10*time.Second, "second list", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.lists >= 4 })
	time.Sleep(500 * time.Millisecond)
	if _, err := e.rt.State("T3-4"); err == nil {
		t.Fatal("a second start admitted again")
	}
}

// Reconcile admits nothing without a current trusted T3 owner, nor when
// the owner changes between the plan and a request's admission; with no
// earlier run recorded there is no downtime to reconcile. Of more changed
// threads than the cap, the newest are probed.
func TestReconcileNeedsCurrentOwner(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	defer func(n int, d time.Duration) { reconcileThreads, aliveEvery = n, d }(reconcileThreads, aliveEvery)
	reconcileThreads, aliveEvery = 2, 10*time.Millisecond
	dir := t.TempDir()
	f := &listIntake{threads: map[string]IntakeThread{"thread-A": marked("a1", "First")}}
	me := &T3Owner{Principal: owner.Principal, Route: owner.Conv}
	b := &t3Owner{o: me}
	opt := Options{Observer: f, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true, T3Owner: b.get}
	e := open(t, dir, opt)
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	e.rt.Task(t3, "intake", "thread-A", "", "prompt_submit")
	e.phase("T3-1", PhaseTracking, 0)
	e.close()
	time.Sleep(20 * time.Millisecond)
	at := time.Now()
	f.mu.Lock()
	for i, id := range []string{"t-1", "t-2", "t-3"} {
		f.threads[id] = marked("r-"+id, "request "+id)
		f.list = append(f.list, ListedThread{ID: id, CreatedBy: "user", Updated: at.Add(time.Hour + time.Duration(i)*time.Second)})
	}
	f.mu.Unlock()
	assertNone := func(why string) {
		t.Helper()
		time.Sleep(400 * time.Millisecond)
		if _, err := e.rt.State("T3-2"); err == nil {
			t.Fatalf("%s: admitted", why)
		}
	}
	// No current owner: nothing.
	b.set(nil)
	e = open(t, dir, opt)
	assertNone("no current owner")
	e.close()
	// The owner is current at the plan but not at admission.
	b.set(me)
	b.mu.Lock()
	b.after = func(n int) *T3Owner {
		if n == 1 {
			return me
		}
		return nil
	}
	b.calls = 0
	b.mu.Unlock()
	e = open(t, dir, opt)
	waitFor(t, 10*time.Second, "probes ran", func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.calls >= 3 })
	assertNone("owner changed before admission")
	e.close()
	// Owner current throughout: only the newest two of three.
	b.mu.Lock()
	b.after = nil
	b.mu.Unlock()
	e = open(t, dir, opt)
	defer e.close()
	waitFor(t, 10*time.Second, "two admitted", func() bool {
		_, err2 := e.rt.State("T3-2")
		_, err3 := e.rt.State("T3-3")
		return err2 == nil && err3 == nil
	})
	got := map[string]bool{}
	for _, n := range []string{"T3-2", "T3-3"} {
		cm, _ := e.rt.input(n)
		got[cm.Intake.Thread] = true
	}
	if !got["t-2"] || !got["t-3"] {
		t.Fatalf("not the newest: %v", got)
	}

	// A fresh data directory has no earlier run: nothing is listed.
	g := &listIntake{list: f.list, threads: f.threads}
	fresh := open(t, t.TempDir(), Options{Observer: g, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true, T3Owner: b.get})
	defer fresh.close()
	time.Sleep(300 * time.Millisecond)
	if g.lists != 0 {
		t.Fatal("reconciled with no earlier run")
	}
}
