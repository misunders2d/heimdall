package durable

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Without a child waiter (assistant wakes or T3 tracking not wired to a
// router), a child in another environment is recorded but not watched:
// its run ending never wakes the task, which sleeps out its WAIT. This is
// the behaviour MEL-857 found on the hub for every laptop child.
func TestRemoteChildWithoutWaiterNotWatched(t *testing.T) {
	fastWakes(t)
	obs := &threadObserver{}
	a := &childAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{
		"MEL-410/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait for the laptop child"},
	}}, kids: map[string][]Child{"MEL-410/1/work/1": {
		{Kind: "launch", Thread: "lap-A", Env: "env-L", Run: "rL"},
	}}}
	e := open(t, t.TempDir(), Options{Assistant: a, Observer: obs, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	e.cmd("add MEL-410 Fix it on the laptop")
	waitFor(t, 10*time.Second, "first wake's WAIT", func() bool {
		st, _ := e.rt.State("MEL-410")
		return strings.HasPrefix(st.Next, "next wake in 1h0m0s")
	})
	time.Sleep(time.Second)
	if got := a.seen("MEL-410/"); len(got) != 1 {
		t.Fatalf("wakes: %q", got)
	}
	if show := e.cmd("show MEL-410"); !strings.Contains(show, "- t3:lap-A@env-L (launch, run rL): in environment env-L: not watched here") {
		t.Fatalf("show:\n%s", show)
	}
}

// fakeChildWaiter answers remote child checks from a per-thread script:
// each check takes the next answer, the last one repeats.
type fakeChildWaiter struct {
	mu     sync.Mutex
	script map[string][]ChildWait
	errs   map[string]int // thread → checks that fail first
	calls  []ChildCheck
}

func (f *fakeChildWaiter) WaitChild(_ context.Context, q ChildCheck) (ChildWait, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, q)
	if f.errs[q.Thread] > 0 {
		f.errs[q.Thread]--
		return ChildWait{}, errors.New("check turn failed")
	}
	s := f.script[q.Thread]
	w := s[0]
	if len(s) > 1 {
		f.script[q.Thread] = s[1:]
	}
	return w, nil
}

func (f *fakeChildWaiter) checks(thread string) []ChildCheck {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ChildCheck
	for _, c := range f.calls {
		if c.Thread == thread {
			out = append(out, c)
		}
	}
	return out
}

// MEL-857: with a child waiter, a child in another environment is active
// and checked by keyed native wait turns, never read by the local CLI. A
// check that fails counts and keeps watching; a run-less launch takes the
// run its first check names and later checks pass it. While the run goes,
// nothing wakes the task; when a check finds it ended, the task's
// hour-long WAIT ends with exactly one wake, whose prompt lists the child
// as finished and says to pass its environmentId when reading it.
func TestRemoteChildCheckedAndOneWakeAfterTerminal(t *testing.T) {
	fastWakes(t)
	obs := &threadObserver{}
	cw := &fakeChildWaiter{errs: map[string]int{"lap-A": 1}, script: map[string][]ChildWait{
		"lap-A": {{Run: "rL", Status: "running"}, {Run: "rL", Status: "running"}, {Run: "rL", Status: "completed", Finished: true}},
	}}
	a := &childAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{
		"MEL-411/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait for the laptop child"},
		"MEL-411/1/work/2": {Turn: 2, Wait: time.Hour, Next: "read it"},
	}}, kids: map[string][]Child{"MEL-411/1/work/1": {
		{Kind: "launch", Thread: "lap-A", Env: "env-L"},
	}}}
	e := open(t, t.TempDir(), Options{Assistant: a, Observer: obs, ChildWaiter: cw, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	e.cmd("add MEL-411 Fix it on the laptop")
	waitFor(t, 30*time.Second, "the child's wake", func() bool { return len(a.seen("MEL-411/")) == 2 })
	time.Sleep(400 * time.Millisecond)
	if got := a.seen("MEL-411/"); len(got) != 2 {
		t.Fatalf("wakes after one ended remote child: %q", got)
	}
	cs := cw.checks("lap-A")
	if len(cs) != 4 {
		t.Fatalf("checks: %+v", cs)
	}
	for i, c := range cs {
		if want := "MEL-411/1/child/1/wait/" + string(rune('1'+i)); c.Key != want || c.Env != "env-L" || c.Issue != "MEL-411" || c.Attempt != 1 || c.Owner.ID == "" {
			t.Fatalf("check %d: %+v, want key %s", i, c, want)
		}
	}
	if cs[0].Run != "" || cs[1].Run != "" || cs[2].Run != "rL" || cs[3].Run != "rL" {
		t.Fatalf("runs passed: %q %q %q %q", cs[0].Run, cs[1].Run, cs[2].Run, cs[3].Run)
	}
	if obs.reads("t3:lap-A") != 0 || obs.reads("t3:lap-A@env-L") != 0 {
		t.Fatal("the local CLI read a remote child")
	}
	p := a.prompt("MEL-411/1/work/2")
	if !strings.Contains(p, "- t3:lap-A@env-L (launch, run rL): finished (completed)") || !strings.Contains(p, "pass that environmentId") {
		t.Fatalf("wake prompt:\n%s", p)
	}
}

// A remote child that never ends is checked at most remoteChecks times;
// then the watcher stops with a visible status and wakes the task once.
func TestRemoteChildChecksCapped(t *testing.T) {
	fastWakes(t)
	old := remoteChecks
	remoteChecks = 3
	t.Cleanup(func() { remoteChecks = old })
	cw := &fakeChildWaiter{script: map[string][]ChildWait{"lap-B": {{Run: "rB", Status: "running"}}}}
	a := &childAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{
		"MEL-412/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait"},
		"MEL-412/1/work/2": {Turn: 2, Wait: time.Hour, Next: "read it"},
	}}, kids: map[string][]Child{"MEL-412/1/work/1": {{Kind: "delegate", Thread: "lap-B", Env: "env-Z", Run: "rB", Task: "tk"}}}}
	e := open(t, t.TempDir(), Options{Assistant: a, ChildWaiter: cw, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.rt = e.rt
	e.cmd("add MEL-412 Long laptop work")
	waitFor(t, 30*time.Second, "the capped wake", func() bool { return len(a.seen("MEL-412/")) == 2 })
	time.Sleep(400 * time.Millisecond)
	if n := len(cw.checks("lap-B")); n != 3 {
		t.Fatalf("checks: %d", n)
	}
	if got := a.seen("MEL-412/"); len(got) != 2 {
		t.Fatalf("wakes: %q", got)
	}
	if show := e.cmd("show MEL-412"); !strings.Contains(show, "- t3:lap-B@env-Z (delegate, run rB, task tk): not watched: still not finished after 3 checks; read it yourself") {
		t.Fatalf("show:\n%s", show)
	}
}

// An active remote child row whose watcher never started (a crash between
// the insert and the workflow start) is checked again at the next start.
func TestRemoteChildReArmedAfterCrash(t *testing.T) {
	cw := &fakeChildWaiter{script: map[string][]ChildWait{"lap": {{Run: "r1", Status: "completed", Finished: true}}}}
	e, a := childTask(t, Options{}, 413)
	if _, err := e.rt.db.Exec(`INSERT INTO heimdall_t3_children(issue,thread,env,run,kind,task,wake,status,at) VALUES('MEL-413','lap','env-L','r1','send','','MEL-413/1/work/1','active',1)`); err != nil {
		t.Fatal(err)
	}
	dir := e.dir
	e.close()
	e = open(t, dir, Options{Assistant: a, ChildWaiter: cw, Observe: 20 * time.Millisecond, Silence: -1, T3Tracking: true})
	defer e.close()
	a.mu.Lock()
	a.rt = e.rt
	a.mu.Unlock()
	waitFor(t, 10*time.Second, "the child's wake", func() bool { return len(a.seen("MEL-413/")) == 2 })
	if cs := cw.checks("lap"); len(cs) != 1 || cs[0].Env != "env-L" || cs[0].Run != "r1" {
		t.Fatalf("checks: %+v", cs)
	}
}
