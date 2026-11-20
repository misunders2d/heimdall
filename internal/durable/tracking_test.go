package durable

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// A live owner turn that started T3 work without task_add gets one
// tracking-only task per thread (a repeat is the same task; a thread in
// another environment is its own). A turn that registered its work, a
// non-live caller and a runtime with the flag off admit nothing.
func TestAutoTrackOncePerThread(t *testing.T) {
	e := open(t, t.TempDir(), Options{Silence: -1, AutoTrack: true})
	defer e.close()
	since := time.Now()
	kids := []Child{{Kind: "launch", Thread: "thread-A", Run: "rA"}, {Kind: "send", Thread: "thread-A", Run: "rA2"}, {Kind: "launch"}}
	ids, err := e.rt.TrackUntracked(owner, since, kids)
	if err != nil || strings.Join(ids, ",") != "T3-1" {
		t.Fatalf("%v %v", ids, err)
	}
	if ids, _ := e.rt.TrackUntracked(owner, since, kids[:1]); len(ids) != 0 {
		t.Fatalf("second admission %v", ids)
	}
	if ids, _ := e.rt.TrackUntracked(owner, since, []Child{{Kind: "send", Thread: "thread-A", Env: "env-L"}}); strings.Join(ids, ",") != "T3-2" {
		t.Fatalf("remote thread %v", ids)
	}
	e.phase("T3-1", PhaseTracking, 1)
	cm, _ := e.rt.input("T3-1")
	if cm.Track == nil || cm.Track.Thread != "thread-A" || cm.Creator != owner.Principal.Key() || cm.Route != owner.Conv || cm.ReplyTo != owner.MsgID || cm.Intake != nil {
		t.Fatalf("%+v", cm)
	}
	if list := e.cmd("list"); !strings.Contains(list, "T3-1") || !strings.Contains(list, "T3-2 ") {
		t.Fatalf("list %q", list)
	}
	if st, _ := e.rt.State("T3-2"); !strings.Contains(st.Next, "not observed here (t3:thread-A@env-L)") {
		t.Fatalf("%+v", st)
	}

	// The turn registered its work: no tracking-only task.
	c := owner
	c.Conv.Chat = "c2"
	since = time.Now()
	if reply, changed := e.rt.Task(c, "add", "MEL-500", "Fix it", "req-500"); !changed {
		t.Fatal(reply)
	}
	if ids, _ := e.rt.TrackUntracked(c, since, []Child{{Kind: "launch", Thread: "thread-B"}}); len(ids) != 0 {
		t.Fatalf("tracked despite task_add %v", ids)
	}
	// A later turn in that chat without task_add is tracked.
	if ids, _ := e.rt.TrackUntracked(c, time.Now().Add(time.Millisecond), []Child{{Kind: "launch", Thread: "thread-B"}}); len(ids) != 1 {
		t.Fatalf("later turn %v", ids)
	}
	notLive := owner
	notLive.Live = false
	if ids, _ := e.rt.TrackUntracked(notLive, time.Now(), []Child{{Kind: "launch", Thread: "thread-C"}}); len(ids) != 0 {
		t.Fatalf("not live %v", ids)
	}

	// A number taken whose task never got admitted (an exit in between) is
	// admitted on the next try, under that number.
	if _, err := e.rt.db.Exec(`INSERT INTO heimdall_t3_intakes(thread,message,run,creator,probe,at) VALUES('thread-O','track','',?,'turn:x',?)`, owner.Principal.Key(), time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = e.rt.db.QueryRow(`SELECT max(n) FROM heimdall_t3_intakes`).Scan(&n)
	if ids, err := e.rt.TrackUntracked(owner, time.Now(), []Child{{Kind: "launch", Thread: "thread-O"}}); err != nil || len(ids) != 1 || ids[0] != fmt.Sprintf("T3-%d", n) {
		t.Fatalf("orphan number %v %v (n=%d)", ids, err, n)
	}

	off := open(t, t.TempDir(), Options{Silence: -1})
	defer off.close()
	if off.rt.AutoTracks() {
		t.Fatal("auto-track on by default")
	}
	if ids, _ := off.rt.TrackUntracked(owner, time.Now(), kids); len(ids) != 0 {
		t.Fatalf("flag off %v", ids)
	}
}

// A tracking-only task never works: no task_wake, child-observer wake or
// owner wake runs a turn for it. When its run finishes it tells the owner
// one line and closes; the assistant was never called.
func TestTrackingOnlyNeverWorks(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:thread-T", Observation{Status: "running", LatestRun: "rT", ActiveRun: "rT", RunCount: 1})
	a := &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Report: "should never run"} }}
	e := open(t, t.TempDir(), Options{Assistant: a, Observer: obs, Observe: 20 * time.Millisecond, Silence: -1, AutoTrack: true, T3Tracking: true})
	defer e.close()
	if ids, _ := e.rt.TrackUntracked(owner, time.Now(), []Child{{Kind: "launch", Thread: "thread-T", Run: "rT"}}); len(ids) != 1 {
		t.Fatal(ids)
	}
	e.phase("T3-1", PhaseTracking, 1)
	if got := e.rt.Wake(owner, "T3-1"); got != "T3-1: not waiting." {
		t.Fatalf("owner wake %q", got)
	}
	if got := e.rt.wakeTask("T3-1", "observer:t3:thread-T"); got != "T3-1: not waiting." {
		t.Fatalf("observer wake %q", got)
	}
	waitFor(t, 10*time.Second, "observed", func() bool { return obs.reads("t3:thread-T") >= 3 })
	obs.set("t3:thread-T", Observation{Status: "completed", LatestRun: "rT", RunCount: 1})
	e.phase("T3-1", PhaseClosed, 0)
	if got := outbound(t, e.dir, "T3-1"); len(got) != 1 || !strings.Contains(got[0], "T3 work t3:thread-T (T3-1) finished (completed); no longer tracked. — heimdall") {
		t.Fatalf("lines %q", got)
	}
	if got := a.seen(""); len(got) != 0 {
		t.Fatalf("a tracking-only task worked: %q", got)
	}
	if r := e.cmd("promote T3-1"); !strings.Contains(r, "already promoted or closed") {
		t.Fatal(r)
	}
}

// A tracking-only task whose run failed stays open with one line; only its
// creator's !work promote hands it to the assistant, whose wake gets the
// thread to finish. Promote refuses ordinary tasks and other callers.
func TestPromoteHandsToHubWork(t *testing.T) {
	obs := &threadObserver{}
	obs.set("t3:thread-P", Observation{Status: "running", LatestRun: "rP", ActiveRun: "rP", RunCount: 1})
	a := &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Wait: time.Hour, Next: "read thread-P"} }}
	e := open(t, t.TempDir(), Options{Assistant: a, Observer: obs, Observe: 20 * time.Millisecond, Silence: -1, AutoTrack: true})
	defer e.close()
	e.rt.TrackUntracked(owner, time.Now(), []Child{{Kind: "delegate", Thread: "thread-P", Run: "rP"}})
	e.phase("T3-1", PhaseTracking, 1)
	obs.set("t3:thread-P", Observation{Status: "failed", LatestRun: "rP", RunCount: 1})
	e.phase("T3-1", PhaseOpen, 1)
	if got := outbound(t, e.dir, "T3-1"); len(got) != 1 || !strings.Contains(got[0], `ended failed. Reply "!work promote T3-1"`) {
		t.Fatalf("lines %q", got)
	}
	if len(a.seen("")) != 0 {
		t.Fatal("worked before promote")
	}
	other := owner
	other.Principal = core.Principal{Platform: "test", ID: "other"}
	other.Private = false
	if r := e.rt.Command(other, "promote T3-1"); !strings.Contains(r, "only its creator can change it") {
		t.Fatal(r)
	}
	e.cmd("add MEL-501 Plain task")
	if r := e.cmd("promote MEL-501"); !strings.Contains(r, "only a tracking-only T3 task") {
		t.Fatal(r)
	}
	if r := e.cmdAs("p1", "promote T3-1"); r != "Promoted T3-1: your assistant works it from now on." {
		t.Fatal(r)
	}
	waitFor(t, 10*time.Second, "promoted wake", func() bool { return len(a.seen("T3-1/")) == 1 })
	if p := a.prompt("T3-1/1/work/1"); !strings.Contains(p, "The owner promoted it to you: read the thread") || !strings.Contains(p, "t3:thread-P") {
		t.Fatalf("prompt %q", p)
	}
	e.phase("T3-1", PhaseWorking, 1)
}
