package durable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeObserver is a worker host whose state the test sets.
type fakeObserver struct {
	mu  sync.Mutex
	obs Observation
	err error
	n   int
}

func (f *fakeObserver) Observable(w string) bool { return strings.HasPrefix(w, "t3:") }

func (f *fakeObserver) Observe(context.Context, string) (Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return f.obs, f.err
}

func (f *fakeObserver) set(o Observation, err error) {
	f.mu.Lock()
	f.obs, f.err = o, err
	f.mu.Unlock()
}

// No owner message is needed for a report: a worker run that finishes after
// the attempt began is observed and reported, still unaccepted. Questions,
// observer outages, stale runs and runs from before the attempt never count.
func TestObserverReportsFinishedRuns(t *testing.T) {
	f := &fakeObserver{obs: Observation{Status: "completed", LatestRun: "r0", RunCount: 1}}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-20 worker=t3:thread-A Observed job")
	e.phase("MEL-20", PhaseWorking, 1)
	time.Sleep(200 * time.Millisecond) // r0 finished before the attempt: not a report
	if st, _ := e.rt.State("MEL-20"); st.Phase != PhaseWorking {
		t.Fatalf("pre-attempt run counted: %+v", st)
	}
	f.set(Observation{}, errors.New("host down"))
	defer func() {
		if t.Failed() {
			st, err := e.rt.State("MEL-20")
			w := e.status("MEL-20")
			t.Logf("state %+v err %v status %s %v calls %d", st, err, w.Status, w.Error, f.n)
		}
	}()
	waitFor(t, 20*time.Second, "observer note", func() bool {
		st, _ := e.rt.State("MEL-20")
		return strings.HasPrefix(st.Note, "observer unavailable: ") && strings.Contains(st.Note, "host down") && st.Phase == PhaseWorking
	})
	f.set(Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", Pending: 1, RunCount: 2}, nil)
	e.phase("MEL-20", PhaseWaiting, 1)
	f.set(Observation{Status: "running", LatestRun: "r1", ActiveRun: "r1", RunCount: 2}, nil)
	e.phase("MEL-20", PhaseWorking, 1)
	f.set(Observation{Status: "completed", LatestRun: "r1", Reply: "done at abc", RunCount: 2}, nil)
	e.phase("MEL-20", PhaseReported, 1)
	st, _ := e.rt.State("MEL-20")
	if st.Evidence != "t3:thread-A run r1 completed: done at abc" || st.Note != "" {
		t.Fatalf("observed report %+v", st)
	}
	if got := outbound(t, e.dir, "UNACCEPTED"); len(got) != 1 {
		t.Fatalf("report deliveries %q", got)
	}
	if got := outbound(t, e.dir, "waiting for an answer"); len(got) != 1 {
		t.Fatalf("waiting notices %q", got)
	}
	e.cmdAs("x1", "reject MEL-20 incomplete")
	e.phase("MEL-20", PhaseWorking, 2)
	time.Sleep(300 * time.Millisecond) // r1 again: stale for attempt 2
	if st, _ := e.rt.State("MEL-20"); st.Phase != PhaseWorking || st.Attempt != 2 {
		t.Fatalf("stale run reused: %+v", st)
	}
	f.set(Observation{Status: "completed", LatestRun: "r2", Reply: "fixed", RunCount: 3}, nil)
	e.phase("MEL-20", PhaseReported, 2)
	e.cmdAs("a1", "accept MEL-20")
	e.phase("MEL-20", PhaseClosed, 2)
}

// The attempt-start read fails. An old completed run is never this
// attempt's result, even when the thread's metadata changed after the
// attempt began; it stays visibly unresolved until a newer run finishes.
func TestObserverOutageOldRunUnresolved(t *testing.T) {
	f := &fakeObserver{err: errors.New("host down")}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-21 worker=t3:thread-B Old run then new")
	e.phase("MEL-21", PhaseWorking, 1)
	// r0 completed long ago; only thread metadata (rename, organize) is new.
	f.set(Observation{Status: "completed", LatestRun: "r0", RunCount: 1}, nil)
	waitFor(t, 20*time.Second, "unresolved note", func() bool {
		st, _ := e.rt.State("MEL-21")
		return strings.HasPrefix(st.Note, "observation unresolved: t3:thread-B run r0")
	})
	time.Sleep(500 * time.Millisecond)
	if st, _ := e.rt.State("MEL-21"); st.Phase != PhaseWorking || len(outbound(t, e.dir, "UNACCEPTED")) != 0 {
		t.Fatalf("old run reported as this attempt: %+v", st)
	}
	f.set(Observation{Status: "completed", LatestRun: "r1", RunCount: 2, Reply: "done"}, nil)
	e.phase("MEL-21", PhaseReported, 1)
	if st, _ := e.rt.State("MEL-21"); !strings.Contains(st.Evidence, "run r1 completed") || st.Note != "" {
		t.Fatalf("%+v", st)
	}
}

// The attempt-start read of attempt 2 fails and the current run finishes
// during the outage: it is recovered by run order after the last read of
// attempt 1, not lost to a late baseline.
func TestObserverOutageRecoversCurrentRun(t *testing.T) {
	f := &fakeObserver{obs: Observation{Status: "completed", LatestRun: "r1", RunCount: 1}}
	e := open(t, t.TempDir(), Options{Observer: f, Observe: 20 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-22 worker=t3:thread-C Finishes during outage")
	e.phase("MEL-22", PhaseWorking, 1)
	f.set(Observation{Status: "completed", LatestRun: "r2", RunCount: 2, Reply: "first"}, nil)
	e.phase("MEL-22", PhaseReported, 1)
	f.set(Observation{}, errors.New("host down"))
	e.cmdAs("x2", "reject MEL-22 incomplete")
	e.phase("MEL-22", PhaseWorking, 2)
	time.Sleep(200 * time.Millisecond)
	f.set(Observation{Status: "completed", LatestRun: "r3", RunCount: 3, Reply: "second"}, nil)
	e.phase("MEL-22", PhaseReported, 2)
	if st, _ := e.rt.State("MEL-22"); !strings.Contains(st.Evidence, "run r3 completed: second") {
		t.Fatalf("%+v", st)
	}
}

// The T3 adapter runs only the read-only `thread show --json` of the
// configured CLI and reads its documented fields.
func TestT3ObserverReadsCLI(t *testing.T) {
	dir := t.TempDir()
	argv := filepath.Join(dir, "argv")
	script := filepath.Join(dir, "t3")
	body := `#!/bin/sh
printf '%s\n' "$@" > ` + argv + `
cat <<'J'
{"thread":{"threadId":"T","status":"completed","latestRunId":"run2","activeRunId":null,"pendingRequestCount":0,"runCount":2,"updatedAt":"2026-11-08T15:39:55.734Z"},"messages":[{"role":"user","runId":"run2","text":"brief"},{"role":"assistant","runId":"run2","text":"pushed 0123"}]}
J
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	o := T3{Command: []string{script}}
	if !o.Observable("t3:T") || o.Observable("codex:T") || (T3{}).Observable("t3:T") {
		t.Fatal("observable")
	}
	got, err := o.Observe(context.Background(), "t3:T")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Observation{Status: "completed", LatestRun: "run2", Pending: 0, RunCount: 2, Reply: "pushed 0123", Replies: map[string]string{"run2": "pushed 0123"}}) {
		t.Fatalf("%+v", got)
	}
	a, _ := os.ReadFile(argv)
	if string(a) != "thread\nshow\nT\n--json\n--messages\n20\n" {
		t.Fatalf("argv %q", a)
	}
	if _, err := o.Observe(context.Background(), "t3:--help"); err == nil {
		t.Fatal("flag-like thread id accepted")
	}
}
