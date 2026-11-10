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

// The real t3 v0.1.5 `thread show --json` output (laptop, 2026-11-10):
// a multi-line log block (whose last line is "}") before the JSON, and
// messages with only role, runId and text. Without createdBy marks no
// owner request can be told apart: refused, never guessed.
func TestParseIntakeRealShape(t *testing.T) {
	logBlock := "[13:04:35.205] WARN (#20): Database migration history diverges from this build {\n  divergent: [\n    '73:ActiveTurnItemIndexes (unknown to this build)'\n  ]\n}\n"
	thread := `{"thread":{"threadId":"T","title":"x","createdBy":"user","creationSource":"web","status":"running","latestRunId":"r2","activeRunId":"r2","runCount":2}`
	unmarked := logBlock + thread + `,"messages":[{"role":"user","runId":"r2","text":"do it"},{"role":"assistant","runId":"r2","text":"ok"}]}` + "\n"
	if _, err := parseIntake([]byte(unmarked)); !errors.Is(err, ErrNoCreatedBy) {
		t.Fatalf("unmarked output: %v", err)
	}
	marked := logBlock + thread + `,"messages":[{"role":"user","createdBy":"user","id":"m1","runId":"r1","text":"first"},{"role":"user","createdBy":"user","messageId":"m2","runId":"r2","text":"second"},{"role":"assistant","runId":"r2","text":"ok"}]}` + "\n"
	th, err := parseIntake([]byte(marked))
	if err != nil || th.Request == nil || *th.Request != (IntakeMessage{ID: "m2", Run: "r2", Text: "second"}) || th.ActiveRun != "r2" || th.Status != "running" || th.Agent {
		t.Fatalf("%+v %v", th, err)
	}
	agent := logBlock + thread + `,"messages":[{"role":"user","createdBy":"user","runId":"r1","text":"first"},{"role":"user","createdBy":"agent","runId":"r2","text":"delegated"}]}`
	if th, err := parseIntake([]byte(agent)); err != nil || !th.Agent || th.Request != nil {
		t.Fatalf("agent prompt: %+v %v", th, err)
	}
	if _, err := parseIntake([]byte(logBlock)); err == nil {
		t.Fatal("no JSON accepted")
	}
}

// fakeIntake is a T3 host for intake: an Observer that also reads threads.
type fakeIntake struct {
	mu sync.Mutex
	th IntakeThread
}

func (f *fakeIntake) Observable(string) bool { return false }
func (f *fakeIntake) Observe(context.Context, string) (Observation, error) {
	return Observation{}, errors.New("unused")
}
func (f *fakeIntake) ReadIntake(context.Context, string) (IntakeThread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.th, nil
}
func (f *fakeIntake) set(th IntakeThread) { f.mu.Lock(); f.th = th; f.mu.Unlock() }

// Run-end watching survives a restart: a T3 request tracked before the
// restart becomes an active task when its run turns out interrupted.
func TestIntakeTrackingSurvivesRestart(t *testing.T) {
	defer SetIntakePause(10 * time.Millisecond)()
	dir := t.TempDir()
	f := &fakeIntake{}
	a := &planAssistant{def: func(string) WorkResult { return WorkResult{Turn: 1, Report: "finished the PO email once"} }}
	opt := Options{Observer: f, Observe: 20 * time.Millisecond, Assistant: a, Silence: -1}
	e := open(t, dir, opt)
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	f.set(IntakeThread{Status: "running", LatestRun: "r1", ActiveRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Email the PO"}})
	if reply, _ := e.rt.Task(owner, "intake", "thread-A", "", "prompt_submit"); !strings.Contains(reply, "only for the trusted T3 hooks") {
		t.Fatal("a live chat caller used the T3 hook", reply)
	}
	if reply, _ := e.rt.Task(t3, "intake", "thread-A", "", "prompt_submit"); reply != "T3 request noted." {
		t.Fatal(reply)
	}
	e.phase("T3-1", PhaseTracking, 0)
	if r := e.cmd("add T3-9 sneaky"); !strings.Contains(r, "reserved") {
		t.Fatal(r)
	}
	if r, changed := e.rt.Task(owner, "add", "t3-2", "Sneaky", "req-t3"); changed || !strings.Contains(r, "reserved") {
		t.Fatal("control add took a T3 id:", r)
	}
	e.close()
	f.set(IntakeThread{Status: "interrupted", LatestRun: "r1", Request: &IntakeMessage{Run: "r1", Text: "Email the PO"}})
	e = open(t, dir, opt)
	defer e.close()
	e.phase("T3-1", PhaseReported, 1)
	if got := a.seen("T3-1/"); len(got) != 1 || !strings.Contains(a.prompt(got[0]), "ended interrupted") {
		t.Fatalf("%q", got)
	}
}
