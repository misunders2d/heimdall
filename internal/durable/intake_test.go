package durable

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// The real t3 v0.1.5 `thread show --json` output (laptop, 2026-11-10):
// a multi-line log block (whose last line is "}") before the JSON, and
// messages with only role, runId and text; only the thread carries
// createdBy. Without message marks the thread's own createdBy=user mark
// admits the latest user-role message (the weaker thread mode); a thread
// not marked user, or not marked at all, is refused, never guessed.
// Message marks, when any message carries one, still decide alone.
func TestParseIntakeRealShape(t *testing.T) {
	logBlock := "[13:04:35.205] WARN (#20): Database migration history diverges from this build {\n  divergent: [\n    '73:ActiveTurnItemIndexes (unknown to this build)'\n  ]\n}\n"
	threadBy := func(by string) string {
		mark := ""
		if by != "" {
			mark = `"createdBy":"` + by + `",`
		}
		return `{"thread":{"threadId":"T","title":"x",` + mark + `"creationSource":"web","status":"running","latestRunId":"r2","activeRunId":"r2","runCount":2}`
	}
	thread := threadBy("user")
	unmarkedMsgs := `,"messages":[{"role":"user","runId":"r1","text":"first"},{"role":"assistant","runId":"r1","text":"ok"},{"role":"user","runId":"r2","text":"do  it\nnow"},{"role":"assistant","runId":"r2","text":"ok"}]}` + "\n"
	// v0.1.5 owner thread: the latest user message, by the thread's mark.
	th, err := parseIntake([]byte(logBlock + thread + unmarkedMsgs))
	if err != nil || th.Agent || th.Request == nil || *th.Request != (IntakeMessage{Run: "r2", Text: "do  it\nnow", ByThread: true}) || th.ActiveRun != "r2" {
		t.Fatalf("owner thread: %+v %v", th, err)
	}
	// An agent-created thread, and a thread with no createdBy at all: refused.
	for _, by := range []string{"agent", "assistant", ""} {
		if th, err := parseIntake([]byte(logBlock + threadBy(by) + unmarkedMsgs)); !errors.Is(err, ErrNoCreatedBy) {
			t.Fatalf("thread createdBy %q: %+v %v", by, th, err)
		}
	}
	marked := logBlock + thread + `,"messages":[{"role":"user","createdBy":"user","id":"m1","runId":"r1","text":"first"},{"role":"user","createdBy":"user","messageId":"m2","runId":"r2","text":"second"},{"role":"assistant","runId":"r2","text":"ok"}]}` + "\n"
	th, err = parseIntake([]byte(marked))
	if err != nil || th.Request == nil || *th.Request != (IntakeMessage{ID: "m2", Run: "r2", Text: "second"}) || th.ActiveRun != "r2" || th.Status != "running" || th.Agent {
		t.Fatalf("%+v %v", th, err)
	}
	// Message marks win over the thread's: an agent's message in an owner
	// thread is not admitted, an owner's message in an agent thread is.
	agent := logBlock + thread + `,"messages":[{"role":"user","createdBy":"user","runId":"r1","text":"first"},{"role":"user","createdBy":"agent","runId":"r2","text":"delegated"}]}`
	if th, err := parseIntake([]byte(agent)); err != nil || !th.Agent || th.Request != nil {
		t.Fatalf("agent prompt: %+v %v", th, err)
	}
	ownerInAgentThread := logBlock + threadBy("agent") + `,"messages":[{"role":"user","createdBy":"user","runId":"r2","text":"mine"}]}`
	if th, err := parseIntake([]byte(ownerInAgentThread)); err != nil || th.Agent || th.Request == nil || *th.Request != (IntakeMessage{Run: "r2", Text: "mine"}) {
		t.Fatalf("owner message in agent thread: %+v %v", th, err)
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

// lockedLog is a log destination tests read while the runtime writes.
type lockedLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// slowT3 is a T3 CLI that shows an owner thread after a pause: thread
// "hang" after 5 s, any other thread after pause. The sleep holds no pipe
// of Heimdall's, so a kill at the limit ends the read at once.
func slowT3(t *testing.T, pause string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "t3")
	body := `#!/bin/sh
p=` + pause + `
[ "$3" = hang ] && p=5
sleep "$p" </dev/null >/dev/null 2>&1
cat <<'J'
{"thread":{"threadId":"T","title":"x","createdBy":"user","status":"running","latestRunId":"r1","activeRunId":"r1"},"messages":[{"role":"user","runId":"r1","text":"Email the PO"}]}
J
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return script
}

// A large T3 thread takes about 20 s to read (measured on the hub,
// 2026-11-10): the read limit leaves room for it. Scaled 1:100 here: a
// 250 ms read (25 s) is admitted under a 900 ms limit (90 s), and a read
// past the limit is still refused with one metadata-only line.
func TestIntakeToleratesSlowCLI(t *testing.T) {
	if intakeTimeout < 90*time.Second {
		t.Fatalf("intake read limit %s is below a large thread's read", intakeTimeout)
	}
	defer func(d time.Duration) { intakeTimeout = d }(intakeTimeout)
	intakeTimeout = 900 * time.Millisecond
	defer SetIntakePause(10 * time.Millisecond)()
	logs := &lockedLog{}
	opt := Options{Observer: T3{Command: []string{slowT3(t, "0.25")}}, Observe: time.Hour, Silence: -1, Log: log.New(logs, "", 0)}
	e := open(t, t.TempDir(), opt)
	defer e.close()
	t3 := Caller{Principal: owner.Principal, Role: core.RoleOwner, Conv: owner.Conv, Live: true, Private: true, Origin: OriginTrustedT3}
	start := time.Now()
	if reply, _ := e.rt.Task(t3, "intake", "slow", "", "prompt_submit"); reply != "T3 request noted." || time.Since(start) > 200*time.Millisecond {
		t.Fatal("hook held by the read:", reply, time.Since(start))
	}
	e.phase("T3-1", PhaseTracking, 0)
	if reply, _ := e.rt.Task(t3, "intake", "hang", "", "prompt_submit"); reply != "T3 request noted." {
		t.Fatal(reply)
	}
	const line = "durable: T3 intake refused (cli), thread hang\n"
	waitFor(t, 5*time.Second, "refusal line", func() bool { return strings.Contains(logs.String(), line) })
	if got := logs.String(); strings.Count(got, "T3 intake refused") != 1 || strings.Contains(got, "Email the PO") {
		t.Fatalf("log %q", got)
	}
	if _, err := e.rt.State("T3-2"); err == nil {
		t.Fatal("a read past the limit was admitted")
	}
}

// A configured T3 timeout never lifts a read past the intake limit.
func TestReadIntakeClampsTimeout(t *testing.T) {
	defer func(d time.Duration) { intakeTimeout = d }(intakeTimeout)
	intakeTimeout = 300 * time.Millisecond
	script := slowT3(t, "0")
	for _, d := range []time.Duration{0, 5 * time.Minute} {
		start := time.Now()
		if _, err := (T3{Command: []string{script}, Timeout: d}).ReadIntake(context.Background(), "hang"); err == nil || time.Since(start) > 3*time.Second {
			t.Fatalf("timeout %s: %v after %s", d, err, time.Since(start))
		}
	}
}
