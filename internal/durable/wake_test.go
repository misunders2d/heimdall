package durable

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// planAssistant answers each wake by its key, else by def.
type planAssistant struct {
	mu      sync.Mutex
	calls   []string
	prompts map[string]string
	plan    map[string]WorkResult
	def     func(key string) WorkResult
}

func (a *planAssistant) Work(_ context.Context, q WorkRequest) (WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, q.Key)
	if a.prompts == nil {
		a.prompts = map[string]string{}
	}
	a.prompts[q.Key] = q.Prompt
	if res, ok := a.plan[q.Key]; ok {
		return res, nil
	}
	if a.def != nil {
		return a.def(q.Key), nil
	}
	return WorkResult{}, nil
}

func (a *planAssistant) seen(prefix string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, k := range a.calls {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func (a *planAssistant) prompt(key string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.prompts[key]
}

func fastWakes(t *testing.T) {
	old := wakeRetryPause
	wakeRetryPause = 20 * time.Millisecond
	t.Cleanup(func() { wakeRetryPause = old })
}

func (e *env) held(issue, want string) State {
	e.t.Helper()
	var st State
	waitFor(e.t, 30*time.Second, issue+" held: "+want, func() bool {
		st, _ = e.rt.State(issue)
		return st.Phase == PhaseHeld && strings.Contains(st.Blocked, want)
	})
	return st
}

const cont = "it may already have acted. First check what it already did"

// Item 1/2: a wake that ends badly is handled by its class, never looping
// forever and never silent: not started retries under a new key; cut off
// continues with a prompt that says it may have acted; 3 failures, two
// malformed answers, an approval request or a refused creator stop the
// task with exactly one plain line to the owner; an owner stop stops it
// with none. !work retry continues it.
func TestWakeClassesAreBounded(t *testing.T) {
	fastWakes(t)
	a := &planAssistant{plan: map[string]WorkResult{
		"MEL-90/1/work/1": {Turn: 1, Failure: "failed: no harness could start", Class: WakeNotStarted},
		"MEL-90/1/work/2": {Turn: 2, Failure: "failed: no harness could start", Class: WakeNotStarted},
		"MEL-90/1/work/3": {Turn: 3, Failure: "failed: no harness could start", Class: WakeNotStarted},
		"MEL-90/1/work/4": {Turn: 4, Report: "done after retry"},
		"MEL-91/1/work/1": {Turn: 5, Failure: "cancelled: timeout", Class: WakeCutOff},
		"MEL-91/1/work/2": {Turn: 6, Report: "found the email already sent; done"},
		"MEL-92/1/work/1": {Turn: 7, Held: "the harness asked for a native approval", Class: WakeApproval},
		"MEL-92/1/work/2": {Turn: 8, Report: "approved by owner, done"},
		"MEL-93/1/work/1": {Turn: 9, Held: "review stopped", Class: WakeStopped},
		"MEL-94/1/work/1": {Turn: 10, Failure: "done: malformed answer", Class: WakeMalformed},
		"MEL-94/1/work/2": {Turn: 11, Failure: "done: malformed answer", Class: WakeMalformed},
		"MEL-89/1/work/1": {Turn: 12, Held: "no verified creator", Class: WakeRefused},
	}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	for _, c := range []string{"add MEL-90 Never starts", "add MEL-91 Cut off once", "add MEL-92 Needs approval", "add MEL-93 Stopped by owner", "add MEL-94 Talks nonsense", "add MEL-89 Refused creator"} {
		e.cmd(c)
	}
	line := func(issue, reason string) {
		t.Helper()
		want := `needs you: ` + reason + `. Reply "!work retry ` + issue + `" or "!work close ` + issue + `". — heimdall`
		waitFor(t, 10*time.Second, issue+" line", func() bool { return len(outbound(t, e.dir, "("+issue+") needs you")) == 1 })
		got := outbound(t, e.dir, "("+issue+") needs you")
		if !strings.HasSuffix(got[0], want) || strings.Contains(got[0], "\n") {
			t.Fatalf("%s line %q, want suffix %q", issue, got[0], want)
		}
	}
	e.held("MEL-90", "3 wakes in a row failed")
	line("MEL-90", "it failed 3 times")
	if p := a.prompt("MEL-90/1/work/3"); strings.Contains(p, cont) {
		t.Fatalf("a wake that never started was treated as one that acted: %q", p)
	}
	e.phase("MEL-91", PhaseReported, 1)
	if p := a.prompt("MEL-91/1/work/2"); !strings.Contains(p, cont) || !strings.Contains(p, "MEL-91/1/work/1") {
		t.Fatalf("continuation prompt %q", p)
	}
	e.held("MEL-92", "approval")
	line("MEL-92", "it asked for an approval")
	e.held("MEL-89", "refused")
	line("MEL-89", "it could not run as you")
	e.held("MEL-93", "stopped")
	e.held("MEL-94", "2 wakes in a row failed")
	line("MEL-94", "it gave an unreadable answer twice")
	time.Sleep(300 * time.Millisecond)
	if n := len(outbound(t, e.dir, "(MEL-93) needs you")); n != 0 {
		t.Fatal("owner stop told the owner")
	}
	if n := len(outbound(t, e.dir, "(MEL-91) needs you")); n != 0 {
		t.Fatal("a recovered cut-off told the owner")
	}
	// No loop: nothing more ran while stopped.
	for issue, n := range map[string]int{"MEL-90": 3, "MEL-92": 1, "MEL-93": 1, "MEL-94": 2, "MEL-89": 1} {
		if got := a.seen(issue + "/"); len(got) != n {
			t.Fatalf("%s wakes %q", issue, got)
		}
	}
	e.cmdAs("r90", "retry MEL-90")
	e.phase("MEL-90", PhaseReported, 1)
	e.cmdAs("r92", "retry MEL-92")
	e.phase("MEL-92", PhaseReported, 1)
	if p := a.prompt("MEL-92/1/work/2"); !strings.Contains(p, cont) {
		t.Fatalf("retry after approval is not a continuation: %q", p)
	}
	for _, issue := range []string{"MEL-90", "MEL-92"} {
		if n := len(outbound(t, e.dir, "("+issue+") needs you")); n != 1 {
			t.Fatalf("%s line repeated: %d", issue, n)
		}
	}
}

// Item 2: WAIT forever is capped at maxWaits wakes per attempt, and three
// automatic rejections stop the task; each stop is one line.
func TestWaitsAndRejectionsAreCapped(t *testing.T) {
	fastWakes(t)
	repo, _ := gitRepo(t)
	a := &planAssistant{def: func(key string) WorkResult {
		if strings.HasPrefix(key, "MEL-95/") {
			return WorkResult{Turn: 1, Wait: time.Millisecond, Next: "check again"}
		}
		return WorkResult{Turn: 2, Report: "done, no commit"}
	}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	for _, q := range e.rt.queues { // 100 queued wakes: poll faster than DBOS's 1 s
		if err := q.SetPollingInterval(e.rt.ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	e.cmd("add MEL-95 Wait forever")
	e.cmd("add MEL-96 gate=git:" + repo + " Bad evidence")
	waitFor(t, 120*time.Second, "100 waits", func() bool {
		st, _ := e.rt.State("MEL-95")
		return st.Phase == PhaseHeld
	})
	e.held("MEL-95", "100 wakes of attempt 1 ended in WAIT")
	if n := len(a.seen("MEL-95/")); n != maxWaits {
		t.Fatalf("wakes %d", n)
	}
	waitFor(t, 10*time.Second, "wait line", func() bool {
		return len(outbound(t, e.dir, `(MEL-95) needs you: it checked back 100 times without finishing.`)) == 1
	})
	st := e.held("MEL-96", "3 results in a row were rejected")
	if st.Attempt != 4 {
		t.Fatalf("%+v", st)
	}
	waitFor(t, 10*time.Second, "reject line", func() bool {
		return len(outbound(t, e.dir, `(MEL-96) needs you: its result was rejected 3 times.`)) == 1
	})
	time.Sleep(200 * time.Millisecond)
	if n := len(a.seen("MEL-96/")); n != 3 {
		t.Fatalf("rejected wakes %d", n)
	}
	e.cmdAs("r96", "retry MEL-96")
	waitFor(t, 10*time.Second, "work after retry", func() bool { return len(a.seen("MEL-96/4/")) == 1 })
}

// A failed send of the owner's line is a note; it does not stop the task
// again, and a retry still continues it.
func TestOwnerLineFailureDoesNotStop(t *testing.T) {
	fastWakes(t)
	chatMode.Store("nofeed", "fail")
	defer chatMode.Delete("nofeed")
	a := &planAssistant{plan: map[string]WorkResult{
		"MEL-98/1/work/1": {Turn: 1, Held: "approval", Class: WakeApproval},
		"MEL-98/1/work/2": {Turn: 2, Report: "done"},
	}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	c := owner
	c.Conv.Chat = "nofeed"
	e.rt.Command(c, "add MEL-98 Broken chat")
	waitFor(t, 20*time.Second, "note", func() bool {
		st, _ := e.rt.State("MEL-98")
		return st.Phase == PhaseHeld && strings.Contains(st.Note, "notice not delivered")
	})
	if got := outbound(t, e.dir, "(MEL-98) needs you"); len(got) != 1 || !strings.HasPrefix(got[0], "failed:") {
		t.Fatalf("%q", got)
	}
	// The chat works again before the retry, so the report's own (blocking)
	// delivery succeeds and "reported" is not a transient phase.
	chatMode.Store("nofeed", "ok")
	c.MsgID = "r98"
	e.rt.Command(c, "retry MEL-98")
	e.phase("MEL-98", PhaseReported, 1)
	waitFor(t, 20*time.Second, "report sent", func() bool {
		got := outbound(t, e.dir, "MEL-98 attempt 1: UNACCEPTED")
		return len(got) == 1 && strings.HasPrefix(got[0], "sent:")
	})
}

// fileAssistant models the router across processes: a key started before
// and not finished was cut off by the restart (the router's recorded
// interrupted turn); work/1 sends the email, then the process may die.
type fileAssistant struct {
	dir   string
	crash bool
}

func (f *fileAssistant) Work(_ context.Context, q WorkRequest) (WorkResult, error) {
	started := filepath.Join(f.dir, "started.log")
	if slices.Contains(lines(started), q.Key) {
		return WorkResult{Turn: 1, Failure: "interrupted: Heimdall restarted during this turn; not re-run", Class: WakeCutOff}, nil
	}
	appendLine(started, q.Key)
	appendLine(filepath.Join(f.dir, "prompts.log"), q.Key+"\t"+strings.ReplaceAll(q.Prompt, "\n", " "))
	effects := filepath.Join(f.dir, "effects.log")
	if len(lines(effects)) == 0 {
		appendLine(effects, "email to supplier sent by "+q.Key)
		if f.crash {
			os.Exit(3) // the effect happened; the wake never finished
		}
	}
	if strings.Contains(q.Prompt, cont) {
		return WorkResult{Turn: 2, Report: "the supplier email was already sent; confirmed once"}, nil
	}
	return WorkResult{Turn: 2, Wait: time.Hour, Next: "wait for the reply"}, nil
}

func appendLine(path, s string) {
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	fh.WriteString(s + "\n")
	fh.Sync()
	fh.Close()
}

// TestWakeCrashHelper is the process that dies in the middle of a wake.
func TestWakeCrashHelper(t *testing.T) {
	dir := os.Getenv("DURABLE_WAKE_CRASH_DIR")
	if dir == "" {
		t.Skip("helper process only")
	}
	e := open(t, dir, Options{Assistant: &fileAssistant{dir: dir, crash: true}, Silence: -1})
	e.cmd("add MEL-99 Email the supplier")
	time.Sleep(20 * time.Second)
	os.Exit(4) // the wake never ran
}

// Restart in the middle of a wake: after reopening, the cut-off wake is
// not replayed blindly; a continuation wake runs under a new key, sees
// that the email was sent, and the task finishes with nothing duplicated
// and nothing sent to the owner but the report.
func TestRestartMidWakeContinues(t *testing.T) {
	fastWakes(t)
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWakeCrashHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "DURABLE_WAKE_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("helper did not crash mid-wake: %v\n%s", err, out)
	}
	e := open(t, dir, Options{Assistant: &fileAssistant{dir: dir}, Silence: -1})
	defer e.close()
	e.phase("MEL-99", PhaseReported, 1)
	if got := lines(filepath.Join(dir, "effects.log")); len(got) != 1 {
		t.Fatalf("effects %q", got)
	}
	if got := lines(filepath.Join(dir, "started.log")); !slices.Equal(got, []string{"MEL-99/1/work/1", "MEL-99/1/work/2"}) {
		t.Fatalf("wakes %q", got)
	}
	p := lines(filepath.Join(dir, "prompts.log"))
	if len(p) != 2 || !strings.Contains(p[1], cont) || !strings.Contains(p[1], "MEL-99/1/work/1") {
		t.Fatalf("prompts %q", p)
	}
	if st, _ := e.rt.State("MEL-99"); st.Evidence != "the supplier email was already sent; confirmed once" {
		t.Fatalf("%+v", st)
	}
	waitFor(t, 10*time.Second, "report", func() bool { return len(outbound(t, dir, "MEL-99 attempt 1: UNACCEPTED")) == 1 })
	if got := outbound(t, dir, "MEL-99"); len(got) != 1 {
		t.Fatalf("deliveries %q", got)
	}
}

// A task stopped on the old path (before the patch) keeps the old
// behaviour across the upgrade: no line, no automatic retry. The owner's
// !work retry moves it onto the new rules: the next wake is a
// continuation and a later cut-off retries by itself.
func TestOldPathTaskRecoversThenUpgradesOnRetry(t *testing.T) {
	fastWakes(t)
	dir := t.TempDir()
	a := &planAssistant{plan: map[string]WorkResult{
		"MEL-97/1/work/1": {Turn: 1, Failure: "failed: harness exited after sending"},
		"MEL-97/1/work/2": {Turn: 2, Failure: "cancelled: timeout", Class: WakeCutOff},
		"MEL-97/1/work/3": {Turn: 3, Report: "sent once"},
	}}
	forceOldPath = true
	e := open(t, dir, Options{Assistant: a, Silence: -1})
	e.cmd("add MEL-97 Old task")
	e.held("MEL-97", "work MEL-97/1/work/1 (turn 1): failed")
	time.Sleep(200 * time.Millisecond)
	e.close()
	forceOldPath = false

	e = open(t, dir, Options{Assistant: a, Silence: -1})
	e.held("MEL-97", "work MEL-97/1/work/1 (turn 1): failed")
	time.Sleep(300 * time.Millisecond)
	if got := a.seen("MEL-97/"); !slices.Equal(got, []string{"MEL-97/1/work/1"}) {
		t.Fatalf("old task replayed: %q", got)
	}
	if n := len(outbound(t, dir, "needs you")); n != 0 {
		t.Fatal("old-path stop told the owner")
	}
	e.cmdAs("r97", "retry MEL-97")
	e.phase("MEL-97", PhaseReported, 1)
	if got := a.seen("MEL-97/"); !slices.Equal(got, []string{"MEL-97/1/work/1", "MEL-97/1/work/2", "MEL-97/1/work/3"}) {
		t.Fatalf("wakes %q", got)
	}
	if p := a.prompt("MEL-97/1/work/2"); !strings.Contains(p, cont) {
		t.Fatalf("retry was not a continuation: %q", p)
	}
	// And it replays deterministically on the new path after another restart.
	e.close()
	e = open(t, dir, Options{Assistant: a, Silence: -1})
	e.phase("MEL-97", PhaseReported, 1)
	defer e.close()
	e.cmdAs("a97", "accept MEL-97")
	e.phase("MEL-97", PhaseClosed, 1)
}
