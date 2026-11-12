package durable

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
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
	if maxWaits != 100 {
		t.Fatalf("production wait cap %d, want 100", maxWaits)
	}
	maxWaits = 5 // the same rule, without 100 real wakes
	t.Cleanup(func() { maxWaits = 100 })
	repo, _ := gitRepo(t)
	a := &planAssistant{def: func(key string) WorkResult {
		if strings.HasPrefix(key, "MEL-95/") {
			return WorkResult{Turn: 1, Wait: time.Millisecond, Next: "check again"}
		}
		return WorkResult{Turn: 2, Report: "done, no commit"}
	}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	for _, q := range e.rt.queues { // queued wakes: poll faster than DBOS's 1 s
		if err := q.SetPollingInterval(e.rt.ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	e.cmd("add MEL-95 Wait forever")
	e.cmd("add MEL-96 gate=git:" + repo + " Bad evidence")
	waitFor(t, 30*time.Second, "capped waits", func() bool {
		st, _ := e.rt.State("MEL-95")
		return st.Phase == PhaseHeld
	})
	e.held("MEL-95", fmt.Sprintf("%d wakes of attempt 1 ended in WAIT", maxWaits))
	if n := len(a.seen("MEL-95/")); n != maxWaits {
		t.Fatalf("wakes %d", n)
	}
	waitFor(t, 10*time.Second, "wait line", func() bool {
		return len(outbound(t, e.dir, fmt.Sprintf(`(MEL-95) needs you: it checked back %d times without finishing.`, maxWaits))) == 1
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

// gateAssistant holds each key in hold until its channel closes, then
// answers by plan.
type gateAssistant struct {
	*planAssistant
	started sync.Map
	hold    map[string]chan struct{}
}

func (g *gateAssistant) Work(ctx context.Context, q WorkRequest) (WorkResult, error) {
	g.started.Store(q.Key, true)
	if ch := g.hold[q.Key]; ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return WorkResult{}, ctx.Err()
		}
	}
	return g.planAssistant.Work(ctx, q)
}

func (g *gateAssistant) running(t *testing.T, key string) {
	t.Helper()
	waitFor(t, 10*time.Second, key+" running", func() bool { _, ok := g.started.Load(key); return ok })
}

func (e *env) wake(c Caller, issue, id string) string {
	reply, _ := e.rt.Task(c, "wake", issue, "", id)
	return reply
}

// task_wake ends a WAIT at once: a signal in a 1h pause wakes within
// seconds; signals during a running wake (twice) give exactly one more
// wake; with no signal the timer wakes as before. Paused, worker,
// finished and other people's tasks are refused; the same person under
// another identity is not.
func TestTaskWakeEndsWaitEarly(t *testing.T) {
	fastWakes(t)
	rel := make(chan struct{})
	a := &gateAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{
		"MEL-300/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait for the worker"},
		"MEL-300/1/work/2": {Turn: 2, Wait: time.Hour, Next: "wait again"},
		"MEL-300/1/work/3": {Turn: 3, Report: "worker done"},
		"MEL-301/1/work/1": {Turn: 4, Wait: 50 * time.Millisecond, Next: "check"},
		"MEL-301/1/work/2": {Turn: 5, Report: "timer woke it"},
		"MEL-302/1/work/1": {Turn: 6, Wait: time.Hour, Next: "wait"},
		"MEL-302/1/work/2": {Turn: 7, Report: "woken by the same person"},
		"MEL-303/1/work/1": {Turn: 8, Wait: time.Hour, Next: "wait"},
	}}, hold: map[string]chan struct{}{"MEL-300/1/work/1": rel}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	for _, c := range []string{"add MEL-300 Wait for a worker", "add MEL-301 Timer only", "add MEL-302 Alias", "add MEL-303 Paused", "add MEL-304 worker=t3:thread-W Worker task"} {
		e.cmd(c)
	}
	e.phase("MEL-301", PhaseReported, 1) // no signal: the timer, as before

	a.running(t, "MEL-300/1/work/1")
	for _, id := range []string{"w1", "w2"} {
		if got := e.wake(owner, "MEL-300", id); got != "Sent wake to MEL-300." {
			t.Fatal(got)
		}
	}
	close(rel)
	waitFor(t, 5*time.Second, "one more wake", func() bool { return len(a.seen("MEL-300/")) == 2 })
	time.Sleep(500 * time.Millisecond)
	if got := a.seen("MEL-300/"); len(got) != 2 {
		t.Fatalf("signals during a wake gave %q", got)
	}
	start := time.Now()
	if got := e.wake(owner, "MEL-300", "w3"); got != "Sent wake to MEL-300." {
		t.Fatal(got)
	}
	e.phase("MEL-300", PhaseReported, 1)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("woke after %s", d)
	}
	if got := e.wake(owner, "MEL-300", "w4"); got != "MEL-300: not waiting." {
		t.Fatal(got)
	}

	e.phase("MEL-302", PhaseWorking, 1)
	a.running(t, "MEL-303/1/work/1")
	stranger := Caller{Principal: core.Principal{Platform: "test2", ID: "owner"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "test2", Chat: "dm"}, Live: true, Private: true}
	if got := e.wake(stranger, "MEL-302", "x1"); got != "MEL-302 is not tracked here." {
		t.Fatal(got)
	}
	alias := stranger
	alias.Same = func(p core.Principal) bool { return p.Key() == owner.Principal.Key() }
	waitFor(t, 10*time.Second, "MEL-302 waiting", func() bool { return len(a.seen("MEL-302/")) == 1 })
	if got := e.wake(alias, "MEL-302", "x2"); got != "Sent wake to MEL-302." {
		t.Fatal(got)
	}
	e.phase("MEL-302", PhaseReported, 1)

	waitFor(t, 10*time.Second, "MEL-303 waiting", func() bool { return len(a.seen("MEL-303/")) == 1 })
	e.cmd("pause MEL-303")
	notLive := owner
	notLive.Live = false
	for _, tc := range []struct {
		c         Caller
		issue, id string
		want      string
	}{
		{owner, "MEL-303", "p1", "MEL-303: paused: resume first."},
		{owner, "MEL-304", "p2", "MEL-304: not woken by your assistant; task_wake wakes only those tasks."},
		{owner, "MEL-399", "p3", "MEL-399 is not tracked here."},
		{owner, "MEL-303", "", "Unsupported task action."},
		{notLive, "MEL-303", "p4", "!work is for the owner's live messages only."},
	} {
		if got := e.wake(tc.c, tc.issue, tc.id); got != tc.want {
			t.Fatalf("%s: %q, want %q", tc.issue, got, tc.want)
		}
	}
	if got := a.seen("MEL-303/"); len(got) != 1 {
		t.Fatalf("paused task woke: %q", got)
	}
}

// A signal stored while wake 1 ran is stale once that wake reports: the
// owner's accept queued behind it still wins over the supervisor's
// reject.
func TestTaskWakeStaleSignalLosesToOwnerVerdict(t *testing.T) {
	relWork, relReview, reviewing := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	a := &gateAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{"MEL-310/1/work/1": {Turn: 1, Report: "done"}}},
		hold: map[string]chan struct{}{"MEL-310/1/work/1": relWork}}
	rv := reviewerFunc(func(context.Context, ReviewRequest) (ReviewResult, error) {
		reviewing <- struct{}{}
		<-relReview
		return ReviewResult{Turn: 2, Verdict: "reject", Reason: "looks wrong"}, nil
	})
	e := open(t, t.TempDir(), Options{Assistant: a, Reviewer: rv, Silence: -1})
	defer e.close()
	e.cmd("add MEL-310 Verdict race")
	a.running(t, "MEL-310/1/work/1")
	if got := e.wake(owner, "MEL-310", "s1"); got != "Sent wake to MEL-310." {
		t.Fatal(got)
	}
	close(relWork)
	select {
	case <-reviewing:
	case <-time.After(10 * time.Second):
		t.Fatal("no review")
	}
	if got := e.cmdAs("acc", "accept MEL-310"); got != "Sent accept to MEL-310." {
		t.Fatal(got)
	}
	close(relReview)
	e.phase("MEL-310", PhaseClosed, 1)
	if st, _ := e.rt.State("MEL-310"); !strings.HasPrefix(st.Verdict, "accepted by "+owner.Principal.Key()) {
		t.Fatalf("%+v", st)
	}
}

// The signal is in durable.db when task_wake answers: stored while no
// workflow runs (the runtime is not launched), it still wakes the task
// after the restart, long before its 1h timer. A signal sent while the
// restarted runtime is still recovering also wakes.
func TestTaskWakeStoredBeforeAckSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	a := &planAssistant{plan: map[string]WorkResult{
		"MEL-320/1/work/1": {Turn: 1, Wait: time.Hour, Next: "wait"},
		"MEL-320/1/work/2": {Turn: 2, Report: "woken after restart"},
		"MEL-321/1/work/1": {Turn: 3, Wait: time.Hour, Next: "wait"},
		"MEL-321/1/work/2": {Turn: 4, Report: "woken while recovering"},
	}}
	e := open(t, dir, Options{Assistant: a, Silence: -1})
	for _, issue := range []string{"MEL-320", "MEL-321"} {
		e.cmd("add " + issue + " Survive a restart")
		waitFor(t, 10*time.Second, "pause", func() bool { st, _ := e.rt.State(issue); return strings.HasPrefix(st.Next, "next wake in 1h") })
	}
	e.close()

	st, err := store.Open(filepath.Join(dir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	rt, err := Open(Options{Path: filepath.Join(dir, "durable.db"), Outbox: st, Assistant: a, Silence: -1})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := rt.Task(owner, "wake", "MEL-320", "", "r1"); got != "Sent wake to MEL-320." {
		t.Fatal(got)
	}
	rt.Close()
	st.Close()

	e = open(t, dir, Options{Assistant: a, Silence: -1})
	defer e.close()
	// Right after a restart DBOS shows a recovering workflow as enqueued:
	// a signal then still counts.
	if got := e.wake(owner, "MEL-321", "r2"); got != "Sent wake to MEL-321." {
		t.Fatal(got)
	}
	e.phase("MEL-321", PhaseReported, 1)
	e.phase("MEL-320", PhaseReported, 1)
	if got := a.seen("MEL-320/"); !slices.Equal(got, []string{"MEL-320/1/work/1", "MEL-320/1/work/2"}) {
		t.Fatalf("wakes %q", got)
	}
}

// Without a supervisor, a signal stored while wake 1 ran is stale in the
// owner-verdict wait: it is not silence, so no notice goes out, and the
// owner's accept still closes the task.
func TestTaskWakeStaleSignalIsNotSilence(t *testing.T) {
	rel := make(chan struct{})
	a := &gateAssistant{planAssistant: &planAssistant{plan: map[string]WorkResult{"MEL-330/1/work/1": {Turn: 1, Report: "done"}}},
		hold: map[string]chan struct{}{"MEL-330/1/work/1": rel}}
	e := open(t, t.TempDir(), Options{Assistant: a}) // silence notices on (1h)
	defer e.close()
	e.cmd("add MEL-330 Manual verdict")
	a.running(t, "MEL-330/1/work/1")
	if got := e.wake(owner, "MEL-330", "s1"); got != "Sent wake to MEL-330." {
		t.Fatal(got)
	}
	close(rel)
	waitFor(t, 10*time.Second, "report", func() bool { return len(outbound(t, e.dir, "MEL-330 attempt 1: UNACCEPTED")) == 1 })
	e.cmdAs("acc", "accept MEL-330") // queued behind the stale signal
	e.phase("MEL-330", PhaseClosed, 1)
	if got := outbound(t, e.dir, "no signal for"); len(got) != 0 {
		t.Fatalf("stale signal sent a notice: %q", got)
	}
}
