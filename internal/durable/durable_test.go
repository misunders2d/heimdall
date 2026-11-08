package durable

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// chatMode makes the test delivery loop fail ("fail") or leave in flight
// ("hang") the replies of one chat.
var chatMode sync.Map

var owner = Caller{Principal: core.Principal{Platform: "test", ID: "owner"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "test", Chat: "c1"}, MsgID: "m0", Live: true}

type env struct {
	t    *testing.T
	dir  string
	st   *store.Store
	rt   *Runtime
	stop chan struct{}
	done sync.WaitGroup
}

// open starts a runtime on dir (reusing its databases) with a delivery loop
// that marks every reply of interface "test" sent.
func open(t *testing.T, dir string, opt Options) *env {
	t.Helper()
	st, err := store.Open(filepath.Join(dir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	opt.Path = filepath.Join(dir, "durable.db")
	if opt.Outbox == nil {
		opt.Outbox = st
	} else if w, ok := opt.Outbox.(*crashOutbox); ok {
		w.st = st
	}
	if opt.Silence == 0 {
		opt.Silence = time.Hour
	}
	rt, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, dir: dir, st: st, rt: rt, stop: make(chan struct{})}
	if err := rt.Launch(); err != nil {
		t.Fatal(err)
	}
	e.done.Add(1)
	go func() {
		defer e.done.Done()
		for {
			select {
			case <-e.stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			for {
				row, ok, err := st.NextOutbound("test")
				if err != nil || !ok {
					break
				}
				switch mode, _ := chatMode.Load(row.Conv.Chat); mode {
				case "fail":
					_ = st.Failed(row.ID, "chat not found")
				case "hang": // handed to the platform; outcome unknown
					_ = st.Sending(row.ID)
				default:
					_ = st.Sent(row.ID)
				}
			}
		}
	}()
	return e
}

func (e *env) close() {
	close(e.stop)
	e.done.Wait()
	e.rt.Close()
	e.st.Close()
}

func (e *env) cmd(text string) string { return e.rt.Command(owner, text) }

func (e *env) cmdAs(msg, text string) string {
	c := owner
	c.MsgID = msg
	return e.rt.Command(c, text)
}

// waitFor polls cond for up to d.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *env) phase(issue, phase string, attempt int) {
	e.t.Helper()
	waitFor(e.t, 20*time.Second, issue+" "+phase, func() bool {
		st, err := e.rt.State(issue)
		return err == nil && st.Phase == phase && (attempt == 0 || st.Attempt == attempt)
	})
}

func (e *env) status(issue string) dbos.WorkflowStatus {
	e.t.Helper()
	ws, err := dbos.ListWorkflows(e.rt.ctx, dbos.WithFilterWorkflowIDs(issue))
	if err != nil || len(ws) != 1 {
		e.t.Fatalf("status %s: %v %d", issue, err, len(ws))
	}
	return ws[0]
}

// outbound lists "state: text" of the replies whose text contains needle.
func outbound(t *testing.T, dir, needle string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "heimdall.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT state,text FROM outbound ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var state, text string
		if err := rows.Scan(&state, &text); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(text, needle) {
			out = append(out, state+": "+text)
		}
	}
	return out
}

func inboundCount(t *testing.T, dir string) (n int) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "heimdall.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT count(*) FROM inbound`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func gitRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q")
	run("commit", "-q", "--allow-empty", "-m", "x")
	return dir, run("rev-parse", "HEAD")
}

func TestAcceptBeforeDeliverAndRetire(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	if r := e.cmd("add MEL-1 Ship the thing"); !strings.Contains(r, "Tracking MEL-1") {
		t.Fatal(r)
	}
	e.phase("MEL-1", PhaseWorking, 1)
	e.cmdAs("m1", "report MEL-1 done, see PR")
	e.phase("MEL-1", PhaseReported, 1)
	if st := e.status("MEL-1"); st.Status != dbos.WorkflowStatusPending {
		t.Fatalf("reported but not accepted must stay open: %s", st.Status)
	}
	e.cmdAs("m2", "accept MEL-1")
	e.phase("MEL-1", PhaseClosed, 1)
	st, _ := e.rt.State("MEL-1")
	if st.Verdict != "accepted by test:owner" || len(st.Receipts) != 2 {
		t.Fatalf("state %+v", st)
	}
	got := outbound(t, e.dir, "MEL-1")
	if len(got) != 2 || !strings.Contains(got[0], "sent: MEL-1 attempt 1: UNACCEPTED worker report") || !strings.Contains(got[1], "sent: MEL-1 accepted") {
		t.Fatalf("deliveries %q", got)
	}
}

func TestEmptyOrBadEvidenceIsRejected(t *testing.T) {
	repo, sha := gitRepo(t)
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	e.cmd("add MEL-2 gate=git:" + repo + " Fix it")
	e.phase("MEL-2", PhaseWorking, 1)
	// A terminal worker with no evidence is not accepted.
	if err := e.rt.Signal("MEL-2", Event{Kind: "report", By: "test:owner"}, "k1"); err != nil {
		t.Fatal(err)
	}
	e.phase("MEL-2", PhaseWorking, 2)
	e.cmdAs("m2", "report MEL-2 done at deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	e.phase("MEL-2", PhaseWorking, 3)
	e.cmdAs("m3", "report MEL-2 done, no sha")
	e.phase("MEL-2", PhaseWorking, 4)
	// An accept for the working attempt is ignored: there is nothing to accept.
	e.cmdAs("m4", "accept MEL-2")
	time.Sleep(200 * time.Millisecond)
	if st, _ := e.rt.State("MEL-2"); st.Phase != PhaseWorking || strings.HasPrefix(st.Verdict, "accepted") {
		t.Fatalf("accepted without evidence: %+v", st)
	}
	e.cmdAs("m5", "report MEL-2 commit "+sha)
	e.phase("MEL-2", PhaseReported, 4)
	if got := outbound(t, e.dir, "evidence rejected"); len(got) != 3 {
		t.Fatalf("rejections %q", got)
	}
}

func TestRemainingWorkStaysOpen(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	e.cmd("add MEL-3 Build release | next: deploy after owner approval")
	e.phase("MEL-3", PhaseWorking, 1)
	e.cmdAs("m1", "report MEL-3 built")
	e.phase("MEL-3", PhaseReported, 1)
	e.cmdAs("m2", "accept MEL-3")
	e.phase("MEL-3", PhaseOpen, 1)
	st, _ := e.rt.State("MEL-3")
	if !strings.Contains(st.Next, "deploy after owner approval") || e.status("MEL-3").Status != dbos.WorkflowStatusPending {
		t.Fatalf("open commitment %+v", st)
	}
	e.cmdAs("m3", "close MEL-3 deployed")
	e.phase("MEL-3", PhaseClosed, 1)
}

// Silence is the progress clock, not the main chat: no inbound message or
// turn exists, yet the commitment keeps supervising. Unchanged state is
// noticed once, then the wait continues quietly.
func TestSilenceIsNotTerminalAndNotRepeated(t *testing.T) {
	e := open(t, t.TempDir(), Options{Silence: 150 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-4 Long job")
	waitFor(t, 10*time.Second, "a silence notice", func() bool { return len(outbound(t, e.dir, "MEL-4 attempt 1: no signal")) == 1 })
	time.Sleep(time.Second)
	if n := len(outbound(t, e.dir, "no signal")); n != 1 {
		t.Fatalf("unchanged state noticed %d times", n)
	}
	if s := e.status("MEL-4").Status; s != dbos.WorkflowStatusPending {
		t.Fatalf("silence ended supervision: %s", s)
	}
	if n := inboundCount(t, e.dir); n != 0 {
		t.Fatalf("progress needed %d inbound messages", n)
	}
	e.cmdAs("m1", "report MEL-4 finally")
	e.phase("MEL-4", PhaseReported, 1)
	// A new phase may notice once more.
	waitFor(t, 10*time.Second, "reported-phase notice", func() bool { return len(outbound(t, e.dir, "no signal")) == 2 })
}

func TestSilenceNoticesOff(t *testing.T) {
	e := open(t, t.TempDir(), Options{Silence: -1})
	defer e.close()
	e.cmd("add MEL-12 Quiet job")
	e.phase("MEL-12", PhaseWorking, 1)
	time.Sleep(500 * time.Millisecond)
	if n := len(outbound(t, e.dir, "MEL-12")); n != 0 {
		t.Fatalf("notices with silence off: %d", n)
	}
}

// A permanent delivery failure is not replayed: the commitment shows
// delivery_blocked on the failed receipt across restarts until an explicit
// retry, and events meanwhile are kept.
func TestPermanentDeliveryFailureBlocksWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	bad := owner
	bad.Conv.Chat = "bad"
	chatMode.Store("bad", "fail")
	defer chatMode.Delete("bad")
	e := open(t, dir, Options{})
	e.rt.Command(bad, "add MEL-10 Broken route")
	e.phase("MEL-10", PhaseWorking, 1)
	bad.MsgID = "b1"
	e.rt.Command(bad, "report MEL-10 done")
	e.phase("MEL-10", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-10"); !strings.Contains(st.Blocked, "chat not found") || !strings.Contains(st.Next, "!work retry") {
		t.Fatalf("blocked state %+v", st)
	}
	e.close()
	e = open(t, dir, Options{})
	defer e.close()
	e.phase("MEL-10", PhaseHeld, 1)
	time.Sleep(300 * time.Millisecond)
	if got := outbound(t, dir, "MEL-10"); len(got) != 1 || !strings.HasPrefix(got[0], "failed:") {
		t.Fatalf("replayed after restart: %q", got)
	}
	if l := e.rt.Command(bad, "list"); !strings.Contains(l, "held: outbound") {
		t.Fatal(l)
	}
	// The verdict arrives while blocked: kept, not lost.
	bad.MsgID = "b2"
	e.rt.Command(bad, "accept MEL-10")
	chatMode.Store("bad", "ok")
	bad.MsgID = "b3"
	if r := e.rt.Command(bad, "retry MEL-10"); r != "Sent retry to MEL-10." {
		t.Fatal(r)
	}
	e.phase("MEL-10", PhaseClosed, 1)
	got := outbound(t, dir, "UNACCEPTED")
	if len(got) != 2 || !strings.HasPrefix(got[0], "failed:") || !strings.HasPrefix(got[1], "sent:") {
		t.Fatalf("deliveries %q", got)
	}
}

// A reply handed to the platform when Heimdall died has an unknown outcome.
// The commitment never enqueues it again; the outbox's own restart policy
// (sending → pending, at least once) decides, and its receipt is adopted.
func TestAmbiguousSendIsReconciledNotReenqueued(t *testing.T) {
	dir := t.TempDir()
	hang := owner
	hang.Conv.Chat = "hang"
	chatMode.Store("hang", "hang")
	defer chatMode.Delete("hang")
	e := open(t, dir, Options{})
	e.rt.Command(hang, "add MEL-11 In flight")
	e.phase("MEL-11", PhaseWorking, 1)
	hang.MsgID = "h1"
	e.rt.Command(hang, "report MEL-11 done")
	waitFor(t, 10*time.Second, "in flight", func() bool {
		got := outbound(t, dir, "UNACCEPTED")
		return len(got) == 1 && strings.HasPrefix(got[0], "sending:")
	})
	e.close()
	chatMode.Store("hang", "ok")
	st, err := store.Open(filepath.Join(dir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
		t.Fatal(err)
	}
	st.Close()
	e = open(t, dir, Options{})
	defer e.close()
	e.phase("MEL-11", PhaseReported, 1)
	waitFor(t, 10*time.Second, "receipt adopted", func() bool {
		got := outbound(t, dir, "UNACCEPTED")
		return len(got) == 1 && strings.HasPrefix(got[0], "sent:")
	})
	hang.MsgID = "h2"
	e.rt.Command(hang, "accept MEL-11")
	e.phase("MEL-11", PhaseClosed, 1)
	final, _ := e.rt.State("MEL-11")
	if got := outbound(t, dir, "UNACCEPTED"); len(got) != 1 || !strings.HasPrefix(got[0], "sent:") || len(final.Receipts) != 2 {
		t.Fatalf("deliveries %q receipts %v", got, final.Receipts)
	}
}

// Listing pages through every commitment: none disappears past a page.
func TestListingDoesNotTruncate(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	const n = 501
	for i := 1; i <= n; i++ {
		// One worker: one runs, the rest stay queued.
		if _, err := e.rt.Admit(Commitment{Issue: fmt.Sprintf("L-%d", i), Title: "t", Creator: "test:owner", Route: owner.Conv, Resource: defaultRes, Worker: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	vs, err := e.rt.Views()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, v := range vs {
		seen[v.Issue] = true
	}
	if len(vs) != n || len(seen) != n || !seen["L-1"] || !seen[fmt.Sprintf("L-%d", n)] {
		t.Fatalf("listed %d (%d distinct) of %d", len(vs), len(seen), n)
	}
}

func TestOwnerPauseNeverExpires(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, Options{Silence: 100 * time.Millisecond})
	e.cmd("add MEL-5 Paused job")
	e.phase("MEL-5", PhaseWorking, 1)
	if r := e.cmd("pause MEL-5"); !strings.Contains(r, "paused by owner") {
		t.Fatal(r)
	}
	waitFor(t, 5*time.Second, "cancelled", func() bool { return e.status("MEL-5").Status == dbos.WorkflowStatusCancelled })
	before := len(outbound(t, dir, "MEL-5"))
	e.close()
	e = open(t, dir, Options{Silence: 100 * time.Millisecond}) // restart: recovery must not resume it
	defer e.close()
	time.Sleep(600 * time.Millisecond)
	if s := e.status("MEL-5").Status; s != dbos.WorkflowStatusCancelled {
		t.Fatalf("owner pause expired: %s", s)
	}
	if after := len(outbound(t, dir, "MEL-5")); after != before {
		t.Fatalf("paused commitment delivered %d→%d", before, after)
	}
	if l := e.cmd("list"); !strings.Contains(l, "paused by owner test:owner") {
		t.Fatal(l)
	}
	if r := e.cmd("resume MEL-5"); r != "MEL-5 resumed." {
		t.Fatal(r)
	}
	waitFor(t, 5*time.Second, "resumed", func() bool { return e.status("MEL-5").Status == dbos.WorkflowStatusPending })
	if r := e.cmdAs("m1", "report MEL-5 ok"); r != "Sent report to MEL-5." {
		t.Fatal(r)
	}
	e.phase("MEL-5", PhaseReported, 1)
}

func TestStaleAndDuplicateEventsIgnored(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	e.cmd("add MEL-6 Job")
	e.phase("MEL-6", PhaseWorking, 1)
	e.cmdAs("r1", "report MEL-6 first")
	e.phase("MEL-6", PhaseReported, 1)
	e.cmdAs("x1", "reject MEL-6 wrong")
	e.phase("MEL-6", PhaseWorking, 2)
	// The same source message again (redelivery) is stored once: no new report.
	e.cmdAs("r1", "report MEL-6 first")
	// A late result of attempt 1 does not count for attempt 2.
	if err := dbos.Send(e.rt.ctx, "MEL-6", Event{Kind: "report", Attempt: 1, By: "test:owner", Evidence: "late"}, topic); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if st, _ := e.rt.State("MEL-6"); st.Phase != PhaseWorking || st.Attempt != 2 || st.Evidence != "" {
		t.Fatalf("stale or duplicate event counted: %+v", st)
	}
	e.cmdAs("r2", "report MEL-6 second")
	e.phase("MEL-6", PhaseReported, 2)
}

func TestCommandFences(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	emp := owner
	emp.Role = core.RoleEmployee
	sched := owner
	sched.Live = false
	for _, c := range []Caller{emp, sched} {
		if r := e.rt.Command(c, "add MEL-7 x"); !strings.Contains(r, "owner's live messages only") {
			t.Fatal(r)
		}
		if r := e.rt.Command(c, "list"); !strings.Contains(r, "owner's live messages only") {
			t.Fatal(r)
		}
	}
	// Route and creator come from the verified caller; text cannot set them.
	e.cmd("add MEL-7 route=test:other creator=x Title")
	if r := e.cmd("add MEL-7 Other title"); !strings.Contains(r, "already tracked") {
		t.Fatal(r)
	}
	ws, err := dbos.ListWorkflows(e.rt.ctx, dbos.WithFilterWorkflowIDs("MEL-7"), dbos.WithFilterLoadInput(true))
	if err != nil || len(ws) != 1 {
		t.Fatal(err)
	}
	cm, ok := decodeInput(ws[0].Input)
	if !ok || cm.Route != owner.Conv || cm.Creator != "test:owner" || cm.Title != "route=test:other creator=x Title" {
		t.Fatalf("input %+v %v (%T)", cm, ok, ws[0].Input)
	}
	if r := e.cmd("add mel-x y"); !strings.Contains(r, "Not a Linear issue") {
		t.Fatal(r)
	}
	// Privacy comes from stored ingress: a task created in the owner's
	// private chat is invisible from a shared chat, and vice versa only its
	// own route shows there.
	priv := owner
	priv.Conv, priv.Private, priv.MsgID = core.ConvKey{Interface: "test", Chat: "dm"}, true, "p1"
	e.rt.Command(priv, "add MEL-8 Private plan")
	shared := owner
	shared.Conv, shared.MsgID = core.ConvKey{Interface: "test", Chat: "group"}, "g1"
	for _, q := range []string{"show MEL-8", "report MEL-8 x", "pause MEL-8", "add MEL-8 again"} {
		if r := e.rt.Command(shared, q); strings.Contains(r, "Private plan") || !strings.Contains(r, "not tracked here") && !strings.Contains(r, "tracked elsewhere") {
			t.Fatalf("%s from shared chat: %s", q, r)
		}
	}
	if l := e.rt.Command(shared, "list"); strings.Contains(l, "MEL-8") || strings.Contains(l, "MEL-7") {
		t.Fatalf("shared list leaks: %s", l)
	}
	if l := e.rt.Command(priv, "list"); !strings.Contains(l, "MEL-8") || !strings.Contains(l, "MEL-7") {
		t.Fatalf("private owner view: %s", l)
	}
	other := owner
	other.Principal.ID = "owner2"
	if r := e.rt.Command(other, "accept MEL-7"); !strings.Contains(r, "only its creator") {
		t.Fatalf("another owner changed it: %s", r)
	}
}

// memDispatcher is a worker host with native idempotency keys; deny makes
// a key's dispatch a permanent denial.
type memDispatcher struct {
	mu    sync.Mutex
	at    map[string]time.Time
	calls map[string]int
	deny  map[string]bool
}

func newMem() *memDispatcher {
	return &memDispatcher{at: map[string]time.Time{}, calls: map[string]int{}, deny: map[string]bool{}}
}

func (m *memDispatcher) Dispatch(_ context.Context, worker, key, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[key]++
	if m.deny[key] {
		return "", Permanent{fmt.Errorf("native denial for %s", key)}
	}
	if _, ok := m.at[key]; !ok {
		m.at[key] = time.Now()
	}
	return "rcpt-" + key, nil
}

func (m *memDispatcher) count(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[key]
}

// 60 commitments, one rate-limited resource: none dropped, all visible,
// the unlimited resource is not held back by the limited one.
func TestSixtyQueuedOneRateLimited(t *testing.T) {
	d := newMem()
	e := open(t, t.TempDir(), Options{Dispatcher: d, Resources: map[string]Resource{"limited": {RateLimit: 2, RatePeriod: time.Second}}})
	defer e.close()
	var issues []string
	start := time.Now()
	for i := 1; i <= 60; i++ {
		issue, res := fmt.Sprintf("Q-%d", i), "default"
		if i > 50 {
			res = "limited"
		}
		e.cmd(fmt.Sprintf("add %s resource=%s worker=w%d Task %d", issue, res, i%10, i))
		issues = append(issues, issue)
	}
	vs, err := e.rt.Views()
	if err != nil || len(vs) != 60 {
		t.Fatalf("visible %d %v", len(vs), err)
	}
	finished := map[string]bool{}
	waitFor(t, 60*time.Second, "all 60 closed", func() bool {
		for _, is := range issues {
			if finished[is] {
				continue
			}
			st, err := e.rt.State(is)
			if err != nil {
				continue
			}
			switch st.Phase {
			case PhaseWorking:
				_ = e.rt.Signal(is, Event{Kind: "report", By: "test:owner", Evidence: "ok"}, is+"/r")
			case PhaseReported:
				_ = e.rt.Signal(is, Event{Kind: "accept", By: "test:owner"}, is+"/a")
			case PhaseClosed:
				finished[is] = true
			}
		}
		for _, w := range must(dbos.ListWorkflows(e.rt.ctx, dbos.WithFilterName(workflowName))) {
			if w.Status == dbos.WorkflowStatusCancelled || w.Status == dbos.WorkflowStatusError {
				t.Fatalf("%s %s", w.ID, w.Status)
			}
		}
		return len(finished) == 60
	})
	var lastDefault, firstLimited, lastLimited time.Duration
	firstLimited = time.Hour
	for i := 1; i <= 60; i++ {
		at := d.at[effectKey(fmt.Sprintf("Q-%d", i), 1, "dispatch")].Sub(start)
		if i > 50 {
			firstLimited, lastLimited = min(firstLimited, at), max(lastLimited, at)
		} else {
			lastDefault = max(lastDefault, at)
		}
	}
	t.Logf("dispatches: default resource all by %s; rate-limited from %s to %s", lastDefault, firstLimited, lastLimited)
	if lastLimited-firstLimited < 3*time.Second {
		t.Fatalf("rate limit not applied: %s", lastLimited-firstLimited)
	}
}

// DBOS v1.8.0 counts a workflow that is merely waiting (Recv) against its
// queue's concurrency: this is why commitments are not queued themselves.
func TestDBOSQueueCountsWaitingWorkflows(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rt, err := Open(Options{Path: filepath.Join(dir, "durable.db"), Outbox: st})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	q, err := dbos.RegisterQueue(rt.ctx, "probe-one", dbos.WithGlobalConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	dbos.RegisterWorkflow(rt.ctx, probeWait, dbos.WithWorkflowName("probe-wait"))
	if err := rt.Launch(); err != nil {
		t.Fatal(err)
	}
	status := func(id string) dbos.WorkflowStatusType {
		h, err := dbos.RetrieveWorkflow[string](rt.ctx, id)
		if err != nil {
			return ""
		}
		s, _ := h.GetStatus()
		return s.Status
	}
	for _, id := range []string{"wait-a", "wait-b"} {
		if _, err := dbos.RunWorkflow(rt.ctx, probeWait, id, dbos.WithWorkflowID(id), dbos.WithQueue(q)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "a running", func() bool { return status("wait-a") == dbos.WorkflowStatusPending })
	time.Sleep(time.Second)
	if s := status("wait-b"); s != dbos.WorkflowStatusEnqueued {
		t.Fatalf("b %s: DBOS did not hold the slot for a waiting workflow", s)
	}
	if err := dbos.Send(rt.ctx, "wait-a", "go", "go"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "b starts after a ends", func() bool { return status("wait-b") == dbos.WorkflowStatusPending })
}

func probeWait(ctx dbos.Context, in string) (string, error) {
	return dbos.Recv[string](ctx, "go", time.Hour)
}

// One actual-compute slot: a stalled commitment (waiting on its worker)
// does not keep an unrelated ready one from dispatching.
func TestStalledCommitmentHoldsNoSlot(t *testing.T) {
	d := newMem()
	e := open(t, t.TempDir(), Options{Dispatcher: d, Resources: map[string]Resource{"one": {Concurrency: 1}}})
	defer e.close()
	e.cmd("add MEL-30 resource=one worker=wa Stalls")
	e.phase("MEL-30", PhaseWorking, 1)
	e.cmd("add MEL-31 resource=one worker=wb Ready")
	e.phase("MEL-31", PhaseWorking, 1)
	if d.count("MEL-31/1/dispatch") != 1 || d.count("MEL-30/1/dispatch") != 1 {
		t.Fatalf("dispatches %v", d.calls)
	}
	if l := e.cmd("list"); !strings.Contains(l, "MEL-30") || !strings.Contains(l, "MEL-31") {
		t.Fatal(l)
	}
}

// A native denial is held, never replayed, across restarts, until an
// explicit retry (under a new key).
func TestDispatchDenialHeldNotReplayed(t *testing.T) {
	dir := t.TempDir()
	d := newMem()
	d.deny["MEL-32/1/dispatch"] = true
	e := open(t, dir, Options{Dispatcher: d})
	e.cmd("add MEL-32 worker=wa Denied")
	e.phase("MEL-32", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-32"); !strings.Contains(st.Blocked, "native denial") {
		t.Fatalf("%+v", st)
	}
	e.close()
	e = open(t, dir, Options{Dispatcher: d})
	defer e.close()
	e.phase("MEL-32", PhaseHeld, 1)
	time.Sleep(300 * time.Millisecond)
	if n := d.count("MEL-32/1/dispatch"); n != 1 {
		t.Fatalf("denied dispatch replayed %d times", n)
	}
	e.cmdAs("r1", "retry MEL-32")
	e.phase("MEL-32", PhaseWorking, 1)
	if d.count("MEL-32/1/dispatch#retry1") != 1 {
		t.Fatalf("retry %v", d.calls)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// fileDispatcher is a worker with a native idempotency key, backed by a file
// so it survives the crashed process. It records the worker it was given.
type fileDispatcher struct {
	path  string
	crash bool
}

func (f *fileDispatcher) Dispatch(_ context.Context, worker, key, brief string) (string, error) {
	for _, l := range lines(f.path) {
		if p := strings.Split(l, "\t"); p[0] == key {
			return p[2], nil
		}
	}
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(fh, "%s\t%s\trcpt-%s\t%s\n", key, worker, key, brief)
	fh.Sync()
	fh.Close()
	if f.crash {
		os.Exit(3) // the effect happened; DBOS never saw the step finish
	}
	return "rcpt-" + key, nil
}

func lines(path string) []string {
	fh, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer fh.Close()
	var out []string
	s := bufio.NewScanner(fh)
	for s.Scan() {
		out = append(out, s.Text())
	}
	return out
}

// crashOutbox exits the process around one effect key: before the enqueue
// or after it (before DBOS checkpoints the step).
type crashOutbox struct {
	st     *store.Store
	effect string
	after  bool
}

func (c *crashOutbox) EnqueueEffect(effect string, out core.Outbound) (int64, error) {
	if effect == c.effect && !c.after {
		os.Exit(3)
	}
	id, err := c.st.EnqueueEffect(effect, out)
	if effect == c.effect && c.after {
		os.Exit(3)
	}
	return id, err
}

func (c *crashOutbox) OutboundReceipt(id int64) (store.Receipt, bool, error) {
	return c.st.OutboundReceipt(id)
}

// TestCrashHelper is the crashing process of TestCrashAtEverySeam.
func TestCrashHelper(t *testing.T) {
	dir, seam := os.Getenv("DURABLE_CRASH_DIR"), os.Getenv("DURABLE_CRASH_SEAM")
	if dir == "" {
		t.Skip("helper process only")
	}
	opt := Options{Dispatcher: &fileDispatcher{path: filepath.Join(dir, "dispatch.log"), crash: seam == "dispatch"}}
	switch seam {
	case "acceptance":
		opt.Outbox = &crashOutbox{effect: "MEL-9/accepted/1"}
	case "delivery":
		opt.Outbox = &crashOutbox{effect: "MEL-9/accepted/1", after: true}
	}
	e := open(t, dir, opt)
	e.cmd("add MEL-9 worker=t3:thread-A Crash me")
	if seam == "admission" {
		os.Exit(3)
	}
	e.phase("MEL-9", PhaseWorking, 1)
	if seam == "wait" {
		os.Exit(3)
	}
	e.cmdAs("m1", "report MEL-9 done")
	e.phase("MEL-9", PhaseReported, 1)
	e.cmdAs("m2", "accept MEL-9")
	time.Sleep(10 * time.Second)
	os.Exit(4) // the seam was never reached
}

func TestCrashAtEverySeam(t *testing.T) {
	for _, seam := range []string{"admission", "dispatch", "wait", "acceptance", "delivery"} {
		t.Run(seam, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$", "-test.count=1")
			cmd.Env = append(os.Environ(), "DURABLE_CRASH_DIR="+dir, "DURABLE_CRASH_SEAM="+seam)
			out, err := cmd.CombinedOutput()
			if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
				t.Fatalf("helper did not crash at %s: %v\n%s", seam, err, out)
			}
			e := open(t, dir, Options{Dispatcher: &fileDispatcher{path: filepath.Join(dir, "dispatch.log")}})
			defer e.close()
			if r := e.cmd("add MEL-9 worker=t3:thread-A Crash me"); !strings.Contains(r, "already tracked") {
				t.Fatalf("admission not durable: %s", r)
			}
			waitFor(t, 20*time.Second, "closed", func() bool {
				st, err := e.rt.State("MEL-9")
				if err != nil {
					return false
				}
				switch st.Phase {
				case PhaseWorking:
					e.cmdAs("m1", "report MEL-9 done")
				case PhaseReported:
					e.cmdAs("m2", "accept MEL-9")
				}
				return st.Phase == PhaseClosed
			})
			d := lines(filepath.Join(dir, "dispatch.log"))
			if len(d) != 1 || !strings.HasPrefix(d[0], "MEL-9/1/dispatch\tt3:thread-A\t") {
				t.Fatalf("dispatch effects %q", d)
			}
			acc := outbound(t, dir, "MEL-9 accepted")
			if len(acc) != 1 || !strings.HasPrefix(acc[0], "sent:") {
				t.Fatalf("accepted deliveries %q", acc)
			}
			if rep := outbound(t, dir, "UNACCEPTED"); len(rep) != 1 {
				t.Fatalf("report deliveries %q", rep)
			}
		})
	}
}

// probeV1 and probeV2 are one workflow ("probe") before and after a
// breaking change: v2 adds a step behind dbos.Patch.
func probeV1(ctx dbos.Context, in string) (string, error) {
	if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) { return "", nil }, dbos.WithStepName("a")); err != nil {
		return "", err
	}
	msg, err := dbos.Recv[string](ctx, "go", time.Hour)
	return "old+" + msg, err
}

func probeV2(ctx dbos.Context, in string) (string, error) {
	path := "old"
	p, err := dbos.Patch(ctx, "probe-new-step")
	if err != nil {
		return "", err
	}
	if p {
		if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) { return "", nil }, dbos.WithStepName("new")); err != nil {
			return "", err
		}
		path = "new"
	}
	if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) { return "", nil }, dbos.WithStepName("a")); err != nil {
		return "", err
	}
	msg, err := dbos.Recv[string](ctx, "go", time.Hour)
	return path + "+" + msg, err
}

// A breaking change ships behind dbos.Patch on the pinned AppVersion: a
// workflow started before the upgrade recovers on the old path after a
// restart; new ones take the new path.
func TestPatchedUpgradeRecoversOldWorkflows(t *testing.T) {
	dir := t.TempDir()
	start := func(wf dbos.Workflow[string, string]) (*store.Store, *Runtime) {
		st, err := store.Open(filepath.Join(dir, "heimdall.db"))
		if err != nil {
			t.Fatal(err)
		}
		rt, err := Open(Options{Path: filepath.Join(dir, "durable.db"), Outbox: st})
		if err != nil {
			t.Fatal(err)
		}
		dbos.RegisterWorkflow(rt.ctx, wf, dbos.WithWorkflowName("probe"))
		if err := rt.Launch(); err != nil {
			t.Fatal(err)
		}
		return st, rt
	}
	st, rt := start(probeV1)
	if _, err := dbos.RunWorkflow(rt.ctx, probeV1, "x", dbos.WithWorkflowID("old")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	rt.Close()
	st.Close()
	st, rt = start(probeV2)
	defer st.Close()
	defer rt.Close()
	if _, err := dbos.RunWorkflow(rt.ctx, probeV2, "x", dbos.WithWorkflowID("new")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old", "new"} {
		if err := dbos.Send(rt.ctx, id, "go", "go"); err != nil {
			t.Fatal(err)
		}
		h, err := dbos.RetrieveWorkflow[string](rt.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.GetResult()
		if err != nil || got != id+"+go" {
			t.Fatalf("%s: %q %v", id, got, err)
		}
	}
}
