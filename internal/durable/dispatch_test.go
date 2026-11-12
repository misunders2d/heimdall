package durable

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// scriptDispatcher is a worker host without native dedupe (like the T3
// CLI): every Dispatch call that reaches it is a new message. Each call
// takes the next behaviour: ok, lost (sent, response lost), unknown (not
// sent, outcome unknown to the caller), unsent (NotSent), deny.
type scriptDispatcher struct {
	mu     sync.Mutex
	plan   []string
	calls  int
	sent   []DispatchRequest
	onSend func(DispatchRequest)
}

func (d *scriptDispatcher) Dispatch(_ context.Context, q DispatchRequest) (Receipt, error) {
	d.mu.Lock()
	d.calls++
	how := "ok"
	if len(d.plan) > 0 {
		how, d.plan = d.plan[0], d.plan[1:]
	}
	switch how {
	case "unsent":
		d.mu.Unlock()
		return Receipt{}, NotSent{errors.New("host not running")}
	case "unknown":
		d.mu.Unlock()
		return Receipt{}, errors.New("timed out")
	case "deny":
		d.mu.Unlock()
		return Receipt{}, Permanent{errors.New("native denial")}
	}
	d.sent = append(d.sent, q)
	f := d.onSend
	d.mu.Unlock()
	if f != nil {
		f(q)
	}
	if how == "lost" {
		return Receipt{}, errors.New("connection reset after send")
	}
	return Receipt{Thread: q.Worker, Run: "run-" + q.Key, Message: "msg-" + q.Key, Delivery: "queued"}, nil
}

func (d *scriptDispatcher) Reconcile(_ context.Context, q DispatchRequest) (Receipt, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.sent {
		if strings.HasPrefix(s.Message, q.Marker+"\n") {
			return Receipt{Thread: q.Worker, Run: "run-" + s.Key, Delivery: "reconciled"}, true, nil
		}
	}
	return Receipt{}, false, nil
}

func (d *scriptDispatcher) counts() (calls, sent int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls, len(d.sent)
}

func (d *scriptDispatcher) messages() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, s := range d.sent {
		out = append(out, s.Message)
	}
	return out
}

// A send whose response is lost is reconciled from the worker's own record
// and adopted, never sent again, also across a restart.
func TestDispatchLostResponseReconciled(t *testing.T) {
	dir := t.TempDir()
	d := &scriptDispatcher{plan: []string{"lost"}}
	e := open(t, dir, Options{Dispatcher: d})
	e.cmd("add MEL-60 worker=t3:thread-L Lost response")
	e.phase("MEL-60", PhaseWorking, 1)
	if st, _ := e.rt.State("MEL-60"); st.Dispatched != "run-MEL-60/1/dispatch" {
		t.Fatalf("%+v", st)
	}
	e.close()
	e = open(t, dir, Options{Dispatcher: d})
	defer e.close()
	e.phase("MEL-60", PhaseWorking, 1)
	time.Sleep(300 * time.Millisecond)
	if calls, sent := d.counts(); calls != 1 || sent != 1 {
		t.Fatalf("calls %d sent %d", calls, sent)
	}
	if m := d.messages(); !strings.HasPrefix(m[0], "[heimdall MEL-60/1/dispatch]\nMEL-60: Lost response") {
		t.Fatalf("message %q", m[0])
	}
}

// An unknown outcome that the worker does not show is held visibly, never
// resent, across restarts; only an explicit retry sends, under a new key.
func TestDispatchUnknownOutcomeHeld(t *testing.T) {
	defer func(g time.Duration) { reconcileGrace = g }(reconcileGrace)
	reconcileGrace = 0
	dir := t.TempDir()
	d := &scriptDispatcher{plan: []string{"unknown"}}
	e := open(t, dir, Options{Dispatcher: d})
	e.cmd("add MEL-61 worker=t3:thread-U Unknown outcome")
	e.phase("MEL-61", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-61"); !strings.Contains(st.Blocked, "outcome unknown") || !strings.Contains(st.Blocked, "not resent") {
		t.Fatalf("%+v", st)
	}
	e.close()
	e = open(t, dir, Options{Dispatcher: d})
	defer e.close()
	e.phase("MEL-61", PhaseHeld, 1)
	time.Sleep(300 * time.Millisecond)
	if calls, sent := d.counts(); calls != 1 || sent != 0 {
		t.Fatalf("resent: calls %d sent %d", calls, sent)
	}
	e.cmdAs("r1", "retry MEL-61")
	e.phase("MEL-61", PhaseWorking, 1)
	if m := d.messages(); len(m) != 1 || !strings.HasPrefix(m[0], "[heimdall MEL-61/1/dispatch#retry1]\n") {
		t.Fatalf("retry %q", m)
	}
}

// A host that certainly received nothing is retried; one message results.
func TestDispatchNotSentRetried(t *testing.T) {
	d := &scriptDispatcher{plan: []string{"unsent"}}
	e := open(t, t.TempDir(), Options{Dispatcher: d})
	defer e.close()
	e.cmd("add MEL-62 worker=t3:thread-N Not sent")
	e.phase("MEL-62", PhaseWorking, 1)
	if calls, sent := d.counts(); calls != 2 || sent != 1 {
		t.Fatalf("calls %d sent %d", calls, sent)
	}
}

// A creator who is no longer an admitted owner on the route sends nothing:
// held before any call, with the claim released.
func TestDispatchStaleCreatorHeld(t *testing.T) {
	d := &scriptDispatcher{}
	var allowed sync.Map
	authorize := func(p core.Principal, r core.ConvKey) bool { _, ok := allowed.Load(p.Key() + "@" + r.Chat); return ok }
	e := open(t, t.TempDir(), Options{Dispatcher: d, Authorize: authorize})
	defer e.close()
	e.cmd("add MEL-63 worker=t3:thread-S Stale creator")
	e.phase("MEL-63", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-63"); !strings.Contains(st.Blocked, "refused before sending") {
		t.Fatalf("%+v", st)
	}
	if calls, _ := d.counts(); calls != 0 {
		t.Fatalf("stale creator dispatched %d", calls)
	}
	var n int
	if err := e.rt.db.QueryRow(`SELECT count(*) FROM heimdall_dispatch_claims`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("claims %d %v", n, err)
	}
	allowed.Store(owner.Principal.Key()+"@"+owner.Conv.Chat, true)
	e.cmdAs("r1", "retry MEL-63")
	e.phase("MEL-63", PhaseWorking, 1)
	if calls, sent := d.counts(); calls != 1 || sent != 1 {
		t.Fatalf("calls %d sent %d", calls, sent)
	}
}

// The report is the receipt's own run: a later run of the same thread (its
// latest) is never attributed to the attempt.
func TestDispatchBindsReceiptRun(t *testing.T) {
	o := &fakeObserver{obs: Observation{Status: "completed", LatestRun: "r0", RunCount: 1}}
	d := &scriptDispatcher{onSend: func(q DispatchRequest) {
		// Someone else's message ran after ours and finished first.
		o.set(Observation{Status: "completed", LatestRun: "r9", RunCount: 3, Reply: "other", Replies: map[string]string{"r9": "other"}}, nil)
	}}
	e := open(t, t.TempDir(), Options{Dispatcher: d, Observer: o, Observe: 20 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-64 worker=t3:thread-B Bound run")
	waitFor(t, 20*time.Second, "unresolved note", func() bool {
		st, _ := e.rt.State("MEL-64")
		return strings.HasPrefix(st.Note, "observation unresolved: dispatched run run-MEL-64/1/dispatch")
	})
	if st, _ := e.rt.State("MEL-64"); st.Phase != PhaseWorking {
		t.Fatalf("another run reported: %+v", st)
	}
	o.set(Observation{Status: "completed", LatestRun: "r9", RunCount: 3, Reply: "other", Replies: map[string]string{"r9": "other", "run-MEL-64/1/dispatch": "mine"}}, nil)
	e.phase("MEL-64", PhaseReported, 1)
	if st, _ := e.rt.State("MEL-64"); st.Evidence != "t3:thread-B run run-MEL-64/1/dispatch finished: mine" || st.Note != "" {
		t.Fatalf("%+v", st)
	}
}

// A rejected attempt is redispatched with bounded repair feedback inside
// the admitted brief; after acceptance only the pre-authorized next step is
// dispatched, once, and the commitment then retires. Nothing about the
// internal review or the rejection reaches the chat.
func TestDispatchRepairThenNextStep(t *testing.T) {
	o := &fakeObserver{obs: Observation{Status: "completed", RunCount: 0}}
	d := &scriptDispatcher{}
	n := 0
	d.onSend = func(q DispatchRequest) {
		n++
		run := "run-" + q.Key
		reply := "\n\ndid " + q.Key + "\n\nproof: a long tail the owner never sees"
		o.set(Observation{Status: "completed", LatestRun: run, RunCount: n, Reply: reply, Replies: map[string]string{run: reply}}, nil)
	}
	review := reviewerFunc(func(_ context.Context, q ReviewRequest) (ReviewResult, error) {
		if q.Attempt == 1 {
			return ReviewResult{Turn: 1, Verdict: "reject", Reason: "missing tests"}, nil
		}
		return ReviewResult{Turn: int64(q.Attempt), Verdict: "accept", Reason: fmt.Sprintf("checked attempt %d", q.Attempt)}, nil
	})
	e := open(t, t.TempDir(), Options{Dispatcher: d, Observer: o, Observe: 20 * time.Millisecond, Reviewer: review})
	defer e.close()
	e.cmd("add MEL-65 worker=t3:thread-R Fix parser | next: update the parser docs")
	e.phase("MEL-65", PhaseClosed, 3)
	time.Sleep(300 * time.Millisecond)
	m := d.messages()
	if len(m) != 3 {
		t.Fatalf("dispatches %q", m)
	}
	if !strings.HasPrefix(m[0], "[heimdall MEL-65/1/dispatch]\nMEL-65: Fix parser") || strings.Contains(m[0], "Repair") {
		t.Fatalf("first %q", m[0])
	}
	if !strings.HasPrefix(m[1], "[heimdall MEL-65/2/dispatch]\nMEL-65: Fix parser") || !strings.Contains(m[1], "Repair, attempt 2") || !strings.Contains(m[1], "missing tests") || !strings.Contains(m[1], "do not widen its scope") {
		t.Fatalf("repair %q", m[1])
	}
	if !strings.HasPrefix(m[2], "[heimdall MEL-65/3/dispatch]\n") || !strings.Contains(m[2], "authorized next step") || !strings.Contains(m[2], "update the parser docs") || strings.Contains(m[2], "Repair") {
		t.Fatalf("next %q", m[2])
	}
	for _, needle := range []string{"missing tests", "UNACCEPTED", "rejected"} {
		if got := outbound(t, e.dir, needle); len(got) != 0 {
			t.Fatalf("internal state %q reached the chat: %q", needle, got)
		}
	}
	got := outbound(t, e.dir, "MEL-65 done: ")
	if len(got) != 2 {
		t.Fatalf("accepted deliveries %q", got)
	}
	for i, m := range got { // the worker's first reply line, never its blank lead or proof
		key := fmt.Sprintf("MEL-65/%d/dispatch", i+2)
		if !strings.HasPrefix(m, "sent: MEL-65 done: t3:thread-R run run-"+key+" ") || !strings.HasSuffix(m, ": did "+key+"\n— heimdall") {
			t.Fatalf("accepted delivery %q", m)
		}
	}
}

// A settled claim replays its stored receipt exactly, with no send.
func TestDispatchSettledClaimReplay(t *testing.T) {
	d := &scriptDispatcher{}
	e := open(t, t.TempDir(), Options{Dispatcher: d})
	defer e.close()
	q := DispatchRequest{Key: "MEL-66/1/dispatch", Worker: "t3:thread-P", Marker: "[heimdall MEL-66/1/dispatch]"}
	want := Receipt{Thread: "thread-P", Run: "run:thread:P:ordinal:3", Message: "message:mcp:client%3As1:thread-send:x", Delivery: "queued"}
	if _, _, _, err := e.rt.claim(q.Key); err != nil {
		t.Fatal(err)
	}
	if err := e.rt.settle(q.Key, want); err != nil {
		t.Fatal(err)
	}
	got, err := e.rt.dispatchOnce(context.Background(), q)
	if err != nil || got != want {
		t.Fatalf("replay %+v %v", got, err)
	}
	if calls, _ := d.counts(); calls != 0 {
		t.Fatalf("settled claim sent %d", calls)
	}
}

// waitDispatcher dispatches like scriptDispatcher and answers each short
// native wait check from plan: "running", "done" or "deny".
type waitDispatcher struct {
	scriptDispatcher
	wmu   sync.Mutex
	waits []string // keys of the checks made
	plan  []string
	reads []string // read outcomes in order: "missing" holds, else the body
}

func (d *waitDispatcher) Read(_ context.Context, q DispatchRequest, run string) (string, error) {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	d.waits = append(d.waits, q.Key+" "+run)
	body := "missing"
	if len(d.reads) > 0 {
		body, d.reads = d.reads[0], d.reads[1:]
	}
	if body == "missing" {
		return "", Permanent{errors.New("no assistant result of run " + run)}
	}
	return body, nil
}

func (d *waitDispatcher) Wait(_ context.Context, q DispatchRequest, run string) (string, bool, error) {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	d.waits = append(d.waits, q.Key+" "+run)
	how := "running"
	if len(d.plan) > 0 {
		how, d.plan = d.plan[0], d.plan[1:]
	}
	switch how {
	case "deny":
		return "", false, Permanent{errors.New("native wait denied")}
	case "done":
		return "completed", true, nil
	}
	return "running", false, nil
}

func (d *waitDispatcher) checks() []string {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	return slices.Clone(d.waits)
}

// A worker in another environment is never read by the local observer: its
// dispatched run is checked by short keyed native waits, DBOS waits between
// them, a denied check holds until retry, and the finished run reports.
func TestDispatchLinkedWorkerWaits(t *testing.T) {
	if (T3{Command: []string{"/bin/t3"}}).Observable("t3:thread-L@env-2") {
		t.Fatal("local observer claims a linked-environment worker")
	}
	d := &waitDispatcher{plan: []string{"running", "deny", "running", "done"}, reads: []string{"missing", "Shipped: PR 12, tests green."}}
	e := open(t, t.TempDir(), Options{Dispatcher: d, Observer: T3{Command: []string{"/bin/false"}}, Observe: 20 * time.Millisecond})
	defer e.close()
	e.cmd("add MEL-67 worker=t3:thread-L@env-2 Linked")
	e.phase("MEL-67", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-67"); !strings.Contains(st.Blocked, "native wait denied") {
		t.Fatalf("%+v", st)
	}
	e.cmdAs("r1", "retry MEL-67")
	// Finished, but no bound result body: held visibly, never reported.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		st, _ := e.rt.State("MEL-67")
		if st.Phase == PhaseHeld && strings.Contains(st.Blocked, "read MEL-67/1/read/1") && strings.Contains(st.Blocked, "no assistant result") && st.Evidence == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%+v", st)
		}
	}
	e.cmdAs("r2", "retry MEL-67")
	e.phase("MEL-67", PhaseReported, 1)
	st, _ := e.rt.State("MEL-67")
	if st.Evidence != "t3:thread-L@env-2 run run-MEL-67/1/dispatch completed: Shipped: PR 12, tests green." {
		t.Fatalf("%+v", st)
	}
	want := []string{"MEL-67/1/wait/1 run-MEL-67/1/dispatch", "MEL-67/1/wait/2 run-MEL-67/1/dispatch", "MEL-67/1/wait/3 run-MEL-67/1/dispatch", "MEL-67/1/wait/4 run-MEL-67/1/dispatch", "MEL-67/1/read/1 run-MEL-67/1/dispatch", "MEL-67/1/read/2 run-MEL-67/1/dispatch"}
	if got := d.checks(); !slices.Equal(got, want) {
		t.Fatalf("checks %q", got)
	}
}
