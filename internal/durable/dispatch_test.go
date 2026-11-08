package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
		o.set(Observation{Status: "completed", LatestRun: run, RunCount: n, Reply: "did " + q.Key, Replies: map[string]string{run: "did " + q.Key}}, nil)
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
	if got := outbound(t, e.dir, "accepted"); len(got) != 2 {
		t.Fatalf("accepted deliveries %q", got)
	}
}

// The T3 adapter sends with the CLI's documented `thread send <id> -
// --json` (message on stdin) and returns T3's own receipt; only a server
// that is not running is NotSent; every other failure is an unknown
// outcome. Reconcile finds the marked user message's run.
func TestT3DispatchCLI(t *testing.T) {
	dir := t.TempDir()
	argv, stdin, mode := filepath.Join(dir, "argv"), filepath.Join(dir, "stdin"), filepath.Join(dir, "mode")
	script := filepath.Join(dir, "t3")
	body := `#!/bin/sh
printf '%s\n' "$@" > ` + argv + `
case "$(cat ` + mode + `)" in
down) echo "No running T3 Code server uses /home/x/.t3. Start T3 Code, then retry." >&2; exit 1;;
denied) echo "Thread T was not found." >&2; exit 1;;
other) echo '{"threadId":"OTHER","runId":"run:x","messageId":"m"}'; exit 0;;
esac
if [ "$2" = send ]; then
cat > ` + stdin + `
echo '{"threadId":"T","link":"<t3-thread://v2/env/T>","messageId":"message:mcp:client%3As1:thread-send:undefined%3Aundefined","runId":"run:thread:T:ordinal:3","status":"starting","delivery":"queued"}'
exit 0
fi
cat <<'J'
{"thread":{"threadId":"T","status":"running","latestRunId":"run:thread:T:ordinal:4","activeRunId":"run:thread:T:ordinal:4","pendingRequestCount":0,"runCount":4},"messages":[{"role":"user","runId":"run:thread:T:ordinal:3","text":"[heimdall MEL-70/1/dispatch]\nMEL-70: brief"},{"role":"user","runId":"run:thread:T:ordinal:4","text":"later"}]}
J
`
	for _, f := range []struct{ p, s string }{{script, body}, {mode, "ok"}} {
		if err := os.WriteFile(f.p, []byte(f.s), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	q := DispatchRequest{Key: "MEL-70/1/dispatch", Worker: "t3:T", Marker: "[heimdall MEL-70/1/dispatch]", Message: "[heimdall MEL-70/1/dispatch]\nMEL-70: brief"}
	c := T3{Command: []string{script}}
	rc, err := c.Dispatch(context.Background(), q)
	if err != nil || rc != (Receipt{Thread: "T", Run: "run:thread:T:ordinal:3", Message: "message:mcp:client%3As1:thread-send:undefined%3Aundefined", Delivery: "queued"}) {
		t.Fatalf("%+v %v", rc, err)
	}
	if a, _ := os.ReadFile(argv); string(a) != "thread\nsend\nT\n-\n--json\n" {
		t.Fatalf("argv %q", a)
	}
	if s, _ := os.ReadFile(stdin); string(s) != q.Message {
		t.Fatalf("stdin %q", s)
	}
	got, found, err := c.Reconcile(context.Background(), q)
	if err != nil || !found || got.Run != "run:thread:T:ordinal:3" {
		t.Fatalf("reconcile %+v %v %v", got, found, err)
	}
	if a, _ := os.ReadFile(argv); string(a) != "thread\nshow\nT\n--json\n--messages\n50\n" {
		t.Fatalf("argv %q", a)
	}
	if _, found, _ := c.Reconcile(context.Background(), DispatchRequest{Worker: "t3:T", Marker: "[heimdall MEL-70/2/dispatch]"}); found {
		t.Fatal("found an unsent marker")
	}
	for m, want := range map[string]string{"down": "notsent", "denied": "unknown", "other": "unknown"} {
		os.WriteFile(mode, []byte(m), 0o600)
		_, err := c.Dispatch(context.Background(), q)
		var ns NotSent
		var p Permanent
		got := "unknown"
		switch {
		case err == nil:
			got = "ok"
		case errors.As(err, &ns):
			got = "notsent"
		case errors.As(err, &p):
			got = "permanent"
		}
		if got != want {
			t.Fatalf("%s: %s (%v)", m, got, err)
		}
	}
	if _, err := c.Dispatch(context.Background(), DispatchRequest{Worker: "t3:-x"}); !errors.As(err, new(Permanent)) {
		t.Fatalf("flag-like worker: %v", err)
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
