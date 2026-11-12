package durable

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeReviewer answers per review key; keys records every invocation.
type fakeReviewer struct {
	mu     sync.Mutex
	answer func(ReviewRequest) ReviewResult
	keys   []string
	reqs   []ReviewRequest
}

func (f *fakeReviewer) Review(_ context.Context, q ReviewRequest) (ReviewResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys, f.reqs = append(f.keys, q.Key), append(f.reqs, q)
	return f.answer(q), nil
}

func (f *fakeReviewer) calls(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, k := range f.keys {
		if k == key {
			n++
		}
	}
	return n
}

// Automated review runs with no pre-review outbound: no UNACCEPTED report,
// no review or rejection chatter, even when the route cannot deliver. The
// verdict is bound to the commitment and attempt by the runtime; only the
// accepted result is delivered (here it fails, and holds on its receipt).
func TestReviewIsInternalUntilAccepted(t *testing.T) {
	dir := t.TempDir()
	chatMode.Store("silent", "fail")
	defer chatMode.Delete("silent")
	f := &fakeReviewer{answer: func(q ReviewRequest) ReviewResult {
		if q.Attempt == 1 {
			return ReviewResult{Turn: 7, Verdict: "reject", Reason: "tests missing"}
		}
		return ReviewResult{Turn: 8, Verdict: "accept", Reason: "verified abc"}
	}}
	e := open(t, dir, Options{Reviewer: f})
	defer e.close()
	c := owner
	c.Conv.Chat, c.MsgID = "silent", "s0"
	e.rt.Command(c, "add MEL-50 Silent route")
	e.phase("MEL-50", PhaseWorking, 1)
	c.MsgID = "s1"
	e.rt.Command(c, "report MEL-50 first")
	e.phase("MEL-50", PhaseWorking, 2)
	c.MsgID = "s2"
	e.rt.Command(c, "report MEL-50 second")
	e.phase("MEL-50", PhaseHeld, 2)
	st, _ := e.rt.State("MEL-50")
	if !strings.Contains(st.Blocked, "accepted/2") || st.Verdict != "accepted by supervisor (turn 8): verified abc" {
		t.Fatalf("%+v", st)
	}
	all := outbound(t, dir, "MEL-50")
	if len(all) != 1 || !strings.Contains(all[0], "MEL-50 done: second") {
		t.Fatalf("outbound %q", all)
	}
	if q := f.reqs[0]; q.Issue != "MEL-50" || q.Attempt != 1 || q.Route != c.Conv || q.Owner != c.Principal || q.Key != "MEL-50/1/review" {
		t.Fatalf("request %+v", q)
	}
}

// A held review (native denial, refused creator, stop, not current) is
// never re-invoked: not by replay, not across a restart, not under a new
// key, until an explicit !work retry.
func TestReviewHeldNotReinvoked(t *testing.T) {
	dir := t.TempDir()
	f := &fakeReviewer{answer: func(q ReviewRequest) ReviewResult {
		if strings.Contains(q.Key, "#retry") {
			return ReviewResult{Turn: 2, Verdict: "accept", Reason: "verified after the owner's recovery"}
		}
		return ReviewResult{Turn: 1, Held: "the harness asked for a native approval or input; nothing was approved"}
	}}
	e := open(t, dir, Options{Reviewer: f})
	e.cmd("add MEL-51 Denied review")
	e.phase("MEL-51", PhaseWorking, 1)
	e.cmdAs("d1", "report MEL-51 done")
	e.phase("MEL-51", PhaseHeld, 1)
	if st, _ := e.rt.State("MEL-51"); !strings.Contains(st.Blocked, "MEL-51/1/review: the harness asked for a native approval") {
		t.Fatalf("%+v", st)
	}
	time.Sleep(300 * time.Millisecond)
	e.close()
	e = open(t, dir, Options{Reviewer: f})
	defer e.close()
	e.phase("MEL-51", PhaseHeld, 1)
	time.Sleep(300 * time.Millisecond)
	if n := f.calls("MEL-51/1/review"); n != 1 || len(f.keys) != 1 {
		t.Fatalf("held review re-invoked: %v", f.keys)
	}
	if e.rt.Reviewable("MEL-51", 1) {
		t.Fatal("held commitment reviewable")
	}
	e.cmdAs("d2", "retry MEL-51")
	e.phase("MEL-51", PhaseClosed, 1)
	if f.calls("MEL-51/1/review#retry1") != 1 || len(f.keys) != 2 {
		t.Fatalf("recovery %v", f.keys)
	}
}

// Reviewable follows the actual workflow, not only its last published
// state: a paused commitment still published as reported is not reviewable.
func TestReviewableFollowsWorkflowStatus(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	f := &fakeReviewer{answer: func(ReviewRequest) ReviewResult { return ReviewResult{Failure: "unused"} }}
	blocking := reviewerFunc(func(ctx context.Context, q ReviewRequest) (ReviewResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return ReviewResult{}, ctx.Err()
		}
		return f.Review(ctx, q)
	})
	e := open(t, t.TempDir(), Options{Reviewer: blocking})
	defer e.close()
	defer once.Do(func() { close(release) })
	e.cmd("add MEL-52 Paused during review")
	e.phase("MEL-52", PhaseWorking, 1)
	e.cmdAs("p1", "report MEL-52 done")
	e.phase("MEL-52", PhaseReported, 1)
	if !e.rt.Reviewable("MEL-52", 1) || e.rt.Reviewable("MEL-52", 2) {
		t.Fatal("reviewable while reviewing attempt 1")
	}
	e.cmdAs("p2", "pause MEL-52")
	waitFor(t, 5*time.Second, "paused", func() bool { return !e.rt.Reviewable("MEL-52", 1) })
	if st, _ := e.rt.State("MEL-52"); st.Phase != PhaseReported {
		t.Fatalf("published %+v", st)
	}
}

type reviewerFunc func(context.Context, ReviewRequest) (ReviewResult, error)

func (f reviewerFunc) Review(ctx context.Context, q ReviewRequest) (ReviewResult, error) {
	return f(ctx, q)
}

// The observation floor is read before dispatch: a run that the dispatch
// starts and that completes before the first post-dispatch read is still
// this attempt's result.
func TestObserverFloorBeforeDispatch(t *testing.T) {
	o := &fakeObserver{obs: Observation{Status: "completed", LatestRun: "r1", RunCount: 1}}
	d := instantDispatcher{o: o}
	e := open(t, t.TempDir(), Options{Observer: o, Observe: 20 * time.Millisecond, Dispatcher: d})
	defer e.close()
	e.cmd("add MEL-53 worker=t3:thread-D Instant run")
	e.phase("MEL-53", PhaseReported, 1)
	if st, _ := e.rt.State("MEL-53"); !strings.Contains(st.Evidence, "run r2 finished: instant") {
		t.Fatalf("%+v", st)
	}
}

// instantDispatcher's dispatched run completes at once.
type instantDispatcher struct{ o *fakeObserver }

func (d instantDispatcher) Dispatch(_ context.Context, q DispatchRequest) (Receipt, error) {
	d.o.set(Observation{Status: "completed", LatestRun: "r2", RunCount: 2, Reply: "instant", Replies: map[string]string{"r2": "instant"}}, nil)
	return Receipt{Thread: q.Worker, Run: "r2"}, nil
}

func (d instantDispatcher) Reconcile(context.Context, DispatchRequest) (Receipt, bool, error) {
	return Receipt{}, false, nil
}
