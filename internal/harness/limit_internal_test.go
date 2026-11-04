package harness

import (
	"context"
	"os/exec"
	"testing"
)

// Native terminal event and limit firing in either order: published answers
// survive, but an interrupt preceding publication makes the session unclean.
func TestLimitPublicationOrdering(t *testing.T) {
	for _, before := range []bool{true, false} {
		b := codexTestBridge()
		r := &Run{bridge: b, done: make(chan struct{}), exited: make(chan struct{}), worker: &worker{}}
		if !before {
			r.cancelled.Store(true)
			r.cause = CauseTimeout
		}
		b.event([]byte(`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"receipt","phase":"final_answer"}}}`))
		b.event([]byte(`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`))
		if before && r.cancelFor(CauseTimeout) {
			t.Fatal("limit overrode published terminal")
		}
		r.early()
		got := r.Result()
		if got.Answer != "receipt" || got.Cancelled || got.Unclean == before {
			t.Fatalf("before=%v: %+v", before, got)
		}
	}
}

func TestStopOverridesPendingLimitAndFencesSteer(t *testing.T) {
	b := codexTestBridge()
	r := &Run{bridge: b, done: make(chan struct{}), exited: make(chan struct{}), worker: &worker{}, cause: CauseTimeout}
	r.cancelled.Store(true)
	r.Cancel()
	if r.cause != CauseStop {
		t.Fatal(r.cause)
	}
	if r.Steer(context.Background(), "id", "no", nil) != SteerQueued || r.SteerContext(context.Background(), "id", "no", nil, "context") != SteerQueued {
		t.Fatal("input sent after cancel")
	}
	b.event([]byte(`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"receipt","phase":"final_answer"}}}`))
	b.event([]byte(`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`))
	r.early()
	select {
	case <-r.Done():
		t.Fatal("stop published an answer")
	default:
	}
	// Stop after Done must still disqualify the worker, unlike a timer.
	r.finalize(Result{Answer: "already sent"})
	r.Cancel()
	if !r.cancelled.Load() {
		t.Fatal("worker reusable after stop")
	}
}

type authFailureBridge struct{ stubBridge }

func (b *authFailureBridge) outcome() outcome { return outcome{failure: "sign-in or credentials"} }

func TestTimeoutKeepsSettledAuthFailure(t *testing.T) {
	b := &authFailureBridge{stubBridge{ready: make(chan struct{}), answered: make(chan struct{})}}
	r := &Run{bridge: b, ctx: context.Background(), cancel: func() {}, cmd: exec.Command("true"), done: make(chan struct{}), exited: make(chan struct{}), cause: CauseTimeout}
	r.cancelled.Store(true)
	r.finishExit(nil)
	got := r.Result()
	if !got.Cancelled || got.Cause != CauseTimeout || got.Failure != "sign-in or credentials" {
		t.Fatalf("auth outcome lost: %+v", got)
	}
}
