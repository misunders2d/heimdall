package harness

import (
	"context"
	"testing"
	"time"
)

// stubBridge publishes an answer without ever becoming ready.
type stubBridge struct{ ready, answered chan struct{} }

func (s *stubBridge) Write(p []byte) (int, error)                                 { return len(p), nil }
func (s *stubBridge) readyCh() <-chan struct{}                                    { return s.ready }
func (s *stubBridge) answeredCh() <-chan struct{}                                 { return s.answered }
func (s *stubBridge) steer(context.Context, string, string, []Image) SteerOutcome { return SteerQueued }
func (s *stubBridge) settled() bool                                               { return true }
func (s *stubBridge) finish()                                                     {}
func (s *stubBridge) close()                                                      {}
func (s *stubBridge) outcome() outcome                                            { return outcome{answer: "x"} }

// An answer published before the turn was ready is not final until the
// process exited: the router never treats it as a ready turn, nor falls
// back beside a live process (review of 5b3c62f, finding 7: Ready was set
// unconditionally).
func TestNoEarlyAnswerBeforeReady(t *testing.T) {
	b := &stubBridge{ready: make(chan struct{}), answered: make(chan struct{})}
	close(b.answered)
	r := &Run{bridge: b, done: make(chan struct{}), exited: make(chan struct{}), grace: time.Hour, cancel: func() {}}
	ended := make(chan struct{})
	go func() { r.early(); close(ended) }()
	select {
	case <-r.done:
		t.Fatalf("answer before ready made final: %+v", r.result)
	case <-time.After(200 * time.Millisecond):
	}
	close(r.exited)
	<-ended
}
