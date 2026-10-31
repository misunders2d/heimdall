package harness

import (
	"context"
	"os/exec"
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

// An answer before ready takes the turn and has the same exit deadline.
func TestEarlyAnswerBeforeReadyHasExitDeadline(t *testing.T) {
	b := &stubBridge{ready: make(chan struct{}), answered: make(chan struct{})}
	close(b.answered)
	r := &Run{bridge: b, done: make(chan struct{}), exited: make(chan struct{}), grace: 20 * time.Millisecond, cancel: func() {}}
	// No process is launched: observe the termination log after the deadline.
	r.cmd = exec.Command("true")
	r.term = time.Hour
	stopped := make(chan string, 1)
	r.log = func(s string) { stopped <- s }
	ended := make(chan struct{})
	go func() { r.early(); close(ended) }()
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("answer not published")
	}
	if !r.Result().Ready {
		t.Fatal("answer did not commit turn")
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("exit deadline not armed")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("exit deadline did not terminate process")
	}
	close(r.exited)
}
