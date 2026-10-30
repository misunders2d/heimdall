package harness

import (
	"context"
	"io"
	"strings"
	"testing"
)

type discard struct{ io.Writer }

func (discard) Close() error { return nil }

func codexTestBridge() *codexBridge {
	return &codexBridge{ctx: context.Background(), input: discard{io.Discard}, ready: make(chan struct{}), answered: make(chan struct{}), steering: map[string]chan SteerOutcome{}, thread: "t1", turn: "turn-1"}
}

// A steer acknowledgement that arrives after Codex completed the turn
// leaves the published answer alone (a regression of 47135e8 found in
// review: it reset the answer to empty), and the steer counts as unknown:
// the answer went out without it (review of d05b748: it counted as
// accepted, so the sender was never told).
func TestCodexSteerAckAfterCompletionKeepsTheAnswer(t *testing.T) {
	b := codexTestBridge()
	ack := make(chan SteerOutcome, 1)
	b.steering[`"heimdall-steer-1"`] = ack
	for _, line := range []string{
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"ALPHA","phase":"final_answer"}}}`,
		`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`,
		`{"id":"heimdall-steer-1","result":{"turnId":"turn-1"}}`,
	} {
		b.event([]byte(line))
	}
	if o := b.outcome(); o.answer != "ALPHA" || strings.Join(o.answers, "|") != "ALPHA" {
		t.Fatalf("outcome %+v", o)
	}
	if got := <-ack; got != SteerUnknown {
		t.Fatalf("steer acknowledged after the turn completed: %s, want %s", got, SteerUnknown)
	}
}

// Acknowledged before the turn completed, the steer is accepted and gets
// its own answer after the one given before it.
func TestCodexSteerAckBeforeCompletionIsAccepted(t *testing.T) {
	b := codexTestBridge()
	ack := make(chan SteerOutcome, 1)
	b.steering[`"heimdall-steer-1"`] = ack
	for _, line := range []string{
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"ALPHA","phase":"final_answer"}}}`,
		`{"id":"heimdall-steer-1","result":{"turnId":"turn-1"}}`,
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i2","type":"agentMessage","text":"BRAVO","phase":"final_answer"}}}`,
		`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`,
	} {
		b.event([]byte(line))
	}
	if got := <-ack; got != SteerAccepted {
		t.Fatalf("steer acknowledged before completion: %s, want %s", got, SteerAccepted)
	}
	if o := b.outcome(); strings.Join(o.answers, "|") != "ALPHA|BRAVO" {
		t.Fatalf("outcome %+v", o)
	}
}
