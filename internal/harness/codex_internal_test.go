package harness

import (
	"context"
	"io"
	"strings"
	"testing"
)

type discard struct{ io.Writer }

func (discard) Close() error { return nil }

// A steer acknowledgement that arrives after Codex completed the turn
// leaves the published answer alone (a regression of 47135e8 found in
// review: it reset the answer to empty).
func TestCodexSteerAckAfterCompletionKeepsTheAnswer(t *testing.T) {
	b := &codexBridge{ctx: context.Background(), input: discard{io.Discard}, ready: make(chan struct{}), answered: make(chan struct{}), steering: map[string]chan SteerOutcome{}, thread: "t1", turn: "turn-1"}
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
	<-ack
}
