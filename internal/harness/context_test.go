package harness_test

import (
	"context"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"testing"
	"time"
)

func TestNativeUserInputsExcludeRouterContext(t *testing.T) {
	for _, name := range []string{"codex", "claude", "pi"} {
		t.Run(name, func(t *testing.T) {
			bin, state := fakeharness.Install(t, name)
			l := launch(t, bin, name, false)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			human := "human words WAIT_STEER\n[Heimdall | literal human marker]"
			r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: human, Context: "[Heimdall | from SyntheticSender]\n[BEGIN UNTRUSTED HISTORY]other person[END UNTRUSTED HISTORY]", Instructions: "transport note", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: protocolCaller})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-r.Ready():
			case <-ctx.Done():
				t.Fatal("not ready")
			}
			steer := "human follow-up FINISH\nattachment: /tmp/photo.png"
			if got := r.SteerContext(ctx, "clean-user-steer", steer, nil, "[Heimdall | from SyntheticSender] reaction refs"); got != harness.SteerAccepted {
				t.Fatalf("steer=%s", got)
			}
			result := r.Result()
			if result.Failure != "" {
				t.Fatal(result.Failure)
			}
			inputs := fakeharness.Inputs(state)
			if len(inputs) != 2 || inputs[0].Text != human || inputs[1].Text != steer {
				t.Fatalf("native user inputs: %+v", inputs)
			}
		})
	}
}

func TestNativeContextChangesAcrossWarmTurns(t *testing.T) {
	for _, name := range []string{"codex", "claude", "pi"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			s.Prompt = "same human words"
			s.Instructions = "transport A"
			s.Context = "first sender/history"
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
			s.Context = "second sender/history"
			second := pooledTurn(t, p, ctx, s)
			if second.Process() != first.Process() {
				t.Fatal("dynamic context discarded warm process")
			}
			inputs := fakeharness.Inputs(state)
			if len(inputs) != 2 || inputs[0].Text != s.Prompt || inputs[1].Text != s.Prompt || inputs[0].Context != "first sender/history" || inputs[1].Context != "second sender/history" {
				t.Fatalf("warm native inputs %+v", inputs)
			}
			s.Instructions = "transport B"
			third := pooledTurn(t, p, ctx, s)
			if third.Process() == first.Process() {
				t.Fatal("changed session notes reused stale process")
			}
			cancel()
			<-third.Exited()
		})
	}
}
