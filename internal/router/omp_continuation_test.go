package router

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

func TestOMPBackgroundSettleUnblocksNextPrompt(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		wantOutput   bool
	}{
		{"answer", "AUTONOMOUS", true},
		{"silent", "AUTONOMOUS_SILENT", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
			e.stop()
			stopOnce.Delete(e.st)
			e.platform = "slack"
			e.opt.Accounts = warmAccounts{e.opt.Accounts}
			e.opt.Harness = harness.WarmConfig{RunTimeoutSeconds: 8}
			e.start()

			who := core.Principal{Platform: "fake", ID: "owner"}
			thread := core.ConvKey{Interface: "slack", Chat: "D1", Thread: "root"}
			first := e.sendIn(who, thread, tc.prompt)
			want(t, e.reply(), "turn=1")
			eventuallyTrue(t, "detached turn persisted", func() bool {
				return countTurns(t, e, "chat='D1' AND state='running'") == 1
			})

			next := e.sendIn(who, thread, "next user prompt OMP_LATE_SETTLE")
			eventuallyTrue(t, "next prompt waiting for the warm worker", func() bool {
				return len(e.r.warm.Status(thread)) == 0 &&
					countTurns(t, e, "chat='D1' AND state='running'") == 2
			})

			if err := os.WriteFile(filepath.Join(e.state, "release-autonomous"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case reply := <-e.f.out:
				if reply.ReplyTo != first {
					t.Fatalf("detached reply addressed to %s, want %s", reply.ReplyTo, first)
				}
				if tc.wantOutput {
					want(t, reply, "AUTONOMOUS_ANSWER")
				} else if reply.Text != "" || reply.File != nil || len(reply.Reactions) != 0 {
					// Slack uses the empty delivery to clear the turn's status.
					t.Fatalf("silent settle produced chat output: %+v", reply)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("OMP did not complete the detached turn before the native wait limit")
			}

			// The fake sends a duplicate settle after the background result,
			// then a late settle after the next prompt's agent_end. Hold that
			// prompt's own terminal frame so a premature finish is observable.
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(e.state, "omp-late-settle-sent")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("OMP settle did not start the queued prompt before the native wait limit")
				}
				time.Sleep(5 * time.Millisecond)
			}
			e.noReply(150 * time.Millisecond)
			if err := os.WriteFile(filepath.Join(e.state, "release-prompt-result"), nil, 0600); err != nil {
				t.Fatal(err)
			}

			reply := e.reply()
			if reply.ReplyTo != next {
				t.Fatalf("next reply addressed to %s, want %s", reply.ReplyTo, next)
			}
			want(t, reply, "turn=2", "next user prompt")
			if done := countTurns(t, e, "chat='D1' AND state='done'"); done != 3 {
				t.Fatalf("completed turn count = %d, want 3", done)
			}
			assertNoRunningNative(t, e)
		})
	}
}
