package durable

import (
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// MEL-855: Slack conversation work the owner answered first closes as
// "the Slack owner replied", never as a refusal; a genuine refusal still
// closes as before.
func TestSlackWorkOwnerRepliedClosesWithItsOwnLabel(t *testing.T) {
	fastWakes(t)
	a := &planAssistant{plan: map[string]WorkResult{
		"S-1/1/work/1": {Turn: 1, Held: "owner replied; skipped", Class: WakeOwnerReplied},
		"S-2/1/work/1": {Turn: 2, Held: "creator admission or role changed; review refused", Class: WakeRefused},
	}}
	e := open(t, t.TempDir(), Options{Assistant: a, Silence: -1})
	defer e.close()
	work := &core.SlackWork{Rule: "r", Watch: core.SlackWatch{Mode: core.SlackWatchAnswer}}
	for _, issue := range []string{"S-1", "S-2"} {
		if _, err := e.rt.Admit(Commitment{Issue: issue, Title: "t", Creator: "test:owner", Route: owner.Conv, Resource: defaultRes, SlackWork: work}); err != nil {
			t.Fatal(err)
		}
	}
	for issue, want := range map[string]string{"S-1": ownerRepliedClosed, "S-2": "none: conversation work stayed silent"} {
		waitFor(t, 20*time.Second, issue+" closed", func() bool {
			st, err := e.rt.State(issue)
			return err == nil && st.Phase == PhaseClosed
		})
		if st, _ := e.rt.State(issue); st.Next != want {
			t.Fatalf("%s closed with %q, want %q", issue, st.Next, want)
		}
	}
}
