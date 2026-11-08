package scheduler

import (
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// This uses the real scheduler -> EndTurn -> saveCheckpoint outbox path,
// rather than manufacturing an ordinary scheduled Reply row.
func TestCheckpointAlertFeedsMatchingDeliveredContext(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"harness":"codex","prompt":"PRIVATE_JOB_INSTRUCTION","quiet":true,"checkpoint":true}`)
	r.run(id)
	r.finish(checkpointAnswer(0, true, r.now.UTC().Format(time.RFC3339Nano), []store.Alert{{Key: "item", Revision: "v1", Text: "Confirmed checkpoint alert"}}), Outcome{Harness: "codex"})
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil || len(cp.Receipts) != 1 {
		t.Fatalf("checkpoint %+v %v", cp, err)
	}
	input := core.Inbound{Conv: bossDM, From: boss, At: time.Now().Add(time.Second)}
	got, _, err := r.st.SentScheduledResults(input, store.DeliveryCursor{})
	if err != nil || len(got) != 0 {
		t.Fatalf("pending checkpoint alert exposed %+v %v", got, err)
	}
	if err := r.st.Sent(cp.Receipts[0].Outbound); err != nil {
		t.Fatal(err)
	}
	got, next, err := r.st.SentScheduledResults(input, store.DeliveryCursor{})
	if err != nil || len(got) != 1 || got[0].Text != "Confirmed checkpoint alert" {
		t.Fatalf("source checkpoint alert missing %+v %v", got, err)
	}
	for _, kind := range []string{"caller", "account", "installation", "platform alias", "target", "thread"} {
		t.Run(kind, func(t *testing.T) {
			other := input
			switch kind {
			case "caller":
				other.From = worker
			case "account":
				other.OAuthActor = "different-account"
			case "installation":
				other.OAuthInstallation = "different-installation"
			case "platform alias":
				other.From = core.Principal{Platform: core.PlatformSlack, ID: boss.ID}
			case "target":
				other.Conv.Chat = workDM.Chat
			case "thread":
				other.Conv.Thread = "other"
			}
			rows, _, err := r.st.SentScheduledResults(other, store.DeliveryCursor{})
			if err != nil || len(rows) != 0 {
				t.Fatalf("checkpoint crossed %s: %+v %v", kind, rows, err)
			}
		})
	}
	got, _, err = r.st.SentScheduledResults(input, next)
	if err != nil || len(got) != 0 {
		t.Fatal("source-produced receipt repeated", got, err)
	}
}
