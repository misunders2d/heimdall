package router

import (
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/store"
)

// Item 7: an owner's job proposal applies at once only in the owner's
// private chat. In a group, other people's text reached the model, so the
// same proposal waits for the owner's confirmation card.
func TestOwnerGroupProposalNeedsConfirmation(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	block := "```heimdall-job\n{\"name\":\"grouped\",\"schedule\":{\"every\":\"1h\"},\"prompt\":\"read\"}\n```"
	card := e.ask(group, say(block))
	if !strings.Contains(card.Text, "Reply *Yes*") {
		t.Fatalf("group proposal card %q, want a confirmation", card.Text)
	}
	if jobs, _ := e.st.Jobs(store.JobFilter{States: []string{store.JobActive}}); len(jobs) != 0 {
		t.Fatalf("group proposal applied without confirmation: %+v", jobs)
	}
	if drafts, _ := e.st.Jobs(store.JobFilter{States: []string{store.JobDraft}}); len(drafts) != 1 {
		t.Fatalf("drafts = %+v, want the group proposal waiting", drafts)
	}
	// The private chat keeps direct apply.
	out := e.ask(ownerDM, say("```heimdall-job\n{\"name\":\"direct\",\"schedule\":{\"every\":\"1h\"},\"prompt\":\"read\"}\n```"))
	want(t, out, "*🟢 Active*.")
	if jobs, _ := e.st.Jobs(store.JobFilter{States: []string{store.JobActive}}); len(jobs) != 1 || jobs[0].Name != "direct" {
		t.Fatalf("active = %+v, want only the private proposal", jobs)
	}
}
