package scheduler

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// Proposals from a trusted local launcher (owner, 2026-11-20; docs/
// external-origin.md): a launcher-vouched create, edit, removal or task_add
// is a draft with a card in the owner's private Heimdall chat. The draft
// row carries everything the Yes needs (the issue and title, or the job
// definition, and the launcher's verified answer in Origin), so the tap
// never asks the launcher again: the run has usually ended by then.

// KindTask marks a task proposal's draft row. It is never a runnable job:
// its definition holds only the proposed task.
const KindTask = "task"

// maxProposals is how many launcher proposals may wait per person.
const maxProposals = 5

type taskProposal struct {
	Task struct {
		Issue string `json:"issue"`
		Title string `json:"title"`
	} `json:"task"`
}

// taskOf is a task proposal draft's issue and title; ok false for jobs.
func taskOf(j store.Job) (issue, title string, ok bool) {
	if j.Kind != KindTask {
		return "", "", false
	}
	var p taskProposal
	if json.Unmarshal([]byte(j.Def), &p) != nil || p.Task.Issue == "" {
		return "", "", false
	}
	return p.Task.Issue, p.Task.Title, true
}

// vouchOf is the launcher answer stored with a proposal; nil for drafts
// from Heimdall chats.
func vouchOf(j store.Job) *Vouch {
	if j.Origin == "" {
		return nil
	}
	var v Vouch
	if json.Unmarshal([]byte(j.Origin), &v) != nil {
		return &Vouch{}
	}
	return &v
}

func (s *Scheduler) taskDraft(c Caller, issue, title string) store.Job {
	var p taskProposal
	p.Task.Issue, p.Task.Title = issue, title
	now := s.now()
	return store.Job{ID: newID(), Name: clip(title, 80), Kind: KindTask, Creator: c.Principal, CreatedIn: c.Conv, CreatedPrivate: c.Private, Def: mustJSON(p), State: store.JobDraft, DraftExpires: now.Add(draftTTL), Created: now, Origin: mustJSON(c.Vouch)}
}

// proposals are the person's launcher proposals still waiting.
func (s *Scheduler) proposals(p core.Principal) []store.Job {
	all, _ := s.st.Jobs(store.JobFilter{Creator: p.Key(), States: []string{store.JobDraft}})
	var out []store.Job
	for _, j := range all {
		if j.Origin != "" && j.DraftExpires.After(s.now()) {
			out = append(out, j)
		}
	}
	return out
}

// waitingTask reports whether the issue already waits for an answer.
func (s *Scheduler) waitingTask(p core.Principal, issue string) bool {
	for _, j := range s.proposals(p) {
		if i, _, ok := taskOf(j); ok && i == issue {
			return true
		}
	}
	return false
}

// proposalCap refuses a person's proposal beyond maxProposals waiting.
func (s *Scheduler) proposalCap(c Caller) string {
	if len(s.proposals(c.Principal)) >= maxProposals {
		return fmt.Sprintf("%d requests already wait for the owner's answer; nothing was sent. Try again after they answer.", maxProposals)
	}
	return ""
}

// asks is the card's first line: who asks, from the launcher's configured
// label and the verified author only (never request text).
func asks(k core.ConvKey, v *Vouch, what string) string {
	label := "an external launcher"
	if v != nil && v.Label != "" {
		label = literal(v.Label)
	}
	if v != nil && v.Author == "human" {
		return bold(k, "📱 You asked on "+label+" to "+what)
	}
	return bold(k, "🤖 Your agent on "+label+" wants to "+what+" for you")
}

// quoted is untrusted proposal text on a card: one line, trimmed, escaped,
// in quotes.
func quoted(text string, n int) string {
	return "“" + literal(clip(strings.Join(strings.Fields(text), " "), n)) + "”"
}

// taskCard is the plain card face (owner, 2026-11-20: "clearly, concisely
// and without tons of technical babble"): who asks, the job in one quoted
// line, where the done-report goes, Yes/No. IDs, run and person are only
// in Details. Until the launcher's contract names the channel the work
// started in, the report goes to this private chat.
func (s *Scheduler) taskCard(k core.ConvKey, j store.Job) string {
	_, title, _ := taskOf(j)
	return asks(k, vouchOf(j), "start a job") + "\n\n" + quoted(title, 160) + "\n\n" +
		"📬 When done, I'll tell you in: this private chat" +
		"\n\n👉 Reply " + bold(k, "Yes") + " to start it or " + bold(k, "No") + " to cancel. " + bold(k, "Details") + " shows the full request.\n— heimdall"
}

// showTask is a task proposal's Details: the raw request and provenance.
func (s *Scheduler) showTask(j store.Job) string {
	issue, title, _ := taskOf(j)
	v := vouchOf(j)
	if v == nil {
		v = &Vouch{}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Task proposal %s — waiting for your answer (nothing is tracked yet)\n", j.ID)
	fmt.Fprintf(&b, "Issue: %s\nTitle (untrusted, as sent): %s\n", issue, clip(title, 500))
	fmt.Fprintf(&b, "From: %s, author %s, person %s, run %s\n", v.Launcher, v.Author, v.Person, v.Run)
	fmt.Fprintf(&b, "Report: this private chat\nExpires: %s\n", j.DraftExpires.UTC().Format("2006-01-02 15:04 UTC"))
	fmt.Fprintf(&b, "Answer: !job confirm %s · !job discard %s", j.ID, j.ID)
	return b.String()
}

// confirmTask admits a task proposal on its creator's own Yes in the
// private chat it was sent to. It reads only the stored draft: no launcher
// is asked. Admission is idempotent by issue in durable.db, so a crash
// before the draft's drop below leaves a later Yes answering "already
// tracked", which also drops it. When admission fails the draft stays and
// the owner may answer again.
func (s *Scheduler) confirmTask(c Caller, j store.Job) (string, *store.JobChange) {
	issue, title, _ := taskOf(j)
	switch {
	case c.Role != core.RoleOwner || !c.Private || !j.CreatedPrivate:
		return "Only the owner can start this, in their private chat.", nil
	case s.Tasks == nil:
		return "Durable tasks are not enabled here; the request still waits.", nil
	case c.Principal.Platform == core.PlatformSlack && (c.OAuthActor == "" || c.OAuthInstallation == ""):
		return "Your Slack sign-in could not be verified for this message; nothing was started. Reply Yes again.", nil
	}
	// Shown in the task's wakes as "Registered through …": the title came
	// from outside Heimdall, the admission from the owner's own Yes.
	who := c
	who.Proposed = "the owner's Yes to an external proposal"
	if v := vouchOf(j); v != nil && v.Label != "" {
		who.Proposed = "the owner's Yes to an " + v.Label + " agent proposal"
		if v.Author == "human" {
			who.Proposed = "the owner's Yes to their own " + v.Label + " request"
		}
	}
	reply, changed := s.Tasks(who, "add", issue, title, "proposal:"+j.ID)
	drop := &store.JobChange{Drop: j.ID}
	switch {
	case changed:
		return "✅ Started: " + quoted(title, 160) + ". I'll tell you here when it's done.", drop
	case strings.Contains(reply, "already tracked elsewhere"):
		return "That job is already being worked on elsewhere; nothing new was started.", drop
	case strings.Contains(reply, "already tracked"):
		return "✅ That job is already being worked on; I'll tell you here when it's done.", drop
	}
	return "Not started: " + literal(clip(reply, 200)) + " The request still waits; reply Yes to try again or No to cancel.", nil
}

// expiredLine is the one line an unanswered proposal leaves when it
// expires, in the private chat it was sent to.
func (s *Scheduler) expiredLine(j store.Job) []core.Outbound {
	what := "Nothing was changed."
	name := literal(clip(j.Name, 80))
	if _, title, ok := taskOf(j); ok {
		what, name = "Nothing was started.", quoted(title, 120)
	}
	return []core.Outbound{{Conv: j.CreatedIn, Text: "⌛ A request waited 24 hours without an answer and expired: " + name + ". " + what + "\n— heimdall"}}
}
