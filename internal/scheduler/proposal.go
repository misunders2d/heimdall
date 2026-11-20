package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

// Proposals from a trusted local launcher (owner, 2026-11-20; docs/
// external-origin.md): a launcher-vouched create, edit, removal or task_add
// is a draft with a card in the owner's private Heimdall chat. The draft
// row carries everything the Yes needs (the issue and title, or the job
// definition, and the launcher's verified answer in Origin), so the owner's
// answer never asks the launcher again: the run has usually ended by then.

// KindTask marks a task proposal's draft row. It is never a runnable job:
// its definition holds only the proposed task.
const KindTask = "task"

// maxProposals is how many launcher proposals may wait per person.
const maxProposals = 5

// maxTitle is the longest proposed task title, in characters: the card
// shows all of it, so what the owner approves is exactly what is admitted.
const maxTitle = 160

type taskProposal struct {
	Task struct {
		Issue string `json:"issue"`
		Title string `json:"title"`
	} `json:"task"`
}

// proposalTitle is a proposed task title as it is stored, shown and
// admitted: one line of visible text. A title with control, format
// (invisible, bidi, tag), private-use or unassigned characters, or longer
// than the card shows, is refused rather than altered, so the card never
// hides part of what the Yes admits.
func proposalTitle(raw string) (string, error) {
	title := strings.Join(strings.Fields(raw), " ")
	if strings.ContainsFunc(title, func(r rune) bool { return r != ' ' && !unicode.IsGraphic(r) }) {
		return "", errors.New("the title has hidden or control characters; send it as plain visible text")
	}
	if n := len([]rune(title)); n > maxTitle {
		return "", fmt.Errorf("the title is %d characters; send one line of at most %d", n, maxTitle)
	}
	return title, nil
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
	return store.Job{ID: newID(), Name: title, Kind: KindTask, Creator: c.Principal, CreatedIn: c.Conv, CreatedPrivate: c.Private, Def: mustJSON(p), State: store.JobDraft, DraftExpires: now.Add(draftTTL), Created: now, Origin: mustJSON(c.Vouch)}
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

// stillWaiting reports whether draft id is a proposal that still waits.
func (s *Scheduler) stillWaiting(id string) bool {
	j, ok, err := s.st.Job(id)
	return err == nil && ok && j.State == store.JobDraft && j.DraftExpires.After(s.now())
}

// proposalCap refuses a person's proposal beyond maxProposals waiting.
func (s *Scheduler) proposalCap(c Caller) string {
	if len(s.proposals(c.Principal)) >= maxProposals {
		return fmt.Sprintf("%d requests already wait for the owner's answer; nothing was sent. Try again after they answer.", maxProposals)
	}
	return ""
}

// cardIsLatest reports whether the newest launcher proposal among drafts
// still has the latest word in its chat: no turn ran or runs there and
// nothing unprompted was delivered after its card. A store error counts
// as no: the word is then an ordinary message.
func (s *Scheduler) cardIsLatest(drafts []store.Job) bool {
	var newest *store.Job
	for i := range drafts {
		if j := &drafts[i]; j.Origin != "" && (newest == nil || j.Updated.After(newest.Updated)) {
			newest = j
		}
	}
	if newest == nil {
		return false
	}
	ok, err := s.st.CardIsLatest(newest.ID)
	return err == nil && ok
}

// revoked is why a stored proposal may no longer be answered Yes: its
// launcher is no longer configured, or it is an agent's and the launcher's
// agent_proposals opt-in is now off (config changes take effect on
// restart). "" when it may.
func (s *Scheduler) revoked(j store.Job) string {
	v := vouchOf(j)
	if v == nil {
		return ""
	}
	i := slices.IndexFunc(s.External, func(o ExternalOrigin) bool { return o.Name == v.Launcher })
	switch {
	case v.Launcher == "" || i < 0:
		return "no longer configured"
	case v.Author != "human" && !s.External[i].Agents:
		return "no longer allowed to send agent requests"
	}
	return ""
}

func label(v *Vouch) string {
	if v != nil && v.Label != "" {
		return v.Label
	}
	return "an outside launcher"
}

// asks is the card's first line: who asks, from the launcher's configured
// label and the verified author only (never request text). The v1 answer
// names no agent or device, so an agent is "one of your agents".
func asks(k core.ConvKey, v *Vouch, task bool) string {
	l := literal(label(v))
	switch {
	case v != nil && v.Author == "human" && task:
		return bold(k, "🧑 You asked through "+l+" to start a job")
	case v != nil && v.Author == "human":
		return bold(k, "🧑 You asked through "+l+":")
	case task:
		return bold(k, "🤖 One of your agents on "+l+" wants to start a job for you")
	}
	return bold(k, "🤖 One of your agents on "+l+" asks for this change:")
}

// replyHow is the card's answer line; on Slack the card sits in the DM's
// registered thread and only a reply there answers it.
func replyHow(k core.ConvKey, yes, no string) string {
	where := ""
	if k.Interface == core.PlatformSlack && k.Thread != "" {
		where = " in this thread"
	}
	return "👉 Reply " + bold(k, "Yes") + where + " to " + yes + " or " + bold(k, "No") + " to " + no + ". " + bold(k, "Details") + " shows the full request.\nIf other messages come first, send !jobs to answer it."
}

// quoted is untrusted proposal text on a card (Markdown): one line,
// trimmed, escaped, in quotes.
func quoted(text string, n int) string { return "“" + literal(plainLine(text, n)) + "”" }

// plainQuoted is the same for a plain-text line (sent unformatted, so not
// escaped: escapes would show).
func plainQuoted(text string, n int) string { return "“" + plainLine(text, n) + "”" }

// plainLine is text as one trimmed line without control or format
// characters.
func plainLine(text string, n int) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == ' ' || r == ' ' {
			return ' '
		}
		return r
	}, text)
	return clip(strings.Join(strings.Fields(text), " "), n)
}

// taskCard is the plain card face (owner, 2026-11-20: "clearly, concisely
// and without tons of technical babble"): who asks, the job in one quoted
// line, where the done-report goes, Yes/No. IDs, run and person are only
// in Details. Until the launcher's contract names the channel the work
// started in, the report goes to this private chat.
func (s *Scheduler) taskCard(k core.ConvKey, j store.Job) string {
	_, title, _ := taskOf(j)
	return asks(k, vouchOf(j), true) + "\n\n" + quoted(title, maxTitle) + "\n\n" +
		"📬 When done, I'll tell you in: this private chat" +
		"\n\n" + replyHow(k, "start it", "skip it") + "\n— heimdall"
}

// zoned is t in the scheduler's default zone, as the other cards show.
func (s *Scheduler) zoned(t time.Time) string {
	loc, err := schedule.Zone(s.cfg.DefaultTZ)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format("2 Jan 2006 at 15:04 MST")
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
	fmt.Fprintf(&b, "Issue: %s\nTitle (untrusted, as sent): %s\n", issue, plainLine(title, maxTitle))
	b.WriteString("The assistant works from the Linear issue's own text, not only this title: check the issue if in doubt.\n")
	fmt.Fprintf(&b, "From: %s (%s), author %s, person %s, run %s\n", label(v), v.Launcher, v.Author, v.Person, v.Run)
	fmt.Fprintf(&b, "Report: this private chat\nExpires: %s\n", s.zoned(j.DraftExpires))
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
	v := vouchOf(j)
	who := c
	who.Proposed = "the owner's Yes to an agent proposal from " + label(v)
	if v != nil && v.Author == "human" {
		who.Proposed = "the owner's Yes to their own request through " + label(v)
	}
	reply, changed := s.Tasks(who, "add", issue, title, "proposal:"+j.ID)
	drop := &store.JobChange{Drop: j.ID}
	switch {
	case changed:
		return "✅ Started " + plainQuoted(title, maxTitle) + " (" + issue + "). I'll tell you here when it's done.", drop
	case strings.Contains(reply, "already tracked elsewhere"):
		return "That job (" + issue + ") is already being worked on elsewhere; nothing new was started.", drop
	case strings.Contains(reply, "already tracked"):
		return "✅ That job (" + issue + ") is already being worked on; I'll tell you here when it's done.", drop
	}
	// The durable reply may carry technical text: the owner gets a fixed
	// line, the service log the metadata.
	s.log.Printf("scheduler: proposal %s not admitted", j.ID)
	return "Couldn't start " + plainQuoted(title, maxTitle) + ". It still waits: reply Yes to try again or No to skip it.", nil
}

// discarded is the line a No leaves on a launcher proposal (plain text).
func discarded(j store.Job) string {
	if _, title, ok := taskOf(j); ok {
		return "Cancelled: " + plainQuoted(title, maxTitle) + ". Nothing was started."
	}
	return "Cancelled: " + plainQuoted(j.Name, 80) + ". Nothing was changed."
}

// revokedLine answers a Yes to a proposal whose launcher lost its rights.
func revokedLine(j store.Job, why string) string {
	what := "Nothing was changed."
	if _, _, ok := taskOf(j); ok {
		what = "Nothing was started."
	}
	return "This request came through " + label(vouchOf(j)) + ", which is " + why + ". " + what
}

// expiredLine is the one line an unanswered (or revoked) proposal leaves
// when it is dropped, in the private chat it was sent to.
func (s *Scheduler) expiredLine(j store.Job, why string) []core.Outbound {
	// Sent unformatted (plain text): nothing is escaped.
	what, name := "Nothing was changed.", plainQuoted(j.Name, 80)
	if _, title, ok := taskOf(j); ok {
		what, name = "Nothing was started.", plainQuoted(title, maxTitle)
	}
	whose := "Your agent's request"
	if v := vouchOf(j); v != nil && v.Author == "human" {
		whose = "Your request"
	}
	text := "⌛ " + whose + " expired after 24 hours with no answer: " + name + ". " + what
	if why != "" {
		text = "🚫 " + whose + " " + name + " was withdrawn: " + label(vouchOf(j)) + " is " + why + ". " + what
	}
	return []core.Outbound{{Conv: j.CreatedIn, Text: text + "\n— heimdall"}}
}
