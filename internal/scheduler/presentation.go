package scheduler

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// Literal escapes both legacy Telegram Markdown and the Slack adapter's
// Markdown input. Angle brackets are escaped by Slack itself, exactly once.
func literal(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			return ' '
		}
		return r
	}, s)
	return strings.NewReplacer("\\", "\\\\", "*", "\\*", "~", "\\~", "#", "\\#", "_", "\\_", "`", "\\`", "[", "\\[", "]", "\\]", "<", "\\<", ">", "\\>").Replace(s)
}
func (s *Scheduler) heading(c Caller, text string) string { return bold(c.Conv, text) }
func bold(k core.ConvKey, text string) string {
	mark := "**"
	if k.Interface == core.PlatformTelegram {
		mark = "*"
	}
	return mark + text + mark
}

// Formatted identifies only the trusted renderer's headings, not operator output.
func Formatted(text string) bool { return strings.HasPrefix(text, "*") }
func (s *Scheduler) name(c Caller, d Def) string {
	if !c.Private {
		return "Scheduled task"
	}
	return literal(d.Name)
}
func purpose(c Caller, d Def) string {
	if !c.Private {
		return "Run the saved task."
	}
	if d.Summary != "" {
		return literal(d.Summary)
	}
	if d.Kind == KindNotify {
		return "Send the saved reminder."
	}
	return "Run the saved instructions."
}
func (s *Scheduler) destination(c Caller, j store.Job, d Def) string {
	t, err := s.resolve(d.To, Caller{Principal: j.Creator, Conv: j.CreatedIn, Private: j.CreatedPrivate})
	if err != nil {
		return "Saved destination (currently unavailable)"
	}
	if t.AgentNet != "" {
		return "Configured AgentNet recipient; the agent delivers"
	}
	if t.Conv == c.Conv {
		if j.CreatedPrivate {
			if t.Conv.Thread != "" {
				return "This private conversation topic"
			}
			return "This private conversation"
		}
		return "This conversation"
	}
	if t.Private {
		platform := "Telegram"
		if t.Conv.Interface == core.PlatformSlack {
			platform = "Slack"
		}
		where := "Your private " + platform + " conversation"
		if t.Conv.Thread != "" {
			where += " topic"
		}
		return where
	}
	switch t.Conv.Interface {
	case core.PlatformTelegram:
		if t.Conv.Thread != "" {
			return "Configured Telegram chat topic"
		}
		return "Configured Telegram chat"
	case core.PlatformSlack:
		if t.Conv.Thread != "" {
			return "Configured Slack conversation thread"
		}
		return "Configured Slack conversation"
	}
	return "Saved destination"
}
func (s *Scheduler) status(c Caller, j store.Job, d Def) string {
	label := bold(c.Conv, "Status:") + " "
	switch {
	case j.State == store.JobDeleted:
		return label + "Removed. Future checks and pending retries cancelled; any current check is being stopped. Already delivered messages remain."
	case j.State == store.JobPaused || d.Paused:
		return label + "Paused; automatic checks are not active."
	case j.State == store.JobDone:
		return label + "Finished; automatic checks are not active."
	case j.State == store.JobDraft:
		return label + "Awaiting your approval; no change applied."
	default:
		text := label + "Active; scheduled checks are enabled."
		if !j.NextDue.IsZero() {
			text += "\n" + bold(c.Conv, "Next scheduled check:") + " " + literal(when(d, j.NextDue))
		}
		return text
	}
}
func (s *Scheduler) overview(c Caller, j store.Job, d Def) string {
	text := purpose(c, d) + "\n" + bold(c.Conv, "When:") + " " + literal(scheduleWords(d.Schedule)) + " · " + literal(d.TZ) + "\n" + bold(c.Conv, "Delivery:") + " " + literal(s.destination(c, j, d))
	if d.Until != "" {
		text += "\n" + bold(c.Conv, "Ends:") + " " + literal(d.Until)
	}
	if d.Quiet {
		text += "\nRoutine checks stay silent when there is no update."
	}
	return text
}
func repeatWarning(c Caller, d Def) string {
	if !d.RerunOnTimeout {
		return ""
	}
	return "\n" + bold(c.Conv, "Repeat-safe rerun:") + " Approving asserts all work and effects are safe to repeat. One full rerun after timeout, in a fresh session, within twice the timeout (" + literal(d.Timeout) + "). Work may repeat."
}
func (s *Scheduler) request(c Caller, j store.Job) string {
	if id := removalID(j); id != "" {
		orig, _, _ := s.st.Job(id)
		d, _, _ := parse(orig)
		return s.heading(c, "Remove scheduled task?") + "\n" + s.name(c, d) + "\n" + s.overview(c, orig, d) + "\nFuture checks and pending retries will stop; any current check will be stopped. Already delivered messages remain." + s.confirmAction(c)
	}
	d, _, _ := parse(j)
	title := "Set up scheduled task?"
	changes := ""
	display := j
	state := j.State
	if j.EditOf != "" {
		title = "Update scheduled task?"
		orig, _, _ := s.st.Job(j.EditOf)
		before, _, _ := parse(orig)
		state = orig.State
		display = orig
		changes = "\n" + s.material(c, orig, before, d)
	}
	status := s.heading(c, "Status:") + " Awaiting your approval; automatic checks will start after approval. Approval does not verify a successful test."
	if j.EditOf != "" {
		display.State = state
		status = s.status(c, display, d) + " Current settings stay until approval."
	}
	if d.Paused {
		status = s.heading(c, "Status:") + " Will be created paused; automatic checks will not be active. Approval does not verify a successful test."
	}
	return s.heading(c, title) + "\n" + s.name(c, d) + "\n" + s.overview(c, display, d) + changes + "\n" + status + repeatWarning(c, d) + s.confirmAction(c)
}
func (s *Scheduler) confirmAction(c Caller) string {
	return "\nReply " + bold(c.Conv, "Yes") + " to approve or " + bold(c.Conv, "No") + " to cancel. " + bold(c.Conv, "Details") + " shows full settings. Only you, here, within 24 hours."
}
func (s *Scheduler) result(c Caller, j store.Job, d Def, title string, before *Def) string {
	text := s.heading(c, title) + "\n" + s.name(c, d) + "\n" + s.overview(c, j, d)
	if before != nil {
		text += "\n" + s.material(c, j, *before, d)
	}
	return text + "\n" + s.status(c, j, d)
}
func (s *Scheduler) material(c Caller, j store.Job, a, b Def) string {
	var rows []string
	add := func(label, before, after string) {
		if before != after {
			rows = append(rows, bold(c.Conv, label+":")+" "+literal(before)+" → "+literal(after))
		}
	}
	if c.Private {
		add("Name", a.Name, b.Name)
		add("Purpose", or(a.Summary, "Saved instructions"), or(b.Summary, "Saved instructions"))
	}
	add("When", scheduleWords(a.Schedule)+" · "+a.TZ, scheduleWords(b.Schedule)+" · "+b.TZ)
	if mustJSON(a.Schedule) != mustJSON(b.Schedule) && scheduleWords(a.Schedule) == scheduleWords(b.Schedule) {
		rows = append(rows, "Schedule: changed (exact settings in Details).")
	}
	add("Ends", or(a.Until, "No expiry"), or(b.Until, "No expiry"))
	if a.Quiet != b.Quiet {
		rows = append(rows, fmt.Sprintf("Silent when no update: %t → %t", a.Quiet, b.Quiet))
	}
	add("Timeout", or(a.Timeout, "Configured limit"), or(b.Timeout, "Configured limit"))
	add("Delivery", s.destination(c, j, a), s.destination(c, j, b))
	if a.To != b.To && s.destination(c, j, a) == s.destination(c, j, b) {
		rows = append(rows, "Delivery destination: changed (see Details).")
	}
	if a.Prompt != b.Prompt || a.Text != b.Text {
		rows = append(rows, "Saved instructions or reminder: changed (see Details).")
	}
	if a.RerunOnTimeout != b.RerunOnTimeout {
		rows = append(rows, fmt.Sprintf("Repeat-safe timeout rerun: %t → %t", a.RerunOnTimeout, b.RerunOnTimeout))
	}
	// Retain material option visibility without dumping execution internals or JSON.
	aa, bb := a, b
	aa.Name, bb.Name, aa.Summary, bb.Summary, aa.Prompt, bb.Prompt, aa.Text, bb.Text, aa.To, bb.To, aa.TZ, bb.TZ = "", "", "", "", "", "", "", "", "", "", "", ""
	aa.Schedule, bb.Schedule = b.Schedule, b.Schedule
	aa.RerunOnTimeout, bb.RerunOnTimeout = false, false
	aa.Until, bb.Until, aa.Timeout, bb.Timeout = "", "", "", ""
	aa.Quiet, bb.Quiet = false, false
	if mustJSON(aa) != mustJSON(bb) {
		rows = append(rows, "Execution or delivery settings: changed (see Details).")
	}
	if len(rows) == 0 {
		return "No settings changed."
	}
	return strings.Join(rows, "\n")
}
