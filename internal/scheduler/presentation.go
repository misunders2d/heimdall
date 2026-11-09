package scheduler

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/schedule"
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
		if strings.Contains(text, "\\") {
			// Legacy Markdown permits literal escapes only outside entities.
			var b strings.Builder
			for text != "" {
				span, rest, escaped := strings.Cut(text, "\\")
				if strings.TrimSpace(span) == "" {
					b.WriteString(span)
				} else {
					b.WriteString(mark + span + mark)
				}
				if escaped {
					b.WriteByte('\\')
					if rest != "" {
						b.WriteByte(rest[0])
						rest = rest[1:]
					}
				}
				text = rest
			}
			return b.String()
		}
	} else if k.Interface == core.PlatformSlack {
		// The Slack converter escapes a bold span as a whole. Match its
		// literal glyphs here so names stay literal without visible escapes.
		text = strings.NewReplacer("\\\\", "\\", "\\*", "∗", "\\_", "＿", "\\`", "ˋ", "\\~", "∼", "\\#", "#", "\\[", "[", "\\]", "]", "\\<", "<", "\\>", ">").Replace(text)
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
func (s *Scheduler) taskTitle(c Caller, action string, d Def) string {
	text := s.heading(c, action) + "\n\n" + bold(c.Conv, s.name(c, d))
	if c.Private && d.Summary != "" && !strings.EqualFold(d.Summary, d.Name) {
		text += "\n" + literal(d.Summary)
	}
	return text
}
func (s *Scheduler) destination(c Caller, j store.Job, d Def) string {
	t, err := s.resolve(d.To, Caller{Principal: j.Creator, Conv: j.CreatedIn, Private: j.CreatedPrivate})
	if err != nil {
		return "Saved destination unavailable"
	}
	if t.AgentNet != "" {
		return "AgentNet recipient; agent delivers"
	}
	if t.Conv == c.Conv {
		if c.External != "" && j.CreatedPrivate {
			platform := "Heimdall"
			switch t.Conv.Interface {
			case core.PlatformSlack:
				platform = "Slack"
			case core.PlatformTelegram:
				platform = "Telegram"
			}
			where := "Your private " + platform + " conversation"
			if t.Conv.Thread != "" {
				where += " topic"
			}
			return where
		}
		if j.CreatedPrivate {
			if t.Conv.Thread != "" {
				return "This private conversation topic"
			}
			return "This private conversation"
		}
		return "This conversation"
	}
	// Cross-platform targets have no verified display name or DM alias here.
	// A positive Telegram ID alone is not evidence of a private destination.
	switch t.Conv.Interface {
	case core.PlatformTelegram:
		if t.Conv.Thread != "" {
			return "Telegram topic"
		}
		return "Telegram"
	case core.PlatformSlack:
		if t.Conv.Thread != "" {
			return "Slack thread"
		}
		return "Slack"
	}
	return "Saved destination"
}
func (s *Scheduler) status(c Caller, j store.Job, d Def) string {
	switch {
	case j.State == store.JobDeleted:
		return bold(c.Conv, "🗑️ Removed") + ".\nFuture checks and pending retries cancelled.\nAny current check is being stopped.\nAlready delivered messages remain."
	case j.State == store.JobPaused || d.Paused:
		return bold(c.Conv, "⏸️ Paused") + "; no automatic checks."
	case j.State == store.JobDone:
		return bold(c.Conv, "✅ Finished") + "; no automatic checks."
	case j.State == store.JobDraft:
		return bold(c.Conv, "📝 Awaiting your approval") + "; no change applied."
	default:
		text := bold(c.Conv, "🟢 Active") + "; scheduled checks enabled."
		if !j.NextDue.IsZero() {
			text = bold(c.Conv, "🟢 Active") + ".\n" + bold(c.Conv, "Next scheduled check:") + " " + literal(shortWhen(d, j.NextDue))
		}
		return text
	}
}
func (s *Scheduler) overview(c Caller, j store.Job, d Def, before *Def) string {
	when := scheduleWords(d.Schedule) + " · " + d.TZ
	to := s.destination(c, j, d)
	if before != nil {
		if mustJSON(before.Schedule) != mustJSON(d.Schedule) || before.TZ != d.TZ {
			old := scheduleWords(before.Schedule) + " · " + before.TZ
			if old == when {
				when += " (changed; exact settings in Details)"
			} else {
				when = old + " → " + when
			}
		}
		if before.To != d.To {
			old := s.destination(c, j, *before)
			if old == to {
				to += " (destination changed; see Details)"
			} else {
				to = old + " → " + to
			}
		}
	}
	return bold(c.Conv, "🕒 When:") + " " + literal(when) + "\n" + bold(c.Conv, "📬 Delivery:") + " " + literal(to)
}
func repeatWarning(c Caller, d Def) string {
	if !d.RerunOnTimeout {
		return ""
	}
	return "\n\n" + bold(c.Conv, "⚠️ Repeat-safe rerun:") + " Approving asserts all work and effects are safe to repeat. One full rerun after timeout (" + literal(d.Timeout) + "), fresh session, within twice the timeout."
}
func (s *Scheduler) request(c Caller, j store.Job) string {
	if id := removalID(j); id != "" {
		orig, _, _ := s.st.Job(id)
		d, _, _ := parse(orig)
		return s.taskTitle(c, "🗑️ Remove scheduled task?", d) + "\n\n" + s.overview(c, orig, d, nil) + "\n\nFuture checks and pending retries will stop.\nAny current check will be stopped.\nAlready delivered messages remain." + s.confirmAction(c)
	}
	d, _, _ := parse(j)
	title := "📅 Schedule task?"
	changes := ""
	display := j
	state := ""
	var before *Def
	if j.EditOf != "" {
		title = "📝 Update scheduled task?"
		orig, _, _ := s.st.Job(j.EditOf)
		old, _, _ := parse(orig)
		before = &old
		display = orig
		// Schedule and destination changes are already shown in this one diff.
		changes = s.material(c, old, d)
		if orig.State == store.JobPaused {
			state = bold(c.Conv, "⏸️ Still paused") + ". No automatic checks."
		}
	}
	if d.Paused {
		state = bold(c.Conv, "⏸️ Starts paused") + ". No automatic checks."
	}
	body := s.overview(c, display, d, before)
	if changes != "" {
		body += "\n\n" + bold(c.Conv, "📋 Changes") + "\n" + changes
	}
	if state != "" {
		body += "\n\n" + state
	}
	return s.taskTitle(c, title, d) + "\n\n" + body + repeatWarning(c, d) + s.confirmAction(c)
}
func (s *Scheduler) confirmAction(c Caller) string {
	return "\n\n👉 Reply " + bold(c.Conv, "Yes") + " to apply or " + bold(c.Conv, "No") + " to cancel. " + bold(c.Conv, "Details") + " shows full settings."
}
func (s *Scheduler) result(c Caller, j store.Job, d Def, title string, before *Def) string {
	icon := "📅"
	switch title {
	case "Updated":
		icon = "📝"
	case "Paused":
		icon = "⏸️"
	case "Resumed":
		icon = "🟢"
	case "Removed":
		icon = "🗑️"
	case "Status":
		icon = "📋"
	}
	text := s.taskTitle(c, icon+" Scheduled task "+strings.ToLower(title), d) + "\n\n" + s.overview(c, j, d, before)
	if before != nil {
		if changes := s.material(c, *before, d); changes != "" {
			text += "\n\n" + bold(c.Conv, "📋 Changes") + "\n" + changes
		}
	} else if title == "Renewed" {
		if until, err := time.Parse(time.RFC3339, d.Until); err == nil {
			text += "\n" + bold(c.Conv, "Ends:") + " " + literal(shortWhen(d, until))
		}
	}
	return text + "\n\n" + s.status(c, j, d)
}
func (s *Scheduler) material(c Caller, a, b Def) string {
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
	add("Ends", or(a.Until, "No expiry"), or(b.Until, "No expiry"))
	if a.Quiet != b.Quiet {
		rows = append(rows, fmt.Sprintf("Silent when no update: %t → %t", a.Quiet, b.Quiet))
	}
	add("Timeout", or(a.Timeout, "Configured limit"), or(b.Timeout, "Configured limit"))
	if a.Prompt != b.Prompt || a.Text != b.Text {
		label := "Instructions updated."
		if a.Prompt == b.Prompt {
			label = "Reminder text updated."
		}
		rows = append(rows, label+" Details shows the full text.")
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
	return strings.Join(rows, "\n")
}

// Human receipts use one local time. Operator Details retain UTC as well.
func shortWhen(d Def, t time.Time) string {
	loc, err := schedule.Zone(d.TZ)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format("2 Jan 2006 at 15:04") + " · " + d.TZ
}
