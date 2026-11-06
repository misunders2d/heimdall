package scheduler

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

// HelpLines are the job commands, for !help.
var HelpLines = []string{
	"!jobs [all|details] — status; operator list; pending request controls",
	"!job show|history <id> — a job's schedule, next runs and options; its last runs",
	"!job add <json> / !job edit <id> <json> — create (active, or paused with \"paused\":true) or change a job (docs/scheduler.md)",
	"!job confirm|discard <id> — accept or drop a draft (or reply yes / no)",
	"!job pause|resume|run|stop|rm <id> — pause, resume, run now, stop the running run, delete",
	"!job renew <id> [duration] — extend its expiry (default 7d)",
}

const draftTTL = 24 * time.Hour

// Note tells a harness, in the first message of each native session, how to
// propose a job: Heimdall asks the person to confirm it.
const Note = `[Heimdall jobs: to propose a scheduled job or reminder, end your answer with one fenced block ` + "```heimdall-job" + ` holding JSON: {"name","schedule":{"cron":["0 9 * * MON"]} or {"every":"15m"} or {"at":"YYYY-MM-DDTHH:MM"},"tz","to":"here","summary":"short purpose, no backstory","prompt"} (or "kind":"notify" with "text"). To change a job: {"edit":"<id>", changed fields}. To remove a job: {"remove":"<id>"}. Removal also requires human confirmation. Heimdall shows a card and the person confirms; you cannot.]`

// OwnerNote describes the direct owner request path; risky changes still ask.
const OwnerNote = `[Heimdall jobs: propose only when the person asked for a job or reminder. End your answer with one fenced block ` + "```heimdall-job" + ` holding JSON: {"name","schedule":{"cron":["0 9 * * MON"]} or {"every":"15m"} or {"at":"YYYY-MM-DDTHH:MM"},"tz","to":"here","summary":"short purpose, no backstory","prompt"} (or "kind":"notify" with "text"). To edit: {"edit":"<id>", changed fields}. Heimdall applies new jobs and edits at once. Removal ({"remove":"<id>"}) and any job with rerun_on_timeout enabled still ask for human confirmation.]`

var blockRE = regexp.MustCompile("(?ms)^[ \t]*```heimdall-job[ \t]*\r?\n(.*?)^[ \t]*```[ \t]*$\n?")

// cutBlocks takes job blocks out of an answer.
func cutBlocks(text string) (string, []string) {
	var blocks []string
	for _, m := range blockRE.FindAllStringSubmatch(text, -1) {
		blocks = append(blocks, m[1])
	}
	return strings.TrimSpace(blockRE.ReplaceAllString(text, "")), blocks
}

// yes and no are whole replies that confirm or discard the caller's one
// draft in this conversation (whole reply, any case, trailing punctuation ignored).
var (
	yes = []string{"yes", "ok", "confirm", "да", "так"}
	no  = []string{"no", "cancel", "нет", "ні"}
)

// Answer handles a bare yes or no while the caller has drafts waiting in
// this conversation; handled false otherwise (the text is then an ordinary
// message, or cancel's usual command). The change is stored with the reply.
func (s *Scheduler) Answer(c Caller, text string) (reply string, change *store.JobChange, handled bool) {
	// A question is not consent to confirm or discard a draft.
	if strings.HasSuffix(strings.TrimSpace(text), "?") {
		return "", nil, false
	}
	w := strings.ToLower(strings.TrimRightFunc(strings.TrimSpace(text), func(r rune) bool {
		return unicode.IsPunct(r) || unicode.IsSpace(r)
	}))
	confirm, discard := slices.Contains(yes, w), slices.Contains(no, w)
	details := strings.EqualFold(strings.TrimSpace(text), "details")
	if !confirm && !discard && !details {
		return "", nil, false
	}
	drafts := s.drafts(c)
	switch {
	case len(drafts) == 0:
		return "", nil, false
	case len(drafts) > 1:
		return "Several requests wait here. Open !jobs details to choose one; no request was changed.", nil, true
	case details:
		j := drafts[0]
		if id := removalID(j); id != "" {
			return s.show(c, id), nil, true
		}
		return s.show(c, j.ID), nil, true
	case confirm:
		reply, change = s.confirm(c, drafts[0].ID)
	default:
		reply, change = s.discard(c, drafts[0].ID)
	}
	return reply, change, true
}

func (s *Scheduler) drafts(c Caller) []store.Job {
	all, _ := s.st.Jobs(store.JobFilter{Creator: c.Principal.Key(), States: []string{store.JobDraft}, Conv: c.Conv})
	now := s.now()
	return slices.DeleteFunc(all, func(j store.Job) bool { return !j.DraftExpires.After(now) })
}

// Draft takes job blocks out of the answers of c's turn. A valid block is
// stored as a draft (by the caller, with the turn) and a card asks c to
// confirm; an invalid one gets a card naming the error. Without a block the
// answers are returned as they are.
func (s *Scheduler) Draft(c Caller, answers []string) (clean []string, draft *store.Job, card string) {
	var blocks []string
	for _, a := range answers {
		a, b := cutBlocks(a)
		clean, blocks = append(clean, a), append(blocks, b...)
	}
	switch len(blocks) {
	case 0:
		return answers, nil, ""
	case 1:
	default:
		return clean, nil, "⚠ The answer proposed more than one job, so none was kept. Ask for one job at a time.\n— heimdall"
	}
	j, err := s.proposal(c, []byte(blocks[0]))
	if err != nil {
		return clean, nil, "⚠ The proposed job is not valid, so nothing was stored: " + err.Error() + "\n— heimdall"
	}
	return clean, &j, s.card(c, j)
}

// Prepare keeps confirmation for employees, removals and repeat-safe assertions.
// The caller persists both draft and change in the reply's transaction.
func (s *Scheduler) Prepare(c Caller, answers []string, direct bool) ([]string, *store.Job, *store.JobChange, string) {
	clean, j, notice := s.Draft(c, answers)
	if j == nil {
		return clean, nil, nil, strings.ReplaceAll(notice, "\n", " ")
	}
	if removalID(*j) != "" {
		return clean, j, nil, notice
	}
	if j.EditOf == "" {
		if msg := s.capped(c); msg != "" {
			return clean, nil, nil, "⚠ " + msg + " — heimdall"
		}
	}
	d, _, _ := parse(*j)
	if !direct || c.Role != core.RoleOwner || d.RerunOnTimeout {
		return clean, j, nil, notice
	}
	msg, change := s.confirmJob(c, *j)
	if change == nil {
		return clean, nil, nil, "⚠ " + strings.ReplaceAll(msg, "\n", " ") + " — heimdall"
	}
	return clean, j, change, msg + "\n— heimdall"
}

// removal is stored in a draft's definition; it never becomes a runnable job.
type removal struct {
	Remove string `json:"remove"`
}

func removalID(j store.Job) string {
	var r removal
	_ = json.Unmarshal([]byte(j.Def), &r)
	return r.Remove
}

// proposal validates a block's definition, creation, edit or removal, as a draft.
func (s *Scheduler) proposal(c Caller, raw []byte) (store.Job, error) {
	var head map[string]json.RawMessage
	if err := json.Unmarshal(raw, &head); err != nil {
		return store.Job{}, fmt.Errorf("job definition: %v", err)
	}
	now := s.now()
	j := store.Job{ID: newID(), Creator: c.Principal, CreatedIn: c.Conv, CreatedPrivate: c.Private, State: store.JobDraft, DraftExpires: now.Add(draftTTL), Created: now}
	if rawID, ok := head["remove"]; ok {
		var id string
		if len(head) != 1 || json.Unmarshal(rawID, &id) != nil || strings.TrimSpace(id) == "" {
			return j, errors.New(`removal requires only {"remove":"<job id>"}`)
		}
		orig, err := s.own(c, id)
		if err != nil {
			return j, err
		}
		j.Name, j.Kind, j.Def = orig.Name, orig.Kind, mustJSON(removal{Remove: id})
		return j, nil
	}
	var d Def
	if edit, ok := head["edit"]; ok {
		var id string
		_ = json.Unmarshal(edit, &id)
		orig, err := s.own(c, id)
		if err != nil {
			return j, err
		}
		od, _, _ := parse(orig)
		if d, err = merge(od, head); err != nil {
			return j, err
		}
		if _, err := s.validate(&d, s.creator(orig, c), orig.Created, now); err != nil {
			return j, err
		}
		j.EditOf = orig.ID
	} else {
		if err := decode(raw, &d); err != nil {
			return j, err
		}
		if _, err := s.validate(&d, c, now, now); err != nil {
			return j, err
		}
	}
	def, _ := json.Marshal(d)
	j.Name, j.Kind, j.Def = d.Name, d.Kind, string(def)
	return j, nil
}

// validate is check plus what only creating or changing a job needs: a
// future run, and an expiry still ahead. A due run is never refused for
// those (its slot is past by then).
func (s *Scheduler) validate(d *Def, c Caller, anchor, now time.Time) (*schedule.Schedule, error) {
	_, sched, err := s.check(d, c, anchor, now)
	if err != nil {
		return nil, err
	}
	if _, ok := sched.Next(now); !ok {
		return nil, errors.New("schedule: no future run (a one-off in the past?)")
	}
	if until, err := time.Parse(time.RFC3339, d.Until); err == nil && !until.After(now) {
		return nil, errors.New(untilUsage)
	}
	return sched, nil
}

// creator is the caller whose role rules a job keeps: its creator, with
// the role it has now.
func (s *Scheduler) creator(j store.Job, asking Caller) Caller {
	c := Caller{Principal: j.Creator, Conv: j.CreatedIn, Private: j.CreatedPrivate, Role: asking.Role}
	if j.Creator.Key() != asking.Principal.Key() {
		c.Role, _, _ = s.access.Decide(j.Creator, j.CreatedIn)
	}
	return c
}

// card uses fixed templates; full operator settings stay behind Details.
func (s *Scheduler) card(c Caller, j store.Job) string {
	return s.request(c, j) + "\n— heimdall"
}

// describe writes a job's schedule, next fires, target and content. Cards
// preview the content everywhere (§5.3); other replies show it in full only
// in private chats (§4).
func (s *Scheduler) describe(b *strings.Builder, c Caller, j store.Job, d Def, sched *schedule.Schedule, card bool) {
	fmt.Fprintf(b, "Kind: %s, harness %s\n", d.Kind, or(d.Harness, "default order"))
	if d.RerunOnTimeout {
		fmt.Fprintf(b, "Repeat-safe timeout rerun: ENABLED. Confirming asserts the whole prompt and its effects are safe to repeat (idempotent). One rerun, fresh session; timeout %s; rerun deadline twice timeout from due time.\n", d.Timeout)
	}
	fmt.Fprintf(b, "Schedule: %s in %s\n", scheduleWords(d.Schedule), d.TZ)
	if len(d.Schedule.Cron) > 0 {
		fmt.Fprintf(b, "Cron: %s\n", strings.Join(d.Schedule.Cron, "; "))
	}
	if sched != nil && j.State != store.JobPaused && !d.Paused {
		var fires []string
		n := 3
		if card {
			n = 1
		}
		for _, f := range sched.Fires(s.now(), n) {
			fires = append(fires, when(d, f.At))
		}
		fmt.Fprintf(b, "Next: %s\n", or(strings.Join(fires, "; "), "none"))
		if w := sched.Warning(s.now()); w != "" {
			fmt.Fprintf(b, "Note: %s\n", w)
		}
	}
	fmt.Fprintf(b, "To: %s (created in %s)\n", d.To, j.CreatedIn)
	body, label := d.Prompt, "Prompt"
	if d.Kind == KindNotify {
		body, label = d.Text, "Text"
	}
	switch {
	case !card && !c.Private:
		body = "(shown only in private chats)"
	case card && len([]rune(body)) > 200:
		body = string([]rune(body)[:200]) + "…"
	}
	fmt.Fprintf(b, "%s: %s\n", label, body)
	if card {
		fmt.Fprintf(b, "Full prompt: !job show %s\n", j.ID)
	}
}

func scheduleWords(sp schedule.Spec) string { return schedule.Describe(sp) }

// diff lists the fields an edit changes; hide leaves out the prompt's and
// text's values (a shared chat).
func diff(before, after string, hide bool) string {
	var a, z map[string]json.RawMessage
	_ = json.Unmarshal([]byte(before), &a)
	_ = json.Unmarshal([]byte(after), &z)
	var keys []string
	for k := range z {
		keys = append(keys, k)
	}
	for k := range a {
		if _, ok := z[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		switch {
		case string(a[k]) == string(z[k]):
		case hide && (k == "prompt" || k == "text"):
			fmt.Fprintf(&b, "  %s: changed (shown only in private chats)\n", k)
		default:
			fmt.Fprintf(&b, "  %s: %s → %s\n", k, clip(string(a[k]), 80), clip(string(z[k]), 80))
		}
	}
	return or(b.String(), "  (none)\n")
}

func clip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return or(s, "—")
}

// newID is a short job ID.
func newID() string {
	const letters = "abcdefghjkmnpqrstuvwxyz23456789"
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return "j" + string(b)
}

// Command answers !jobs and !job … (text after the "!"). It changes
// nothing itself: the router stores the change with the reply, in one
// transaction, then wakes the due loop.
func (s *Scheduler) Command(c Caller, text string) (reply string, change *store.JobChange) {
	word, rest := cut(text)
	if strings.ToLower(word) == "jobs" {
		if strings.EqualFold(rest, "details") {
			var b strings.Builder
			b.WriteString("Pending requests:\n")
			for _, j := range s.drafts(c) {
				fmt.Fprintf(&b, "%s %s: !job show %s; !job confirm %s; !job discard %s\n", j.ID, j.Name, j.ID, j.ID, j.ID)
			}
			return strings.TrimSpace(b.String()), nil
		}
		return s.list(c, strings.EqualFold(rest, "all")), nil
	}
	sub, rest := cut(rest)
	id, arg := cut(rest)
	switch strings.ToLower(sub) {
	case "add":
		return s.add(c, rest)
	case "edit":
		return s.edit(c, id, arg)
	case "show":
		return s.show(c, id), nil
	case "history":
		return s.history(c, id), nil
	case "confirm":
		return s.confirm(c, id)
	case "discard":
		return s.discard(c, id)
	case "pause", "resume", "run", "stop", "rm", "renew":
		return s.manage(c, strings.ToLower(sub), id, arg)
	}
	return "Job commands:\n" + strings.Join(HelpLines, "\n"), nil
}

func cut(s string) (string, string) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, unicode.IsSpace)
	if i < 0 {
		return s, ""
	}
	return s[:i], strings.TrimSpace(s[i:])
}

// own is a job c may manage: its creator's or, for the owner, anyone's.
func (s *Scheduler) own(c Caller, id string) (store.Job, error) {
	j, ok, err := s.st.Job(id)
	switch {
	case err != nil:
		return j, err
	case !ok || j.State == store.JobDraft || j.State == store.JobDeleted || c.Role != core.RoleOwner && j.Creator.Key() != c.Principal.Key():
		return j, fmt.Errorf("no job %q of yours", id)
	}
	return j, nil
}

func (s *Scheduler) add(c Caller, raw string) (string, *store.JobChange) {
	var d Def
	if err := decode([]byte(raw), &d); err != nil {
		return "Usage: !job add {\"name\":…,\"schedule\":{\"cron\":[\"0 9 * * MON\"]},\"prompt\":…}. " + err.Error(), nil
	}
	now := s.now()
	sched, err := s.validate(&d, c, now, now)
	if err != nil {
		return "Job not created: " + err.Error(), nil
	}
	if msg := s.capped(c); msg != "" {
		return msg, nil
	}
	next, _ := sched.Next(now)
	j := store.Job{ID: newID(), Name: d.Name, Kind: d.Kind, Creator: c.Principal, CreatedIn: c.Conv, CreatedPrivate: c.Private, State: store.JobActive, NextSlot: next.Key, NextDue: next.At, Created: now}
	if d.Paused {
		j.State, j.Reason = store.JobPaused, pausedAtCreation(c)
		d.Paused = false
	}
	j.Def = mustJSON(d)
	return s.result(c, j, d, "Created", nil), &store.JobChange{Add: &j}
}

// pausedAtCreation is the reason of a job created paused: the creator's own
// pause, never a scheduler one (heimdall sched health).
func pausedAtCreation(c Caller) string { return "paused by " + c.Principal.Key() + " at creation" }

// capped refuses an employee's 11th active job.
func (s *Scheduler) capped(c Caller) string {
	if c.Role == core.RoleOwner {
		return ""
	}
	if jobs, _ := s.st.Jobs(store.JobFilter{Creator: c.Principal.Key(), States: []string{store.JobActive, store.JobPaused}}); len(jobs) >= 10 {
		return "Task not created: at most 10 scheduled tasks per employee. Remove one first."
	}
	return ""
}

func (s *Scheduler) edit(c Caller, id, raw string) (string, *store.JobChange) {
	var changes map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &changes); err != nil || id == "" {
		return "Usage: !job edit <id> {\"field\": value, …}", nil
	}
	j, err := s.own(c, id)
	if err != nil {
		return err.Error(), nil
	}
	od, _, _ := parse(j)
	d, err := merge(od, changes)
	if err != nil {
		return "Job not changed: " + err.Error(), nil
	}
	now := s.now()
	sched, err := s.validate(&d, s.creator(j, c), j.Created, now)
	if err != nil {
		return "Job not changed: " + err.Error(), nil
	}
	before := od
	j.NextDue = time.Time{}
	if next, ok := sched.Next(now); ok {
		j.NextDue = next.At
	}
	return s.result(c, j, d, "Updated", &before), &store.JobChange{Update: j.ID, Set: s.applied(d, sched, j.Version+1)}
}

// applied are the columns a job gets from definition d: its next slot from
// now on.
func (s *Scheduler) applied(d Def, sched *schedule.Schedule, version int) store.Set {
	next, _ := sched.Next(s.now())
	return store.Set{"name": d.Name, "kind": d.Kind, "def": mustJSON(d), "version": version, "next_slot": next.Key, "next_due": next.At}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func (s *Scheduler) confirm(c Caller, id string) (string, *store.JobChange) {
	j, ok, err := s.st.Job(id)
	if err != nil || !ok || j.State != store.JobDraft || j.Creator.Key() != c.Principal.Key() || j.CreatedIn != c.Conv {
		return fmt.Sprintf("No draft %q of yours in this conversation.", id), nil
	}
	now := s.now()
	if !j.DraftExpires.After(now) {
		return "Request expired; nothing was changed.", &store.JobChange{Drop: j.ID}
	}
	if id := removalID(j); id != "" {
		// Recheck current authority and reuse every rm side effect and retry fence.
		reply, change := s.manage(c, "rm", id, "")
		if change != nil {
			change.Drop = j.ID
		}
		return reply, change
	}
	return s.confirmJob(c, j)
}

// confirmJob shares activation checks for human confirmation and direct owner requests.
func (s *Scheduler) confirmJob(c Caller, j store.Job) (string, *store.JobChange) {
	now := s.now()
	d, _, _ := parse(j)
	who, version := c, 1
	if j.EditOf != "" {
		orig, err := s.own(c, j.EditOf)
		if err != nil {
			return err.Error(), nil
		}
		who, version = s.creator(orig, c), orig.Version+1
		j.Created = orig.Created
	} else if msg := s.capped(c); msg != "" {
		return msg, nil
	}
	sched, err := s.validate(&d, who, j.Created, now)
	if err != nil {
		return "Request not approved: " + err.Error(), nil
	}
	paused := d.Paused
	d.Paused = false
	set := s.applied(d, sched, version)
	delete(set, "version") // confirming counts an edit's version
	next, _ := sched.Next(now)
	change := &store.JobChange{Confirm: &j, Set: set}
	j.NextDue = next.At
	if j.EditOf != "" {
		orig, _, _ := s.st.Job(j.EditOf)
		before, _, _ := parse(orig)
		orig.NextDue = j.NextDue
		return s.result(c, orig, d, "Updated", &before), change
	}
	j.State = store.JobActive
	if paused {
		set["state"], set["reason"] = store.JobPaused, pausedAtCreation(c)
		j.State = store.JobPaused
	}
	return s.result(c, j, d, "Created", nil), change
}

func (s *Scheduler) discard(c Caller, id string) (string, *store.JobChange) {
	j, ok, err := s.st.Job(id)
	if err != nil || !ok || j.State != store.JobDraft || j.Creator.Key() != c.Principal.Key() || j.CreatedIn != c.Conv {
		return fmt.Sprintf("No draft %q of yours in this conversation.", id), nil
	}
	return "Request cancelled; nothing was changed.", &store.JobChange{Drop: id}
}

func (s *Scheduler) manage(c Caller, sub, id, arg string) (string, *store.JobChange) {
	j, err := s.own(c, id)
	if err != nil {
		return err.Error(), nil
	}
	d, sched, _ := parse(j)
	now := s.now()
	set := func(set store.Set, what string) (string, *store.JobChange) {
		change := &store.JobChange{Update: id, Set: set}
		if d.RerunOnTimeout && (what == "paused" || what == "deleted") {
			change.CancelPendingRetries = id
			if what == "deleted" {
				change.StopJob = id
				change.CancelRetries = id
			}
		}
		if state, ok := set["state"].(string); ok {
			j.State = state
		}
		if next, ok := set["next_due"].(time.Time); ok {
			j.NextDue = next
		}
		title := "Updated"
		switch {
		case what == "paused":
			title = "Paused"
		case strings.HasPrefix(what, "resumed"):
			title = "Resumed"
		case strings.HasPrefix(what, "renewed"):
			title = "Renewed"
		}
		if j.State == store.JobDeleted {
			title = "Removed"
		}
		return s.result(c, j, d, title, nil), change
	}
	switch sub {
	case "pause":
		if j.State != store.JobActive {
			return s.result(c, j, d, "Status", nil), nil
		}
		return set(store.Set{"state": store.JobPaused, "reason": "paused by " + c.Principal.Key()}, "paused")
	case "resume":
		if j.State != store.JobPaused || sched == nil {
			return s.result(c, j, d, "Status", nil), nil
		}
		next, ok := sched.Next(now)
		if !ok {
			return "No future check is scheduled; edit its schedule.", nil
		}
		return set(store.Set{"state": store.JobActive, "reason": "", "fails": 0, "skips": 0, "next_slot": next.Key, "next_due": next.At}, "resumed; next run "+when(d, next.At))
	case "run":
		if j.State != store.JobActive && j.State != store.JobPaused {
			return s.result(c, j, d, "Status", nil), nil
		}
		r := store.Run{Job: j.ID, Version: j.Version, Slot: fmt.Sprintf("manual:%d", now.UnixMilli()), Due: now, Trigger: "manual", State: store.RunPending}
		return s.heading(c, "Check queued") + "\nOne manual check is queued; completion is not yet known.\n" + s.status(c, j, d), &store.JobChange{Run: &r}
	case "stop":
		if d.RerunOnTimeout {
			return s.heading(c, "Stopping current check") + "\nStopping any current check; pending timeout retry cancelled. Future schedule stays unchanged.", &store.JobChange{CancelRetries: id, StopJob: id}
		}
		running, err := s.st.Runs(store.RunFilter{Job: id, States: []string{store.RunRunning}})
		if err != nil {
			return "Could not check running job.", nil
		}
		if len(running) == 0 {
			return "No check is running.", nil
		}
		return s.heading(c, "Stopping current check") + "\nCurrent work is being stopped. Future schedule stays unchanged.", &store.JobChange{StopJob: id, CancelRetries: id}
	case "rm":
		msg, change := set(store.Set{"state": store.JobDeleted, "reason": "removed by " + c.Principal.Key()}, "deleted")
		change.StopJob = j.ID
		change.CancelRetries = j.ID
		return msg, change
	case "renew":
		dur, err := span(or(arg, "7d"))
		if err != nil || dur <= 0 || dur > 90*24*time.Hour {
			return "Usage: !job renew <id> [duration up to 90d]", nil
		}
		from := now
		if u, err := time.Parse(time.RFC3339, d.Until); err == nil && u.After(now) {
			from = u
		}
		d.Until = from.Add(dur).UTC().Format(time.RFC3339)
		return set(store.Set{"def": mustJSON(d)}, "renewed until "+when(d, from.Add(dur)))
	}
	return "", nil
}

func (s *Scheduler) stopRun(id string) bool {
	s.mu.Lock()
	stop := s.stop
	s.mu.Unlock()
	return stop != nil && stop(id)
}

func (s *Scheduler) list(c Caller, all bool) string {
	f := store.JobFilter{Creator: c.Principal.Key(), States: []string{store.JobActive, store.JobPaused, store.JobDone}}
	if all && c.Role == core.RoleOwner {
		f.Creator = ""
	}
	jobs, err := s.st.Jobs(f)
	if err != nil {
		return "Jobs not read: " + err.Error()
	}
	if len(jobs) == 0 {
		return "No jobs."
	}
	var b strings.Builder
	for _, j := range jobs {
		if !all {
			d, _, _ := parse(j)
			b.WriteString(s.result(c, j, d, "Status", nil) + "\n\n")
			continue
		}
		fmt.Fprintf(&b, "• %s %q — %s", j.ID, j.Name, j.State)
		if c.Private {
			d, _, _ := parse(j)
			if j.State == store.JobActive && !j.NextDue.IsZero() {
				fmt.Fprintf(&b, ", next %s", when(d, j.NextDue))
			}
			if all {
				fmt.Fprintf(&b, ", by %s", j.Creator.Key())
			}
		}
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func (s *Scheduler) show(c Caller, id string) string {
	j, err := s.own(c, id)
	if err != nil {
		drafts := s.drafts(c)
		i := slices.IndexFunc(drafts, func(d store.Job) bool { return d.ID == id })
		if i < 0 {
			return err.Error()
		}
		j = drafts[i]
		if target := removalID(j); target != "" {
			return "Removal request (only genuine human confirmation applies):\n" + s.show(c, target)
		}
	}
	d, sched, _ := parse(j)
	var b strings.Builder
	fmt.Fprintf(&b, "Job %s %q — %s", j.ID, j.Name, j.State)
	if j.Reason != "" {
		fmt.Fprintf(&b, " (%s)", j.Reason)
	}
	fmt.Fprintf(&b, ", by %s, version %d\n", j.Creator.Key(), j.Version)
	s.describe(&b, c, j, d, sched, false)
	// The options are the definition without what describe showed.
	var opts map[string]json.RawMessage
	_ = json.Unmarshal([]byte(mustJSON(d)), &opts)
	for _, k := range []string{"name", "kind", "schedule", "tz", "to", "prompt", "text", "harness"} {
		delete(opts, k)
	}
	fmt.Fprintf(&b, "Options: %s\n", mustJSON(opts))
	b.WriteString(s.runs(j, d, 5, c.Private))
	return strings.TrimSpace(b.String())
}

func (s *Scheduler) history(c Caller, id string) string {
	j, err := s.own(c, id)
	if err != nil {
		return err.Error()
	}
	d, _, _ := parse(j)
	return strings.TrimSpace(fmt.Sprintf("Job %s %q\n%s", j.ID, j.Name, s.runs(j, d, 20, c.Private)))
}

func (s *Scheduler) runs(j store.Job, d Def, n int, private bool) string {
	runs, _ := s.st.Runs(store.RunFilter{Job: j.ID, Limit: n})
	if len(runs) == 0 {
		return "Runs: none yet"
	}
	var b strings.Builder
	b.WriteString("Runs (newest first):\n")
	for _, r := range runs {
		fmt.Fprintf(&b, "  %s %s", r.Slot, r.State)
		if !r.Started.IsZero() {
			fmt.Fprintf(&b, ", late %s", r.Started.Sub(r.Due).Round(time.Second))
		}
		if r.Attempt > 0 {
			fmt.Fprintf(&b, ", attempts %d", r.Attempt+1)
		}
		if r.Cause != "" {
			cause := r.Cause
			if !private {
				cause = causeLabel(cause)
			}
			fmt.Fprintf(&b, ", %s", cause)
		}
		if r.State == store.RunPending && r.Attempt > 0 && !r.NotBefore.IsZero() {
			fmt.Fprintf(&b, ", retry automatically at %s (attempt %d)", when(d, r.NotBefore), r.Attempt+1)
		} else if r.State == store.RunFailed {
			b.WriteString(", retry: no")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// Status is the scheduler's line in !status.
func (s *Scheduler) Status() string {
	tick, _ := s.st.Meta(MetaTick)
	runs, _ := s.st.Runs(store.RunFilter{States: []string{store.RunPending, store.RunRunning}})
	running := 0
	for _, r := range runs {
		if r.State == store.RunRunning {
			running++
		}
	}
	line := fmt.Sprintf("Scheduler: tick %s ago, %d running, %d pending", s.now().Sub(time.UnixMilli(tick)).Round(time.Second), running, len(runs)-running)
	if due, _ := s.st.Jobs(store.JobFilter{States: []string{store.JobActive}, DueBy: s.now().AddDate(10, 0, 0)}); len(due) > 0 {
		next := slices.MinFunc(due, func(a, b store.Job) int { return a.NextDue.Compare(b.NextDue) })
		line += ", next due " + next.NextDue.UTC().Format("2006-01-02 15:04 UTC")
	}
	return line
}
