package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/misunders2d/heimdall/internal/core"
)

// Caller is the verified live message behind a command, built by the router
// from stored ingress exactly as for !job. Nothing in the command text can
// change who it is or where replies go.
type Caller struct {
	Principal core.Principal
	Role      core.Role
	Conv      core.ConvKey
	MsgID     string
	Live      bool // core.OriginLive: a real person's message, not a scheduled run
	Private   bool // the stored ingress is the caller's private chat with the bot
	// The interface-verified native OAuth actor of the stored message: the
	// creator's own native sign-ins (as for !job runs); never role authority.
	OAuthActor, OAuthInstallation string
	// Origin names a registration source that is not a live chat message
	// (the owner-trusted T3 launcher); empty for messages and person turns.
	Origin string
}

// sees reports whether c may see cm. As for !job: in the creator's private
// chat, all of the creator's commitments; elsewhere only commitments routed
// to this exact conversation.
func (c Caller) sees(cm Commitment) bool {
	if c.Private && cm.Creator == c.Principal.Key() {
		return true
	}
	return cm.Route == c.Conv
}

// controls reports whether c may change cm: it must see it and be its
// creator.
func (c Caller) controls(cm Commitment) bool { return c.sees(cm) && cm.Creator == c.Principal.Key() }

const usage = `!work — durable commitments (owner only; Linear is canonical).
Slice 1 is PARTIAL: a configured T3 observer turns finished worker runs into reports; with supervisor_review a server-initiated review turn gives the verdict, otherwise verdicts come from you; with t3_dispatch each attempt is sent to its t3: worker, never resent (laptop tests only; docs/durable.md).
!work [list]
!work show ISSUE
!work add ISSUE [worker=REF] [resource=NAME] [gate=git:/repo]... title [| next: authorized work left after acceptance]
!work report ISSUE evidence…   (worker result for the current attempt; unaccepted until verified)
!work accept ISSUE | !work reject ISSUE reason…
!work retry ISSUE   (after reconciling a permanently failed delivery)
!work pause ISSUE | !work resume ISSUE   (owner pause never expires)
!work close ISSUE [note…]`

// Task is the durable-task surface of the scheduler control for a bound
// live owner turn: "add" (a plain title only: no worker, resource, gate or
// next step can come from a tool call), "list" and "show". It is the same
// path as !work, with the same live-owner rule; admitted reports a new
// commitment.
func (r *Runtime) Task(c Caller, action, issue, title string) (reply string, admitted bool) {
	switch action {
	case "list":
		return r.Command(c, "list"), false
	case "show":
		return r.Command(c, "show "+issue), false
	case "add":
		f := strings.Fields(title)
		if len(f) == 0 || strings.Contains(f[0], "=") || strings.Contains(title, "| next:") || strings.ContainsAny(issue, " \t\r\n") {
			return "A task needs a plain title: no worker, resource, gate or next step from a tool call.", false
		}
		reply = r.Command(c, "add "+issue+" "+title)
		return reply, strings.HasPrefix(reply, "Tracking ")
	}
	return "Unsupported task action.", false
}

// Command answers one !work command. Only live owner messages act.
func (r *Runtime) Command(c Caller, text string) string {
	if !c.Live || c.Role != core.RoleOwner {
		return "!work is for the owner's live messages only."
	}
	f := strings.Fields(text)
	if len(f) > 0 && strings.EqualFold(f[0], "work") {
		f = f[1:]
	}
	if len(f) == 0 || strings.EqualFold(f[0], "list") {
		return r.list(c)
	}
	sub := strings.ToLower(f[0])
	if sub == "help" || len(f) < 2 {
		return usage
	}
	issue := strings.ToUpper(f[1])
	if !IssuePattern.MatchString(issue) {
		return "Not a Linear issue identifier: " + f[1]
	}
	rest := strings.Join(f[2:], " ")
	var err error
	if sub != "add" {
		cm, ok := r.input(issue)
		if !ok || !c.sees(cm) {
			return issue + " is not tracked here."
		}
		if sub != "show" && !c.controls(cm) {
			return issue + ": only its creator can change it."
		}
	}
	switch sub {
	case "show":
		return r.show(issue)
	case "add":
		var cm Commitment
		if cm, err = parseAdd(c, issue, f[2:]); err == nil {
			var existing bool
			if existing, err = r.Admit(cm); err == nil {
				if existing {
					if cur, ok := r.input(issue); ok && c.sees(cur) {
						return issue + " is already tracked:\n" + r.show(issue)
					}
					return issue + " is already tracked elsewhere."
				}
				return "Tracking " + issue + " (" + r.linearURL(issue) + "). It stays visible until closed."
			}
		}
	case "report", "accept", "reject", "close", "retry":
		if sub == "report" && rest == "" {
			return "A report needs evidence."
		}
		ev := Event{Kind: sub, By: c.Principal.Key(), Note: rest}
		if sub == "report" {
			ev.Evidence, ev.Note = rest, ""
		}
		if err = r.Signal(issue, ev, c.Conv.Interface+":"+c.Conv.Chat+":"+c.MsgID); err == nil {
			return "Sent " + sub + " to " + issue + "."
		}
	case "pause":
		if err = r.Pause(issue, c.Principal.Key()); err == nil {
			return issue + " paused by owner. It stays paused until !work resume " + issue + "."
		}
	case "resume":
		if err = r.Resume(issue); err == nil {
			return issue + " resumed."
		}
	default:
		return usage
	}
	return issue + ": " + err.Error()
}

func parseAdd(c Caller, issue string, args []string) (Commitment, error) {
	cm := Commitment{Issue: issue, Creator: c.Principal.Key(), Owner: c.Principal, OAuthActor: c.OAuthActor, OAuthInstallation: c.OAuthInstallation, Route: c.Conv, Private: c.Private, ReplyTo: c.MsgID, Resource: defaultRes, Origin: c.Origin}
	var title []string
	for i, a := range args {
		k, v, ok := strings.Cut(a, "=")
		switch {
		case ok && k == "worker" && len(title) == 0:
			cm.Worker = v
		case ok && k == "resource" && len(title) == 0:
			cm.Resource = v
		case ok && k == "gate" && len(title) == 0:
			cm.Gates = append(cm.Gates, v)
		default:
			title = args[i:]
		}
		if len(title) > 0 {
			break
		}
	}
	t := strings.Join(title, " ")
	if before, after, ok := strings.Cut(t, "| next:"); ok {
		t, cm.Next = strings.TrimSpace(before), strings.TrimSpace(after)
	}
	cm.Title, cm.Brief = t, issue+": "+t
	if cm.Title == "" {
		return cm, errors.New("a title is required")
	}
	for _, g := range cm.Gates {
		if !strings.HasPrefix(g, "git:/") {
			return cm, fmt.Errorf("unknown gate %q (supported: git:/absolute/repo)", g)
		}
	}
	return cm, nil
}

// Admit durably admits cm: when it returns, the commitment is committed to
// durable.db and queued on its resource. A repeated issue returns the
// existing commitment unchanged (existing=true).
func (r *Runtime) Admit(cm Commitment) (existing bool, err error) {
	if _, ok := r.queues[cm.Resource]; !ok {
		return false, fmt.Errorf("unknown resource %q", cm.Resource)
	}
	if _, err := dbos.RetrieveWorkflow[State](r.ctx, cm.Issue); err == nil {
		return true, nil
	}
	// Not queued: waiting holds no capacity. Only dispatch is queued.
	_, err = dbos.RunWorkflow(r.ctx, r.commitment, cm, dbos.WithWorkflowID(cm.Issue))
	return false, err
}

// Signal delivers an attributed event. key is the source message identity:
// DBOS stores it at most once, so a redelivered message is not a second event.
func (r *Runtime) Signal(issue string, ev Event, key string) error {
	if _, err := dbos.RetrieveWorkflow[State](r.ctx, issue); err != nil {
		return errors.New("not tracked")
	}
	if ev.Kind != "close" {
		st, err := r.State(issue)
		ev.Attempt = st.Attempt
		if err != nil || st.Attempt == 0 {
			return errors.New("not started yet (queued)")
		}
	}
	return dbos.Send(r.ctx, issue, ev, topic, dbos.WithIdempotencyKey(key))
}

// Pause is the explicit owner pause: the workflow is cancelled with a
// recorded owner attribute and never resumes by itself.
func (r *Runtime) Pause(issue, by string) error {
	if _, err := dbos.RetrieveWorkflow[State](r.ctx, issue); err != nil {
		return errors.New("not tracked")
	}
	if err := dbos.SetWorkflowAttributes(r.ctx, issue, map[string]any{"paused_by": by, "paused_at": time.Now().UTC().Format(time.RFC3339)}, dbos.WithMergeAttributes()); err != nil {
		return err
	}
	return dbos.CancelWorkflow(r.ctx, issue)
}

// Resume ends an owner pause; the commitment continues from its last step.
func (r *Runtime) Resume(issue string) error {
	ws, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterWorkflowIDs(issue), dbos.WithFilterLoadInput(true))
	if err != nil || len(ws) == 0 {
		return errors.New("not tracked")
	}
	if ws[0].Status != dbos.WorkflowStatusCancelled {
		return errors.New("not paused")
	}
	if err := dbos.SetWorkflowAttributes(r.ctx, issue, map[string]any{"paused_by": nil, "paused_at": nil}, dbos.WithMergeAttributes()); err != nil {
		return err
	}
	// The commitment itself holds no capacity; a pending dispatch child
	// keeps its own resource queue.
	_, err = dbos.ResumeWorkflow[State](r.ctx, issue)
	return err
}

// State reads a commitment's last published state (zero while queued).
func (r *Runtime) State(issue string) (State, error) {
	return dbos.GetEvent[State](r.ctx, issue, stateKey, 20*time.Millisecond)
}

const pageSize = 200

// View is one commitment as listed.
type View struct {
	Issue, Title, Status, Worker string
	State                        State
	PausedBy                     string
	cm                           Commitment
}

// Views lists every commitment, open ones first. It only reads.
func (r *Runtime) Views() ([]View, error) {
	var ws []dbos.WorkflowStatus
	for {
		// Oldest first, page by page: listing never truncates.
		page, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterName(workflowName), dbos.WithFilterLoadInput(true), dbos.WithFilterLimit(pageSize), dbos.WithFilterOffset(len(ws)))
		if err != nil {
			return nil, err
		}
		ws = append(ws, page...)
		if len(page) < pageSize {
			break
		}
	}
	var out []View
	for _, w := range ws {
		v := View{Issue: w.ID, Status: string(w.Status)}
		if cm, ok := decodeInput(w.Input); ok {
			v.Title, v.Worker, v.cm = cm.Title, cm.Worker, cm
		}
		if p, ok := w.Attributes["paused_by"].(string); ok && p != "" {
			v.PausedBy = p
		}
		v.State, _ = r.State(w.ID)
		out = append(out, v)
	}
	slices.SortStableFunc(out, func(a, b View) int {
		return boolInt(a.State.Phase == PhaseClosed) - boolInt(b.State.Phase == PhaseClosed)
	})
	return out, nil
}

// input reads commitment issue's stored input.
func (r *Runtime) input(issue string) (Commitment, bool) {
	ws, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterWorkflowIDs(issue), dbos.WithFilterName(workflowName), dbos.WithFilterLoadInput(true))
	if err != nil || len(ws) == 0 {
		return Commitment{}, false
	}
	return decodeInput(ws[0].Input)
}

func (r *Runtime) list(c Caller) string {
	all, err := r.Views()
	if err != nil {
		return "Commitments unavailable: " + err.Error()
	}
	var vs []View
	for _, v := range all {
		if c.sees(v.cm) {
			vs = append(vs, v)
		}
	}
	if len(vs) == 0 {
		return "No tracked commitments. " + strings.SplitN(usage, "\n", 2)[0]
	}
	var b strings.Builder
	for _, v := range vs {
		fmt.Fprintf(&b, "%s %s — %s\n", v.Issue, v.Title, v.line())
	}
	return strings.TrimSpace(b.String())
}

func (r *Runtime) show(issue string) string {
	vs, err := r.Views()
	if err != nil {
		return err.Error()
	}
	for _, v := range vs {
		if v.Issue == issue {
			s := v.State
			return fmt.Sprintf("%s %s\n%s\nLinear: %s\nworker: %s\nattempt: %d%s\ndispatched run: %s\nevidence: %s\nverdict: %s\nreceipts: %v", v.Issue, v.Title, v.line(), r.linearURL(v.Issue), or(v.Worker, "owner-dispatched"), s.Attempt, map[bool]string{true: " (authorized next step)"}[s.Stage == "next"], or(s.Dispatched, "none"), or(s.Evidence, "none"), or(s.Verdict, "none"), s.Receipts)
		}
	}
	return issue + " is not tracked."
}

// line is the human status: paused, queued, failed and phase are distinct.
func (v View) line() string {
	switch {
	case v.State.Phase == PhaseHeld:
		return "held: " + v.State.Blocked + "; next: reconcile, then !work retry or !work close"
	case v.PausedBy != "":
		return "paused by owner " + v.PausedBy + " (never expires; !work resume)"
	case v.Status == string(dbos.WorkflowStatusEnqueued):
		return "queued for capacity (kept, not dropped)"
	case v.Status == string(dbos.WorkflowStatusCancelled):
		return "cancelled outside Heimdall; commitment still open in Linear: !work resume"
	case v.Status == string(dbos.WorkflowStatusError) || v.Status == string(dbos.WorkflowStatusMaxRecoveryAttemptsExceeded):
		return "execution failed (" + v.Status + "); commitment still open in Linear; recovery: !work resume"
	}
	line := fmt.Sprintf("%s since %s; next: %s", v.State.Phase, v.State.Since.Format(time.RFC3339), v.State.Next)
	if v.State.Note != "" {
		line += " (" + v.State.Note + ")"
	}
	return line
}

// decodeInput reads a listed workflow's input: DBOS returns it encoded.
func decodeInput(in any) (Commitment, bool) {
	var cm Commitment
	switch v := in.(type) {
	case Commitment:
		return v, true
	case string:
		return cm, json.Unmarshal([]byte(v), &cm) == nil && cm.Issue != ""
	}
	return cm, false
}

var shaPattern = regexp.MustCompile(`\b[0-9a-f]{40}\b`)

// precheck rejects evidence that cannot be right; it never accepts. Empty
// evidence fails; a git gate needs a full commit SHA that exists in its
// repository. Passing evidence still waits for the owner's verdict.
func precheck(gates []string, evidence string) string {
	if strings.TrimSpace(evidence) == "" {
		return "no evidence"
	}
	for _, g := range gates {
		repo, ok := strings.CutPrefix(g, "git:")
		if !ok {
			return "unknown gate " + g
		}
		sha := shaPattern.FindString(evidence)
		if sha == "" {
			return g + " needs a full 40-character commit SHA in the evidence"
		}
		if err := exec.Command("git", "-C", repo, "cat-file", "-e", sha+"^{commit}").Run(); err != nil {
			return "commit " + sha + " not found in " + repo
		}
	}
	return ""
}

func (r *Runtime) linearURL(issue string) string {
	if r.opt.Linear == "" {
		return issue
	}
	return "https://linear.app/" + r.opt.Linear + "/issue/" + issue
}

func or(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
