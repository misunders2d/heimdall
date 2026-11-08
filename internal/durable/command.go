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
}

const usage = `!work — durable commitments (owner only; Linear is canonical).
Slice 1 is PARTIAL: a configured T3 observer turns finished worker runs into reports; there is no qualified dispatcher or verifier, so verdicts come from you (docs/durable.md).
!work [list]
!work show ISSUE
!work add ISSUE [worker=REF] [resource=NAME] [gate=git:/repo]... title [| next: authorized work left after acceptance]
!work report ISSUE evidence…   (worker result for the current attempt; unaccepted until verified)
!work accept ISSUE | !work reject ISSUE reason…
!work retry ISSUE   (after reconciling a permanently failed delivery)
!work pause ISSUE | !work resume ISSUE   (owner pause never expires)
!work close ISSUE [note…]`

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
		return r.list()
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
	switch sub {
	case "show":
		return r.show(issue)
	case "add":
		var cm Commitment
		if cm, err = parseAdd(c, issue, f[2:]); err == nil {
			var existing bool
			if existing, err = r.Admit(cm); err == nil {
				if existing {
					return issue + " is already tracked:\n" + r.show(issue)
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
	cm := Commitment{Issue: issue, Creator: c.Principal.Key(), Route: c.Conv, ReplyTo: c.MsgID, Resource: defaultRes}
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
	q, ok := r.queues[cm.Resource]
	if !ok {
		return false, fmt.Errorf("unknown resource %q", cm.Resource)
	}
	if _, err := dbos.RetrieveWorkflow[State](r.ctx, cm.Issue); err == nil {
		return true, nil
	}
	part := cm.Worker
	if part == "" {
		part = "owner"
	}
	_, err = dbos.RunWorkflow(r.ctx, r.commitment, cm, dbos.WithWorkflowID(cm.Issue), dbos.WithQueue(q), dbos.WithQueuePartitionKey(part))
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
	// Back onto its own resource queue: capacity still applies (a cancelled
	// workflow no longer records its queue).
	cm, ok := decodeInput(ws[0].Input)
	if !ok {
		return errors.New("commitment input unreadable")
	}
	_, err = dbos.ResumeWorkflow[State](r.ctx, issue, dbos.WithResumeQueue("work:"+cm.Resource))
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
			v.Title, v.Worker = cm.Title, cm.Worker
		}
		if p, ok := w.Attributes["paused_by"].(string); ok && p != "" {
			v.PausedBy = p
		}
		if w.Status != dbos.WorkflowStatusEnqueued || w.Attempts > 0 {
			v.State, _ = r.State(w.ID)
		}
		out = append(out, v)
	}
	slices.SortStableFunc(out, func(a, b View) int {
		return boolInt(a.State.Phase == PhaseClosed) - boolInt(b.State.Phase == PhaseClosed)
	})
	return out, nil
}

func (r *Runtime) list() string {
	vs, err := r.Views()
	if err != nil {
		return "Commitments unavailable: " + err.Error()
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
			return fmt.Sprintf("%s %s\n%s\nLinear: %s\nworker: %s\nattempt: %d\nevidence: %s\nverdict: %s\nreceipts: %v", v.Issue, v.Title, v.line(), r.linearURL(v.Issue), or(v.Worker, "owner-dispatched"), s.Attempt, or(s.Evidence, "none"), or(s.Verdict, "none"), s.Receipts)
		}
	}
	return issue + " is not tracked."
}

// line is the human status: paused, queued, failed and phase are distinct.
func (v View) line() string {
	switch {
	case v.State.Phase == PhaseBlocked:
		return "delivery blocked: " + v.State.Blocked + "; next: reconcile, then !work retry or !work close"
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
