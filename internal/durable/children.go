package durable

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Hub T3 tracking (durable.t3_tracking, off by default; docs/durable.md).
// A wake of an assistant-worked task may start T3 work with its own native
// tools (t3_thread_launch, delegate_task, t3_thread_send). The router
// decodes those calls' native results as they finish, never the model's
// text, and records each child here at once, so a wake that is cut off
// still leaves its children recorded. Each child is listed in every later
// wake prompt and in !work show. A child on this host is watched read-only
// by its own workflow (heimdall.t3child, one per row), which ends the
// task's wait once (the task_wake signal) when the child's run ends. Every
// active row is watched again at startup, so a crash between recording a
// child and starting its watcher loses nothing. Children in another
// environment are recorded but not watched here.

const childName = "heimdall.t3child"

// Child is one T3 thread a task's wake started or sent work to, from the
// engine's decoded native tool result.
type Child struct {
	Kind   string `json:"kind"`             // launch | delegate | send
	Thread string `json:"thread"`           // "" when the result named no thread (unbound)
	Env    string `json:"env,omitempty"`    // another environment: not watched here
	Run    string `json:"run,omitempty"`    // the run it started, when the result named one
	Task   string `json:"task,omitempty"`   // delegate_task's taskId
	Status string `json:"status,omitempty"` // a terminal native status already in the result
	Call   string `json:"call,omitempty"`   // the native tool call's id: tells run-less results apart
}

// TracksChildren reports whether hub T3 tracking is on: the router then
// decodes a wake's native T3 calls and records them (Child).
func (r *Runtime) TracksChildren() bool { return r.opt.T3Tracking }

// terminalRun is a T3 run or delegated task status that ends it (T3's own
// OrchestrationV2RunStatus and delegated-task statuses).
func terminalRun(status string) bool {
	return slices.Contains([]string{"completed", "failed", "cancelled", "interrupted", "rolled_back"}, status)
}

// Child statuses that are not "active" or "finished (...)".
const (
	childNoCLI    = "not watched: no T3 CLI here"
	childTaskGone = "not watched: the task closed"
)

// RecordChild records a child of issue's wake key (ISSUE/attempt/work/N)
// and starts its watcher. A child with a thread and a run is one row per
// (thread, environment, run), however often it is reported; any other
// child is one row per delegated task id, else per native call. Only ids
// and statuses are stored, never a message. The watcher is started
// whenever the row is active, not only when it is new: its workflow id is
// the row's, so a repeat is a no-op and a missed start is made good.
func (r *Runtime) RecordChild(issue, key string, c Child) error {
	if !r.opt.T3Tracking {
		return nil
	}
	ref := ""
	switch {
	case c.Thread != "" && c.Run != "":
	case c.Task != "": // a delegated task is one row, however often reported
		ref = "task/" + c.Task
	default:
		ref = key + "/" + or(c.Call, c.Kind)
	}
	if c.Thread == "" {
		c.Env = ""
	}
	status := "active"
	switch {
	case c.Thread == "":
		status = "unbound: the " + c.Kind + " result named no thread"
	case terminalRun(c.Status):
		status = "finished (" + c.Status + ")"
	case c.Env != "":
		status = "in environment " + c.Env + ": not watched here"
	case r.opt.Observer == nil || !r.opt.Observer.Observable("t3:"+c.Thread):
		status = childNoCLI
	}
	if _, err := r.db.Exec(`INSERT INTO heimdall_t3_children(issue,thread,env,run,ref,kind,task,wake,status,at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(issue,thread,env,run,ref) DO NOTHING`,
		issue, c.Thread, c.Env, c.Run, ref, c.Kind, c.Task, key, status, time.Now().UnixMilli()); err != nil {
		return err
	}
	var id int64
	if err := r.db.QueryRow(`SELECT id,status FROM heimdall_t3_children WHERE issue=? AND thread=? AND env=? AND run=? AND ref=?`, issue, c.Thread, c.Env, c.Run, ref).Scan(&id, &status); err != nil {
		return err
	}
	if status != "active" {
		return nil
	}
	return r.watch(childWatch{ID: id, Issue: issue, Thread: c.Thread, Run: c.Run})
}

// watch starts (or finds) the watcher of one child row.
func (r *Runtime) watch(in childWatch) error {
	_, err := dbos.RunWorkflow(r.ctx, r.watchChild, in, dbos.WithWorkflowID(fmt.Sprintf("t3child/%s/%d", in.Issue, in.ID)))
	return err
}

// rearmChildren starts the watcher of every active child row at startup.
// A watcher already recorded is found, never run twice (its id is the
// row's); one whose start was lost starts now.
func (r *Runtime) rearmChildren() {
	if !r.opt.T3Tracking {
		return
	}
	rows, err := r.db.Query(`SELECT id,issue,thread,run FROM heimdall_t3_children WHERE status='active' AND thread<>'' AND env=''`)
	if err != nil {
		r.opt.Log.Printf("durable: T3 children not re-armed: %v", err)
		return
	}
	var ws []childWatch
	for rows.Next() {
		var w childWatch
		if rows.Scan(&w.ID, &w.Issue, &w.Thread, &w.Run) == nil {
			ws = append(ws, w)
		}
	}
	rows.Close()
	for _, w := range ws {
		if err := r.watch(w); err != nil {
			r.opt.Log.Printf("durable: %s: T3 child %d not watched: %v", w.Issue, w.ID, err)
		}
	}
}

// ChildRow is one recorded child as shown.
type ChildRow struct {
	Child
	Wake   string
	Status string
}

// shownChildren bounds the children listed in a wake prompt or !work show.
const shownChildren = 50

// Children lists issue's newest recorded children, oldest first, and how
// many older ones are left out.
func (r *Runtime) Children(issue string) ([]ChildRow, error) {
	cs, _, err := r.children(issue)
	return cs, err
}

func (r *Runtime) children(issue string) ([]ChildRow, int, error) {
	var total int
	if err := r.db.QueryRow(`SELECT count(*) FROM heimdall_t3_children WHERE issue=?`, issue).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := r.db.Query(`SELECT kind,thread,env,run,task,wake,status FROM (SELECT * FROM heimdall_t3_children WHERE issue=? ORDER BY id DESC LIMIT ?) ORDER BY id`, issue, shownChildren)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []ChildRow
	for rows.Next() {
		var c ChildRow
		if err := rows.Scan(&c.Kind, &c.Thread, &c.Env, &c.Run, &c.Task, &c.Wake, &c.Status); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
	}
	return out, total - len(out), rows.Err()
}

// childLines is the listing shown in a wake prompt and !work show.
func (r *Runtime) childLines(issue string) []string {
	cs, older, err := r.children(issue)
	if err != nil || len(cs) == 0 {
		return nil
	}
	var out []string
	if older > 0 {
		out = append(out, fmt.Sprintf("(%d older children not shown)", older))
	}
	for _, c := range cs {
		out = append(out, c.line())
	}
	return out
}

func (c ChildRow) line() string {
	if c.Thread == "" {
		s := fmt.Sprintf("- %s from wake %s", c.Kind, c.Wake)
		if c.Task != "" {
			s += " (task " + c.Task + ")"
		}
		return s + ": " + c.Status
	}
	ref := "t3:" + c.Thread
	if c.Env != "" {
		ref += "@" + c.Env
	}
	s := fmt.Sprintf("- %s (%s", ref, c.Kind)
	if c.Run != "" {
		s += ", run " + c.Run
	}
	if c.Task != "" {
		s += ", task " + c.Task
	}
	return s + "): " + c.Status
}

// childNote is the children part of a wake prompt ("" with none).
func (r *Runtime) childNote(issue string) string {
	ls := r.childLines(issue)
	if len(ls) == 0 {
		return ""
	}
	return "\nT3 work you started for this task (recorded by Heimdall from your native tool results, not from your text):\n" + strings.Join(ls, "\n") +
		"\nRead a finished child with t3_thread_read before acting on it; never start the same work again."
}

type childWatch struct {
	ID     int64  `json:"id"` // the row
	Issue  string `json:"issue"`
	Thread string `json:"thread"`
	Run    string `json:"run,omitempty"` // "" until the first read shows one
}

// childObs is what a watcher keeps of one read: ids, counts and statuses,
// never message text (step outputs are stored).
type childObs struct {
	Status    string `json:"status"`
	LatestRun string `json:"latest_run,omitempty"`
	ActiveRun string `json:"active_run,omitempty"`
	Pending   int    `json:"pending,omitempty"`
	RunCount  int    `json:"run_count,omitempty"`
	Replied   bool   `json:"replied,omitempty"` // the watched run has an assistant reply among the latest messages
}

type childRead struct {
	Obs  childObs `json:"obs"`
	Err  string   `json:"err,omitempty"`
	Stop string   `json:"stop,omitempty"` // stop watching: the row's status
}

// watchChild follows one child run read-only, backing off from
// Options.Observe to 10 minutes. The run has ended when it is not active
// and nothing waits for an answer, and either it has its own reply among
// the latest messages, or it is the latest run with a terminal status, or
// a later run started after this one was seen. Another run is never taken
// for it: a run queued behind another is still waiting. Then it marks the
// row and ends the task's wait once; it never sends, approves or answers
// anything in T3. A read error, or a status it does not know, keeps
// watching. It stops when the task closes, or where no T3 CLI is
// configured (any more).
func (r *Runtime) watchChild(ctx dbos.Context, in childWatch) (string, error) {
	poll := r.opt.Observe
	run, seen, seenAt := in.Run, false, 0
	for {
		if _, err := dbos.Sleep(ctx, poll); err != nil {
			return "", err
		}
		poll = min(poll*2, 10*time.Minute)
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (childRead, error) {
			stop := ""
			switch o := r.opt.Observer; {
			case !r.open(in.Issue):
				stop = childTaskGone
			case o == nil || !o.Observable("t3:"+in.Thread):
				stop = childNoCLI
			}
			if stop != "" {
				_, err := r.db.Exec(`UPDATE heimdall_t3_children SET status=? WHERE id=? AND status='active'`, stop, in.ID)
				return childRead{Stop: stop}, err
			}
			obs, err := r.opt.Observer.Observe(sctx, "t3:"+in.Thread)
			if err != nil {
				return childRead{Err: clip(err.Error(), 200)}, nil
			}
			_, replied := obs.Replies[or(run, or(obs.ActiveRun, obs.LatestRun))]
			return childRead{Obs: childObs{Status: obs.Status, LatestRun: obs.LatestRun, ActiveRun: obs.ActiveRun, Pending: obs.Pending, RunCount: obs.RunCount, Replied: replied}}, nil
		}, dbos.WithStepName("child-observe"))
		if err != nil {
			return "", err
		}
		if got.Stop != "" {
			return got.Stop, nil
		}
		o := got.Obs
		if got.Err != "" {
			continue
		}
		if run == "" {
			run = or(o.ActiveRun, o.LatestRun) // launched without a run: the first one it shows
			if run == "" {
				continue // no run yet: not finished
			}
		}
		if !seen && (o.ActiveRun == run || o.LatestRun == run) {
			seen, seenAt = true, o.RunCount
		}
		ended := o.ActiveRun != run && o.Pending == 0 &&
			(o.Replied || o.LatestRun == run && terminalRun(o.Status) || seen && o.RunCount > seenAt)
		if !ended {
			continue
		}
		status := o.Status
		if o.LatestRun != run {
			status = "ended; a later run followed"
		}
		if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
			_, err := r.db.Exec(`UPDATE heimdall_t3_children SET status=?, run=? WHERE id=?`, "finished ("+status+")", run, in.ID)
			return "", err
		}, dbos.WithStepName("child-ended")); err != nil {
			return "", err
		}
		return r.childWake(ctx, in.Issue, "observer:t3:"+in.Thread)
	}
}

// wakePlan is one recorded decision to wake a task: the key to send with,
// or why not now.
type wakePlan struct {
	Key     string `json:"key,omitempty"`
	Attempt int    `json:"attempt"`
	Wakes   int    `json:"wakes"`
	Wait    string `json:"wait,omitempty"` // refused for now: try again
	Stop    string `json:"stop,omitempty"` // never: the task closed, or no assistant works it
}

// childWake ends the task's wait once for an ended child. The key is
// chosen in one step and sent in the next, so a crash between the send
// and its record re-sends the same key (one event), never a new one. A
// task that cannot take it now (paused, held, not in its working phase,
// or a read error) is asked again with backoff, until it takes it, closes,
// or has started a later wake on its own, which lists the child anyway.
func (r *Runtime) childWake(ctx dbos.Context, issue, by string) (string, error) {
	poll := r.opt.Observe
	first := wakePlan{Attempt: -1}
	for {
		p, err := dbos.RunAsStep(ctx, func(context.Context) (wakePlan, error) { return r.planWake(issue), nil }, dbos.WithStepName("child-wake-plan"))
		if err != nil {
			return "", err
		}
		if first.Attempt < 0 && p.Attempt > 0 {
			first = p // the first healthy read: the wake count the child ended at
		}
		switch {
		case p.Stop != "":
			return p.Stop, nil
		case first.Attempt >= 0 && (p.Attempt != first.Attempt || p.Wakes > first.Wakes):
			return "a later wake lists it", nil
		case p.Key != "":
			sent, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
				if err := dbos.Send(r.ctx, issue, Event{Attempt: p.Attempt, By: by, Wake: true}, topic, dbos.WithIdempotencyKey(p.Key)); err != nil {
					return clip(err.Error(), 200), nil
				}
				return "", nil
			}, dbos.WithStepName("child-wake"))
			if err != nil {
				return "", err
			}
			if sent == "" {
				return "Sent wake to " + issue + ".", nil
			}
		}
		if _, err := dbos.Sleep(ctx, poll); err != nil {
			return "", err
		}
		poll = min(poll*2, 10*time.Minute)
	}
}

// planWake reads whether issue takes a child's wake now, and with which key.
func (r *Runtime) planWake(issue string) wakePlan {
	ws, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterWorkflowIDs(issue), dbos.WithFilterName(workflowName), dbos.WithFilterLoadInput(true))
	if err != nil {
		return wakePlan{Wait: clip(err.Error(), 200)}
	}
	if len(ws) != 1 {
		return wakePlan{Stop: issue + " is not tracked here."}
	}
	if cm, ok := decodeInput(ws[0].Input); !ok || cm.Worker != "" || r.opt.Assistant == nil {
		return wakePlan{Stop: issue + ": not woken by its assistant."}
	}
	key, attempt, wakes, refused, closed := r.wakeKey(issue, ws[0])
	switch {
	case closed:
		return wakePlan{Stop: refused}
	case refused != "":
		return wakePlan{Attempt: attempt, Wakes: wakes, Wait: refused}
	}
	return wakePlan{Key: key, Attempt: attempt, Wakes: wakes}
}

// open reports whether issue's workflow may still take a signal: running,
// or paused by the owner (it resumes later).
func (r *Runtime) open(issue string) bool {
	ws, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterWorkflowIDs(issue), dbos.WithFilterName(workflowName))
	if err != nil || len(ws) != 1 {
		return err != nil // a read error is not a close
	}
	if p, _ := ws[0].Attributes["paused_by"].(string); p != "" {
		return true
	}
	st, _ := r.State(issue)
	return (ws[0].Status == dbos.WorkflowStatusPending || ws[0].Status == dbos.WorkflowStatusEnqueued) && st.Phase != PhaseClosed
}
