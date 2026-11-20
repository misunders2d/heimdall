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
// by its own workflow (heimdall.t3child), which ends the task's wait once
// (the task_wake signal) when the child's run ends. Children in another
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
}

// TracksChildren reports whether hub T3 tracking is on: the router then
// decodes a wake's native T3 calls and records them (Child).
func (r *Runtime) TracksChildren() bool { return r.opt.T3Tracking }

// terminalRun is a T3 run or delegated task status that ends it (T3's own
// OrchestrationV2RunStatus and delegated-task statuses).
func terminalRun(status string) bool {
	return slices.Contains([]string{"completed", "failed", "cancelled", "interrupted", "rolled_back"}, status)
}

// RecordChild records a child of issue's wake key (ISSUE/attempt/work/N)
// and starts its watcher. A repeated child (same thread, environment and
// run) is one row. Only ids and statuses are stored, never a message.
func (r *Runtime) RecordChild(issue, key string, c Child) error {
	if !r.opt.T3Tracking {
		return nil
	}
	if c.Thread == "" { // visible, never dropped: the wake's own key keeps it apart
		c.Run, c.Env = key, ""
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
		status = "not watched: no T3 CLI here"
	}
	res, err := r.db.Exec(`INSERT INTO heimdall_t3_children(issue,thread,env,run,kind,task,wake,status,at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(issue,thread,env,run) DO NOTHING`,
		issue, c.Thread, c.Env, c.Run, c.Kind, c.Task, key, status, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 || status != "active" {
		return nil
	}
	in := childWatch{Issue: issue, Thread: c.Thread, Run: c.Run}
	_, err = dbos.RunWorkflow(r.ctx, r.watchChild, in, dbos.WithWorkflowID(fmt.Sprintf("t3child/%s/%s/%s", issue, c.Thread, c.Run)))
	return err
}

// ChildRow is one recorded child as shown.
type ChildRow struct {
	Child
	Wake   string
	Status string
}

// Children lists issue's recorded children, oldest first.
func (r *Runtime) Children(issue string) ([]ChildRow, error) {
	rows, err := r.db.Query(`SELECT kind,thread,env,run,task,wake,status FROM heimdall_t3_children WHERE issue=? ORDER BY id LIMIT 50`, issue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChildRow
	for rows.Next() {
		var c ChildRow
		if err := rows.Scan(&c.Kind, &c.Thread, &c.Env, &c.Run, &c.Task, &c.Wake, &c.Status); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (c ChildRow) line() string {
	if c.Thread == "" {
		return fmt.Sprintf("- %s from wake %s: %s", c.Kind, c.Wake, c.Status)
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
	cs, err := r.Children(issue)
	if err != nil || len(cs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nT3 work you started for this task (recorded by Heimdall from your native tool results, not from your text):\n")
	for _, c := range cs {
		b.WriteString(c.line() + "\n")
	}
	b.WriteString("Read a finished child with t3_thread_read before acting on it; never start the same work again.")
	return b.String()
}

type childWatch struct {
	Issue  string `json:"issue"`
	Thread string `json:"thread"`
	Run    string `json:"run,omitempty"` // "" until the first read shows one
}

type childRead struct {
	Obs  Observation `json:"obs"`
	Err  string      `json:"err,omitempty"`
	Gone bool        `json:"gone,omitempty"` // the task closed or ended: stop watching
}

// watchChild follows one child run read-only, backing off from
// Options.Observe to 10 minutes. When the run ends (it is no longer
// active and a later run followed, or the thread shows a terminal status)
// it marks the child and ends the task's wait once; it never sends,
// approves or answers anything. A read error, or a status it does not
// know, keeps watching. It stops when the task closes.
func (r *Runtime) watchChild(ctx dbos.Context, in childWatch) (string, error) {
	poll := r.opt.Observe
	for {
		if _, err := dbos.Sleep(ctx, poll); err != nil {
			return "", err
		}
		poll = min(poll*2, 10*time.Minute)
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (childRead, error) {
			if !r.open(in.Issue) {
				return childRead{Gone: true}, nil
			}
			obs, err := r.opt.Observer.Observe(sctx, "t3:"+in.Thread)
			if err != nil {
				return childRead{Err: clip(err.Error(), 200)}, nil
			}
			return childRead{Obs: obs}, nil
		}, dbos.WithStepName("child-observe"))
		if err != nil {
			return "", err
		}
		if got.Gone {
			return "task closed", nil
		}
		o := got.Obs
		if got.Err != "" {
			continue
		}
		run := in.Run
		if run == "" {
			run = or(o.ActiveRun, o.LatestRun) // launched without a run: the first one it shows
			if run == "" {
				continue // no run yet: not finished
			}
			in.Run = run
		}
		ended := o.ActiveRun != run && (o.LatestRun != run && o.LatestRun != "" || o.LatestRun == run && terminalRun(o.Status))
		if !ended {
			continue
		}
		status := o.Status
		if o.LatestRun != run {
			status = "ended; a later run followed"
		}
		out, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
			if _, err := r.db.Exec(`UPDATE OR IGNORE heimdall_t3_children SET status=?, run=CASE WHEN run='' THEN ? ELSE run END WHERE issue=? AND thread=? AND env='' AND (run=? OR run='')`, "finished ("+status+")", run, in.Issue, in.Thread, run); err != nil {
				return "", err
			}
			return r.wakeTask(in.Issue, "observer:t3:"+in.Thread), nil
		}, dbos.WithStepName("child-ended"))
		return out, err
	}
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
