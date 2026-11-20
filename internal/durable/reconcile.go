package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Downtime reconcile (T3 tracking). A T3 hook fired while Heimdall was down
// is lost; nothing replays it. At startup Heimdall lists the threads of the
// T3 projects it has taken requests from, read-only, and runs the ordinary
// intake probe for each owner-created thread changed since its last intake
// there, bound to the creator, route and sign-ins of that project's latest
// T3 request. The probe still decides alone from T3's own createdBy mark,
// and only a per-message mark admits here (never the thread's mark alone);
// its (thread, message, run) row makes a repeat a no-op. A reconciled task
// works only if its run ended interrupted or failed, and each wake
// revalidates its creator as it starts.

const reconcileName = "heimdall.t3reconcile"

// reconcileThreads bounds the threads probed per project.
const reconcileThreads = 20

// reconcileWindow bounds how far back a reconcile looks.
const reconcileWindow = 7 * 24 * time.Hour

// ListedThread is one thread of a project as `thread list --json` shows it.
type ListedThread struct {
	ID        string    `json:"threadId"`
	Title     string    `json:"title"`
	CreatedBy string    `json:"createdBy"`
	Parent    *string   `json:"parentThreadId"`
	Updated   time.Time `json:"updatedAt"`
}

// ThreadLister lists a T3 project's threads, read-only (the T3 CLI).
type ThreadLister interface {
	ListThreads(ctx context.Context, project string) ([]ListedThread, error)
}

// ListThreads runs `thread list --project <id> --json`. The CLI may print
// log lines first; the list is the first line-initial JSON array.
func (t T3) ListThreads(ctx context.Context, project string) ([]ListedThread, error) {
	if len(t.Command) == 0 || project == "" || !validThread(project) {
		return nil, errors.New("t3 thread list: no CLI or project")
	}
	tt := t
	if tt.Timeout <= 0 || tt.Timeout > intakeTimeout {
		tt.Timeout = intakeTimeout
	}
	out, err := tt.run(ctx, "", "thread", "list", "--project", project, "--json")
	if err != nil {
		return nil, fmt.Errorf("t3 thread list: %w", err)
	}
	return parseList(out)
}

func parseList(out []byte) ([]ListedThread, error) {
	for off := 0; off < len(out); {
		line := out[off:]
		if n := bytes.IndexByte(line, '\n'); n >= 0 {
			line = line[:n+1]
		}
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("[")) {
			var v []ListedThread
			if json.NewDecoder(bytes.NewReader(out[off:])).Decode(&v) == nil {
				return v, nil
			}
		}
		off += len(line)
	}
	return nil, errors.New("t3 thread list: unexpected output")
}

type reconcileProject struct {
	Project string `json:"project"`
	Issue   string `json:"issue"` // the project's latest T3-<n>: its binding
	Since   int64  `json:"since"` // unix ms of that intake
}

type reconcilePlan struct {
	Projects []reconcileProject `json:"projects"`
}

type reconcileList struct {
	Threads []ListedThread `json:"threads"`
	Err     string         `json:"err,omitempty"`
}

// startReconcile starts one reconcile at startup (T3 tracking only); a
// failure to start is one log line, never a startup failure.
func (r *Runtime) startReconcile() {
	if !r.opt.T3Tracking {
		return
	}
	if _, ok := r.opt.Observer.(ThreadLister); !ok {
		return
	}
	id := "t3reconcile/" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := dbos.RunWorkflow(r.ctx, r.reconcile, id, dbos.WithWorkflowID(id)); err != nil {
		r.opt.Log.Printf("durable: T3 reconcile not started: %v", err)
	}
}

// reconcile probes owner threads changed while Heimdall was down.
func (r *Runtime) reconcile(ctx dbos.Context, id string) (string, error) {
	lister, ok := r.opt.Observer.(ThreadLister)
	if !ok {
		return "no lister", nil
	}
	plan, err := dbos.RunAsStep(ctx, func(context.Context) (reconcilePlan, error) { return r.reconcilePlan() }, dbos.WithStepName("reconcile-plan"))
	if err != nil {
		return "", err
	}
	probes := 0
	for _, p := range plan.Projects {
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (reconcileList, error) {
			ts, err := lister.ListThreads(sctx, p.Project)
			if err != nil {
				return reconcileList{Err: clip(err.Error(), 200)}, nil
			}
			return reconcileList{Threads: ts}, nil
		}, dbos.WithStepName("reconcile-list"))
		if err != nil {
			return "", err
		}
		if got.Err != "" {
			r.opt.Log.Printf("durable: T3 reconcile: project %s unreadable", p.Project)
			continue
		}
		in, ok := r.probeBinding(p.Issue)
		if !ok {
			continue
		}
		since := time.UnixMilli(p.Since).Add(-time.Hour) // the last hook may have raced the read
		n := 0
		for _, t := range got.Threads {
			if n >= reconcileThreads {
				break
			}
			if t.CreatedBy != "user" || t.Parent != nil && *t.Parent != "" || !validThread(t.ID) || Delegated(t.ID) || Delegated(t.Title) || t.Updated.Before(since) {
				continue
			}
			n++
			pin := in
			pin.Thread = t.ID
			if _, err := dbos.RunWorkflow(ctx, r.probe, pin, dbos.WithWorkflowID("t3intake/"+t.ID+"/"+id)); err != nil {
				return "", err
			}
		}
		probes += n
	}
	if probes > 0 {
		r.opt.Log.Printf("durable: T3 reconcile probed %d thread(s)", probes)
	}
	return fmt.Sprintf("probed %d", probes), nil
}

// reconcilePlan reads each project's latest intake, within the window.
func (r *Runtime) reconcilePlan() (reconcilePlan, error) {
	rows, err := r.db.Query(`SELECT project, n, at FROM heimdall_t3_intakes i WHERE project<>'' AND n=(SELECT max(n) FROM heimdall_t3_intakes WHERE project=i.project) AND at>=? ORDER BY n`, time.Now().Add(-reconcileWindow).UnixMilli())
	if err != nil {
		return reconcilePlan{}, err
	}
	defer rows.Close()
	var p reconcilePlan
	for rows.Next() {
		var rp reconcileProject
		var n int64
		if err := rows.Scan(&rp.Project, &n, &rp.Since); err != nil {
			return p, err
		}
		rp.Issue = fmt.Sprintf("T3-%d", n)
		p.Projects = append(p.Projects, rp)
	}
	return p, rows.Err()
}

// probeBinding is the probe input of issue's stored T3 request: the same
// verified creator, route and native sign-ins its hook was bound to.
func (r *Runtime) probeBinding(issue string) (intakeProbe, bool) {
	cm, ok := r.input(issue)
	if !ok || cm.Intake == nil || cm.Origin != OriginTrustedT3 {
		return intakeProbe{}, false
	}
	return intakeProbe{Creator: cm.Creator, Owner: cm.Owner, Route: cm.Route, Private: cm.Private, OAuthActor: cm.OAuthActor, OAuthInst: cm.OAuthInstallation, Follow: true, Reconcile: true}, true
}
