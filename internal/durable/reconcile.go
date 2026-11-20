package durable

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/misunders2d/heimdall/internal/core"
)

// Downtime reconcile (T3 tracking). A T3 hook fired while Heimdall was down
// is lost; nothing replays it. At startup Heimdall lists the threads of the
// T3 projects it has taken requests from, read-only, and runs the ordinary
// intake probe for each owner-created thread changed since this runtime
// was last alive (a heartbeat in durable.db; with none recorded, nothing
// is reconciled). The probe is bound to the trusted T3 launcher's owner as
// the scheduler control binds it now (T3Owner: the registered private
// conversation, a current owner, Slack verified live), never to a stored
// request; it is asked again as each request is admitted, and with no such
// owner nothing is admitted. The probe still decides alone from T3's own
// createdBy mark, and only a per-message mark admits here (never the
// thread's mark alone); its (thread, message, run) row makes a repeat a
// no-op. A reconciled task works only if its run ended interrupted or
// failed, and each wake revalidates its creator as it starts.
//
// Limit: T3 lists a thread's last change (updatedAt), not when its runs
// started, so a thread only renamed or organized during the downtime is
// probed too; its latest owner request is then admitted like a missed one.

const reconcileName = "heimdall.t3reconcile"

// reconcileThreads bounds the threads probed per project, newest first
// (a variable for tests).
var reconcileThreads = 20

// reconcileWindow bounds how far back a reconcile looks.
const reconcileWindow = 7 * 24 * time.Hour

// reconcileLists bounds the reads of one project's thread list (T3 may
// start after Heimdall); they back off from Options.Observe to 10 minutes.
const reconcileLists = 8

// aliveEvery is the last-alive heartbeat's period (a variable for tests).
var aliveEvery = time.Minute

// T3Owner is the owner a trusted T3 registration binds to now.
type T3Owner struct {
	Principal         core.Principal `json:"principal"`
	Route             core.ConvKey   `json:"route"`
	OAuthActor        string         `json:"oauth_actor,omitempty"` // a Slack owner's, from the live check
	OAuthInstallation string         `json:"oauth_installation,omitempty"`
}

// T3OwnerFunc reads the trusted T3 owner binding now; an error means no
// current owner (no launcher, no registered conversation, not an owner,
// or a failed live check).
type T3OwnerFunc func(ctx context.Context) (T3Owner, error)

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

type reconcileStart struct {
	ID    string `json:"id"`
	Since int64  `json:"since"` // unix ms: when this runtime was last alive before this start
}

type reconcilePlan struct {
	Projects []string `json:"projects"`
	Owner    T3Owner  `json:"owner"`
	Skip     string   `json:"skip,omitempty"` // why nothing is reconciled
}

type reconcileList struct {
	Threads []ListedThread `json:"threads"`
	Err     string         `json:"err,omitempty"`
}

// startReconcile starts one reconcile at startup (T3 tracking only); a
// failure to start is one log line, never a startup failure.
func (r *Runtime) startReconcile(since time.Time) {
	if !r.opt.T3Tracking {
		return
	}
	if _, ok := r.opt.Observer.(ThreadLister); !ok {
		return
	}
	if since.IsZero() {
		r.opt.Log.Printf("durable: T3 reconcile skipped: no earlier run recorded")
		return
	}
	id := "t3reconcile/" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := dbos.RunWorkflow(r.ctx, r.reconcile, reconcileStart{ID: id, Since: since.UnixMilli()}, dbos.WithWorkflowID(id)); err != nil {
		r.opt.Log.Printf("durable: T3 reconcile not started: %v", err)
	}
}

// reconcile probes owner threads changed while Heimdall was down.
func (r *Runtime) reconcile(ctx dbos.Context, in reconcileStart) (string, error) {
	lister, ok := r.opt.Observer.(ThreadLister)
	if !ok {
		return "no lister", nil
	}
	plan, err := dbos.RunAsStep(ctx, func(sctx context.Context) (reconcilePlan, error) { return r.reconcilePlan(sctx) }, dbos.WithStepName("reconcile-plan"))
	if err != nil {
		return "", err
	}
	if plan.Skip != "" {
		r.opt.Log.Printf("durable: T3 reconcile skipped: %s", plan.Skip)
		return "skipped: " + plan.Skip, nil
	}
	// The last hook may have raced the last heartbeat.
	since := time.UnixMilli(in.Since).Add(-aliveEvery)
	probes := 0
	for _, project := range plan.Projects {
		var got reconcileList
		poll := r.opt.Observe
		for try := 1; ; try++ {
			got, err = dbos.RunAsStep(ctx, func(sctx context.Context) (reconcileList, error) {
				ts, err := lister.ListThreads(sctx, project)
				if err != nil {
					return reconcileList{Err: clip(err.Error(), 200)}, nil
				}
				return reconcileList{Threads: ts}, nil
			}, dbos.WithStepName("reconcile-list"))
			if err != nil {
				return "", err
			}
			if got.Err == "" || try >= reconcileLists {
				break
			}
			if _, err := dbos.Sleep(ctx, poll); err != nil {
				return "", err
			}
			poll = min(poll*2, 10*time.Minute)
		}
		if got.Err != "" {
			r.opt.Log.Printf("durable: T3 reconcile: project %s unreadable", project)
			continue
		}
		var ts []ListedThread
		for _, t := range got.Threads {
			if t.CreatedBy != "user" || t.Parent != nil && *t.Parent != "" || !validThread(t.ID) || Delegated(t.ID) || Delegated(t.Title) || t.Updated.Before(since) {
				continue
			}
			ts = append(ts, t)
		}
		// Newest first, so a cap leaves out the oldest.
		slices.SortStableFunc(ts, func(a, b ListedThread) int { return cmp.Compare(b.Updated.UnixNano(), a.Updated.UnixNano()) })
		if len(ts) > reconcileThreads {
			r.opt.Log.Printf("durable: T3 reconcile: project %s: %d changed thread(s) left out (cap %d)", project, len(ts)-reconcileThreads, reconcileThreads)
			ts = ts[:reconcileThreads]
		}
		o := plan.Owner
		for _, t := range ts {
			pin := intakeProbe{Thread: t.ID, Creator: o.Principal.Key(), Owner: o.Principal, Route: o.Route, Private: true, OAuthActor: o.OAuthActor, OAuthInst: o.OAuthInstallation, Follow: true, Reconcile: true}
			if _, err := dbos.RunWorkflow(ctx, r.probe, pin, dbos.WithWorkflowID("t3intake/"+t.ID+"/"+in.ID)); err != nil {
				return "", err
			}
		}
		probes += len(ts)
	}
	if probes > 0 {
		r.opt.Log.Printf("durable: T3 reconcile probed %d thread(s)", probes)
	}
	return fmt.Sprintf("probed %d", probes), nil
}

// reconcilePlan reads the trusted T3 owner now and the projects of the
// requests taken within the window.
func (r *Runtime) reconcilePlan(ctx context.Context) (reconcilePlan, error) {
	if r.opt.T3Owner == nil {
		return reconcilePlan{Skip: "no trusted T3 owner binding"}, nil
	}
	o, err := r.opt.T3Owner(ctx)
	if err != nil {
		return reconcilePlan{Skip: "no current trusted T3 owner"}, nil
	}
	rows, err := r.db.Query(`SELECT DISTINCT project FROM heimdall_t3_intakes WHERE project<>'' AND at>=? ORDER BY project`, time.Now().Add(-reconcileWindow).UnixMilli())
	if err != nil {
		return reconcilePlan{}, err
	}
	defer rows.Close()
	p := reconcilePlan{Owner: o}
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return p, err
		}
		p.Projects = append(p.Projects, project)
	}
	return p, rows.Err()
}

// stillT3Owner asks again, as a reconciled request is admitted, that the
// trusted T3 owner is still the one the probe was bound to.
func (r *Runtime) stillT3Owner(ctx context.Context, in intakeProbe) bool {
	if r.opt.T3Owner == nil {
		return false
	}
	o, err := r.opt.T3Owner(ctx)
	return err == nil && o.Principal.Key() == in.Creator && o.Route == in.Route
}

// alive reads when this runtime was last alive (zero: never recorded).
func (r *Runtime) alive() time.Time {
	var ms int64
	if err := r.db.QueryRow(`SELECT value FROM heimdall_meta WHERE key='alive'`).Scan(&ms); err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func (r *Runtime) markAlive() {
	if _, err := r.db.Exec(`INSERT INTO heimdall_meta(key,value) VALUES('alive',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, time.Now().UnixMilli()); err != nil {
		r.opt.Log.Printf("durable: last-alive not recorded: %v", err)
	}
}

// heartbeat records the last-alive time (T3 tracking only) every
// aliveEvery and at Close: the start of the next downtime window.
func (r *Runtime) heartbeat() {
	if !r.opt.T3Tracking {
		return
	}
	r.markAlive()
	r.beat = make(chan struct{})
	r.beatDone.Add(1)
	go func() {
		defer r.beatDone.Done()
		t := time.NewTicker(aliveEvery)
		defer t.Stop()
		for {
			select {
			case <-r.beat:
				return
			case <-t.C:
				r.markAlive()
			}
		}
	}()
}
