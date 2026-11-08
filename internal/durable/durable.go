// Package durable keeps owner-admitted commitments alive across restarts.
// DBOS Transact (embedded, SQLite) owns persistence, recovery, waits, timers
// and queues; this package only declares the commitment's steps and adapts
// them to Heimdall's outbox. Linear stays the canonical record: the workflow
// ID is the Linear issue identifier, and this is its execution view.
//
// Slice 1 is explicit opt-in (docs/durable.md): owner `!work` commands admit
// and steer commitments. Execution is at least once per step. External
// effects carry structured idempotency keys and are reconciled before any
// repeat; nothing here is exactly once.
package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	_ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// AppVersion is pinned: DBOS recovers only workflows of the running
// application version, so it never follows the build revision. A change to
// which steps run, or their order, ships behind dbos.Patch (docs/durable.md).
const AppVersion = "heimdall-durable-1"

const (
	appName      = "heimdall"
	workflowName = "heimdall.commitment"
	topic        = "event"
	stateKey     = "state"
	defaultRes   = "default"
)

// IssuePattern is a Linear identifier.
var IssuePattern = regexp.MustCompile(`^[A-Z][A-Z0-9]{0,9}-[1-9][0-9]{0,8}$`)

// Outbox is the slice of the store a commitment delivers through.
type Outbox interface {
	EnqueueEffect(effect string, out core.Outbound) (int64, error)
	OutboundReceipt(id int64) (store.Receipt, bool, error)
}

// Dispatcher hands a brief to the owner-selected worker. It must use key as
// a native idempotency key: a repeated key returns the first receipt and
// causes no second effect. It never chooses a provider or model; the worker
// reference already names an existing native session. No production
// dispatcher is qualified yet (docs/durable.md); without one, commitments
// are dispatched by the owner and Heimdall supervises them.
type Dispatcher interface {
	Dispatch(ctx context.Context, worker, key, brief string) (receipt string, err error)
}

// Resource is one admission queue: a capacity class such as a host or
// provider. Commitments over capacity wait visibly in the queue.
type Resource struct {
	Concurrency int // active commitments on the resource; 0 = unlimited
	PerWorker   int // active commitments per worker; default 1
	RateLimit   int // starts per RatePeriod; 0 = none
	RatePeriod  time.Duration
}

type Options struct {
	Path       string // durable.db, beside heimdall.db
	Outbox     Outbox
	Dispatcher Dispatcher    // nil: owner-dispatched commitments only
	Observer   Observer      // nil: reports come only from attributed events
	Observe    time.Duration // first observation interval; doubles to 10 minutes
	Resources  map[string]Resource
	Silence    time.Duration // one no-signal notice per attempt and phase after this; <0 = none
	Wake       func(iface string)
	Linear     string // Linear workspace slug for issue links
	Log        *log.Logger
}

type Runtime struct {
	opt    Options
	ctx    dbos.Context
	db     *sql.DB
	queues map[string]dbos.Queue
}

// Open prepares the runtime; Launch starts it and recovers pending work.
func Open(opt Options) (*Runtime, error) {
	if opt.Outbox == nil || opt.Path == "" {
		return nil, errors.New("durable: outbox and path are required")
	}
	if opt.Silence == 0 {
		opt.Silence = 24 * time.Hour
	}
	if opt.Observe <= 0 {
		opt.Observe = 30 * time.Second
	}
	if opt.Log == nil {
		opt.Log = log.Default()
	}
	if opt.Resources == nil {
		opt.Resources = map[string]Resource{}
	}
	if _, ok := opt.Resources[defaultRes]; !ok {
		opt.Resources[defaultRes] = Resource{}
	}
	// DBOS's own SQLite pool settings (immediate transactions, foreign keys,
	// WAL), except synchronous FULL: admission is acknowledged only once its
	// row is durable.
	db, err := sql.Open("sqlite", "file:"+opt.Path+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	ctx, err := dbos.NewContext(context.Background(), dbos.Config{AppName: appName, SQLiteSystemDB: db, ApplicationVersion: AppVersion})
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &Runtime{opt: opt, ctx: ctx, db: db, queues: map[string]dbos.Queue{}}
	for name, res := range opt.Resources {
		per := res.PerWorker
		if per <= 0 {
			per = 1
		}
		qo := []dbos.QueueOption{dbos.WithPartitionConcurrency(per), dbos.WithQueueOnConflict(dbos.QueueConflictAlwaysUpdate)}
		if res.Concurrency > 0 {
			qo = append(qo, dbos.WithGlobalConcurrency(res.Concurrency))
		}
		if res.RateLimit > 0 && res.RatePeriod > 0 {
			qo = append(qo, dbos.WithRateLimiter(&dbos.RateLimiter{Limit: res.RateLimit, Period: res.RatePeriod}))
		}
		q, err := dbos.RegisterQueue(ctx, "work:"+name, qo...)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("durable: queue %s: %w", name, err)
		}
		r.queues[name] = q
	}
	dbos.RegisterWorkflow(ctx, r.commitment, dbos.WithWorkflowName(workflowName))
	return r, nil
}

// SetWake connects delivery to the router; call before Launch.
func (r *Runtime) SetWake(f func(iface string)) { r.opt.Wake = f }

// Launch starts queues and recovers every pending commitment.
func (r *Runtime) Launch() error { return dbos.Launch(r.ctx) }

// Close stops the runtime; pending commitments resume at the next Launch.
func (r *Runtime) Close() {
	dbos.Shutdown(r.ctx, 10*time.Second)
	r.db.Close()
}

// Commitment is the workflow input. Creator and Route come from the
// verified live owner message that admitted it, never from arguments.
type Commitment struct {
	Issue    string       `json:"issue"` // Linear identifier; the workflow ID
	Title    string       `json:"title"`
	Creator  string       `json:"creator"` // principal key
	Route    core.ConvKey `json:"route"`   // replies go only here
	ReplyTo  string       `json:"reply_to"`
	Resource string       `json:"resource"`
	Worker   string       `json:"worker"`          // owner-selected worker reference; also the partition
	Brief    string       `json:"brief,omitempty"` // dispatched when a Dispatcher is configured
	Gates    []string     `json:"gates,omitempty"` // evidence prechecks, e.g. git:/repo
	Next     string       `json:"next,omitempty"`  // authorized work left after acceptance; "" = single step
}

// Event is an attributed signal to a commitment. By is the verified
// principal; Attempt binds reports and verdicts to one attempt.
type Event struct {
	Kind     string `json:"kind"` // report | accept | reject | close
	Attempt  int    `json:"attempt"`
	By       string `json:"by"`
	Evidence string `json:"evidence,omitempty"`
	Note     string `json:"note,omitempty"`
}

// State is the commitment's published execution state.
type State struct {
	Phase    string    `json:"phase"`
	Attempt  int       `json:"attempt"`
	Evidence string    `json:"evidence,omitempty"`
	Verdict  string    `json:"verdict,omitempty"` // accepted by …, rejected: …
	Next     string    `json:"next"`              // the owned next action
	Since    time.Time `json:"since"`
	Receipts []int64   `json:"receipts,omitempty"` // outbound ids delivered
	Blocked  string    `json:"blocked,omitempty"`  // last permanently failed delivery
	Note     string    `json:"note,omitempty"`     // observer state, e.g. unavailable
	Runs     []string  `json:"runs,omitempty"`     // worker runs already used as evidence
}

// Phases. Only closed is terminal.
const (
	PhaseDispatching = "dispatching"
	PhaseWorking     = "working"
	PhaseWaiting     = "worker_waiting" // the worker asked a question in its own host
	PhaseReported    = "reported"
	PhaseAccepted    = "accepted"
	PhaseOpen        = "open"
	PhaseBlocked     = "delivery_blocked"
	PhaseClosed      = "closed"
)

// run is one execution of a commitment. buf holds events that arrived
// while the workflow waited for something else; replay rebuilds it from the
// checkpointed Recv results.
type run struct {
	c   Commitment
	st  State
	buf []Event
}

// closed carries the owner's close event out of any wait.
type closed struct{ ev Event }

func (closed) Error() string { return "closed by owner" }

func (r *Runtime) commitment(ctx dbos.Context, c Commitment) (State, error) {
	w := &run{c: c, st: State{Attempt: 1}}
	err := r.supervise(ctx, w)
	var cl closed
	if errors.As(err, &cl) {
		return r.close(ctx, w, cl.ev)
	}
	return w.st, err
}

func (r *Runtime) supervise(ctx dbos.Context, w *run) error {
	c := w.c
	for {
		if r.opt.Dispatcher != nil && c.Worker != "" {
			if err := r.publish(ctx, w, PhaseDispatching, "dispatch attempt to "+c.Worker); err != nil {
				return err
			}
			key := effectKey(c.Issue, w.st.Attempt, "dispatch")
			if _, err := retry(ctx, "dispatch", func(sctx context.Context) (string, error) {
				return r.opt.Dispatcher.Dispatch(sctx, c.Worker, key, c.Brief)
			}); err != nil {
				return err
			}
		}
		if err := r.publish(ctx, w, PhaseWorking, "wait for the attempt's report"); err != nil {
			return err
		}
		ev, err := r.awaitReport(ctx, w)
		if err != nil {
			return err
		}
		w.st.Evidence = ev.Evidence
		why, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) { return precheck(c.Gates, ev.Evidence), nil }, dbos.WithStepName("precheck"))
		if err != nil {
			return err
		}
		if why != "" {
			if err := r.reject(ctx, w, "evidence rejected: "+why); err != nil {
				return err
			}
			continue
		}
		if err := r.publish(ctx, w, PhaseReported, "verify the evidence: !work accept or !work reject"); err != nil {
			return err
		}
		if err := r.deliver(ctx, w, fmt.Sprintf("report/%d", w.st.Attempt), fmt.Sprintf("%s attempt %d: UNACCEPTED worker report. Evidence: %s\nNot done until verified: !work accept %s or !work reject %s <reason>", c.Issue, w.st.Attempt, ev.Evidence, c.Issue, c.Issue), true); err != nil {
			return err
		}
		v, err := r.await(ctx, w, "accept", "reject")
		if err != nil {
			return err
		}
		if v.Kind == "reject" {
			if err := r.reject(ctx, w, "rejected by "+v.By+": "+v.Note); err != nil {
				return err
			}
			continue
		}
		w.st.Verdict = "accepted by " + v.By
		next := "retire"
		if c.Next != "" {
			next = "authorized next action: " + c.Next
		}
		if err := r.publish(ctx, w, PhaseAccepted, next); err != nil {
			return err
		}
		if err := r.deliver(ctx, w, fmt.Sprintf("accepted/%d", w.st.Attempt), fmt.Sprintf("%s accepted (attempt %d, %s). Evidence: %s", c.Issue, w.st.Attempt, w.st.Verdict, w.st.Evidence), true); err != nil {
			return err
		}
		if c.Next == "" {
			return r.publish(ctx, w, PhaseClosed, "none: delivered and retired")
		}
		// Work remains: the commitment stays open until the owner closes it.
		if err := r.publish(ctx, w, PhaseOpen, "authorized next action: "+c.Next+" (owner authority; !work close when done)"); err != nil {
			return err
		}
		_, err = r.await(ctx, w)
		return err
	}
}

func (r *Runtime) publish(ctx dbos.Context, w *run, phase, next string) error {
	w.st.Phase, w.st.Next = phase, next
	now, err := dbos.RunAsStep(ctx, func(context.Context) (time.Time, error) { return time.Now().UTC(), nil }, dbos.WithStepName("clock"))
	if err != nil {
		return err
	}
	w.st.Since = now
	return dbos.SetEvent(ctx, stateKey, w.st)
}

// next returns the oldest buffered event, else waits durably for one;
// ok=false means wait passed in silence.
func (r *Runtime) next(ctx dbos.Context, w *run, wait time.Duration) (ev Event, ok bool, err error) {
	if len(w.buf) > 0 {
		ev, w.buf = w.buf[0], w.buf[1:]
		return ev, true, nil
	}
	ev, err = dbos.Recv[Event](ctx, topic, wait)
	if errors.Is(err, dbos.ErrTimeout) {
		return Event{}, false, nil
	}
	return ev, err == nil && ev.Kind != "", err
}

// await waits durably for an event of the current attempt with one of
// kinds; close always ends the wait (as a closed error). Stale attempts and
// kinds that mean nothing in this phase are dropped. Silence is not
// terminal: at most one notice per attempt and phase (none if Silence < 0),
// then the wait continues quietly; !work shows how long it has been.
func (r *Runtime) await(ctx dbos.Context, w *run, kinds ...string) (Event, error) {
	wait, noticed := r.opt.Silence, r.opt.Silence < 0
	if wait <= 0 {
		wait = maxWait
	}
	for {
		ev, ok, err := r.next(ctx, w, wait)
		if err != nil {
			return Event{}, err
		}
		if !ok {
			if !noticed {
				noticed = true
				text := fmt.Sprintf("%s attempt %d: no signal for %s (%s). Still open; next: %s. No further notice until it changes.", w.c.Issue, w.st.Attempt, wait, w.st.Phase, w.st.Next)
				if err := r.deliver(ctx, w, fmt.Sprintf("silence/%d/%s", w.st.Attempt, w.st.Phase), text, false); err != nil {
					return Event{}, err
				}
			}
			wait = maxWait
			continue
		}
		if ev.Kind == "close" {
			return ev, closed{ev}
		}
		if ev.Attempt == w.st.Attempt && slices.Contains(kinds, ev.Kind) {
			return ev, nil
		}
	}
}

const maxWait = 7 * 24 * time.Hour

// awaitReport waits for the current attempt's report. With an observer for
// the worker it also reads the worker's own state, backing off from
// Options.Observe to 10 minutes: a run that finished after the attempt
// began becomes the report (attributed to the observer, still UNACCEPTED).
// A run already used as evidence, or finished before the attempt, never
// counts. A worker question is shown as worker_waiting; an unavailable
// observer is a visible technical note, never terminal.
func (r *Runtime) awaitReport(ctx dbos.Context, w *run) (Event, error) {
	o := r.opt.Observer
	if o == nil || !o.Observable(w.c.Worker) {
		return r.await(ctx, w, "report")
	}
	observe := func() (Observation, error) {
		return dbos.RunAsStep(ctx, func(sctx context.Context) (Observation, error) { return o.Observe(sctx, w.c.Worker) },
			dbos.WithStepName("observe"), dbos.WithStepMaxRetries(2), dbos.WithStepBaseInterval(time.Second))
	}
	base, baseErr := observe()
	poll, quiet, noticed := r.opt.Observe, time.Duration(0), r.opt.Silence < 0
	for {
		ev, ok, err := r.next(ctx, w, poll)
		if err != nil {
			return Event{}, err
		}
		if ok {
			if ev.Kind == "close" {
				return ev, closed{ev}
			}
			if ev.Kind == "report" && ev.Attempt == w.st.Attempt {
				return ev, nil
			}
			continue
		}
		quiet += poll
		poll = min(poll*2, 10*time.Minute)
		obs, err := observe()
		note := ""
		if err != nil {
			note = "observer unavailable: " + err.Error()
		}
		if note != w.st.Note {
			w.st.Note = note
			if err := r.publish(ctx, w, w.st.Phase, w.st.Next); err != nil {
				return Event{}, err
			}
		}
		if err != nil {
			continue
		}
		if baseErr != nil { // first good read is the attempt's baseline
			base, baseErr = obs, nil
			continue
		}
		if (obs.Pending > 0) != (w.st.Phase == PhaseWaiting) {
			phase, next := PhaseWorking, "wait for the attempt's report"
			if obs.Pending > 0 {
				phase, next = PhaseWaiting, "the worker asked for input in "+w.c.Worker
			}
			if err := r.publish(ctx, w, phase, next); err != nil {
				return Event{}, err
			}
			if obs.Pending > 0 {
				if err := r.deliver(ctx, w, fmt.Sprintf("waiting/%d/%s", w.st.Attempt, obs.LatestRun), fmt.Sprintf("%s attempt %d: the worker (%s) is waiting for an answer.", w.c.Issue, w.st.Attempt, w.c.Worker), false); err != nil {
					return Event{}, err
				}
			}
			continue
		}
		fresh := obs.LatestRun != "" && obs.ActiveRun == "" && obs.Pending == 0 &&
			(obs.LatestRun != base.LatestRun || base.ActiveRun != "") && !slices.Contains(w.st.Runs, obs.LatestRun)
		if fresh {
			w.st.Runs = append(w.st.Runs, obs.LatestRun)
			return Event{Kind: "report", Attempt: w.st.Attempt, By: "observer:" + w.c.Worker,
				Evidence: fmt.Sprintf("%s run %s %s: %s", w.c.Worker, obs.LatestRun, obs.Status, or(obs.Reply, "(no reply text)"))}, nil
		}
		if !noticed && r.opt.Silence > 0 && quiet >= r.opt.Silence {
			noticed = true
			text := fmt.Sprintf("%s attempt %d: no worker result for %s (%s). Still open; observing %s. No further notice until it changes.", w.c.Issue, w.st.Attempt, quiet, w.st.Phase, w.c.Worker)
			if err := r.deliver(ctx, w, fmt.Sprintf("silence/%d/%s", w.st.Attempt, w.st.Phase), text, false); err != nil {
				return Event{}, err
			}
		}
	}
}

func (r *Runtime) reject(ctx dbos.Context, w *run, why string) error {
	w.st.Verdict = why
	text := fmt.Sprintf("%s attempt %d %s. Next: attempt %d", w.c.Issue, w.st.Attempt, why, w.st.Attempt+1)
	if err := r.deliver(ctx, w, fmt.Sprintf("rejected/%d", w.st.Attempt), text, true); err != nil {
		return err
	}
	w.st.Attempt++
	w.st.Evidence = ""
	return nil
}

func (r *Runtime) close(ctx dbos.Context, w *run, ev Event) (State, error) {
	w.st.Verdict = "closed by " + ev.By
	if ev.Note != "" {
		w.st.Verdict += ": " + ev.Note
	}
	if err := r.deliver(ctx, w, "closed", fmt.Sprintf("%s closed (%s).", w.c.Issue, w.st.Verdict), false); err != nil {
		return w.st, err
	}
	return w.st, r.publish(ctx, w, PhaseClosed, "none: closed by owner")
}

// deliver enqueues text to the commitment's original route once per effect
// key and waits durably for that reply's receipt. Transient failures are
// the outbox's own retry policy. A permanent failure is never replayed
// blindly: blocking deliveries publish delivery_blocked, keep the failed
// receipt, and wait for an explicit !work retry (re-enqueued under a new
// key) or close; other events are kept for afterwards. A non-blocking
// delivery (notices) records the failure and moves on.
func (r *Runtime) deliver(ctx dbos.Context, w *run, effect, text string, blocking bool) error {
	c := w.c
	for n := 0; ; n++ {
		key := c.Issue + "/" + effect
		if n > 0 {
			key = fmt.Sprintf("%s#retry%d", key, n)
		}
		id, err := retry(ctx, "deliver", func(context.Context) (int64, error) {
			return r.opt.Outbox.EnqueueEffect(key, core.Outbound{Conv: c.Route, ReplyTo: c.ReplyTo, Text: text + "\n— heimdall"})
		})
		if err != nil {
			return err
		}
		rc, err := r.receipt(ctx, c, id)
		if err != nil {
			return err
		}
		if rc.State == store.OutSent {
			w.st.Receipts = append(w.st.Receipts, id)
			return nil
		}
		w.st.Blocked = fmt.Sprintf("outbound %d (%s) failed: %s", id, key, rc.Error)
		if !blocking {
			return nil
		}
		phase, next := w.st.Phase, w.st.Next
		if err := r.publish(ctx, w, PhaseBlocked, "reconcile "+w.st.Blocked+"; then !work retry or !work close"); err != nil {
			return err
		}
		for {
			ev, err := dbos.Recv[Event](ctx, topic, maxWait)
			if errors.Is(err, dbos.ErrTimeout) {
				continue
			}
			if err != nil {
				return err
			}
			if ev.Kind == "close" {
				return closed{ev}
			}
			if ev.Kind == "retry" {
				break
			}
			w.buf = append(w.buf, ev)
		}
		w.st.Blocked = ""
		if err := r.publish(ctx, w, phase, next); err != nil {
			return err
		}
	}
}

// receipt waits durably until outbound id is sent or failed.
func (r *Runtime) receipt(ctx dbos.Context, c Commitment, id int64) (store.Receipt, error) {
	pause := time.Second
	for {
		rc, err := retry(ctx, "receipt", func(context.Context) (store.Receipt, error) {
			if r.opt.Wake != nil {
				r.opt.Wake(c.Route.Interface)
			}
			rc, ok, err := r.opt.Outbox.OutboundReceipt(id)
			if err == nil && !ok {
				err = fmt.Errorf("outbound %d missing", id)
			}
			return rc, err
		})
		if err != nil || rc.State == store.OutSent || rc.State == store.OutFailed {
			return rc, err
		}
		if _, err := dbos.Sleep(ctx, pause); err != nil {
			return rc, err
		}
		pause = min(pause*2, 10*time.Minute)
	}
}

// retry runs fn as a checkpointed step. Technical failures retry with
// backoff inside the step, then durably every 10 minutes: they never end the
// commitment and never wait for the owner.
func retry[R any](ctx dbos.Context, name string, fn func(context.Context) (R, error)) (R, error) {
	for {
		v, err := dbos.RunAsStep(ctx, fn, dbos.WithStepName(name), dbos.WithStepMaxRetries(5), dbos.WithStepBaseInterval(time.Second), dbos.WithStepBackoffFactor(2))
		if err == nil || ctx.Err() != nil {
			return v, err
		}
		if _, err := dbos.Sleep(ctx, 10*time.Minute); err != nil {
			return v, err
		}
	}
}

func effectKey(issue string, attempt int, effect string) string {
	return fmt.Sprintf("%s/%d/%s", issue, attempt, effect)
}
