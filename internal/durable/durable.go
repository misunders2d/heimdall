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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"strings"
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
	dispatchName = "heimdall.dispatch"
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

// Dispatcher hands one attempt's brief to the owner-selected worker. The
// runtime owns idempotency: before Dispatch it durably claims req.Key
// (dispatch_claims), so a key is sent at most once by Heimdall, whatever the
// native route dedupes. Dispatch returns the native receipt, or:
//   - NotSent: nothing was sent (the host was not reachable at all); the
//     claim is released and the dispatch retried.
//   - Permanent: refused before or by the host (stale creator, invalid
//     worker, native denial) with no effect; held, never replayed.
//   - any other error: the outcome is unknown. The claim stays and the next
//     try calls Reconcile instead of sending again.
//
// Reconcile reads the worker (read-only) for the exact message carrying
// req.Marker; not found is held visibly, never resent. A dispatcher never
// chooses a provider or model: the worker reference names an existing
// native thread.
type Dispatcher interface {
	Dispatch(ctx context.Context, req DispatchRequest) (Receipt, error)
	Reconcile(ctx context.Context, req DispatchRequest) (rcpt Receipt, found bool, err error)
}

// Waiter is a Dispatcher that also waits, read-only, for the run its
// dispatch started, through the same native route: for a worker the local
// observer cannot read (another T3 environment). finished=false is a
// timeout or a run still going; a Permanent error holds.
type Waiter interface {
	Wait(ctx context.Context, req DispatchRequest, run string) (status string, finished bool, err error)
	// Read returns a finished run's own result body, read natively; a
	// Permanent error (no bound read, another run, no or incomplete body)
	// holds.
	Read(ctx context.Context, req DispatchRequest, run string) (body string, err error)
}

// DispatchRequest is one attempt's dispatch, built by the runtime from the
// admitted commitment only.
type DispatchRequest struct {
	Key     string // ISSUE/attempt/dispatch[#retryN]: the claim and the marker
	Issue   string
	Attempt int
	Owner   core.Principal // the commitment's creator; revalidated before sending
	Route   core.ConvKey
	Private bool
	// The creator's verified native OAuth actor from admission.
	OAuthActor, OAuthInstallation string
	// ClientRequestID is the native idempotency key: ISSUE/attempt/dispatch,
	// the same for !work retry turns, so a retry within the same native
	// namespace returns the first send's work.
	ClientRequestID string
	Worker          string // owner-selected at admission
	Marker          string // first line of Message: "[heimdall KEY]"
	Message         string // Marker, then the brief (or repair feedback / next step)
}

// Receipt is the worker host's own record of a dispatch: the thread, the
// run it started or joined, and the message id when the host returned one.
type Receipt struct {
	Thread   string `json:"thread"`
	Run      string `json:"run"`
	Message  string `json:"message,omitempty"`
	Delivery string `json:"delivery,omitempty"` // host's delivery, or "reconciled"
}

// NotSent marks a dispatch that certainly had no effect; it is retried.
type NotSent struct{ Err error }

func (n NotSent) Error() string { return n.Err.Error() }
func (n NotSent) Unwrap() error { return n.Err }

// Authorizer revalidates the commitment's creator on its original route
// immediately before a dispatch: still an admitted owner there.
type Authorizer func(owner core.Principal, route core.ConvKey) bool

// Reviewer runs the supervisor's acceptance review of one reported attempt
// as a server-initiated turn of the owner-selected harness (the router).
// Review is idempotent by req.Key: a repeated key never starts a second
// turn; it waits for, or returns, the first turn's persisted outcome. Only
// the runtime builds a request, from the admitted commitment.
type Reviewer interface {
	Review(ctx context.Context, req ReviewRequest) (ReviewResult, error)
}

// ReviewRequest binds a review to its commitment, attempt and route.
type ReviewRequest struct {
	Key     string // ISSUE/attempt/review[#retryN]: the turn's identity
	Issue   string
	Attempt int
	Owner   core.Principal // the commitment's creator; revalidated at launch
	Route   core.ConvKey   // the creator's original route (harness choice, admission)
	Private bool
	Prompt  string
}

// ReviewResult is a finished review turn's persisted outcome. Verdict is
// accept or reject, parsed by the router from the turn's whole answer.
// Held is a native denial or approval request, a refused creator, a stop,
// or a commitment no longer current: it is held under the same key, never
// re-invoked without an explicit !work retry. Failure is a technical or
// malformed outcome: a bounded owned retry under a new key. Neither accepts.
type ReviewResult struct {
	Turn    int64  `json:"turn"`
	Verdict string `json:"verdict,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Held    string `json:"held,omitempty"`
	Failure string `json:"failure,omitempty"`
}

// reviewRetries bounds automatic re-reviews after technical failures; the
// commitment then holds.
const reviewRetries = 3

// Permanent marks an error that retrying cannot fix (a native denial, a
// route conflict, an unreconcilable effect). The commitment holds visibly
// until an explicit !work retry or close.
type Permanent struct{ Err error }

func (p Permanent) Error() string { return p.Err.Error() }
func (p Permanent) Unwrap() error { return p.Err }

// Resource is one capacity class such as a host or provider. Its DBOS queue
// limits only active execution (dispatch), never a commitment's waits: a
// commitment waiting on a worker or a verdict holds no slot. Dispatches
// over capacity wait visibly in the queue.
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
	Authorize  Authorizer    // required with a Dispatcher
	Reviewer   Reviewer      // nil: verdicts come from the live owner (!work accept|reject)
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
	// The dispatch claim ledger: written before any send, outside DBOS's
	// step records (a step is checkpointed only when it returns).
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS heimdall_dispatch_claims(key TEXT PRIMARY KEY, state TEXT NOT NULL, receipt TEXT NOT NULL DEFAULT '', at INTEGER NOT NULL)`); err != nil {
		r.Close()
		return nil, fmt.Errorf("durable: dispatch claims: %w", err)
	}
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
	dbos.RegisterWorkflow(ctx, r.dispatch, dbos.WithWorkflowName(dispatchName))
	return r, nil
}

// SetWake connects delivery to the router; call before Launch.
func (r *Runtime) SetWake(f func(iface string)) { r.opt.Wake = f }

// SetDispatcher connects autonomous dispatch; authorize revalidates the
// creator before each send. Call before Launch.
func (r *Runtime) SetDispatcher(d Dispatcher, authorize Authorizer) {
	r.opt.Dispatcher, r.opt.Authorize = d, authorize
}

// SetReviewer connects supervisor reviews to the router; call before Launch.
func (r *Runtime) SetReviewer(v Reviewer) { r.opt.Reviewer = v }

// Reviewable reports whether issue still waits for a verdict on attempt:
// its workflow is running (not paused, cancelled or ended) and its current
// attempt is in the reported phase. The router checks it as a review turn
// starts and again as it ends, before recording a verdict.
func (r *Runtime) Reviewable(issue string, attempt int) bool {
	return r.current(issue, attempt, PhaseReported)
}

// Waitable reports whether issue still waits for attempt's work.
func (r *Runtime) Waitable(issue string, attempt int) bool {
	return r.current(issue, attempt, PhaseWorking)
}

// Dispatchable reports whether issue still dispatches attempt; the router
// checks it as a dispatch turn starts and ends.
func (r *Runtime) Dispatchable(issue string, attempt int) bool {
	return r.current(issue, attempt, PhaseDispatching)
}

// current reports whether issue's workflow is running (not paused,
// cancelled or ended) with attempt in phase.
func (r *Runtime) current(issue string, attempt int, phase string) bool {
	ws, err := dbos.ListWorkflows(r.ctx, dbos.WithFilterWorkflowIDs(issue), dbos.WithFilterName(workflowName))
	if err != nil || len(ws) != 1 || ws[0].Status != dbos.WorkflowStatusPending {
		return false
	}
	st, err := r.State(issue)
	return err == nil && st.Phase == phase && st.Attempt == attempt
}

// Launch starts queues and recovers every pending commitment.
func (r *Runtime) Launch() error {
	if r.opt.Dispatcher != nil && r.opt.Authorize == nil {
		return errors.New("durable: a dispatcher needs creator revalidation (Authorize)")
	}
	return dbos.Launch(r.ctx)
}

// Close stops the runtime; pending commitments resume at the next Launch.
func (r *Runtime) Close() {
	dbos.Shutdown(r.ctx, 10*time.Second)
	r.db.Close()
}

// Commitment is the workflow input. Creator and Route come from the
// verified live owner message that admitted it, never from arguments.
type Commitment struct {
	Issue   string         `json:"issue"` // Linear identifier; the workflow ID
	Title   string         `json:"title"`
	Creator string         `json:"creator"` // principal key
	Owner   core.Principal `json:"owner"`   // the creator as verified at admission
	// The admitting message's verified native OAuth actor: a dispatch turn
	// runs with the creator's own native sign-ins, as a !job run does.
	OAuthActor        string       `json:"oauth_actor,omitempty"`
	OAuthInstallation string       `json:"oauth_installation,omitempty"`
	Route             core.ConvKey `json:"route"`             // replies go only here
	Private           bool         `json:"private,omitempty"` // admitted in the creator's private chat
	ReplyTo           string       `json:"reply_to"`
	Resource          string       `json:"resource"`
	Worker            string       `json:"worker"`          // owner-selected worker reference; also the partition
	Brief             string       `json:"brief,omitempty"` // dispatched when a Dispatcher is configured
	Gates             []string     `json:"gates,omitempty"` // evidence prechecks, e.g. git:/repo
	Next              string       `json:"next,omitempty"`  // authorized work left after acceptance; "" = single step
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
	Blocked  string    `json:"blocked,omitempty"`  // why the commitment is held
	Note     string    `json:"note,omitempty"`     // observer or technical-retry state
	Runs     []string  `json:"runs,omitempty"`     // worker runs already used as evidence
	Seen     int       `json:"seen,omitempty"`     // the worker host's run count at the last read
	// Dispatched is the worker run the current attempt's dispatch receipt
	// names; its report is that run's result, never another run's.
	Dispatched string `json:"dispatched,omitempty"`
	Stage      string `json:"stage,omitempty"` // "next": the pre-authorized next step is the work
}

// Phases. Only closed is terminal.
const (
	PhaseDispatching = "dispatching"
	PhaseWorking     = "working"
	PhaseWaiting     = "worker_waiting" // the worker asked a question in its own host
	PhaseReported    = "reported"
	PhaseAccepted    = "accepted"
	PhaseOpen        = "open"
	PhaseHeld        = "held" // a permanent failure; waits for !work retry or close
	PhaseClosed      = "closed"
)

// run is one execution of a commitment. buf holds events that arrived
// while the workflow waited for something else; replay rebuilds it from the
// checkpointed Recv results.
type run struct {
	c   Commitment
	st  State
	buf []Event
	// The attempt's observation floor, read before dispatch: a run counts
	// only if the host's run order passed it (or it was active at the
	// floor of an owner-dispatched attempt).
	floor    int
	floorOK  bool
	floorRun string
	feedback string // the last rejection, sent as bounded repair feedback
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
		if err := r.observeFloor(ctx, w); err != nil {
			return err
		}
		if r.opt.Dispatcher != nil && c.Worker != "" {
			if err := r.publish(ctx, w, PhaseDispatching, "dispatch attempt to "+c.Worker+" (queued for "+c.Resource+" capacity)"); err != nil {
				return err
			}
			w.st.Dispatched = ""
			for n := 0; ; n++ {
				key := effectKey(c.Issue, w.st.Attempt, "dispatch")
				if n > 0 {
					key = fmt.Sprintf("%s#retry%d", key, n)
				}
				marker := "[heimdall " + key + "]"
				req := DispatchRequest{Key: key, Issue: c.Issue, Attempt: w.st.Attempt, Owner: c.Owner, Route: c.Route, Private: c.Private, OAuthActor: c.OAuthActor, OAuthInstallation: c.OAuthInstallation,
					ClientRequestID: effectKey(c.Issue, w.st.Attempt, "dispatch"), Worker: c.Worker, Marker: marker, Message: marker + "\n" + brief(w)}
				// Only the dispatch occupies the resource queue.
				h, err := dbos.RunWorkflow(ctx, r.dispatch, dispatchInput{Req: req}, dbos.WithWorkflowID(key), dbos.WithQueue(r.queues[c.Resource]), dbos.WithQueuePartitionKey(c.Worker))
				if err != nil {
					return err
				}
				res, err := h.GetResult()
				if err != nil {
					return err
				}
				if res.Held == "" {
					w.st.Dispatched = res.Receipt.Run
					break
				}
				if err := r.hold(ctx, w, "dispatch "+key+": "+res.Held); err != nil {
					return err
				}
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
			if err := r.reject(ctx, w, "evidence rejected: "+why, r.opt.Reviewer == nil); err != nil {
				return err
			}
			continue
		}
		var v Event
		if r.opt.Reviewer != nil {
			// Automated review stays internal: the report, the review and
			// its rejections are execution state (!work show), not chat
			// messages. Only the accepted result is delivered.
			if err := r.publish(ctx, w, PhaseReported, "supervisor review of the evidence"); err != nil {
				return err
			}
			v, err = r.review(ctx, w)
		} else {
			if err := r.publish(ctx, w, PhaseReported, "verify the evidence: !work accept or !work reject"); err != nil {
				return err
			}
			if err := r.deliver(ctx, w, fmt.Sprintf("report/%d", w.st.Attempt), fmt.Sprintf("%s attempt %d: UNACCEPTED worker report. Evidence: %s\nNot done until verified: !work accept %s or !work reject %s <reason>", c.Issue, w.st.Attempt, ev.Evidence, c.Issue, c.Issue), true); err != nil {
				return err
			}
			v, err = r.await(ctx, w, "accept", "reject")
		}
		if err != nil {
			return err
		}
		if v.Kind == "reject" {
			if err := r.reject(ctx, w, "rejected by "+v.By+": "+v.Note, r.opt.Reviewer == nil); err != nil {
				return err
			}
			continue
		}
		w.st.Verdict = "accepted by " + v.By
		if v.Note != "" {
			w.st.Verdict += ": " + v.Note
		}
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
		if c.Next == "" || w.st.Stage == "next" {
			return r.publish(ctx, w, PhaseClosed, "none: delivered and retired")
		}
		if r.opt.Dispatcher != nil && c.Worker != "" {
			// Advance exactly the step the owner pre-authorized at
			// admission, once, as a new attempt; then retire.
			w.st.Stage, w.feedback = "next", ""
			w.st.Attempt++
			w.st.Evidence, w.st.Verdict = "", ""
			continue
		}
		// Work remains and nothing can dispatch it: the commitment stays
		// open, an owned visible dependency, until the owner closes it.
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

// observeFloor reads the worker host once, before the attempt dispatches,
// and fixes the attempt's floor. Chronology is the host's run order
// (runCount), never thread metadata such as updatedAt. If the read fails,
// the floor is the last read of an earlier attempt; with neither, the first
// healthy read in awaitReport sets it and the run it shows stays
// unresolved, never reported.
func (r *Runtime) observeFloor(ctx dbos.Context, w *run) error {
	w.floor, w.floorOK, w.floorRun = w.st.Seen, w.st.Seen > 0, ""
	o := r.opt.Observer
	if o == nil || !o.Observable(w.c.Worker) {
		return nil
	}
	type read struct {
		Obs Observation `json:"obs"`
		Err string      `json:"err,omitempty"`
	}
	got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (read, error) {
		obs, err := o.Observe(sctx, w.c.Worker)
		if err != nil {
			return read{Err: err.Error()}, nil
		}
		return read{Obs: obs}, nil
	}, dbos.WithStepName("observe-floor"))
	if err != nil || got.Err != "" {
		return err
	}
	w.floor, w.floorOK = got.Obs.RunCount, true
	w.st.Seen = max(w.st.Seen, got.Obs.RunCount)
	if r.opt.Dispatcher == nil {
		// Owner-dispatched: a run already active at the floor is the work.
		w.floorRun = got.Obs.ActiveRun
	}
	return nil
}

// awaitReport waits for the current attempt's report. With an observer for
// the worker it also reads the worker's own state, backing off from
// Options.Observe to 10 minutes. A run counts as the attempt's result only
// if it started after the attempt's floor (observeFloor). A run already
// used as evidence never counts. The result is a report, still UNACCEPTED: a
// finished run is progress, not acceptance. A worker question shows as
// worker_waiting; an unavailable observer is a visible note.
func (r *Runtime) awaitReport(ctx dbos.Context, w *run) (Event, error) {
	o := r.opt.Observer
	if wt, ok := r.opt.Dispatcher.(Waiter); ok && w.st.Dispatched != "" && (o == nil || !o.Observable(w.c.Worker)) {
		return r.awaitRun(ctx, w, wt)
	}
	if o == nil || !o.Observable(w.c.Worker) {
		return r.await(ctx, w, "report")
	}
	observe := func() (Observation, error) {
		return dbos.RunAsStep(ctx, func(sctx context.Context) (Observation, error) { return o.Observe(sctx, w.c.Worker) },
			dbos.WithStepName("observe"), dbos.WithStepMaxRetries(2), dbos.WithStepBaseInterval(time.Second))
	}
	floor, known, startRun, unresolved := w.floor, w.floorOK, w.floorRun, ""
	run := w.st.Dispatched // bound by the dispatch receipt: only this run reports
	known = known || run != ""
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
		if err == nil {
			w.st.Seen = max(w.st.Seen, obs.RunCount)
			if !known {
				known, floor = true, obs.RunCount
				if obs.LatestRun != "" {
					unresolved = fmt.Sprintf("observation unresolved: %s run %s (%s) may predate attempt %d (no read before it); waiting for a newer run", w.c.Worker, obs.LatestRun, obs.Status, w.st.Attempt)
				}
			}
			if obs.RunCount > floor && run == "" {
				unresolved = ""
			}
			if run != "" {
				_, replied := obs.Replies[run]
				unresolved = ""
				if !replied && obs.ActiveRun == "" && obs.LatestRun != "" && obs.LatestRun != run {
					unresolved = fmt.Sprintf("observation unresolved: dispatched run %s has no reply among %s's latest messages (latest run %s); waiting, never attributing another run", run, w.c.Worker, obs.LatestRun)
				}
			}
		}
		note := unresolved
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
		done, reply, status := obs.LatestRun, obs.Reply, obs.Status
		finished := obs.LatestRun != "" && obs.ActiveRun == "" && obs.Pending == 0 && !slices.Contains(w.st.Runs, obs.LatestRun) &&
			(obs.RunCount > floor || startRun != "" && obs.LatestRun == startRun)
		if run != "" {
			var replied bool
			done, status = run, "finished"
			reply, replied = obs.Replies[run]
			finished = replied && obs.ActiveRun != run && obs.Pending == 0 && !slices.Contains(w.st.Runs, run)
		}
		if finished {
			w.st.Runs = append(w.st.Runs, done)
			return Event{Kind: "report", Attempt: w.st.Attempt, By: "observer:" + w.c.Worker,
				Evidence: fmt.Sprintf("%s run %s %s: %s", w.c.Worker, done, status, or(reply, "(no reply text)"))}, nil
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

// awaitRun checks the attempt's dispatched run through the Waiter, one
// short keyed check at a time (ISSUE/attempt/wait/N): each is a bounded
// native call that releases the harness at once. Between checks DBOS waits
// durably (from Options.Observe, doubling to 10 minutes) and takes owner
// events. The finished run is the report, still unaccepted.
func (r *Runtime) awaitRun(ctx dbos.Context, w *run, wt Waiter) (Event, error) {
	c, run := w.c, w.st.Dispatched
	type waited struct {
		Status   string `json:"status"`
		Finished bool   `json:"finished"`
	}
	poll := r.opt.Observe
	for n := 1; ; n++ {
		req := DispatchRequest{Key: effectKey(c.Issue, w.st.Attempt, fmt.Sprintf("wait/%d", n)), Issue: c.Issue, Attempt: w.st.Attempt, Owner: c.Owner, Route: c.Route, Private: c.Private,
			OAuthActor: c.OAuthActor, OAuthInstallation: c.OAuthInstallation, Worker: c.Worker}
		res, err := step(r, ctx, w, "wait", func(sctx context.Context) (waited, error) {
			s, f, err := wt.Wait(sctx, req, run)
			return waited{s, f}, err
		})
		var p Permanent
		if errors.As(err, &p) {
			if err := r.hold(ctx, w, "wait "+req.Key+": "+p.Error()); err != nil {
				return Event{}, err
			}
			continue
		}
		if err != nil {
			return Event{}, err
		}
		if res.Finished && !slices.Contains(w.st.Runs, run) {
			// The report is the run's own result, read natively; a read
			// that cannot bind it holds (visible), never a substitute.
			for m := 1; ; m++ {
				rq := req
				rq.Key = effectKey(c.Issue, w.st.Attempt, fmt.Sprintf("read/%d", m))
				body, err := step(r, ctx, w, "read", func(sctx context.Context) (string, error) { return wt.Read(sctx, rq, run) })
				if errors.As(err, &p) {
					if err := r.hold(ctx, w, "read "+rq.Key+": "+p.Error()); err != nil {
						return Event{}, err
					}
					continue
				}
				if err != nil {
					return Event{}, err
				}
				w.st.Runs = append(w.st.Runs, run)
				w.st.Note = ""
				return Event{Kind: "report", Attempt: w.st.Attempt, By: "waiter:" + c.Worker,
					Evidence: fmt.Sprintf("%s run %s %s: %s", c.Worker, run, res.Status, body)}, nil
			}
		}
		if note := fmt.Sprintf("waiting on %s run %s through the creator's native T3 tools (%s)", c.Worker, run, or(res.Status, "not finished")); note != w.st.Note {
			w.st.Note = note
			if err := r.publish(ctx, w, w.st.Phase, w.st.Next); err != nil {
				return Event{}, err
			}
		}
		for waited := false; !waited; {
			ev, ok, err := r.next(ctx, w, poll)
			switch {
			case err != nil:
				return Event{}, err
			case !ok:
				waited = true
			case ev.Kind == "close":
				return ev, closed{ev}
			case ev.Kind == "report" && ev.Attempt == w.st.Attempt:
				return ev, nil
			}
		}
		poll = min(poll*2, 10*time.Minute)
	}
}

// review asks the supervisor for the attempt's verdict. The verdict is
// bound here to this commitment and attempt; the turn supplies only
// accept/reject and a reason. Owner events that arrived during the review
// win over its verdict. A held outcome (native denial, refused creator,
// stop, commitment not current) holds under the same key: replay and
// restarts return the recorded outcome, and only an explicit !work retry
// reviews again. A technical or malformed outcome is retried under a new
// key with durable backoff, at most reviewRetries times, then held. A live
// owner's verdict or close always ends the wait.
func (r *Runtime) review(ctx dbos.Context, w *run) (Event, error) {
	c := w.c
	pause, failed := 10*time.Minute, 0
	for n := 0; ; n++ {
		key := effectKey(c.Issue, w.st.Attempt, "review")
		if n > 0 {
			key = fmt.Sprintf("%s#retry%d", key, n)
		}
		req := ReviewRequest{Key: key, Issue: c.Issue, Attempt: w.st.Attempt, Owner: c.Owner, Route: c.Route, Private: c.Private, Prompt: reviewPrompt(w)}
		res, err := step(r, ctx, w, "review", func(sctx context.Context) (ReviewResult, error) { return r.opt.Reviewer.Review(sctx, req) })
		var perm Permanent
		switch {
		case errors.As(err, &perm):
			res = ReviewResult{Held: perm.Error()}
		case err != nil:
			return Event{}, err
		}
		for {
			ev, ok, err := r.next(ctx, w, time.Millisecond)
			if err != nil {
				return Event{}, err
			}
			if !ok {
				break
			}
			if ev.Kind == "close" {
				return ev, closed{ev}
			}
			if ev.Attempt == w.st.Attempt && (ev.Kind == "accept" || ev.Kind == "reject") {
				return ev, nil
			}
		}
		if res.Held == "" && res.Failure == "" && (res.Verdict == "accept" || res.Verdict == "reject") && strings.TrimSpace(res.Reason) != "" {
			if strings.HasPrefix(w.st.Note, "supervisor review") {
				w.st.Note = ""
			}
			return Event{Kind: res.Verdict, Attempt: w.st.Attempt, By: fmt.Sprintf("supervisor (turn %d)", res.Turn), Note: clip(res.Reason, 500)}, nil
		}
		if res.Held == "" {
			if failed++; failed > reviewRetries {
				res.Held = fmt.Sprintf("%d reviews failed; last: %s", failed, or(res.Failure, "no verdict"))
			}
		}
		wait := pause
		if res.Held != "" {
			w.st.Blocked = "review " + key + ": " + clip(res.Held, 300)
			if err := r.publish(ctx, w, PhaseHeld, "reconcile "+w.st.Blocked+"; then !work retry, accept, reject or close"); err != nil {
				return Event{}, err
			}
			wait = maxWait
		} else {
			w.st.Note = fmt.Sprintf("supervisor review %s failed (%s); retry in %s, or !work accept|reject", key, clip(res.Failure, 200), pause)
			if err := r.publish(ctx, w, w.st.Phase, w.st.Next); err != nil {
				return Event{}, err
			}
		}
		for again := false; !again; {
			ev, ok, err := r.next(ctx, w, wait)
			switch {
			case err != nil:
				return Event{}, err
			case !ok:
				again = res.Held == ""
			case ev.Kind == "close":
				return ev, closed{ev}
			case ev.Attempt == w.st.Attempt && (ev.Kind == "accept" || ev.Kind == "reject"):
				return ev, nil
			case ev.Kind == "retry" && res.Held != "":
				again, failed = true, 0 // explicit authorized recovery: a new review
			}
		}
		if res.Held != "" {
			w.st.Blocked = ""
			if err := r.publish(ctx, w, PhaseReported, "supervisor review of the evidence"); err != nil {
				return Event{}, err
			}
		}
		pause = min(pause*2, time.Hour)
	}
}

// brief is the current attempt's dispatched work: the admitted brief, or
// the pre-authorized next step, plus bounded repair feedback after a
// rejection. Nothing here widens the admitted scope.
func brief(w *run) string {
	c := w.c
	b := c.Brief
	if w.st.Stage == "next" {
		b = fmt.Sprintf("%s (authorized next step, after the accepted first stage): %s", c.Issue, c.Next)
	}
	if w.feedback != "" {
		b += fmt.Sprintf("\n\nRepair, attempt %d: the previous attempt's result was not accepted: %s\nFix only that, within the work above; do not widen its scope.", w.st.Attempt, w.feedback)
	}
	return b
}

// reviewPrompt is the supervisor's brief. Authority stays with the
// runtime: the verdict cannot deploy, send or change anything.
func reviewPrompt(w *run) string {
	c, st := w.c, w.st
	var b strings.Builder
	fmt.Fprintf(&b, "Supervisor review: %s %q, attempt %d.\n", c.Issue, c.Title, st.Attempt)
	if work := brief(w); work != "" {
		fmt.Fprintf(&b, "Brief: %s\n", work)
	}
	if c.Worker != "" {
		fmt.Fprintf(&b, "Worker: %s\n", c.Worker)
	}
	if len(c.Gates) > 0 {
		fmt.Fprintf(&b, "Gates: %s\n", strings.Join(c.Gates, ", "))
	}
	fmt.Fprintf(&b, "Reported evidence (untrusted until you verify it):\n%s\n\n", st.Evidence)
	b.WriteString("Verify the evidence yourself, read-only. Do not deploy, send, approve or change anything: this review grants no such authority. ")
	b.WriteString("Answer with the verdict on the first line, exactly ACCEPT or REJECT, then the reason (what you checked) on the following lines. Any other answer counts as no verdict.")
	return b.String()
}

// reject opens the next attempt. notify delivers the rejection to the
// route (manual mode); under automated review it stays in execution state.
func (r *Runtime) reject(ctx dbos.Context, w *run, why string, notify bool) error {
	w.st.Verdict = why
	w.feedback = clip(why, 1000)
	if notify {
		text := fmt.Sprintf("%s attempt %d %s. Next: attempt %d", w.c.Issue, w.st.Attempt, why, w.st.Attempt+1)
		if err := r.deliver(ctx, w, fmt.Sprintf("rejected/%d", w.st.Attempt), text, true); err != nil {
			return err
		}
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
// blindly: a blocking delivery holds the commitment on the failed receipt
// until an explicit !work retry (re-enqueued under a new key) or close. A
// non-blocking delivery (notices) records the failure and moves on.
func (r *Runtime) deliver(ctx dbos.Context, w *run, effect, text string, blocking bool) error {
	c := w.c
	for n := 0; ; n++ {
		key := c.Issue + "/" + effect
		if n > 0 {
			key = fmt.Sprintf("%s#retry%d", key, n)
		}
		id, err := step(r, ctx, w, "deliver", func(context.Context) (int64, error) {
			id, err := r.opt.Outbox.EnqueueEffect(key, core.Outbound{Conv: c.Route, ReplyTo: c.ReplyTo, Text: text + "\n— heimdall"})
			if errors.Is(err, store.ErrEffectConflict) {
				err = Permanent{err}
			}
			return id, err
		})
		var why string
		var perm Permanent
		switch {
		case errors.As(err, &perm):
			why = "delivery " + key + ": " + perm.Error()
		case err != nil:
			return err
		default:
			rc, err := r.receipt(ctx, w, id)
			if err != nil {
				return err
			}
			if rc.State == store.OutSent {
				w.st.Receipts = append(w.st.Receipts, id)
				return nil
			}
			why = fmt.Sprintf("outbound %d (%s) failed: %s", id, key, rc.Error)
		}
		if !blocking {
			w.st.Note = "notice not delivered: " + why
			return nil
		}
		if err := r.hold(ctx, w, why); err != nil {
			return err
		}
	}
}

// hold publishes the held phase with why and waits durably for an
// explicit retry (returns nil) or close; other events are kept for later.
func (r *Runtime) hold(ctx dbos.Context, w *run, why string) error {
	phase, next := w.st.Phase, w.st.Next
	w.st.Blocked = why
	if err := r.publish(ctx, w, PhaseHeld, "reconcile "+why+"; then !work retry or !work close"); err != nil {
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
	return r.publish(ctx, w, phase, next)
}

// receipt waits durably until outbound id is sent or failed.
func (r *Runtime) receipt(ctx dbos.Context, w *run, id int64) (store.Receipt, error) {
	c := w.c
	pause := time.Second
	for {
		rc, err := step(r, ctx, w, "receipt", func(context.Context) (store.Receipt, error) {
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

// stepOut is a step's checkpointed outcome: a Permanent error is recorded
// as a result, so replay returns it without running fn again.
type stepOut[R any] struct {
	V    R      `json:"v"`
	Perm string `json:"perm,omitempty"`
}

// attempt runs fn as one checkpointed step with in-step retries for
// technical errors. A Permanent error returns at once.
func attempt[R any](ctx dbos.Context, name string, fn func(context.Context) (R, error)) (R, error) {
	out, err := dbos.RunAsStep(ctx, func(sctx context.Context) (stepOut[R], error) {
		v, err := fn(sctx)
		var p Permanent
		if errors.As(err, &p) {
			return stepOut[R]{Perm: p.Error()}, nil
		}
		return stepOut[R]{V: v}, err
	}, dbos.WithStepName(name), dbos.WithStepMaxRetries(5), dbos.WithStepBaseInterval(time.Second), dbos.WithStepBackoffFactor(2))
	if err == nil && out.Perm != "" {
		err = Permanent{errors.New(out.Perm)}
	}
	return out.V, err
}

// step is attempt with visible technical recovery: after the in-step
// retries it shows "technical retry" on the commitment and tries again with
// durable backoff to 1 hour, never ending the commitment or asking the
// owner. Permanent errors return to the caller, which holds.
func step[R any](r *Runtime, ctx dbos.Context, w *run, name string, fn func(context.Context) (R, error)) (R, error) {
	pause := 10 * time.Minute
	for round := 1; ; round++ {
		v, err := attempt(ctx, name, fn)
		var p Permanent
		if err == nil || errors.As(err, &p) || ctx.Err() != nil {
			if err == nil && strings.HasPrefix(w.st.Note, "technical retry") {
				w.st.Note = ""
				if perr := r.publish(ctx, w, w.st.Phase, w.st.Next); perr != nil {
					return v, perr
				}
			}
			return v, err
		}
		w.st.Note = fmt.Sprintf("technical retry of %s (round %d, next in %s): %v", name, round, pause, err)
		if perr := r.publish(ctx, w, w.st.Phase, w.st.Next); perr != nil {
			return v, perr
		}
		if _, err := dbos.Sleep(ctx, pause); err != nil {
			return v, err
		}
		pause = min(pause*2, time.Hour)
	}
}

type dispatchInput struct {
	Req DispatchRequest `json:"req"`
}

type dispatchResult struct {
	Receipt Receipt `json:"receipt"`
	Held    string  `json:"held,omitempty"` // permanent: never replayed
}

// reconcileGrace is how long an unknown dispatch outcome is re-read before
// it holds (a variable for tests).
var reconcileGrace = 2 * time.Minute

// dispatch is the child workflow that occupies the resource queue: one
// claimed dispatch. Technical errors retry durably (visible as the
// parent's dispatching phase); a Permanent error comes back as Held.
func (r *Runtime) dispatch(ctx dbos.Context, in dispatchInput) (dispatchResult, error) {
	pause := 10 * time.Minute
	for {
		rcpt, err := attempt(ctx, "dispatch", func(sctx context.Context) (Receipt, error) { return r.dispatchOnce(sctx, in.Req) })
		var p Permanent
		if errors.As(err, &p) {
			return dispatchResult{Held: p.Error()}, nil
		}
		if err == nil || ctx.Err() != nil {
			return dispatchResult{Receipt: rcpt}, err
		}
		r.opt.Log.Printf("durable: dispatch %s: %v; retrying in %s", in.Req.Key, err, pause)
		if _, err := dbos.Sleep(ctx, pause); err != nil {
			return dispatchResult{}, err
		}
		pause = min(pause*2, time.Hour)
	}
}

// dispatchOnce sends q at most once. The claim is durable before the send,
// so a send whose step never checkpointed (crash, lost response, timeout)
// is reconciled from the worker's own record, never resent.
func (r *Runtime) dispatchOnce(ctx context.Context, q DispatchRequest) (Receipt, error) {
	state, saved, at, err := r.claim(q.Key)
	if err != nil {
		return Receipt{}, err
	}
	switch state {
	case "sent":
		var rc Receipt
		if err := json.Unmarshal([]byte(saved), &rc); err != nil {
			return Receipt{}, fmt.Errorf("claim %s: stored receipt: %w", q.Key, err)
		}
		return rc, nil
	case "sending":
		rc, found, err := r.opt.Dispatcher.Reconcile(ctx, q)
		if err != nil {
			return Receipt{}, fmt.Errorf("reconcile %s: %w", q.Key, err)
		}
		if found {
			return rc, r.settle(q.Key, rc)
		}
		if time.Since(time.UnixMilli(at)) < reconcileGrace {
			return Receipt{}, fmt.Errorf("reconcile %s: no message marked %s yet", q.Key, q.Marker)
		}
		return Receipt{}, Permanent{fmt.Errorf("outcome unknown: an earlier send of %s was interrupted and %s shows no message marked %s; not resent (check the worker, then !work retry sends under a new key)", q.Key, q.Worker, q.Marker)}
	}
	if r.opt.Authorize == nil || !r.opt.Authorize(q.Owner, q.Route) || !r.current(q.Issue, q.Attempt, PhaseDispatching) {
		return Receipt{}, errors.Join(r.release(q.Key), Permanent{errors.New("refused before sending: the creator is no longer an admitted owner on the route, or the commitment is paused, cancelled or no longer dispatching this attempt")})
	}
	rc, err := r.opt.Dispatcher.Dispatch(ctx, q)
	var ns NotSent
	var p Permanent
	switch {
	case err == nil:
		return rc, r.settle(q.Key, rc)
	case errors.As(err, &ns), errors.As(err, &p):
		// Certainly no effect: release the claim.
		if rerr := r.release(q.Key); rerr != nil {
			return Receipt{}, rerr
		}
		return Receipt{}, err
	default:
		return Receipt{}, fmt.Errorf("dispatch %s outcome unknown (reconciling, not resending): %w", q.Key, err)
	}
}

// claim records key as sending unless it is already claimed; it returns
// the earlier claim's state ("" when this call claimed it).
func (r *Runtime) claim(key string) (state, receipt string, at int64, err error) {
	now := time.Now().UnixMilli()
	res, err := r.db.Exec(`INSERT INTO heimdall_dispatch_claims(key,state,at) VALUES(?,'sending',?) ON CONFLICT(key) DO NOTHING`, key, now)
	if err != nil {
		return "", "", 0, err
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return "", "", now, err
	}
	err = r.db.QueryRow(`SELECT state,receipt,at FROM heimdall_dispatch_claims WHERE key=?`, key).Scan(&state, &receipt, &at)
	return state, receipt, at, err
}

func (r *Runtime) settle(key string, rc Receipt) error {
	b, err := json.Marshal(rc)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`UPDATE heimdall_dispatch_claims SET state='sent',receipt=? WHERE key=?`, string(b), key)
	return err
}

func (r *Runtime) release(key string) error {
	_, err := r.db.Exec(`DELETE FROM heimdall_dispatch_claims WHERE key=? AND state='sending'`, key)
	return err
}

func effectKey(issue string, attempt int, effect string) string {
	return fmt.Sprintf("%s/%d/%s", issue, attempt, effect)
}
