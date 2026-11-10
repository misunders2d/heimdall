// Package router moves messages between interfaces and harness turns: one
// turn at a time per conversation, steering or queueing mid-turn messages,
// falling back only before a turn started, and delivering every reply
// through the persisted outbound queue.
package router

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/googleoauth"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

// Interface is one chat platform. Run must supply only platform-verified
// senders and acknowledge a message upstream only after accept returns nil
// (the message is then persisted).
type Interface interface {
	Name() string
	Run(ctx context.Context, accept func(context.Context, core.Inbound) error) error
	Send(ctx context.Context, out core.Outbound) error
}

// Typer is an optional Interface feature: a "typing…" indicator. The router
// calls Typing when a turn starts, every Options.TypingEvery while it runs and
// once more after a steer was accepted. It is best effort and reports nothing;
// it must return within a few seconds.
type Typer interface {
	Typing(ctx context.Context, conv core.ConvKey)
}

// Prompter is an optional Interface feature: a note on how replies render
// on the platform (for Telegram, its Markdown dialect). The router puts it at
// the top of every turn's prompt and marks the harness's answer Formatted.
type Prompter interface {
	PromptNote() string
}

// PermanentError marks a delivery failure that retrying cannot fix.
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

// Accounts prepares the OS account a role's turns run as: harness.Accounts.
type Accounts interface {
	Prepare(role core.Role, memory bool, bin string) (harness.Launch, error)
}

type Options struct {
	Google            *googleoauth.Broker
	EmployeeHarnesses []string
	Harness           harness.WarmConfig
	OwnerSlack        *harness.OwnerSlack
	SlackTeam         string
	Access            core.Access
	Order             []string // default harness order
	MaxParallel       int
	Accounts          Accounts
	Log               *log.Logger
	TypingEvery       time.Duration        // how often Typing repeats while a turn runs; default 4s
	AlbumWait         time.Duration        // how long a grouped message's parts are held after the last; default 1.5s
	OutboxDir         string               // Heimdall's copies of files to send; "" sends none
	MaxAttempts       int                  // delivery attempts of one reply before it is failed; default 50 (about 3.6 h)
	GuardLog          string               // the prompt-injection log (metadata only); "" logs nothing
	GuardLogMax       int64                // its size before rotation to GuardLog+".1"; default DefaultGuardLogMax
	Jobs              *scheduler.Scheduler // nil: the scheduler is disabled
	Work              *durable.Runtime     // nil: durable commitments are disabled
}

type Router struct {
	warm        *harness.Pool
	noticeRetry time.Duration
	noticeQueue chan core.Inbound
	opt         Options
	store       *store.Store
	ifaces      map[string]Interface
	// sem holds the general turn slots; reserved is one more slot only the
	// owner's live turns may take, so the owner never waits behind others.
	// With max_parallel = 1 there is no reserved slot (reserved is nil):
	// the single slot is general.
	sem      chan struct{}
	reserved chan struct{}
	log      *log.Logger
	// guardLog records messages the hub's prompt-injection scoring flags.
	guardLog *guardLog

	mu      sync.Mutex
	ctx     context.Context
	convs   map[core.ConvKey]*conv
	wakeOut map[string]chan struct{}
	jobs    map[string]*turn // running scheduled turns by job

	ready     chan struct{} // closed once Run replayed pending messages (Review waits)
	readyOnce sync.Once

	// Live harness processes, each until it exited. At shutdown they are
	// stopped and waited for, so none outlives Heimdall.
	starting sync.WaitGroup
	runsMu   sync.Mutex
	runs     map[*harness.Run]struct{}
	procCtx  context.Context // harness processes' context: ends only after shutdown waited for them
	stopping bool            // shutdown began: no harness process starts any more

	// accessRetry is how long a stored message whose caller could not be
	// checked (access files unavailable) waits before it is checked again.
	accessRetry time.Duration

	// endRetry and endAttempts bound how a turn's end is recorded again
	// after a failed write (backoff doubling from endRetry).
	endRetry    time.Duration
	endAttempts int

	// ifaceGrace is how long after its first start an interface's failure
	// stays fatal; ifaceBackoff is the first delay before a later restart.
	ifaceGrace, ifaceBackoff time.Duration
}

func New(opt Options, st *store.Store, ifaces ...Interface) *Router {
	if opt.MaxParallel < 1 {
		opt.MaxParallel = 1
	}
	if opt.Log == nil {
		opt.Log = log.Default()
	}
	if opt.TypingEvery <= 0 {
		opt.TypingEvery = 4 * time.Second
	}
	if opt.AlbumWait <= 0 {
		opt.AlbumWait = 1500 * time.Millisecond
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = 50
	}
	if opt.GuardLogMax <= 0 {
		opt.GuardLogMax = DefaultGuardLogMax
	}
	general, reserved := opt.MaxParallel, chan struct{}(nil)
	if opt.MaxParallel > 1 {
		general, reserved = opt.MaxParallel-1, make(chan struct{}, 1)
	}
	r := &Router{ifaceGrace: 30 * time.Second, ifaceBackoff: 2 * time.Second, endRetry: 250 * time.Millisecond, endAttempts: 5, accessRetry: time.Second, warm: harness.NewPool(opt.Harness), noticeRetry: time.Minute, noticeQueue: make(chan core.Inbound, 128), opt: opt, store: st, ifaces: map[string]Interface{}, sem: make(chan struct{}, general), reserved: reserved, log: opt.Log, convs: map[core.ConvKey]*conv{}, jobs: map[string]*turn{}, ready: make(chan struct{}), wakeOut: map[string]chan struct{}{}, runs: map[*harness.Run]struct{}{}}
	if opt.Jobs != nil {
		opt.Jobs.ResolveControl = r.schedulerCaller
	}
	if opt.GuardLog != "" {
		r.guardLog = &guardLog{path: opt.GuardLog, max: opt.GuardLogMax}
	}
	for _, i := range ifaces {
		r.ifaces[i.Name()] = i
		r.wakeOut[i.Name()] = make(chan struct{}, 1)
		if w, ok := i.(interface{ SetOutboundWake(func()) }); ok {
			name := i.Name()
			w.SetOutboundWake(func() { r.wake(name) })
		}
	}
	return r
}

// Run recovers interrupted work, then routes until ctx ends or an
// interface fails, which stops everything else and is returned. Turns still
// running at shutdown stay recorded as running, so the next start reports
// them exactly like a crash. Before it returns, every harness process still
// running is stopped and waited for (stopRuns).
func (r *Router) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Harness processes do not end with ctx: shutdown stops them with
	// SIGTERM first and kills them only if they do not exit.
	procCtx, kill := context.WithCancel(context.Background())
	defer kill()
	r.runsMu.Lock()
	r.procCtx = procCtx
	r.runsMu.Unlock()
	rec, err := r.store.Recover(interruptedNotice, unconfirmedNotice)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	for _, t := range rec.Turns {
		r.log.Printf("turn %d in %s interrupted by restart (harness %q); reported, not re-run", t.Turn, t.Conv, t.Harness)
	}
	for _, m := range rec.Unconfirmed {
		r.log.Printf("message %d in %s was being steered into turn %d at the restart; reported, not replayed", m.ID, m.Conv, m.TurnID)
	}
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
	if r.opt.Jobs != nil {
		// Runs a restart cut off are settled before pending messages are
		// replayed, so a run's message is never both re-run and reported.
		r.opt.Jobs.Attach(r.dispatch, r.wake, r.stopJob)
		if err := r.opt.Jobs.Recover(); err != nil {
			return fmt.Errorf("scheduler recover: %w", err)
		}
	}
	r.pruneOutbox()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.keepRunning(ctx, "caller notices", func() { r.callerNotices(ctx) }) }()
	for _, i := range r.ifaces {
		wg.Add(1)
		go func() { defer wg.Done(); r.keepRunning(ctx, "delivery for "+i.Name(), func() { r.deliver(ctx, i) }) }()
	}
	pending, err := r.store.Pending()
	if err != nil {
		cancel()
		wg.Wait()
		return err
	}
	for _, row := range pending {
		r.dispatch(row)
	}
	r.readyOnce.Do(func() { close(r.ready) })
	if r.opt.Jobs != nil {
		wg.Add(1)
		go func() { defer wg.Done(); r.opt.Jobs.Run(ctx) }()
	}
	errs := make(chan error, len(r.ifaces))
	for _, i := range r.ifaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runInterface(ctx, i, errs)
		}()
	}
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	cancel()
	wg.Wait()
	r.stopRuns(kill)
	return err
}

// startRun starts a turn's harness process and counts it as live until it
// exited. Once shutdown began nothing starts.
func (r *Router) startRun(spec harness.Spec) (*harness.Run, error) {
	r.runsMu.Lock()
	if r.stopping {
		r.runsMu.Unlock()
		return nil, errors.New("Heimdall is shutting down")
	}
	r.starting.Add(1)
	r.runsMu.Unlock()
	defer r.starting.Done()
	run, err := r.warm.Start(r.procCtx, spec)
	if err != nil {
		return nil, err
	}
	process := run.Process()
	r.runsMu.Lock()
	if _, tracked := r.runs[process]; tracked {
		r.runsMu.Unlock()
		return run, nil
	}
	r.runs[process] = struct{}{}
	r.runsMu.Unlock()
	go func() { <-run.Exited(); r.runsMu.Lock(); delete(r.runs, process); r.runsMu.Unlock() }()
	return run, nil
}

// stopRuns stops every live harness process at shutdown, as after an
// answer: SIGTERM to its process group, SIGKILL TermGrace later. It waits
// up to ExitGrace plus TermGrace for them to exit (within systemd's default
// 90 s stop timeout), then kills the groups of any left (kill) and waits
// for those briefly. A process's group is killed once the process exited,
// so helpers it started in its group do not outlive Heimdall either.
func (r *Router) stopRuns(kill context.CancelFunc) {
	r.runsMu.Lock()
	r.stopping = true
	r.runsMu.Unlock()
	// Run cancels the router context first; every pending StartContext is
	// already cancelled before this wait, even though process contexts survive.
	r.starting.Wait()
	r.runsMu.Lock()
	runs := slices.Collect(maps.Keys(r.runs))
	r.runsMu.Unlock()
	if len(runs) == 0 {
		return
	}
	r.log.Printf("shutdown: stopping %d harness process(es)", len(runs))
	for _, run := range runs {
		run.Stop("at shutdown")
	}
	if left := waitExited(runs, harness.ExitGrace+harness.TermGrace); left > 0 {
		r.log.Printf("shutdown: %d harness process(es) still running: SIGKILL to their process groups", left)
		kill()
		if left := waitExited(runs, 10*time.Second); left > 0 {
			r.log.Printf("shutdown: %d harness process(es) did not exit", left)
		}
	}
}

// waitExited waits up to d for every run's process to exit and returns how
// many have not.
func waitExited(runs []*harness.Run, d time.Duration) int {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	for i, run := range runs {
		select {
		case <-run.Exited():
		case <-deadline.C:
			left := 0
			for _, run := range runs[i:] {
				select {
				case <-run.Exited():
				default:
					left++
				}
			}
			return left
		}
	}
	return 0
}

// accept persists one verified message and hands it to its conversation.
// Senders who cannot be identified are ignored without storing their text.
func (r *Router) accept(ctx context.Context, in core.Inbound) (err error) {
	defer func() {
		if p := recover(); p != nil {
			// Acknowledged and dropped: re-offering it would fail the same
			// way and hold up every later message of the interface.
			r.log.Printf("recovered panic (%T) accepting a message in %s; message dropped", p, in.Conv)
			err = nil
		}
	}()
	return r.acceptMessage(ctx, in)
}

func (r *Router) acceptMessage(ctx context.Context, in core.Inbound) error {
	if in.Origin == "" {
		in.Origin = core.OriginLive
	}
	if err := in.Validate(); err != nil {
		r.release(in)
		return err
	}
	if in.Origin == core.OriginCommitment || in.Review != nil {
		// Only Router.Review builds a commitment review.
		r.release(in)
		return errors.New("interface message claims a commitment review")
	}
	role, _, ok, err := r.opt.Access.Check(in.From, in.Conv)
	if err != nil {
		// Not acknowledged: the interface offers the message again later.
		r.log.Printf("message from a %s caller in %s not accepted yet: %v", in.From.Platform, in.Conv, err)
		r.release(in)
		return fmt.Errorf("caller admission: %w", err)
	}
	if !ok {
		r.log.Printf("ignored message from an unidentified %s caller in %s", in.From.Platform, in.Conv)
		r.release(in)
		return nil
	}
	if r.googleControl(ctx, in, role) {
		r.release(in)
		return nil
	}
	if r.mcpControl(ctx, &in, role) {
		r.release(in)
		return nil
	}
	in.Text = core.RedactMCPCallbacks(in.Text)
	in.Context = core.RedactMCPCallbacks(in.Context)
	if in.Announce {
		if in.Group != "" && !in.Ambient {
			r.dispatch(store.InboundRow{Inbound: in})
		}
		return nil
	}
	if in.Ambient {
		r.keepAmbient(in)
		r.guard(in)
		r.release(in)
		return nil
	}
	row, dup, err := r.store.AddInbound(in)
	if err != nil || dup {
		// Not stored (or stored before, with its own files): the interface
		// downloads again if it re-sends this message.
		r.release(in)
		return err
	}
	r.guard(in)
	if g, ok := r.ifaces[in.Conv.Interface].(GroupContext); ok && g.GroupContext() && !in.Private && in.Origin == core.OriginLive && parseCommand(in.Text).Kind == cmdMessage {
		// Addressed group messages are context for later turns too, as
		// in the hub; commands are not.
		r.keepAmbient(in)
	}
	if role != core.RoleOwner {
		r.newCaller(in)
	}
	r.dispatch(row)
	return nil
}

func (r *Router) dispatch(row store.InboundRow) {
	r.mu.Lock()
	c := r.convs[row.Conv]
	if c == nil {
		c = &conv{r: r, key: row.Conv, signal: make(chan struct{}, 1), events: make(chan any), slot: make(chan chan struct{})}
		r.convs[row.Conv] = c
		go c.loop(r.ctx)
	}
	r.mu.Unlock()
	c.push(row)
}

// Idle reports whether nothing is queued, running or waiting for delivery,
// and every reply has been delivered. Idle workers are stopped at shutdown.
func (r *Router) Idle() bool {
	r.mu.Lock()
	for _, c := range r.convs {
		if c.busy.Load() {
			r.mu.Unlock()
			return false
		}
	}
	r.mu.Unlock()
	n, err := r.store.Undelivered()
	return err == nil && n == 0
}

func (r *Router) enqueue(out core.Outbound) {
	if _, err := r.store.Enqueue(out); err != nil {
		r.log.Printf("reply for %s not stored: %v", out.Conv, err)
		return
	}
	r.wake(out.Conv.Interface)
}

// Wake prompts iface's delivery loop; durable commitments call it after
// enqueueing a reply.
func (r *Router) Wake(iface string) { r.wake(iface) }

func (r *Router) wake(iface string) {
	select {
	case r.wakeOut[iface] <- struct{}{}:
	default:
	}
}

// deliver sends iface's replies in order per chat, retrying transient
// failures. A crash between a successful send and Sent repeats that reply.
func (r *Router) deliver(ctx context.Context, i Interface) {
	name := i.Name()
	for ctx.Err() == nil {
		row, ok, err := r.store.NextOutbound(name)
		if err != nil {
			r.log.Printf("outbound %s: %v", name, err)
			sleep(ctx, time.Second)
			continue
		}
		if !ok {
			var timer <-chan time.Time
			if due, has, err := r.store.NextDue(name); err == nil && has {
				timer = time.After(max(time.Until(due), 10*time.Millisecond))
			}
			select {
			case <-ctx.Done():
			case <-r.wakeOut[name]:
			case <-timer:
			}
			continue
		}
		if err := r.store.Sending(row.ID); err != nil {
			r.log.Printf("outbound %d: %v", row.ID, err)
			sleep(ctx, time.Second)
			continue
		}
		row.Outbound.ID, row.Outbound.Attempt = row.ID, row.Attempts
		err = r.sendSafely(ctx, i, row.Outbound)
		var perm PermanentError
		var record func() error
		switch {
		case err == nil:
			record = func() error { return r.store.Sent(row.ID) }
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		case errors.As(err, &perm):
			r.log.Printf("reply %d to %s failed permanently: %v", row.ID, row.Conv, err)
			msg := err.Error()
			record = func() error { return r.store.Failed(row.ID, msg) }
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		case row.Attempts+1 >= r.opt.MaxAttempts:
			r.log.Printf("reply %d to %s failed after %d attempts: %v", row.ID, row.Conv, row.Attempts+1, err)
			msg := fmt.Sprintf("gave up after %d attempts: %v", row.Attempts+1, err)
			record = func() error { return r.store.Failed(row.ID, msg) }
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		default:
			backoff := min(time.Second<<min(row.Attempts, 9), 5*time.Minute)
			r.log.Printf("reply %d to %s: retry in %s: %v", row.ID, row.Conv, backoff, err)
			at, msg := time.Now().Add(backoff), err.Error()
			record = func() error { return r.store.Retry(row.ID, at, msg) }
		}
		// The outcome is recorded before the next reply: a row left as
		// sending would block its chat until a restart, which resends it.
		for attempt := 0; ctx.Err() == nil; attempt++ {
			err := record()
			if err == nil {
				break
			}
			r.log.Printf("outbound %d: outcome not recorded, retrying: %v", row.ID, err)
			sleep(ctx, min(100*time.Millisecond<<min(attempt, 6), 5*time.Second))
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// item is one message waiting for, or steering, a turn.
type item struct {
	row     store.InboundRow
	role    core.Role
	job     string // a scheduled run's job name
	harness string // !<harness>: this message only
	next    bool   // !next: never steer
	text    string
	react   string // the interface's reaction note for this message, if any
}

// conv serializes one conversation. Only its loop goroutine touches its
// state; turns and steers report back through events.
type conv struct {
	generation map[string]uint64

	continuations []continuationEvent
	nativeTurns   map[continuationKey]*turn
	losses        map[lossKey]bool
	delivered     map[string]deliveredState // c.mu; exact native process scoped cursors

	r      *Router
	key    core.ConvKey
	busy   atomic.Bool
	mu     sync.Mutex
	inbox  []store.InboundRow
	signal chan struct{}
	events chan any
	slot   chan chan struct{} // a waiter hands over the semaphore it took a slot of
	// slotSem is the semaphore whose slot this conversation holds, nil when
	// none (loop-owned); releaseSlot gives it back exactly once.
	slotSem chan struct{}

	queue   []item
	turn    *turn
	waiting bool // a goroutine is waiting for the last process to exit, then for a parallelism slot
	albums  map[string]*album
	exited  <-chan struct{} // the last turn's harness process has exited

	// held are stored messages whose caller could not be checked yet
	// (access files unavailable), in arrival order; a retry is armed.
	held      []store.InboundRow
	heldArmed bool

	routing int64 // the stored message being routed, failed if routing panics
}

// accessRetryEvent asks the loop to check its held messages again.
type accessRetryEvent struct{}

// holdForAccess keeps a stored message whose caller could not be checked
// and checks it again after accessRetry: never rejected for a moment's
// unreadable access files.
func (c *conv) holdForAccess(row store.InboundRow) {
	c.held = append(c.held, row)
	if c.heldArmed {
		return
	}
	c.heldArmed = true
	ctx := c.r.ctx
	time.AfterFunc(c.r.accessRetry, func() { c.send(ctx, accessRetryEvent{}) })
}

// take handles one row from the inbox: album announcements and parts are
// held for their group, other rows routed now. While messages wait for
// access, later ones wait behind them, in order.
func (c *conv) take(ctx context.Context, row store.InboundRow) {
	switch {
	case row.Announce:
		c.expect(ctx, row)
	case len(c.held) > 0:
		c.held = append(c.held, row)
	case row.Group != "":
		c.hold(ctx, row)
	default:
		c.handle(ctx, row)
	}
}

// album is a caller's parts of one grouped message, held until none has
// arrived, or been announced, for AlbumWait.
type album struct {
	rows    []store.InboundRow
	gen     int
	pending int // parts announced, not yet arrived
}

// albumPendingWait bounds how long an announced part is waited for: longer
// than an interface's download (Telegram: 2 minutes).
const albumPendingWait = 3 * time.Minute

type albumEvent struct {
	key string
	gen int
}

type turn struct {
	generation    uint64
	nativeInvalid bool
	startCtx      context.Context
	startCancel   context.CancelFunc

	abandoned     bool        // loop-owned: dropped after a recovered panic; its events are ignored
	typingStopped bool        // loop-owned: typing was closed
	doneSent      atomic.Bool // the runner handed over its end (sendDone)

	interruption string // native process loss; separate from user stop/reset
	detached     bool   // native continuation: no inbound batch or parallelism reservation
	published    bool   // loop-owned: original reply has entered the durable outbox

	id         int64
	role       core.Role
	from       core.Principal // verified caller, passed to the harness process
	who        string         // from.Key(): the turn runs in the caller's own session
	private    bool           // the caller's one-to-one chat with the bot, as its interface said
	candidates []string
	explicit   bool
	replyTo    string
	started    time.Time

	// loop-owned
	harness string

	nativeText, nativeContext, instructions string

	ready        bool
	steerQ       []item
	steering     bool
	steered      int
	done         *doneEvent
	stopped      atomic.Bool
	mu           sync.Mutex
	run          *harness.Run
	typing       chan struct{}     // closed when the turn ends: stops the typing indicator
	reset        bool              // the caller reset their sessions: keep none from this turn
	launch       harness.Launch    // how the harness was launched (t.mu)
	dir          *harness.TurnDir  // the turn's directory, once created (t.mu)
	refs         map[string]string // reaction ref → message ID, for messages the harness was given (t.mu)
	batch        []item            // the messages the turn started with
	job          *scheduler.Turn   // a scheduled run's job; nil for a chat turn
	review       *core.Review      // a commitment review's binding; nil otherwise
	tools        []string          // successful completed native tools, for expect (t.mu)
	toolsStarted []string          // starts are progress only, never proof of success
	toolErrors   []string
	signIn       []string             // services a tool result said need a sign-in, for the job's outcome (t.mu)
	native       []harness.NativeCall // a dispatch turn's own native T3 sends (t.mu)
}

type readyEvent struct {
	t       *turn
	harness string
}

type steerEvent struct {
	t       *turn
	items   []item // one message, or the parts of one album
	refs    []string
	outcome harness.SteerOutcome
}

type doneEvent struct {
	t        *turn
	harness  string // "" when no harness became ready
	res      harness.Result
	notes    []string
	files    []harness.OutFile // collected from the outbox after an answer
	skipped  []string          // outbox files not taken, "name: reason"
	refused  bool              // the caller's admission or role changed: refused before any harness
	identity error             // a scheduled turn's creator check failed (scheduler.Admit): no harness ran
	panicked bool              // the turn runner panicked: Heimdall's own failure
}

func (c *conv) push(row store.InboundRow) {
	c.mu.Lock()
	c.inbox = append(c.inbox, row)
	c.busy.Store(true)
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

func (c *conv) loop(ctx context.Context) {
	for {
		done := false
		if c.r.safely("conversation "+c.key.String(), func() { done = c.step(ctx) }) {
			c.afterPanic(ctx)
		}
		if done {
			return
		}
		c.arm(ctx)
	}
}

// step handles one inbox signal or event; it reports whether ctx ended.
func (c *conv) step(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-c.signal:
		c.mu.Lock()
		rows := c.inbox
		c.inbox = nil
		c.mu.Unlock()
		for _, row := range rows {
			c.take(ctx, row)
		}
	case ev := <-c.events:
		switch ev := ev.(type) {
		case accessRetryEvent:
			rows := c.held
			c.held, c.heldArmed = nil, false
			for _, row := range rows {
				c.take(ctx, row)
			}
			if c.turn != nil {
				c.steerNext(ctx, c.turn)
			}
		case continuationEvent:
			c.continuation(ctx, ev)
		case albumEvent:
			if a := c.albums[ev.key]; a != nil && a.gen == ev.gen {
				delete(c.albums, ev.key)
				// A harness choice or !next in one part's caption is the
				// album's: the parts stay one message.
				var choice *command
				for _, row := range a.rows {
					if cmd := parseCommand(row.Text); cmd.Kind == cmdMessage && (cmd.Harness != "" || cmd.Next) {
						choice = &cmd
						break
					}
				}
				for _, row := range a.rows {
					c.route(row, choice)
				}
				if c.turn != nil {
					c.steerNext(ctx, c.turn)
				}
			}
		case readyEvent:
			if ev.t.abandoned {
				break
			}
			ev.t.harness, ev.t.ready = ev.harness, true
			c.steerNext(ctx, ev.t)
		case steerEvent:
			if ev.t.abandoned {
				break
			}
			c.steerDone(ctx, ev)
		case doneEvent:
			if ev.t.abandoned {
				break
			}
			ev.t.done = &ev
			if !ev.t.steering {
				c.finish(ctx, ev.t)
			}
		}
	case sem := <-c.slot:
		c.waiting = false
		c.slotSem = sem
		c.start(ctx)
	}
	return false
}

// arm starts waiting for a turn slot when messages wait and no turn runs,
// and updates busy.
func (c *conv) arm(ctx context.Context) {
	if c.turn == nil && len(c.queue) > 0 && !c.waiting {
		c.waiting = true
		// Reuse a warm transport immediately; wait for cleanup when a
		// turn could not retain its transport.
		head := c.queue[0]
		var note *store.InboundRow
		if head.row.Origin == core.OriginLive && head.row.Run == nil && head.row.Review == nil {
			note = &head.row
		}
		go c.waitSlot(ctx, c.exited, ownerLive(head), note)
	}
	c.mu.Lock()
	c.busy.Store(c.turn != nil || len(c.nativeTurns) > 0 || len(c.queue) > 0 || c.waiting || len(c.inbox) > 0 || len(c.albums) > 0 || len(c.held) > 0)
	c.mu.Unlock()
}

// queuedNotice tells a live caller, once per wait, that every turn slot
// they may take is busy.
const queuedNotice = "Queued — starting when a slot frees. — heimdall"

// ownerLive reports whether it may take the owner's reserved slot: the
// owner's own live message, never a scheduled run or a review.
func ownerLive(it item) bool {
	return it.role == core.RoleOwner && it.row.Origin == core.OriginLive && it.row.Run == nil && it.row.Review == nil
}

// waitSlot waits for the last turn's process to exit, then for a turn slot:
// a general one, or for the owner's live turn the reserved one. If none is
// free at once, note's sender is told once. The slot is handed to the loop.
func (c *conv) waitSlot(ctx context.Context, exited <-chan struct{}, owner bool, note *store.InboundRow) {
	if exited != nil {
		select {
		case <-exited:
		case <-ctx.Done():
			return
		}
	}
	reserved := c.r.reserved
	if !owner {
		reserved = nil // a nil channel is never ready
	}
	var sem chan struct{}
	select {
	case c.r.sem <- struct{}{}:
		sem = c.r.sem
	default:
		select {
		case reserved <- struct{}{}:
			sem = reserved
		default:
		}
	}
	if sem == nil {
		if note != nil {
			c.reply(*note, queuedNotice)
		}
		select {
		case c.r.sem <- struct{}{}:
			sem = c.r.sem
		case reserved <- struct{}{}:
			sem = reserved
		case <-ctx.Done():
			return
		}
	}
	select {
	case c.slot <- sem:
	case <-ctx.Done():
		<-sem
	}
}

// releaseSlot gives back the conversation's turn slot, if it holds one.
func (c *conv) releaseSlot() {
	if c.slotSem != nil {
		<-c.slotSem
		c.slotSem = nil
	}
}

// hold keeps one part of a grouped message until the caller's group has
// been quiet for AlbumWait (the hub's 1.5 s media group merge). The parts
// are then handled together, so they start one turn, or steer the running
// one once.
func (c *conv) hold(ctx context.Context, row store.InboundRow) {
	key, a := c.album(row)
	a.rows = append(a.rows, row)
	a.pending = max(a.pending-1, 0)
	wait := c.r.opt.AlbumWait
	if a.pending > 0 {
		wait = albumPendingWait
	}
	c.armAlbum(ctx, key, a, wait)
}

// expect notes that a part of the caller's group is on its way: the group
// waits for it, as the hub clears its timer before downloading a part.
func (c *conv) expect(ctx context.Context, row store.InboundRow) {
	key, a := c.album(row)
	a.pending++
	c.armAlbum(ctx, key, a, albumPendingWait)
}

func (c *conv) album(row store.InboundRow) (string, *album) {
	key := row.From.Key() + "\x00" + row.Group
	if c.albums == nil {
		c.albums = map[string]*album{}
	}
	a := c.albums[key]
	if a == nil {
		a = &album{}
		c.albums[key] = a
	}
	return key, a
}

// armAlbum makes the group due after wait; earlier timers no longer count.
func (c *conv) armAlbum(ctx context.Context, key string, a *album, wait time.Duration) {
	a.gen++
	ev := albumEvent{key: key, gen: a.gen}
	time.AfterFunc(wait, func() { c.send(ctx, ev) })
}

func (c *conv) reply(row store.InboundRow, text string) {
	c.r.enqueue(core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: c.r.scrub(text)})
}

// answer settles a ! command and its reply together. If that cannot be
// recorded the message stays pending and is answered after a restart.
func (c *conv) answer(row store.InboundRow, e store.CommandEnd, text string) {
	e.Reply = core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: c.r.scrub(text) + "\n— heimdall", Formatted: (e.Detail == "job" || e.Detail == "job answer") && scheduler.Formatted(text)}
	if err := c.r.store.AnswerCommand(row.ID, c.key, e); err != nil {
		c.r.log.Printf("message %d: command %s not recorded: %v", row.ID, e.Detail, err)
		return
	}
	c.r.release(row.Inbound)
	c.r.wake(c.key.Interface)
	if e.Job != nil {
		if e.Job.StopJob != "" {
			c.r.stopJob(e.Job.StopJob)
		}
		c.r.opt.Jobs.Wake()
	}
}

// handle routes one stored message and steers the running turn with it if
// it is the turn caller's.
func (c *conv) handle(ctx context.Context, row store.InboundRow) {
	if c.route(row, nil) {
		c.steerNext(ctx, c.turn)
	}
}

// route answers a command, or queues a message as a follow-up or a steer of
// the sender's running turn (true). A message without its own harness
// choice or !next takes choice's, if any. The sender's role is evaluated
// now, so a revoked principal (or, later, a scheduled job's creator) fails
// closed.
func (c *conv) route(row store.InboundRow, choice *command) bool {
	c.routing = row.ID
	steer := c.routeRow(row, choice)
	c.routing = 0
	return steer
}

func (c *conv) routeRow(row store.InboundRow, choice *command) bool {
	if row.Review != nil {
		// A commitment review: like a scheduled run, never a command or a
		// steer; the creator is decided against the commitment's route.
		role, _, _, err := c.r.opt.Access.Check(row.From, row.Review.Target)
		if err != nil {
			c.holdForAccess(row)
			return false
		}
		c.queue = append(c.queue, item{row: row, role: role, next: true, text: row.Text})
		return false
	}
	if row.Run != nil {
		// A scheduled run: never a command, never a steer, one turn of its
		// own. The creator's admission is decided against the job's target
		// as the turn starts (runTurn), which settles the run either way.
		role, _, _, err := c.r.opt.Access.Check(row.From, row.Run.Target)
		if err != nil {
			c.holdForAccess(row)
			return false
		}
		c.queue = append(c.queue, item{row: row, role: role, next: true, text: row.Text})
		return false
	}
	role, _, ok, err := c.r.opt.Access.Check(row.From, c.key)
	if err != nil {
		c.r.log.Printf("message %d in %s held: %v; checked again shortly", row.ID, c.key, err)
		c.holdForAccess(row)
		return false
	}
	if !ok {
		c.r.log.Printf("message %d from %s in %s rejected: caller no longer authorized", row.ID, row.From.Key(), c.key)
		_ = c.r.store.SetInbound(row.ID, store.InRejected, "caller not authorized")
		c.r.release(row.Inbound)
		return false
	}
	if row.InterfaceReply != "" {
		c.answer(row, store.CommandEnd{Detail: "interface warning"}, row.InterfaceReply)
		return false
	}
	if j := c.r.opt.Jobs; j != nil && row.Origin == core.OriginLive {
		// A bare yes or no answers the caller's one draft here.
		if text, change, ok := j.Answer(c.caller(row, role), row.Text); ok {
			c.answer(row, store.CommandEnd{Detail: "job answer", Job: change}, text)
			return false
		}
	}
	cmd := parseCommand(row.Text)
	switch cmd.Kind {
	case cmdJob:
		text, change := "The scheduler is not enabled here.", (*store.JobChange)(nil)
		if j := c.r.opt.Jobs; j != nil {
			text, change = j.Command(c.caller(row, role), cmd.Text)
		}
		c.answer(row, store.CommandEnd{Detail: "job", Job: change}, text)
		return false
	case cmdWork:
		text := "Durable commitments are not enabled here."
		if w := c.r.opt.Work; w != nil {
			// The same verified ingress caller as !job; scheduled runs are not live.
			text = w.Command(durable.Caller{Principal: row.From, Role: role, Conv: c.key, MsgID: row.MsgID, Live: row.Origin == core.OriginLive, Private: row.Private, OAuthActor: row.OAuthActor, OAuthInstallation: row.OAuthInstallation}, cmd.Text)
		}
		c.answer(row, store.CommandEnd{Detail: "work"}, text)
		return false
	case cmdInvalid:
		c.answer(row, store.CommandEnd{Detail: "invalid"}, cmd.Error)
		return false
	case cmdHelp:
		text := helpText(c.r.opt.Order)
		if c.r.opt.Jobs != nil {
			text += "\n" + strings.Join(scheduler.HelpLines, "\n")
		}
		if c.r.opt.Work != nil {
			text += "\n!work help — durable commitments (owner)"
		}
		c.answer(row, store.CommandEnd{Detail: "help"}, text)
		return false
	case cmdStatus:
		c.answer(row, store.CommandEnd{Detail: "status"}, c.status())
		return false
	case cmdWhich:
		c.answer(row, store.CommandEnd{Detail: "which"}, c.which(role, row.From.Key()))
		return false
	case cmdUse:
		if role != core.RoleOwner {
			// The conversation's harness is the owner's setup: refused before
			// any session, worker or binding changes.
			c.answer(row, store.CommandEnd{Detail: "use refused"}, "Only the owner can change this conversation's harness.")
			return false
		}
		if c.turn != nil {
			c.turn.nativeInvalid = true
		}
		c.cancelNative(c.r.ctx, "", true, false)
		for who := range c.generation {
			c.generation[who]++
		}
		c.r.warm.Invalidate(c.key, "")
		text := "This conversation now uses " + cmd.Harness + " (no fallback) until !use default."
		if cmd.Harness == "" {
			text = "This conversation is back to the default order: " + strings.Join(c.r.opt.Order, " → ") + "."
		}
		c.answer(row, store.CommandEnd{Detail: "use", SetHarness: true, Harness: cmd.Harness}, text)
		return false
	case cmdStop:
		text := c.stop(role, row.From.Key())
		if row.ControlIngress > 0 {
			text = fmt.Sprintf("Stopped %d earlier ingress message(s). %s", row.ControlIngress, strings.ReplaceAll(text, "No active run to stop.", ""))
		}
		c.answer(row, store.CommandEnd{Detail: "stop"}, strings.TrimSpace(text))
		return false
	case cmdCancel:

		n := c.cancelQueued(role, row.From.Key(), "cancelled by cancel")
		n += row.ControlIngress
		text := fmt.Sprintf("Cancelled %d queued message(s).", n)
		if row.ControlIngress > 0 {
			text = fmt.Sprintf("Cancelled %d queued/ingress message(s).", n)
		}
		if n == 0 {
			text = "No queued message of yours in this conversation."
			if role == core.RoleOwner {
				text = "No queued message in this conversation."
			}
		}
		c.answer(row, store.CommandEnd{Detail: "cancel"}, text)
		return false
	case cmdReset:
		c.answer(row, store.CommandEnd{Detail: "reset", Forget: row.From.Key()}, c.reset(row.From.Key()))
		return false
	case cmdChatID:
		c.answer(row, store.CommandEnd{Detail: "chatid"}, c.chatID(row, role))
		return false
	}
	if choice != nil && cmd.Harness == "" && !cmd.Next {
		cmd.Harness, cmd.Next = choice.Harness, choice.Next
	}
	it := item{row: row, role: role, harness: cmd.Harness, next: cmd.Next || row.MCPControl, text: cmd.Text}
	// Steer only the sender's own turn: sessions are per verified caller, so
	// nobody steers another person's turn.
	if t := c.turn; t != nil && t.done == nil && !t.stopped.Load() && !it.next && it.harness == "" && row.From.Key() == t.who {
		t.steerQ = append(t.steerQ, it)
		return true
	}
	c.queue = append(c.queue, it)
	return false
}

// stop stops the running turn, if the caller may (its sender or the owner),
// and clears the caller's queued messages (the owner's stop clears
// everyone's), as the hub's stop does.
func (c *conv) stop(role core.Role, who string) string {
	t := c.turn
	active := t != nil && t.done == nil && !t.stopped.Load()
	authorized := active && (role == core.RoleOwner || who == t.who)
	// Cancel the pending person request BEFORE stopping a background worker:
	// process exit must not release a gate into a new, already-stopped launch.
	if authorized {
		c.cancelTurn(t)
	}
	backgrounds := c.cancelNative(c.r.ctx, who, role == core.RoleOwner, true)
	if active && !authorized {
		if backgrounds > 0 {
			return "Stopping your background work."
		}
		return "Only the person who started this turn, or the owner, can stop it."
	}
	n := c.cancelQueued(role, who, "cancelled by stop")
	if authorized {
		if n > 0 {
			return fmt.Sprintf("Stopping current run. Cleared %d queued item(s).", n)
		}
		return "Stopping current run."
	}
	text := "No active run to stop."
	if backgrounds > 0 {
		text = "Stopping background work."
	}
	if n > 0 {
		return fmt.Sprintf("%s Cleared %d queued message(s).", text, n)
	}
	return text
}

func (c *conv) cancelTurn(t *turn) {
	if !t.stopped.CompareAndSwap(false, true) {
		return
	}
	if t.startCancel != nil {
		t.startCancel()
	}
	c.observe(c.r.ctx, t, "cancelled", harness.Progress{})
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil {
		run.Cancel()
	}
}

// cancelQueued drops queued messages that have not reached a harness: the
// caller's own, or everyone's for the owner. Messages already handed to the
// running turn are not affected.
func (c *conv) cancelQueued(role core.Role, who, why string) int {
	n := 0
	keep := func(items []item) []item {
		var out []item
		for _, it := range items {
			if role == core.RoleOwner || it.row.From.Key() == who {
				if err := c.r.store.SetInbound(it.row.ID, store.InCancelled, why); err != nil {
					c.r.log.Printf("message %d: cancel not recorded: %v", it.row.ID, err)
					out = append(out, it)
					continue
				}
				c.r.release(it.row.Inbound)
				n++
				continue
			}
			out = append(out, it)
		}
		return out
	}
	c.queue = keep(c.queue)
	if t := c.turn; t != nil {
		t.steerQ = keep(t.steerQ)
	}
	for key, a := range c.albums {
		var rows []store.InboundRow
		for _, it := range keep(albumItems(a)) {
			rows = append(rows, it.row)
		}
		if a.rows = rows; len(rows) == 0 && a.pending == 0 {
			delete(c.albums, key)
		}
	}
	return n
}

func albumItems(a *album) []item {
	items := make([]item, len(a.rows))
	for i, row := range a.rows {
		items[i] = item{row: row}
	}
	return items
}

// reset makes the caller's next message in this conversation start a fresh
// native session on every harness: the store forgets the caller's sessions
// with the command's reply. Sessions are per caller, so nobody else's is
// touched; the caller's own running turn is stopped and its session is not
// kept either. Native session files are left in place.
func (c *conv) reset(who string) string {
	if c.turn != nil && c.turn.who == who {
		c.cancelTurn(c.turn)
	}
	c.cancelNative(c.r.ctx, who, false, true)
	if c.generation == nil {
		c.generation = map[string]uint64{}
	}
	c.generation[who]++
	c.r.warm.Invalidate(c.key, who)
	text := "Session reset. Your next message in this conversation starts a fresh session."
	if t := c.turn; t != nil && t.who == who {
		t.reset = true
		if t.done == nil {
			c.cancelTurn(t)
			text = "Stopping your running turn. " + text
		}
	}
	return text
}

// chatID shows the conversation and the caller as Heimdall and hub tools
// see them (the hub's /chatid).
func (c *conv) chatID(row store.InboundRow, role core.Role) string {
	user, chat := harness.HubIDs(harness.Caller{Principal: row.From, Role: role, Conv: c.key, Private: row.Private})
	var b strings.Builder
	if chat != "" {
		fmt.Fprintf(&b, "chat: %s\n", chat)
	}
	if user != "" {
		fmt.Fprintf(&b, "user: %s\n", user)
	}
	private := "no"
	if row.Private {
		private = "yes"
	}
	fmt.Fprintf(&b, "principal: %s (%s)\nconversation: %s\nprivate chat: %s", row.From.Key(), role, c.key, private)
	return b.String()
}

func (c *conv) status() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Conversation %s\n", c.key)
	if h, err := c.r.store.ChatHarness(c.key); err == nil && h != "" {
		fmt.Fprintf(&b, "Harness: %s (sticky; !use default resets)\n", h)
	} else {
		fmt.Fprintf(&b, "Harness: default order %s\n", strings.Join(c.r.opt.Order, " → "))
	}
	if t := c.turn; t != nil {
		h := t.harness
		if h == "" {
			h = "starting (" + strings.Join(t.candidates, " → ") + ")"
		}
		fmt.Fprintf(&b, "Running: %s turn for %s, %s", h, t.role, time.Since(t.started).Round(time.Second))
		if t.steered > 0 {
			fmt.Fprintf(&b, ", %d steered message(s)", t.steered)
		}
		if len(t.steerQ) > 0 || t.steering {
			fmt.Fprintf(&b, ", steering in progress")
		}
		b.WriteString("\n")
	} else {
		if len(c.nativeTurns) > 0 {
			fmt.Fprintf(&b, "Running: %d native background turn(s)\n", len(c.nativeTurns))
		} else {
			b.WriteString("Running: nothing\n")
		}
	}

	for _, w := range c.r.warm.Status(c.key) {
		fmt.Fprintf(&b, "Warm: %s, idle %s\n", w.Harness, w.Idle.Round(time.Second))
	}
	held := 0
	for _, a := range c.albums {
		held += len(a.rows)
	}
	fmt.Fprintf(&b, "Queued: %d message(s)", len(c.queue)+held)
	if c.r.opt.Jobs != nil {
		b.WriteString("\n" + c.r.opt.Jobs.Status())
	}
	return b.String()
}

// which names the harness this conversation uses now and the model and
// thinking level it is configured with for the asking caller's role. Answers carry no
// signature, so it also names the running turn's harness and, when fallback
// or a one-off !<harness> made it differ, the one the last reply came from.
func (c *conv) which(role core.Role, who string) string {
	order := strings.Join(c.r.opt.Order, " → ")
	var b strings.Builder
	selected, _ := c.r.store.ChatHarness(c.key)
	if selected != "" {
		fmt.Fprintf(&b, "%s (set with !use; !use default goes back to %s)", selected, order)
	} else if len(c.r.opt.Order) > 0 {
		selected = c.r.opt.Order[0]
		fmt.Fprintf(&b, "%s (default order: %s)", selected, order)
	} else {
		b.WriteString("no harness (empty default order)")
	}
	if selected != "" {
		model, level := "", ""
		if launch, err := c.r.opt.Accounts.Prepare(role, false, harness.Binary(selected)); err == nil {
			model, level = harness.Model(selected, launch), harness.Thinking(selected, launch)
			if model == "" && selected == "claude" {
				// Claude Code picks its own default; show what this
				// conversation's last Claude reply actually used.
				if id, _, err := c.r.store.Session(c.key, who, "claude"); err == nil {
					if m := harness.ClaudeSessionModel(launch, id); m != "" {
						model = m + " (last reply)"
					}
				}
			}
		}
		if model == "" {
			model = "default"
		}
		if level == "" {
			level = "default"
		}
		fmt.Fprintf(&b, "\nModel: %s\nThinking: %s", model, level)
	}
	if t := c.turn; t != nil {
		h := t.harness
		if h == "" {
			h = "starting (" + strings.Join(t.candidates, " → ") + ")"
		}
		fmt.Fprintf(&b, "\nRunning now: %s.", h)
	}
	if last, err := c.r.store.LastTurnHarness(c.key); err == nil && last != "" && last != selected {
		fmt.Fprintf(&b, "\nLast reply came from %s.", last)
	}
	return b.String()
}

// start begins a turn with the oldest queued message and the following ones
// from the same caller with the same harness choice. The caller holds a
// parallelism slot.
func (c *conv) start(ctx context.Context) {
	if c.turn != nil || len(c.queue) == 0 {
		c.releaseSlot()
		return
	}
	if c.slotSem != nil && c.slotSem == c.r.reserved && !ownerLive(c.queue[0]) {
		// The owner's message this slot was taken for is gone: the reserved
		// slot is not for anyone else. Wait again for a general one.
		c.releaseSlot()
		return
	}
	first := c.queue[0]
	n := 1
	var job *scheduler.Turn
	if first.row.Run != nil {
		// A scheduled run is a turn of its own, if it may still start.
		ok := false
		if c.r.opt.Jobs != nil {
			job, ok = c.r.opt.Jobs.Begin(first.row)
		} else {
			_ = c.r.store.SetInbound(first.row.ID, store.InCancelled, "scheduler disabled")
		}
		if !ok {
			c.queue = c.queue[1:]
			c.r.release(first.row.Inbound)
			c.releaseSlot()
			return
		}
		first.job = job.Name
		c.queue[0] = first
	}
	review := first.row.Review
	if review != nil && (first.role != core.RoleOwner || c.r.opt.Work == nil || !current(c.r.opt.Work, review)) {
		// Revalidated as the turn actually starts: the creator is still an
		// admitted owner on the route and the attempt still waits.
		if err := c.r.store.SetInbound(first.row.ID, store.InCancelled, "held: review refused at launch: creator no longer an admitted owner on the route, or the commitment is paused, cancelled or no longer waits for this attempt's verdict"); err != nil {
			c.r.log.Printf("message %d: refused review not recorded: %v", first.row.ID, err)
		}
		c.queue = c.queue[1:]
		c.r.release(first.row.Inbound)
		c.releaseSlot()
		return
	}
	for job == nil && review == nil && n < len(c.queue) && !first.row.MCPControl && !c.queue[n].row.MCPControl && c.queue[n].row.From.Key() == first.row.From.Key() && c.queue[n].harness == first.harness {
		n++
	}
	batch := append([]item(nil), c.queue[:n]...)
	ids := make([]int64, len(batch))
	for i, it := range batch {
		ids[i] = it.row.ID
	}
	who := first.row.From.Key()
	id, err := c.r.store.StartTurn(c.key, first.role, who, first.row.MsgID, ids)
	if err != nil {
		c.r.log.Printf("turn in %s not started: %v", c.key, err)
		// A committed job stop can cancel the inbound between Begin and
		// StartTurn. Keep that cancellation; its command already answered.
		cancelled := false
		if job != nil {
			run, readErr := c.r.store.Run(job.Run)
			cancelled = readErr == nil && run.State == store.RunCancelled
		}
		c.releaseSlot()
		c.queue = c.queue[n:]
		for _, it := range batch {
			if !cancelled {
				_ = c.r.store.SetInbound(it.row.ID, store.InFailed, "turn could not be recorded")
			}
			c.r.release(it.row.Inbound)
			if !cancelled && review == nil {
				c.reply(it.row, "Heimdall could not record this turn, so it did not run: "+err.Error()+"\n— heimdall")
			}
		}
		return
	}
	c.queue = c.queue[n:]
	if c.generation == nil {
		c.generation = map[string]uint64{}
	}
	c.generation[who] = c.generation[who]
	startCtx, startCancel := context.WithCancel(ctx)
	t := &turn{startCtx: startCtx, startCancel: startCancel, generation: c.generation[who], id: id, role: first.role, from: first.row.From, who: who, private: first.row.Private, replyTo: first.row.MsgID, started: time.Now(), batch: batch, job: job, review: review}
	// Scheduled sessions stay in sched:<job>, while their routing preference
	// comes from the creator-bound source conversation at actual turn start.
	route := c.key
	if job != nil {
		route = job.Source
	}
	if review != nil {
		route = review.Target // the creator's harness choice there
		t.private, t.replyTo = false, ""
	}
	switch sticky, _ := c.r.store.ChatHarness(route); {
	case job != nil && job.Def.Harness != "":
		t.candidates, t.explicit = []string{job.Def.Harness}, true
	case job != nil && sticky != "":
		t.candidates, t.explicit = []string{sticky}, true
	case job != nil:
		t.candidates = c.r.opt.Order
	case first.harness != "":
		t.candidates, t.explicit = []string{first.harness}, true
	case sticky != "":
		t.candidates, t.explicit = []string{sticky}, true
	default:
		t.candidates = c.r.opt.Order
	}
	c.turn = t
	if job != nil {
		t.private, t.replyTo = false, ""
		c.r.mu.Lock()
		c.r.jobs[job.Job] = t
		c.r.mu.Unlock()
		// Register first, then read the durable command fence. A committed stop
		// either sees this registration or is observed here before launch.
		if r, err := c.r.store.Run(job.Run); err != nil || r.State != store.RunRunning {
			t.stopped.Store(true)
		}
	}
	c.r.log.Printf("turn %d in %s: %d message(s), role %s, candidates %s", id, c.key, len(batch), t.role, strings.Join(t.candidates, ","))
	c.startTyping(ctx, t)
	go c.runTurn(ctx, t, batch)
}

// prompt is a turn's input: the interface's prompt note, then each message
// framed with its caller, context and attachments. It runs once the turn's
// OS account is known, which the attachments are copied for.
func (c *conv) prompt(t *turn, batch []item) (string, []harness.Image) {
	if len(batch) == 1 && batch[0].row.MCPControl {
		return batch[0].text, nil
	}
	var parts, human, contextParts []string
	var images []harness.Image
	if p, ok := c.r.ifaces[c.key.Interface].(Prompter); ok {
		if note := strings.TrimSpace(p.PromptNote()); note != "" {
			parts = append(parts, note)
			t.instructions = note
		}
	}
	if note := c.outboxNote(t); note != "" && t.review == nil {
		parts = append(parts, note)
		contextParts = append(contextParts, note)
	}
	if recent := c.ambient(t, batch); recent != "" {
		parts = append(parts, recent)
		contextParts = append(contextParts, recent)
	}
	if t.job == nil && t.review == nil {
		c.react(t, batch)
	}
	for _, it := range batch {
		blocks, ims := c.files(t, it)
		parts = append(parts, frame(it, blocks))
		human = append(human, humanText(it, blocks))
		contextParts = append(contextParts, frameContext(it))
		images = append(images, ims...)
	}
	if t.job != nil {
		// The job's own instructions (quiet, AgentNet delivery) follow its
		// prompt in the native message: as turn context, Pi ignored them.
		parts, human = append(parts, t.job.Notes...), append(human, t.job.Notes...)
	}
	if len(images) > maxImages {
		images = images[:maxImages]
	}
	t.nativeText, t.nativeContext = strings.Join(human, "\n\n"), strings.Join(contextParts, "\n\n")
	return strings.Join(parts, "\n\n"), images
}

// startTyping shows the interface's typing indicator now and every
// TypingEvery until the turn ends.
func (c *conv) startTyping(ctx context.Context, t *turn) {
	ty, ok := c.r.ifaces[c.key.Interface].(Typer)
	if !ok || t.job != nil || t.review != nil {
		return
	}
	t.typing = make(chan struct{})
	stop := t.typing
	go func() {
		tick := time.NewTicker(c.r.opt.TypingEvery)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if c.r.safely("typing", func() { ty.Typing(ctx, c.key) }) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
}

// typedOnce shows the typing indicator once, without waiting for it.
func (c *conv) typedOnce(ctx context.Context) {
	if ty, ok := c.r.ifaces[c.key.Interface].(Typer); ok {
		go c.r.safely("typing", func() { ty.Typing(ctx, c.key) })
	}
}

func (c *conv) send(ctx context.Context, ev any) {
	select {
	case c.events <- ev:
	case <-ctx.Done():
	}
}

// runTurn tries the candidates in order. A candidate that fails before its
// turn is ready did nothing, so the next one may take over; once a harness
// is ready the turn is committed to it.
func (c *conv) runTurn(ctx context.Context, t *turn, batch []item) {
	defer c.recoverTurn(ctx, t)
	c.observe(ctx, t, "start", harness.Progress{})
	var notes []string
	// gated holds employee-gate skips. They only reach the reply when no
	// harness took the turn: a skipped engine is not news if another answered.
	var gated []string
	var prompt string
	var images []harness.Image
	memory := core.Memory(c.r.opt.Access.Mode, t.role, t.from)
	// A scheduled run is decided against, and runs for, its job's target;
	// it never captures memory.
	target, refused := c.key, false
	from, actor, installation := t.from, "", ""
	if len(batch) > 0 {
		actor, installation = batch[0].row.OAuthActor, batch[0].row.OAuthInstallation
	}
	var identity error
	if t.job != nil {
		target, memory = t.job.Target.Conv, false
		// The creator is checked now, as the turn starts: never only when
		// the run was handed over (it may have waited, or been replayed
		// after a restart).
		v, err := c.r.opt.Jobs.Admit(ctx, t.job)
		if identity = err; err == nil {
			from, actor, installation = v.Principal, v.OAuthActor, v.OAuthInstallation
		}
	}
	if t.review != nil {
		// Decided against, and run for, the commitment's route. A review,
		// dispatch, wait or read captures no memory; an assistant wake
		// keeps the owner's ordinary memory decision.
		target = t.review.Target
		if !t.review.Work {
			memory = false
		}
	}
	for _, h := range t.candidates {
		if ctx.Err() != nil || t.stopped.Load() || identity != nil {
			break
		}
		current, caps, admitted, err := c.r.opt.Access.Check(from, target)
		if err != nil {
			// Unreadable access files are not a refusal: the turn waits
			// for them, before any harness, unless it is stopped.
			c.r.log.Printf("turn %d in %s waits: %v", t.id, c.key, err)
			wait := t.startCtx
			if wait == nil {
				wait = ctx
			}
			for err != nil && wait.Err() == nil && !t.stopped.Load() {
				sleep(wait, c.r.accessRetry)
				current, caps, admitted, err = c.r.opt.Access.Check(from, target)
			}
			if err != nil {
				break
			}
		}
		if !admitted || current != t.role {
			notes = append(notes, "caller admission or role changed; turn refused")
			refused = true
			break
		}
		if t.role == core.RoleEmployee && !slices.Contains(c.r.opt.EmployeeHarnesses, h) {
			gated = append(gated, h+": employee turns unavailable until native isolation and skill reading are validated")
			continue
		}

		launch, err := c.r.opt.Accounts.Prepare(t.role, memory, harness.Binary(h))
		var account *harness.AccountError
		if errors.As(err, &account) {
			notes = append(notes, err.Error())
			break // every harness would fail the same way
		}
		if err != nil {
			notes = append(notes, h+": "+err.Error())
			continue
		}
		if c.r.opt.Jobs != nil && (h == "pi" || h == "omp") {
			// Heimdall schedules: the hub's model-called scheduler tools stay off.
			launch = launch.WithEnv("PI_SCHEDULER_TOOLS", "0")
		}
		if prompt == "" {
			// Every candidate runs as the same account, so the first one
			// prepared fixes the turn's directory and prompt.
			t.mu.Lock()
			t.launch = launch
			t.mu.Unlock()
			prompt, images = c.prompt(t, batch)
		}
		if t.role == core.RoleEmployee {
			dir, e := c.turnDir(t)
			if e != nil {
				notes = append(notes, "employee turn directory unavailable")
				break
			}
			launch.Dir = dir.Path()
		}
		sess := harness.Session{}
		// Capture a retained process BEFORE reading clean state. A native
		// start after the snapshot cannot silently turn into a cold resume.
		retainedID, retainedWorker := c.r.warm.RetainedSession(c.key, t.who, h)
		prev, clean, err := c.r.store.Session(c.key, t.who, h)
		liveID := ""
		var liveWorker *harness.Run
		if err == nil && prev != "" {
			if clean && retainedID == prev {
				liveID, liveWorker = retainedID, retainedWorker
			} else if !clean {
				liveID, liveWorker = c.r.warm.NativeSession(c.key, t.who, h)
			}
		}
		switch {
		case err != nil:
			notes = append(notes, h+": session lookup failed: "+err.Error())
			continue
		case t.job != nil && t.job.Def.Session != "job", t.review != nil:
			sess.ID = harness.NewSessionID(h)
		case prev != "" && (clean || liveID == prev):
			sess = harness.Session{ID: prev, Resume: true}
		default:
			sess.ID = harness.NewSessionID(h)
			if prev != "" {
				notes = append(notes, "new "+h+" session: the previous turn there did not finish cleanly")
			}
		}
		sessionNote := c.sessionPointer(t, h, sess, launch, batch[0].row.MCPControl)
		ownerInputs := c.ownerRequestContext(t, h, sess, prev, clean, batch[0].row.Inbound)
		// Recorded before the harness gets any input: a crash from here on
		// leaves this session not resumable.
		if err := c.r.store.BindTurn(t.id, c.key, t.who, h, sess.ID); err != nil {
			notes = append(notes, "turn not recordable, so it did not run: "+err.Error())
			break
		}
		caller := harness.Caller{Capabilities: caps, Principal: from, Role: t.role, Conv: target, Private: t.private, OAuthActor: actor, OAuthInstallation: installation}
		caller.SlackRead = harness.SlackReadAllowed(c.r.opt.OwnerSlack, batch[0].row.Inbound, t.role, h, c.r.opt.SlackTeam)
		logf := guarded(c.r, "harness log", func(s string) { c.r.log.Printf("turn %d in %s: %s", t.id, c.key, s) })
		var progress func(harness.Progress)
		if _, ok := c.r.ifaces[c.key.Interface].(TurnObserver); ok && t.job == nil && t.review == nil {
			progress = func(p harness.Progress) { c.observe(ctx, t, "progress", p) }
		}
		if t.job != nil {
			// Tool names and sign-in requests only, for the job's outcome;
			// nothing is shown.
			progress = func(p harness.Progress) {
				t.mu.Lock()
				if p.Tool != "" {
					if !p.Completed {
						t.toolsStarted = append(t.toolsStarted, p.Tool)
					}
					if p.Completed && !p.Error && p.SignIn == "" {
						t.tools = append(t.tools, p.Tool)
					}
					if p.Completed && p.Error {
						if len(t.toolErrors) < 2 {
							name := p.Tool
							if strings.TrimLeft(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:-") != "" {
								name = "native_tool"
							}
							name = clipRunes(name, 80)
							detail := core.SanitizeNativeError(p.ErrorSummary, t.job.Def.Prompt)
							if detail != "" {
								name += ": " + detail
							}
							t.toolErrors = append(t.toolErrors, name)
						}
					}
				}
				if p.SignIn != "" && !slices.Contains(t.signIn, p.SignIn) {
					t.signIn = append(t.signIn, p.SignIn)
				}
				t.mu.Unlock()
			}
		}
		nativePrompt, instructions, turnContext := prompt, "", ""
		if h == "codex" || h == "claude" || h == "pi" {
			nativePrompt, instructions, turnContext = t.nativeText, t.instructions, t.nativeContext
			if batch[0].row.MCPControl {
				nativePrompt = prompt
			}
		}
		if c.r.opt.Jobs != nil && t.job == nil && t.review == nil && !t.detached && !batch[0].row.MCPControl {
			// A new native session learns once how to propose a job.
			note := ""
			if !sess.Resume {
				note = scheduler.Note
				if t.role == core.RoleOwner {
					note = scheduler.OwnerNote
				}
			}
			// Every live owner request turn, resumed sessions included,
			// learns how to track accepted work: sessions started before
			// intake existed must not miss it.
			if t.role == core.RoleOwner && batch[0].row.Origin == core.OriginLive && c.r.opt.Jobs.TaskIntake && c.r.opt.Work != nil {
				note = strings.TrimSpace(note + "\n" + scheduler.TaskNote)
			}
			// Pi reads notes after the prompt in the message: as turn
			// context, live Pi ignored them.
			switch {
			case note == "":
			case h == "pi":
				nativePrompt = strings.TrimSpace(nativePrompt + "\n\n" + note)
			case turnContext != "" || nativePrompt != prompt:
				turnContext = strings.TrimSpace(note + "\n\n" + turnContext)
			default:
				nativePrompt = note + "\n\n" + nativePrompt
			}
		}
		if sessionNote != "" {
			if h == "pi" {
				nativePrompt = strings.TrimSpace(nativePrompt + "\n\n" + sessionNote)
			} else if h == "claude" || h == "codex" {
				turnContext = strings.TrimSpace(sessionNote + "\n\n" + turnContext)
			} else {
				nativePrompt = sessionNote + "\n\n" + nativePrompt
			}
		}
		if ownerInputs != "" {
			turnContext = strings.TrimSpace(ownerInputs + "\n\n" + turnContext)
		}
		var notify func(string)
		if t.job == nil && t.review == nil {
			notify = func(text string) {
				if iface := c.r.ifaces[c.key.Interface]; iface != nil {
					_ = iface.Send(ctx, core.Outbound{Conv: c.key, ReplyTo: t.replyTo, Text: text})
				}
			}
		}
		progress, notify = guarded(c.r, "harness progress", progress), guarded(c.r, "harness notice", notify)
		spec := harness.Spec{SchedulerControl: t.job == nil && !batch[0].row.MCPControl && batch[0].row.Origin == core.OriginLive || t.job != nil && t.role == core.RoleOwner, StartContext: t.startCtx, LiveWorkerOnly: liveWorker, MCPNotify: notify, Google: c.r.opt.Google, OwnerSlack: c.r.opt.OwnerSlack, Harness: h, Prompt: nativePrompt, Instructions: instructions, Context: turnContext, Images: images, Session: sess, Launch: launch, Caller: caller, Log: logf, Progress: progress}
		if t.review != nil {
			// One bounded turn; no scheduler or MCP control, no progress,
			// notifications or continuations (spec fields left off).
			spec.OneShot, spec.Timeout = true, reviewTimeout
			if t.review.Worker != "" {
				// A dispatch turn: its result is the engine's own record of
				// the T3 send, never the model's answer.
				tool := t3SendTool
				if t.review.Read {
					tool = t3ReadTool
				} else if t.review.Run != "" {
					tool = t3WaitTool
				}
				spec.NativeCalls = &harness.NativeCalls{Tools: []string{tool}, Done: guarded(c.r, "harness native call", func(n harness.NativeCall) {
					t.mu.Lock()
					t.native = append(t.native, n)
					t.mu.Unlock()
				})}
			}
		}
		if t.job == nil && t.review == nil {
			spec.Continuation = func(ev harness.Continuation) (ok bool) {
				defer func() {
					if p := recover(); p != nil {
						c.r.log.Printf("recovered panic (%T) in harness continuation", p)
						ok = false
					}
				}()
				e := continuationEvent{source: t, harness: h, native: ev}
				if ev.Phase == "start" {
					e.accepted = make(chan bool, 1)
				}
				c.send(ctx, e)
				if e.accepted != nil {
					select {
					case accepted := <-e.accepted:
						return accepted
					case <-ctx.Done():
						return false
					}
				}
				return ctx.Err() == nil
			}
		}
		spec.WarmContext = c.deliveredContext(t, h, batch[0].row.Inbound)
		if t.job != nil {
			// Read-only scheduler control, bound to this exact run; the
			// scheduler re-verifies the run, creator and owner per call.
			spec.ScheduledJob, spec.ScheduledRun = t.job.Job, t.job.Run
			spec.OneShot, spec.Timeout = true, t.job.Timeout
			if t.job.Def.RerunOnTimeout {
				// All candidates share this execution's limit. A rerun additionally
				// shares the slot's absolute recovery deadline.
				deadline := t.started.Add(t.job.Timeout)
				if t.job.Replayed {
					limit, _ := time.ParseDuration(t.job.Def.Timeout)
					if recovery := t.job.Due.Add(2 * limit); recovery.Before(deadline) {
						deadline = recovery
					}
				}
				remaining := time.Until(deadline)
				if remaining <= 0 {
					c.sendDone(ctx, doneEvent{t: t, harness: h, res: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}, notes: notes})
					return
				}
				spec.Timeout = min(spec.Timeout, remaining)
			}
		}
		run, err := c.startCandidate(ctx, t, spec)
		if err != nil {
			c.unbind(t, h, prev, clean)
			if t.stopped.Load() || t.startCtx != nil && t.startCtx.Err() != nil {
				break
			}
			notes = append(notes, err.Error())
			continue
		}
		t.mu.Lock()
		t.run = run
		t.mu.Unlock()
		if t.stopped.Load() {
			run.Cancel()
		}
		select {
		case <-run.Ready():
		case <-run.Done():
		}
		select {
		case <-run.Ready():
			c.commit(ctx, t, h, run, notes)
			return
		default:
		}
		if ctx.Err() != nil {
			// Shutdown: the binding stays, as after a crash, so the next
			// start reports the turn and never resumes the session.
			break
		}
		// An answer commits before ready too; only a failed candidate must exit
		// before fallback.
		res := run.Result()
		if res.Answer != "" || res.NeedsHuman != "" {
			// It answered, or asked for an approval, without signalling
			// ready first: it took the turn, so side effects were possible
			// and no other harness may run it.
			c.commit(ctx, t, h, run, notes)
			return
		}
		<-run.Exited()
		if !res.Cause.Limited() {
			c.unbind(t, h, prev, clean)
		}
		if res.Cancelled && !res.Cause.Limited() {
			break
		}
		if res.Cancelled {
			// Preserve a pre-ready limit result. It must not become a generic
			// startup failure (or a scheduler startup retry).
			c.sendDone(ctx, doneEvent{t: t, harness: h, res: res, notes: notes})
			return
		}
		why := res.Failure
		if why == "" {
			why = res.NeedsHuman
		}
		notes = append(notes, h+" failed before starting: "+why)
	}
	notes = append(gated, notes...)
	c.sendDone(ctx, doneEvent{t: t, res: harness.Result{Cancelled: ctx.Err() != nil || t.stopped.Load()}, notes: notes, refused: refused, identity: identity})
}

// commit ends the turn with harness h's outcome: h took the turn, so no
// other harness runs it.
func (c *conv) commit(ctx context.Context, t *turn, h string, run *harness.Run, notes []string) {
	if t.job != nil {
		// Past the side-effect boundary: a restart now never re-runs it.
		if err := c.r.store.TurnReady(t.id); err != nil {
			c.r.log.Printf("turn %d: ready not recorded: %v", t.id, err)
		}
	}
	c.observe(ctx, t, "progress", harness.Progress{Phase: "running"})
	if err := c.r.store.BindTurn(t.id, c.key, t.who, h, run.SessionID()); err != nil {
		// The session is already recorded as in progress; only a new
		// session's native ID is missing, so a crash now means the next
		// turn starts a fresh one.
		c.r.log.Printf("turn %d: native session ID not recorded: %v", t.id, err)
	}
	c.send(ctx, readyEvent{t: t, harness: h})
	done := doneEvent{t: t, harness: h, res: run.Result(), notes: notes}
	if r := done.res; r.Failure == "" && r.NeedsHuman == "" && !r.Cancelled {
		done.files, done.skipped = c.collect(t)
	}
	c.sendDone(ctx, done)
}

// sessionPointer uses only existing bindings, before BindTurn overwrites them.
func (c *conv) sessionPointer(t *turn, h string, sess harness.Session, launch harness.Launch, control bool) string {
	sessionNote := ""
	if t.role == core.RoleOwner && t.job == nil && t.review == nil && !control {
		earlierHarness, earlierID, err := c.r.store.EarlierSession(c.key, t.who, h, sess.Resume)
		if err != nil {
			c.r.log.Printf("earlier session lookup failed")
		}
		if err == nil {
			if path := harness.SessionPath(earlierHarness, launch, earlierID); path != "" {
				sessionNote = fmt.Sprintf("Earlier work in this conversation is in the %s session file %q. Read the end first, only if you need it.", earlierHarness, path)
			}
		}
	}
	return sessionNote
}

// unbind restores h's session binding after h failed before its turn was
// ready. If that cannot be recorded the session stays not resumable, which
// only costs a fresh session.
func (c *conv) unbind(t *turn, h, prev string, clean bool) {
	if err := c.r.store.UnbindTurn(t.id, c.key, t.who, h, prev, clean); err != nil {
		c.r.log.Printf("turn %d: %s session binding not restored: %v", t.id, h, err)
	}
}

// steerNext hands every waiting steer to the running turn as one steer,
// framed in order with all their images, once the harness is ready and no
// other steer is being handed over. One at a time, each waited for the
// harness's answer to the one before (OMP answered the first alone).
// Waiting steers are all the turn caller's (route).
func (c *conv) steerNext(ctx context.Context, t *turn) {
	if !t.ready || t.steering || t.done != nil || len(t.steerQ) == 0 {
		return
	}
	items := t.steerQ
	t.steerQ = nil
	// Recorded before the hand-over: from here on a crash reports the
	// message as unconfirmed instead of replaying it.
	for i, it := range items {
		if err := c.r.store.Steering(t.id, it.row.ID); err != nil {
			c.r.log.Printf("message %d: not steered, steer not recordable: %v", it.row.ID, err)
			for _, done := range items[:i] {
				if err := c.r.store.Unsteer(done.row.ID); err != nil {
					c.r.log.Printf("message %d: steer not undone: %v", done.row.ID, err)
				}
			}
			c.requeue(items...)
			c.steerNext(ctx, t)
			return
		}
	}
	t.steering = true
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	go func() {
		var refs []string
		defer func() {
			if p := recover(); p != nil {
				// Whether the harness got it is unknown: never replayed.
				c.r.log.Printf("recovered panic (%T) in turn %d steer", p, t.id)
				c.send(ctx, steerEvent{t: t, items: items, refs: refs, outcome: harness.SteerUnknown})
			}
		}()
		var parts, contextParts []string
		var images []harness.Image
		refs = c.react(t, items)
		for _, it := range items {
			blocks, ims := c.files(t, it)
			if t.harness == "codex" || t.harness == "claude" || t.harness == "pi" {
				parts = append(parts, humanText(it, blocks))
				contextParts = append(contextParts, frameContext(it))
			} else {
				parts = append(parts, frame(it, blocks))
			}
			images = append(images, ims...)
		}
		if len(images) > maxImages {
			images = images[:maxImages]
		}
		outcome := run.SteerContext(ctx, fmt.Sprintf("heimdall-%d", items[0].row.ID), strings.Join(parts, "\n\n"), images, strings.Join(contextParts, "\n\n"))
		c.send(ctx, steerEvent{t: t, items: items, refs: refs, outcome: outcome})
	}()
}

func (c *conv) steerDone(ctx context.Context, ev steerEvent) {
	t := ev.t
	t.steering = false
	switch ev.outcome {
	case harness.SteerAccepted:
		c.observe(ctx, t, "steer", harness.Progress{})
		t.steered += messages(ev.items)
		for _, it := range ev.items {
			if err := c.r.store.Steered(t.id, it.row.ID); err != nil {
				c.r.log.Printf("message %d: steer not recorded: %v", it.row.ID, err)
			}
			c.r.release(it.row.Inbound)
		}
		if t.done == nil {
			c.typedOnce(ctx)
		}
	case harness.SteerUnknown:
		n := messages(ev.items)
		c.r.log.Printf("turn %d: message %d (%d in all) steered, acceptance unknown; not replayed", t.id, ev.items[0].row.ID, n)
		for _, it := range ev.items {
			_ = c.r.store.SetInbound(it.row.ID, store.InUnconfirmed, "steer acceptance unknown; not replayed")
			c.r.release(it.row.Inbound)
		}
		what := "This message"
		if n > 1 {
			what = fmt.Sprintf("This message and the %d after it", n-1)
		}
		c.reply(ev.items[0].row, "⚠ "+what+" may or may not have reached the running "+t.harness+" turn. It will not be re-sent automatically; resend it if the answer does not cover it.\n— heimdall")
	default:
		// Not handed over: run them as a follow-up turn.
		c.r.log.Printf("turn %d: message %d (%d in all) not steered (%s); runs as a follow-up", t.id, ev.items[0].row.ID, messages(ev.items), ev.outcome)
		c.unreact(t, ev.refs)
		for _, it := range ev.items {
			if err := c.r.store.Unsteer(it.row.ID); err != nil {
				c.r.log.Printf("message %d: steer not undone: %v", it.row.ID, err)
			}
		}
		c.requeue(ev.items...)
	}
	if t.done != nil {
		c.finish(ctx, t)
		return
	}
	c.steerNext(ctx, t)
}

// messages counts items as the caller sent them: an album is one message.
func messages(items []item) int {
	n := 0
	for i, it := range items {
		if it.row.Group == "" || i == 0 || items[i-1].row.Group != it.row.Group {
			n++
		}
	}
	return n
}

// requeue returns messages to the follow-up queue in arrival order.
func (c *conv) requeue(items ...item) {
	for _, it := range items {
		i := len(c.queue)
		for i > 0 && c.queue[i-1].row.ID > it.row.ID {
			i--
		}
		c.queue = slices.Insert(c.queue, i, it)
	}
}

// finish settles a turn: its messages, its session binding and its reply in
// one transaction. Steers never handed over become follow-ups.
func (c *conv) finish(ctx context.Context, t *turn) {
	d := t.done
	if ctx.Err() != nil {
		// Shutdown: leave the turn recorded as running; the next start reports it.
		return
	}
	c.stopTyping(t)
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil && !t.detached {
		c.exited = run.Exited()
		if run.Warm() {
			c.exited = nil
		}
	}
	if !t.detached {
		c.requeue(t.steerQ...)
		t.steerQ = nil
	}
	res := d.res
	if t.nativeInvalid {
		res.Unclean = true
	}
	if t.stopped.Load() {
		res.Cancelled = true
	}
	if cleaner, ok := c.r.ifaces[c.key.Interface].(AnswerCleaner); ok {
		t.mu.Lock()
		dir := t.dir
		t.mu.Unlock()
		if dir != nil {
			inDir, outDir := dir.Path()+"/in", dir.Outbox()
			res.Answer = cleaner.CleanAnswer(res.Answer, inDir, outDir)
			answers := make([]string, len(res.Answers))
			for i, a := range res.Answers {
				answers[i] = cleaner.CleanAnswer(a, inDir, outDir)
			}
			res.Answers = answers
		}
	}
	if t.review != nil {
		c.settle(ctx, t, c.reviewEnd(t, d, res))
		return
	}
	if t.job != nil {
		// A timeout rerun may be dispatched as soon as EndTurn commits. Reap
		// the old native process before making that durable transition.
		if run != nil && res.Cause.Limited() {
			<-run.Exited()
		}
		c.settle(ctx, t, c.jobEnd(t, d, res))
		return
	}
	var draft *store.Job
	var jobChange *store.JobChange
	var card string
	if t.detached {
		res.Answer = scheduler.WithoutProposals(res.Answer)
		for i, a := range res.Answers {
			res.Answers[i] = scheduler.WithoutProposals(a)
		}
	}
	if j := c.r.opt.Jobs; j != nil && !t.detached && d.harness != "" && res.Failure == "" && res.NeedsHuman == "" && !res.Cancelled {
		// Direct owner requests apply with the notice; other proposals ask.
		// The block itself is never sent.
		answers := res.Answers
		if len(answers) == 0 {
			answers = []string{res.Answer}
		}
		var clean []string
		// Only a live request in the owner's private chat applies at once: in a
		// shared chat other people's text reached the model, so a proposal
		// there waits for the owner's confirmation card.
		direct := t.private && t.job == nil && !t.batch[0].row.MCPControl && t.batch[0].row.Origin == core.OriginLive
		if clean, draft, jobChange, card = j.Prepare(scheduler.Caller{Principal: t.from, Role: t.role, Conv: c.key, Private: t.private}, answers, direct); card != "" {
			res.Answers, res.Answer = clean, strings.Join(slices.DeleteFunc(slices.Clone(clean), func(a string) bool { return a == "" }), "\n\n")
		}
	}
	// Harness answers carry no signature (!which tells); only Heimdall's
	// own notice is signed. Notes stay as a plain footer.
	notes := d.notes
	if res.Warning != "" {
		notes = append(notes, res.Warning)
	}
	for _, s := range d.skipped {
		notes = append(notes, "file not sent: "+s)
	}
	var footer string
	if len(notes) > 0 {
		for i := range notes {
			notes[i] = c.r.scrub(notes[i])
		}
		footer = "(" + strings.Join(clipAll(notes, 160), "; ") + ")"
	}
	if d.harness == "" {
		footer = strings.TrimSpace("— heimdall " + footer)
	}
	if footer != "" {
		footer = "\n\n" + footer
	}
	e := store.TurnEnd{Harness: d.harness, Session: res.SessionID, Forget: t.reset}
	var text string
	switch {
	case res.Cancelled && t.stopped.Load():
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "stopped by stop or reset"
		if t.interruption != "" {
			e.Detail = "pending background work interrupted: " + t.interruption
		}
		text = "⏹ Stopped."
	case res.Cancelled && res.Cause.Limited():
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, string(res.Cause)
		text = "⏱ This turn hit its " + string(res.Cause) + " limit and was stopped. Work may be unfinished and was not re-run. Anything it already did may stand; check existing task IDs, results and sends before resending. Your next message starts a fresh session.\n— heimdall"
	case d.panicked:
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, "internal error"
		text = "⚠ Something went wrong in Heimdall during this turn. It was stopped and not re-run; resend if still needed."
	case d.harness == "":
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, "no harness could start"
		text = "⚠ No harness could take this turn."
	case res.NeedsHuman != "":
		e.State, e.ItemState, e.Detail = store.TurnNeedsHuman, store.InNeedsHuman, res.NeedsHuman
		text = "⚠ " + d.harness + " needs a native approval or input that this turn cannot collect. Nothing was approved.\n" + res.NeedsHuman
	case res.Cancelled:
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "cancelled"
		text = "⏹ The turn was cancelled."
	case res.Failure != "":
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, res.Failure
		text = "⚠ " + d.harness + " failed: " + failureReason(c.r.scrub(res.Failure)) + ". Please retry, or send reset if this keeps happening."
	default:
		e.State, e.ItemState, e.Clean = store.TurnDone, store.InDone, !res.Unclean
		text = strings.TrimSpace(res.Answer)
	}
	e.Reply = &core.Outbound{TurnID: t.id, Conv: c.key, ReplyTo: t.replyTo, Formatted: e.State == store.TurnDone}
	if r, ok := c.r.ifaces[c.key.Interface].(Reactor); ok && e.State == store.TurnDone {
		t.mu.Lock()
		var directives bool
		text, directives, e.Reply.Reactions = answerReactions(res.Answers, t.refs, r.Reaction)
		t.mu.Unlock()
		switch {
		case text != "":
		case len(e.Reply.Reactions) > 0:
			footer = "" // a reaction-only answer (the hub's)
		case directives:
			text, e.Reply.Formatted = "Could not confirm reaction delivery.", false
		}
	}
	if e.State == store.TurnDone && text == "" && len(e.Reply.Reactions) == 0 && len(d.files) > 0 {
		text = "Attachment"
	}
	if text+footer != "" {
		e.Reply.Text = c.r.scrub(stripHarnessTag(text, d.harness) + footer)
	}
	if e.State == store.TurnCancelled && t.stopped.Load() {
		e.Reply = nil
	}
	if e.State == store.TurnDone {
		e.Files = c.fileReplies(t, d.files)
		if card != "" {
			e.Draft, e.Job, e.Notices = draft, jobChange, []core.Outbound{{Conv: c.key, ReplyTo: t.replyTo, Text: c.r.scrub(card), Formatted: scheduler.Formatted(card)}}
			if e.Reply != nil && e.Reply.Text == "" && len(e.Reply.Reactions) == 0 {
				e.Reply = nil // only the block: Heimdall's notice says it
			}
		}
	} else {
		removeFiles(d.files)
	}
	c.settle(ctx, t, e)
}

// settle records a turn's end, with its reply, files and notices, and
// frees its slot.
func (c *conv) settle(ctx context.Context, t *turn, e store.TurnEnd) {
	if t.detached {
		e.PreserveSession = true
	} // never overwrite a newer running session binding
	c.endTurn(ctx, t, e)
	if t.startCancel != nil {
		t.startCancel()
	}
	if !t.detached {
		for _, it := range t.batch {
			c.r.release(it.row.Inbound)
		}
	}
	if !(e.State == store.TurnCancelled && t.stopped.Load()) {
		c.observe(ctx, t, e.State, harness.Progress{})
	}
	if e.Job != nil && c.r.opt.Jobs != nil {
		c.r.opt.Jobs.Wake()
	}
	c.r.wake(c.key.Interface)
	for _, o := range e.Notices {
		c.r.wake(o.Conv.Interface)
	}
	if e.Reply != nil {
		c.r.wake(e.Reply.Conv.Interface)
	}
	c.r.log.Printf("turn %d in %s ended: %s (harness %q, %s)", t.id, c.key, e.State, e.Harness, time.Since(t.started).Round(time.Millisecond))
	if t.job != nil {
		c.r.mu.Lock()
		delete(c.r.jobs, t.job.Job)
		c.r.mu.Unlock()
		c.r.opt.Jobs.Wake()
	}
	t.published = true
	if t.detached {
		return
	}
	c.turn = nil
	// A native result ends the running turn; idle/cleanup processes hold no slot.
	c.releaseSlot()
	c.flushContinuations(ctx)
}

// endTurn records a turn's end with its reply. A failed write is retried
// with backoff, each time only while the turn is still recorded as running,
// so the reply is never queued twice. If it still cannot be recorded, the
// reply text is sent once straight to its chat (best effort, outside the
// outbox) and the turn is left running for restart recovery to report.
func (c *conv) endTurn(ctx context.Context, t *turn, e store.TurnEnd) {
	_, err := c.r.store.EndTurn(t.id, c.key, t.who, e)
	for attempt := 0; err != nil && attempt < c.r.endAttempts && ctx.Err() == nil; attempt++ {
		c.r.log.Printf("turn %d: end not recorded, retrying: %v", t.id, err)
		sleep(ctx, c.r.endRetry<<attempt)
		running, rerr := c.r.store.TurnIsRunning(t.id)
		switch {
		case rerr != nil:
			err = rerr
		case !running:
			return // the earlier write was recorded after all
		default:
			_, err = c.r.store.EndTurn(t.id, c.key, t.who, e)
		}
	}
	if err == nil {
		return
	}
	c.r.log.Printf("turn %d: end not recorded: %v", t.id, err)
	if ctx.Err() != nil || e.Reply == nil || strings.TrimSpace(e.Reply.Text) == "" {
		return
	}
	if running, rerr := c.r.store.TurnIsRunning(t.id); rerr != nil || !running {
		return // unknown, or recorded: the outbox has (or may have) the reply
	}
	i := c.r.ifaces[e.Reply.Conv.Interface]
	if i == nil {
		return
	}
	out := *e.Reply
	out.ID, out.TurnID, out.File = 0, 0, nil
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := i.Send(sctx, out); err != nil {
		c.r.log.Printf("turn %d: unrecorded reply not sent directly: %v", t.id, err)
		return
	}
	c.r.log.Printf("turn %d: reply sent directly, end left for restart recovery", t.id)
}

// jobEnd is a scheduled turn's end as its job decides it: the reply and
// files go to the job's target, not to the run's own conversation.
func (c *conv) jobEnd(t *turn, d *doneEvent, res harness.Result) store.TurnEnd {
	t.mu.Lock()
	tools, signIn, toolErrors := t.tools, t.signIn, t.toolErrors
	t.mu.Unlock()
	files := make([]core.File, len(d.files))
	for i, f := range d.files {
		files[i] = core.File{Path: f.Path, Name: f.Name, MIME: mimeOf(f.Name)}
	}
	// Structured checkpoint state is persisted too; apply the same existing
	// secret/callback redaction before the scheduler parses any result fields.
	res.Answer = c.r.scrub(stripHarnessTag(res.Answer, d.harness))
	e := c.r.opt.Jobs.End(scheduler.Outcome{Turn: t.job, TurnID: t.id, Harness: d.harness, Result: res, Stopped: t.stopped.Load(), Refused: d.refused, Identity: d.identity, Notes: d.notes, Files: files, Tools: tools, ToolErrors: toolErrors, SignIn: signIn})
	e.Harness, e.Session, e.Clean = d.harness, res.SessionID, e.State == store.TurnDone && !res.Unclean
	if e.Reply != nil {
		e.Reply.Text = c.r.scrub(e.Reply.Text)
	}
	if len(e.Files) == 0 {
		removeFiles(d.files)
	}
	return e
}

// frame shows the verified identity with every message handed to a
// harness. Tools read the caller from the process environment
// (harness.WithIdentity) and the OS user the turn runs as, never from this
// text.
func frame(it item, files []string) string {
	name := sanitizeName(it.row.From.Name)
	if name == "" {
		name = it.row.From.Key()
	}
	origin := ""
	if it.row.Origin == core.OriginScheduled {
		origin = fmt.Sprintf(" | scheduled job %q created by this caller", sanitizeName(it.job))
	}
	if r := it.row.Review; r != nil {
		origin = fmt.Sprintf(" | supervisor review of commitment %s attempt %d created by this caller; internal, nothing is sent", r.Issue, r.Attempt)
	}
	context := ""
	if c := strings.TrimSpace(it.row.Context); c != "" {
		context = c + "\n"
	}
	attached := ""
	if len(files) > 0 {
		attached = "\n" + strings.Join(files, "\n")
	}
	react := ""
	if it.react != "" {
		react = it.react + "\n"
	}
	return fmt.Sprintf("[Heimdall | from %q | role %s | verified %s | %s%s]\n%s%s%s%s", name, it.role, it.row.From.Key(), it.row.Conv, origin, react, context, it.text, attached)
}

// humanText is deliberately independent of framing: human-supplied markers are
// never parsed or stripped. Attachment references remain native user input.
func humanText(it item, files []string) string {
	text := it.text
	if len(files) != 0 {
		// Do not manufacture leading whitespace for attachment-only input:
		// Claude projects array text with trim() before UserPromptSubmit.
		if text != "" {
			text += "\n"
		}
		text += strings.Join(files, "\n")
	}
	return text
}
func frameContext(it item) string {
	it.text = ""
	return frame(it, nil)
}

func sanitizeName(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r == '[' || r == ']' || r == '|' || r < 0x20 || r == 0x7f:
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}

func clipAll(in []string, n int) []string {
	out := make([]string, len(in))
	for i, s := range in {
		if len(s) > n {
			s = s[:n] + "…"
		}
		out[i] = s
	}
	return out
}

func unconfirmedNotice(m store.InboundRow) core.Outbound {
	return core.Outbound{Conv: m.Conv, ReplyTo: m.MsgID, Text: "⚠ Heimdall restarted while handing this message to the running turn, so it may or may not have reached it. It will not be re-sent automatically; resend it if still needed.\n— heimdall"}
}

func interruptedNotice(t store.Interrupted) core.Outbound {
	if t.Conv.IsSched() {
		return core.Outbound{} // the scheduler tells the job's creator (Jobs.Recover)
	}
	if t.Conv.IsReview() {
		return core.Outbound{} // the commitment reconciles its review (Router.Review)
	}
	h := t.Harness
	if h == "" {
		h = "the harness"
	}
	if t.Messages == 0 {
		return core.Outbound{Conv: t.Conv, ReplyTo: t.ReplyTo, Text: fmt.Sprintf("⚠ Heimdall restarted while %s background work was outstanding. It was interrupted and was not re-run; check existing results before resending.\n— heimdall", h)}
	}
	return core.Outbound{Conv: t.Conv, ReplyTo: t.ReplyTo, Text: fmt.Sprintf("⚠ Heimdall restarted while %s was working on this (%d message(s)). The turn was not re-run; resend if still needed.\n— heimdall", h, t.Messages)}
}

func stripHarnessTag(text, tag string) string {
	text = strings.TrimSpace(text)
	for strings.HasSuffix(text, "\n— "+tag) {
		text = strings.TrimSpace(strings.TrimSuffix(text, "\n— "+tag))
	}
	return text
}

// An idle process may die between pool lookup and its next write. Retry only
// a reused transport which failed before the native side-effect boundary.
func (c *conv) startCandidate(ctx context.Context, t *turn, spec harness.Spec) (*harness.Run, error) {
	for attempt := 0; attempt < 2; attempt++ {
		run, err := c.r.startRun(spec)
		if err != nil {
			return nil, err
		}
		t.mu.Lock()
		t.run = run
		t.mu.Unlock()
		if t.stopped.Load() {
			run.Cancel()
		}
		select {
		case <-run.Ready():
			run.AcceptContext()
			return run, nil
		case <-run.Done():
		}
		res := run.Result()
		if res.Answer != "" || res.NeedsHuman != "" {
			run.AcceptContext()
		}
		if attempt == 0 && run.Process() != run && !res.Ready && res.Failure != "" && !res.Cancelled && !t.stopped.Load() && ctx.Err() == nil {
			<-run.Exited()
			continue
		}
		return run, nil
	}
	return nil, errors.New("cold retry did not start")
}

// stopJob stops a job's running turn, if any (!job stop, !job rm).
func (r *Router) stopJob(job string) bool {
	r.mu.Lock()
	t := r.jobs[job]
	r.mu.Unlock()
	if t == nil || !t.stopped.CompareAndSwap(false, true) {
		return false
	}
	if t.startCancel != nil {
		t.startCancel()
	}
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil {
		run.Cancel()
	}
	return true
}

// caller is a message's sender as the scheduler sees them.
func (c *conv) caller(row store.InboundRow, role core.Role) scheduler.Caller {
	return scheduler.Caller{Principal: row.From, Role: role, Conv: c.key, Private: row.Private}
}
