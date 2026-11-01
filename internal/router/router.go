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
	"github.com/misunders2d/heimdall/internal/harness"
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
	OwnerSlack  *harness.OwnerSlack
	SlackTeam   string
	Access      core.Access
	Order       []string // default harness order
	MaxParallel int
	Accounts    Accounts
	Log         *log.Logger
	TypingEvery time.Duration // how often Typing repeats while a turn runs; default 4s
	AlbumWait   time.Duration // how long a grouped message's parts are held after the last; default 1.5s
	OutboxDir   string        // Heimdall's copies of files to send; "" sends none
	MaxAttempts int           // delivery attempts of one reply before it is failed; default 50 (about 3.6 h)
	GuardLog    string        // the prompt-injection log (metadata only); "" logs nothing
	GuardLogMax int64         // its size before rotation to GuardLog+".1"; default DefaultGuardLogMax
}

type Router struct {
	noticeRetry time.Duration
	noticeQueue chan core.Inbound
	opt         Options
	store       *store.Store
	ifaces      map[string]Interface
	sem         chan struct{}
	log         *log.Logger
	// guardLog records messages the hub's prompt-injection scoring flags.
	guardLog *guardLog

	mu      sync.Mutex
	ctx     context.Context
	convs   map[core.ConvKey]*conv
	wakeOut map[string]chan struct{}

	// Live harness processes, each until it exited. At shutdown they are
	// stopped and waited for, so none outlives Heimdall.
	runsMu   sync.Mutex
	runs     map[*harness.Run]struct{}
	procCtx  context.Context // harness processes' context: ends only after shutdown waited for them
	stopping bool            // shutdown began: no harness process starts any more
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
	r := &Router{noticeRetry: time.Minute, noticeQueue: make(chan core.Inbound, 128), opt: opt, store: st, ifaces: map[string]Interface{}, sem: make(chan struct{}, opt.MaxParallel), log: opt.Log, convs: map[core.ConvKey]*conv{}, wakeOut: map[string]chan struct{}{}, runs: map[*harness.Run]struct{}{}}
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
	r.pruneOutbox()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.callerNotices(ctx) }()
	for _, i := range r.ifaces {
		wg.Add(1)
		go func() { defer wg.Done(); r.deliver(ctx, i) }()
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
	errs := make(chan error, len(r.ifaces))
	for _, i := range r.ifaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := i.Run(ctx, r.accept); err != nil && ctx.Err() == nil {
				errs <- fmt.Errorf("interface %s: %w", i.Name(), err)
			}
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
	defer r.runsMu.Unlock()
	if r.stopping {
		return nil, errors.New("Heimdall is shutting down")
	}
	run, err := harness.Start(r.procCtx, spec)
	if err != nil {
		return nil, err
	}
	r.runs[run] = struct{}{}
	go func() {
		<-run.Exited()
		r.runsMu.Lock()
		delete(r.runs, run)
		r.runsMu.Unlock()
	}()
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
func (r *Router) accept(ctx context.Context, in core.Inbound) error {
	if in.Origin == "" {
		in.Origin = core.OriginLive
	}
	if err := in.Validate(); err != nil {
		r.release(in)
		return err
	}
	role, ok := r.opt.Access.Role(in.From)
	if !ok {
		r.log.Printf("ignored message from an unidentified %s caller in %s", in.From.Platform, in.Conv)
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
		c = &conv{r: r, key: row.Conv, signal: make(chan struct{}, 1), events: make(chan any), slot: make(chan struct{})}
		r.convs[row.Conv] = c
		go c.loop(r.ctx)
	}
	r.mu.Unlock()
	c.push(row)
}

// Idle reports whether nothing is queued, running or waiting for delivery,
// and every harness process has exited (one may still be running after its
// answer).
func (r *Router) Idle() bool {
	r.runsMu.Lock()
	live := len(r.runs)
	r.runsMu.Unlock()
	if live > 0 {
		return false
	}
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
		err = i.Send(ctx, row.Outbound)
		var perm PermanentError
		switch {
		case err == nil:
			err = r.store.Sent(row.ID)
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		case errors.As(err, &perm):
			r.log.Printf("reply %d to %s failed permanently: %v", row.ID, row.Conv, err)
			err = r.store.Failed(row.ID, err.Error())
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		case row.Attempts+1 >= r.opt.MaxAttempts:
			r.log.Printf("reply %d to %s failed after %d attempts: %v", row.ID, row.Conv, row.Attempts+1, err)
			err = r.store.Failed(row.ID, fmt.Sprintf("gave up after %d attempts: %v", row.Attempts+1, err))
			if row.File != nil {
				os.Remove(row.File.Path)
			}
		default:
			backoff := min(time.Second<<min(row.Attempts, 9), 5*time.Minute)
			r.log.Printf("reply %d to %s: retry in %s: %v", row.ID, row.Conv, backoff, err)
			err = r.store.Retry(row.ID, time.Now().Add(backoff), err.Error())
		}
		if err != nil {
			r.log.Printf("outbound %d: %v", row.ID, err)
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
	harness string // !<harness>: this message only
	next    bool   // !next: never steer
	text    string
	react   string // the interface's reaction note for this message, if any
}

// conv serializes one conversation. Only its loop goroutine touches its
// state; turns and steers report back through events.
type conv struct {
	r      *Router
	key    core.ConvKey
	busy   atomic.Bool
	mu     sync.Mutex
	inbox  []store.InboundRow
	signal chan struct{}
	events chan any
	slot   chan struct{}

	queue   []item
	turn    *turn
	waiting bool // a goroutine is waiting for the last process to exit, then for a parallelism slot
	albums  map[string]*album
	exited  <-chan struct{} // the last turn's harness process has exited
	last    *harness.Run    // the last turn's harness run, which may still be exiting after its answer
	lastWho string          // its caller
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
	harness  string
	ready    bool
	steerQ   []item
	steering bool
	steered  int
	done     *doneEvent
	stopped  atomic.Bool
	mu       sync.Mutex
	run      *harness.Run
	typing   chan struct{}     // closed when the turn ends: stops the typing indicator
	reset    bool              // the caller reset their sessions: keep none from this turn
	launch   harness.Launch    // how the harness was launched (t.mu)
	dir      *harness.TurnDir  // the turn's directory, once created (t.mu)
	refs     map[string]string // reaction ref → message ID, for messages the harness was given (t.mu)
	batch    []item            // the messages the turn started with
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
	t       *turn
	harness string // "" when no harness became ready
	res     harness.Result
	notes   []string
	files   []harness.OutFile // collected from the outbox after an answer
	skipped []string          // outbox files not taken, "name: reason"
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
		select {
		case <-ctx.Done():
			return
		case <-c.signal:
			c.mu.Lock()
			rows := c.inbox
			c.inbox = nil
			c.mu.Unlock()
			for _, row := range rows {
				if row.Announce {
					c.expect(ctx, row)
					continue
				}
				if row.Group != "" {
					c.hold(ctx, row)
					continue
				}
				c.handle(ctx, row)
			}
		case ev := <-c.events:
			switch ev := ev.(type) {
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
				ev.t.harness, ev.t.ready = ev.harness, true
				c.steerNext(ctx, ev.t)
			case steerEvent:
				c.steerDone(ctx, ev)
			case doneEvent:
				ev.t.done = &ev
				if !ev.t.steering {
					c.finish(ctx, ev.t)
				}
			}
		case <-c.slot:
			c.waiting = false
			c.start(ctx)
		}
		if c.turn == nil && len(c.queue) > 0 && !c.waiting {
			c.waiting = true
			// A turn ends at the harness's answer, and its process may still
			// be shutting down: the next turn takes a slot only once it
			// exited, so two processes never share a native session.
			exited := c.exited
			go func() {
				if exited != nil {
					select {
					case <-exited:
					case <-ctx.Done():
						return
					}
				}
				select {
				case c.r.sem <- struct{}{}:
					select {
					case c.slot <- struct{}{}:
					case <-ctx.Done():
						<-c.r.sem
					}
				case <-ctx.Done():
				}
			}()
		}
		c.mu.Lock()
		c.busy.Store(c.turn != nil || len(c.queue) > 0 || c.waiting || len(c.inbox) > 0 || len(c.albums) > 0)
		c.mu.Unlock()
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
	e.Reply = core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: c.r.scrub(text) + "\n— heimdall"}
	if err := c.r.store.AnswerCommand(row.ID, c.key, e); err != nil {
		c.r.log.Printf("message %d: command %s not recorded: %v", row.ID, e.Detail, err)
		return
	}
	c.r.release(row.Inbound)
	c.r.wake(c.key.Interface)
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
	role, ok := c.r.opt.Access.Role(row.From)
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
	cmd := parseCommand(row.Text)
	switch cmd.Kind {
	case cmdInvalid:
		c.answer(row, store.CommandEnd{Detail: "invalid"}, cmd.Error)
		return false
	case cmdHelp:
		c.answer(row, store.CommandEnd{Detail: "help"}, helpText(c.r.opt.Order))
		return false
	case cmdStatus:
		c.answer(row, store.CommandEnd{Detail: "status"}, c.status())
		return false
	case cmdUse:
		text := "This conversation now uses " + cmd.Harness + " (no fallback) until !use default."
		if cmd.Harness == "" {
			text = "This conversation is back to the default order: " + strings.Join(c.r.opt.Order, " → ") + "."
		}
		c.answer(row, store.CommandEnd{Detail: "use", SetHarness: true, Harness: cmd.Harness}, text)
		return false
	case cmdStop:
		text := c.stop(role, row.From.Key())
		if row.ControlIngress > 0 {
			text = fmt.Sprintf("Stopped %d earlier ingress message(s). %s", row.ControlIngress, strings.ReplaceAll(text, "Nothing is running in this conversation.", ""))
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
	if t := c.turn; t != nil && t.done == nil && !it.next && it.harness == "" && row.From.Key() == t.who {
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
	if t == nil || t.done != nil {
		text := "Nothing is running in this conversation."
		if c.stopExiting(role, who) {
			text = "The last turn has answered; stopping its harness process, which was still running."
		}
		if n := c.cancelQueued(role, who, "cancelled by stop"); n > 0 {
			return fmt.Sprintf("%s Cleared %d queued message(s).", text, n)
		}
		return text
	}
	if role != core.RoleOwner && who != t.who {
		return "Only the person who started this turn, or the owner, can stop it."
	}
	n := c.cancelQueued(role, who, "cancelled by stop")
	c.cancelTurn(t)
	if n > 0 {
		return fmt.Sprintf("Stopping the current turn. Cleared %d queued message(s).", n)
	}
	return "Stopping the current turn."
}

// exiting is the harness run of a turn that answered and whose process is
// still running, and its caller.
func (c *conv) exiting() (*harness.Run, string) {
	run, who := c.last, c.lastWho
	if t := c.turn; t != nil && t.done != nil {
		// Answered; ended once its last steer's outcome is in.
		t.mu.Lock()
		run, who = t.run, t.who
		t.mu.Unlock()
	}
	if run == nil {
		return nil, ""
	}
	select {
	case <-run.Exited():
		return nil, ""
	default:
		return run, who
	}
}

// stopExiting stops, now rather than after ExitGrace, the process of a turn
// that already answered, if the caller may stop that turn (its sender or
// the owner).
func (c *conv) stopExiting(role core.Role, who string) bool {
	run, owner := c.exiting()
	if run == nil || role != core.RoleOwner && who != owner {
		return false
	}
	run.Stop("after its answer, stopped by stop")
	return true
}

func (c *conv) cancelTurn(t *turn) {
	c.observe(c.r.ctx, t, "cancelled", harness.Progress{})
	t.stopped.Store(true)
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
		b.WriteString("Running: nothing\n")
	}
	if run, _ := c.exiting(); run != nil {
		fmt.Fprintf(&b, "Exiting: the last turn's %s process (answered)\n", run.Harness())
	}
	held := 0
	for _, a := range c.albums {
		held += len(a.rows)
	}
	fmt.Fprintf(&b, "Queued: %d message(s)", len(c.queue)+held)
	return b.String()
}

// start begins a turn with the oldest queued message and the following ones
// from the same caller with the same harness choice. The caller holds a
// parallelism slot.
func (c *conv) start(ctx context.Context) {
	if c.turn != nil || len(c.queue) == 0 {
		<-c.r.sem
		return
	}
	first := c.queue[0]
	n := 1
	for n < len(c.queue) && !first.row.MCPControl && !c.queue[n].row.MCPControl && c.queue[n].row.From.Key() == first.row.From.Key() && c.queue[n].harness == first.harness {
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
		<-c.r.sem
		c.queue = c.queue[n:]
		for _, it := range batch {
			_ = c.r.store.SetInbound(it.row.ID, store.InFailed, "turn could not be recorded")
			c.r.release(it.row.Inbound)
			c.reply(it.row, "Heimdall could not record this turn, so it did not run: "+err.Error()+"\n— heimdall")
		}
		return
	}
	c.queue = c.queue[n:]
	t := &turn{id: id, role: first.role, from: first.row.From, who: who, private: first.row.Private, replyTo: first.row.MsgID, started: time.Now(), batch: batch}
	switch sticky, _ := c.r.store.ChatHarness(c.key); {
	case first.harness != "":
		t.candidates, t.explicit = []string{first.harness}, true
	case sticky != "":
		t.candidates, t.explicit = []string{sticky}, true
	default:
		t.candidates = c.r.opt.Order
	}
	c.turn = t
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
	var parts []string
	var images []harness.Image
	if p, ok := c.r.ifaces[c.key.Interface].(Prompter); ok {
		if note := strings.TrimSpace(p.PromptNote()); note != "" {
			parts = append(parts, note)
		}
	}
	if note := c.outboxNote(t); note != "" {
		parts = append(parts, note)
	}
	if recent := c.ambient(t, batch); recent != "" {
		parts = append(parts, recent)
	}
	c.react(t, batch)
	for _, it := range batch {
		blocks, ims := c.files(t, it)
		parts = append(parts, frame(it, blocks))
		images = append(images, ims...)
	}
	if len(images) > maxImages {
		images = images[:maxImages]
	}
	return strings.Join(parts, "\n\n"), images
}

// startTyping shows the interface's typing indicator now and every
// TypingEvery until the turn ends.
func (c *conv) startTyping(ctx context.Context, t *turn) {
	ty, ok := c.r.ifaces[c.key.Interface].(Typer)
	if !ok {
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
			ty.Typing(ctx, c.key)
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
		go ty.Typing(ctx, c.key)
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
	c.observe(ctx, t, "start", harness.Progress{})
	var notes []string
	var prompt string
	var images []harness.Image
	memory := core.Memory(c.r.opt.Access.Mode, t.role, t.from)
	for _, h := range t.candidates {
		if ctx.Err() != nil || t.stopped.Load() {
			break
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
		if prompt == "" {
			// Every candidate runs as the same account, so the first one
			// prepared fixes the turn's directory and prompt.
			t.mu.Lock()
			t.launch = launch
			t.mu.Unlock()
			prompt, images = c.prompt(t, batch)
		}
		sess := harness.Session{}
		prev, clean, err := c.r.store.Session(c.key, t.who, h)
		switch {
		case err != nil:
			notes = append(notes, h+": session lookup failed: "+err.Error())
			continue
		case prev != "" && clean:
			sess = harness.Session{ID: prev, Resume: true}
		default:
			sess.ID = harness.NewSessionID(h)
			if prev != "" {
				notes = append(notes, "new "+h+" session: the previous turn there did not finish cleanly")
			}
		}
		// Recorded before the harness gets any input: a crash from here on
		// leaves this session not resumable.
		if err := c.r.store.BindTurn(t.id, c.key, t.who, h, sess.ID); err != nil {
			notes = append(notes, "turn not recordable, so it did not run: "+err.Error())
			break
		}
		caller := harness.Caller{Principal: t.from, Role: t.role, Conv: c.key, Private: t.private, OAuthActor: batch[0].row.OAuthActor, OAuthInstallation: batch[0].row.OAuthInstallation}
		caller.SlackRead = harness.SlackReadAllowed(c.r.opt.OwnerSlack, batch[0].row.Inbound, t.role, h, c.r.opt.SlackTeam)
		logf := func(s string) { c.r.log.Printf("turn %d in %s: %s", t.id, c.key, s) }
		var progress func(harness.Progress)
		if _, ok := c.r.ifaces[c.key.Interface].(TurnObserver); ok {
			progress = func(p harness.Progress) { c.observe(ctx, t, "progress", p) }
		}
		run, err := c.r.startRun(harness.Spec{MCPNotify: func(text string) {
			if iface := c.r.ifaces[c.key.Interface]; iface != nil {
				_ = iface.Send(ctx, core.Outbound{Conv: c.key, ReplyTo: t.replyTo, Text: text})
			}
		}, OwnerSlack: c.r.opt.OwnerSlack, Harness: h, Prompt: prompt, Images: images, Session: sess, Launch: launch, Caller: caller, Log: logf, Progress: progress})
		if err != nil {
			c.unbind(t, h, prev, clean)
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
		c.unbind(t, h, prev, clean)
		if res.Cancelled {
			break
		}
		why := res.Failure
		if why == "" {
			why = res.NeedsHuman
		}
		notes = append(notes, h+" failed before starting: "+why)
	}
	c.send(ctx, doneEvent{t: t, res: harness.Result{Cancelled: ctx.Err() != nil || t.stopped.Load()}, notes: notes})
}

// commit ends the turn with harness h's outcome: h took the turn, so no
// other harness runs it.
func (c *conv) commit(ctx context.Context, t *turn, h string, run *harness.Run, notes []string) {
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
	c.send(ctx, done)
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
		var parts []string
		var images []harness.Image
		refs := c.react(t, items)
		for _, it := range items {
			blocks, ims := c.files(t, it)
			parts = append(parts, frame(it, blocks))
			images = append(images, ims...)
		}
		if len(images) > maxImages {
			images = images[:maxImages]
		}
		outcome := run.Steer(ctx, fmt.Sprintf("heimdall-%d", items[0].row.ID), strings.Join(parts, "\n\n"), images)
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
	if t.typing != nil {
		close(t.typing)
	}
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil {
		c.exited, c.last, c.lastWho = run.Exited(), run, t.who
	}
	c.requeue(t.steerQ...)
	t.steerQ = nil
	res := d.res
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
	tag := d.harness
	if tag == "" {
		tag = "heimdall"
	}
	notes := d.notes
	if res.Warning != "" {
		notes = append(notes, res.Warning)
	}
	for _, s := range d.skipped {
		notes = append(notes, "file not sent: "+s)
	}
	footer := "\n\n— " + tag
	if len(notes) > 0 {
		for i := range notes {
			notes[i] = c.r.scrub(notes[i])
		}
		footer += " (" + strings.Join(clipAll(notes, 160), "; ") + ")"
	}
	e := store.TurnEnd{Harness: d.harness, Session: res.SessionID, Forget: t.reset}
	var text string
	switch {
	case res.Cancelled && t.stopped.Load():
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "stopped by stop or reset"
		text = "⏹ Stopped."
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
		e.Reply.Text = c.r.scrub(text + footer)
	}
	if e.State == store.TurnDone {
		e.Files = c.fileReplies(t, d.files)
	} else {
		removeFiles(d.files)
	}
	if _, err := c.r.store.EndTurn(t.id, c.key, t.who, e); err != nil {
		c.r.log.Printf("turn %d: end not recorded: %v", t.id, err)
	}
	for _, it := range t.batch {
		c.r.release(it.row.Inbound)
	}
	c.observe(ctx, t, e.State, harness.Progress{})
	c.r.wake(c.key.Interface)
	c.r.log.Printf("turn %d in %s ended: %s (harness %q, %s)", t.id, c.key, e.State, d.harness, time.Since(t.started).Round(time.Millisecond))
	c.turn = nil
	if run == nil {
		<-c.r.sem
		return
	}
	// max_parallel counts live harness processes: the slot is free once
	// this one exited (at most ExitGrace plus TermGrace after its answer).
	go func() {
		<-run.Exited()
		<-c.r.sem
	}()
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
		origin = " | scheduled job created by this caller"
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
	h := t.Harness
	if h == "" {
		h = "the harness"
	}
	return core.Outbound{Conv: t.Conv, ReplyTo: t.ReplyTo, Text: fmt.Sprintf("⚠ Heimdall restarted while %s was working on this (%d message(s)). The turn was not re-run; resend if still needed.\n— heimdall", h, t.Messages)}
}
