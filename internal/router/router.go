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

// PermanentError marks a delivery failure that retrying cannot fix.
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

type Options struct {
	Access      core.Access
	Order       []string // default harness order
	MaxParallel int
	RunAs       map[core.Role]core.RunAs
	Log         *log.Logger
}

type Router struct {
	opt    Options
	store  *store.Store
	ifaces map[string]Interface
	sem    chan struct{}
	log    *log.Logger

	mu      sync.Mutex
	ctx     context.Context
	convs   map[core.ConvKey]*conv
	wakeOut map[string]chan struct{}
}

func New(opt Options, st *store.Store, ifaces ...Interface) *Router {
	if opt.MaxParallel < 1 {
		opt.MaxParallel = 1
	}
	if opt.Log == nil {
		opt.Log = log.Default()
	}
	r := &Router{opt: opt, store: st, ifaces: map[string]Interface{}, sem: make(chan struct{}, opt.MaxParallel), log: opt.Log, convs: map[core.ConvKey]*conv{}, wakeOut: map[string]chan struct{}{}}
	for _, i := range ifaces {
		r.ifaces[i.Name()] = i
		r.wakeOut[i.Name()] = make(chan struct{}, 1)
	}
	return r
}

// Run recovers interrupted work, then routes until ctx ends. Turns still
// running at shutdown stay recorded as running, so the next start reports
// them exactly like a crash.
func (r *Router) Run(ctx context.Context) error {
	cut, err := r.store.Recover(interruptedNotice)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	for _, t := range cut {
		r.log.Printf("turn %d in %s interrupted by restart (harness %q); reported, not re-run", t.Turn, t.Conv, t.Harness)
	}
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, i := range r.ifaces {
		wg.Add(1)
		go func() { defer wg.Done(); r.deliver(ctx, i) }()
	}
	pending, err := r.store.Pending()
	if err != nil {
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
	wg.Wait()
	return err
}

// accept persists one verified message and hands it to its conversation.
// Unknown senders are rejected without storing their text.
func (r *Router) accept(_ context.Context, in core.Inbound) error {
	if in.Origin == "" {
		in.Origin = core.OriginLive
	}
	if err := in.Validate(); err != nil {
		return err
	}
	if _, ok := r.opt.Access.Role(in.From); !ok {
		r.log.Printf("rejected message from unknown caller %s in %s", in.From.Key(), in.Conv)
		return nil
	}
	row, dup, err := r.store.AddInbound(in)
	if err != nil || dup {
		return err
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

// Idle reports whether nothing is queued, running or waiting for delivery.
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
		err = i.Send(ctx, row.Outbound)
		var perm PermanentError
		switch {
		case err == nil:
			err = r.store.Sent(row.ID)
		case errors.As(err, &perm):
			r.log.Printf("reply %d to %s failed permanently: %v", row.ID, row.Conv, err)
			err = r.store.Failed(row.ID, err.Error())
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
	waiting bool // a goroutine is waiting for a parallelism slot
}

type turn struct {
	id         int64
	role       core.Role
	who        string // verified caller: the turn runs in their own session
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
}

type readyEvent struct {
	t       *turn
	harness string
}

type steerEvent struct {
	t       *turn
	it      item
	outcome harness.SteerOutcome
}

type doneEvent struct {
	t       *turn
	harness string // "" when no harness became ready
	res     harness.Result
	notes   []string
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
				c.handle(ctx, row)
			}
		case ev := <-c.events:
			switch ev := ev.(type) {
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
			go func() {
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
		c.busy.Store(c.turn != nil || len(c.queue) > 0 || c.waiting || len(c.inbox) > 0)
		c.mu.Unlock()
	}
}

func (c *conv) reply(row store.InboundRow, text string) {
	c.r.enqueue(core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: text})
}

func (c *conv) command(row store.InboundRow, detail string) {
	if err := c.r.store.SetInbound(row.ID, store.InCommand, detail); err != nil {
		c.r.log.Printf("message %d: %v", row.ID, err)
	}
}

// handle routes one stored message. The sender's role is evaluated now, so
// a revoked principal (or, later, a scheduled job's creator) fails closed.
func (c *conv) handle(ctx context.Context, row store.InboundRow) {
	role, ok := c.r.opt.Access.Role(row.From)
	if !ok {
		c.r.log.Printf("message %d from %s in %s rejected: caller no longer authorized", row.ID, row.From.Key(), c.key)
		_ = c.r.store.SetInbound(row.ID, store.InRejected, "caller not authorized")
		return
	}
	cmd := parseCommand(row.Text)
	switch cmd.Kind {
	case cmdInvalid:
		c.command(row, "invalid")
		c.reply(row, cmd.Error+"\n— heimdall")
		return
	case cmdHelp:
		c.command(row, "help")
		c.reply(row, helpText(c.r.opt.Order)+"\n— heimdall")
		return
	case cmdStatus:
		c.command(row, "status")
		c.reply(row, c.status()+"\n— heimdall")
		return
	case cmdUse:
		c.command(row, "use")
		if err := c.r.store.SetChatHarness(c.key, cmd.Harness); err != nil {
			c.reply(row, "Could not save the choice: "+err.Error()+"\n— heimdall")
			return
		}
		if cmd.Harness == "" {
			c.reply(row, "This chat is back to the default order: "+strings.Join(c.r.opt.Order, " → ")+".\n— heimdall")
		} else {
			c.reply(row, "This chat now uses "+cmd.Harness+" (no fallback) until !use default.\n— heimdall")
		}
		return
	case cmdStop:
		c.command(row, "stop")
		c.reply(row, c.stop(role, row.From.Key())+"\n— heimdall")
		return
	}
	it := item{row: row, role: role, harness: cmd.Harness, next: cmd.Next, text: cmd.Text}
	// Steer only the sender's own turn: sessions are per verified caller, so
	// nobody steers another person's turn.
	if t := c.turn; t != nil && t.done == nil && !it.next && it.harness == "" && row.From.Key() == t.who {
		t.steerQ = append(t.steerQ, it)
		c.steerNext(ctx, t)
		return
	}
	c.queue = append(c.queue, it)
}

func (c *conv) stop(role core.Role, who string) string {
	t := c.turn
	if t == nil || t.done != nil {
		return "Nothing is running in this chat."
	}
	if role != core.RoleOwner && who != t.who {
		return "Only the person who started this turn, or the owner, can stop it."
	}
	t.stopped.Store(true)
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil {
		run.Cancel()
	}
	return "Stopping the current turn."
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
	fmt.Fprintf(&b, "Queued: %d message(s)", len(c.queue))
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
	for n < len(c.queue) && c.queue[n].row.From.Key() == first.row.From.Key() && c.queue[n].harness == first.harness {
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
			c.reply(it.row, "Heimdall could not record this turn, so it did not run: "+err.Error()+"\n— heimdall")
		}
		return
	}
	c.queue = c.queue[n:]
	t := &turn{id: id, role: first.role, who: who, replyTo: first.row.MsgID, started: time.Now()}
	switch sticky, _ := c.r.store.ChatHarness(c.key); {
	case first.harness != "":
		t.candidates, t.explicit = []string{first.harness}, true
	case sticky != "":
		t.candidates, t.explicit = []string{sticky}, true
	default:
		t.candidates = c.r.opt.Order
	}
	parts := make([]string, len(batch))
	for i, it := range batch {
		parts[i] = frame(it)
	}
	c.turn = t
	c.r.log.Printf("turn %d in %s: %d message(s), role %s, candidates %s", id, c.key, len(batch), t.role, strings.Join(t.candidates, ","))
	go c.runTurn(ctx, t, strings.Join(parts, "\n\n"))
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
func (c *conv) runTurn(ctx context.Context, t *turn, prompt string) {
	var notes []string
	runAs := c.r.opt.RunAs[t.role]
	memory := core.Memory(c.r.opt.Access.Mode, t.role)
	for _, h := range t.candidates {
		if ctx.Err() != nil || t.stopped.Load() {
			break
		}
		launch, err := harness.Prepare(runAs, memory, harness.Binary(h))
		var account *harness.AccountError
		if errors.As(err, &account) {
			notes = append(notes, err.Error())
			break // every harness would fail the same way
		}
		if err != nil {
			notes = append(notes, h+": "+err.Error())
			continue
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
		run, err := harness.Start(ctx, harness.Spec{Harness: h, Prompt: prompt, Session: sess, Launch: launch})
		if err != nil {
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
			if err := c.r.store.CommitTurn(t.id, c.key, t.who, h, run.SessionID()); err != nil {
				c.r.log.Printf("turn %d: session binding not recorded: %v", t.id, err)
			}
			c.send(ctx, readyEvent{t: t, harness: h})
			c.send(ctx, doneEvent{t: t, harness: h, res: run.Result(), notes: notes})
			return
		default:
		}
		res := run.Result()
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

// steerNext hands the oldest waiting steer to the running turn, one at a
// time and in order, once the harness is ready.
func (c *conv) steerNext(ctx context.Context, t *turn) {
	if !t.ready || t.steering || t.done != nil || len(t.steerQ) == 0 {
		return
	}
	it := t.steerQ[0]
	t.steerQ = t.steerQ[1:]
	t.steering = true
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	go func() {
		c.send(ctx, steerEvent{t: t, it: it, outcome: run.Steer(ctx, fmt.Sprintf("heimdall-%d", it.row.ID), frame(it))})
	}()
}

func (c *conv) steerDone(ctx context.Context, ev steerEvent) {
	t := ev.t
	t.steering = false
	switch ev.outcome {
	case harness.SteerAccepted:
		t.steered++
		if err := c.r.store.Steered(t.id, ev.it.row.ID); err != nil {
			c.r.log.Printf("message %d: steer not recorded: %v", ev.it.row.ID, err)
		}
	case harness.SteerUnknown:
		_ = c.r.store.SetInbound(ev.it.row.ID, store.InUnconfirmed, "steer acceptance unknown; not replayed")
		c.reply(ev.it.row, "⚠ This message may or may not have reached the running "+t.harness+" turn. It will not be re-sent automatically; resend it if the answer does not cover it.\n— heimdall")
	default:
		// Not handed over: run it as a follow-up turn.
		c.requeue(ev.it)
	}
	if t.done != nil {
		c.finish(ctx, t)
		return
	}
	c.steerNext(ctx, t)
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
	c.requeue(t.steerQ...)
	t.steerQ = nil
	res := d.res
	tag := d.harness
	if tag == "" {
		tag = "heimdall"
	}
	footer := "\n\n— " + tag
	if len(d.notes) > 0 {
		footer += " (" + strings.Join(clipAll(d.notes, 160), "; ") + ")"
	}
	e := store.TurnEnd{Harness: d.harness, Session: res.SessionID}
	var text string
	switch {
	case res.Cancelled && t.stopped.Load():
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "stopped by !stop"
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
		text = "⚠ " + d.harness + " failed: " + res.Failure
	default:
		e.State, e.ItemState, e.Clean = store.TurnDone, store.InDone, true
		text = strings.TrimSpace(res.Answer)
	}
	e.Reply = &core.Outbound{Conv: c.key, ReplyTo: t.replyTo, Text: text + footer}
	if _, err := c.r.store.EndTurn(t.id, c.key, t.who, e); err != nil {
		c.r.log.Printf("turn %d: end not recorded: %v", t.id, err)
	}
	c.r.wake(c.key.Interface)
	c.r.log.Printf("turn %d in %s ended: %s (harness %q, %s)", t.id, c.key, e.State, d.harness, time.Since(t.started).Round(time.Millisecond))
	c.turn = nil
	<-c.r.sem
}

// frame carries the verified identity with every message handed to a
// harness. Authority is enforced by the OS user the turn runs as, not by
// this text.
func frame(it item) string {
	name := sanitizeName(it.row.From.Name)
	if name == "" {
		name = it.row.From.Key()
	}
	origin := ""
	if it.row.Origin == core.OriginScheduled {
		origin = " | scheduled job created by this caller"
	}
	return fmt.Sprintf("[Heimdall | from %q | role %s | verified %s | %s%s]\n%s", name, it.role, it.row.From.Key(), it.row.Conv, origin, it.text)
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

func interruptedNotice(t store.Interrupted) core.Outbound {
	h := t.Harness
	if h == "" {
		h = "the harness"
	}
	return core.Outbound{Conv: t.Conv, ReplyTo: t.ReplyTo, Text: fmt.Sprintf("⚠ Heimdall restarted while %s was working on this (%d message(s)). The turn was not re-run; resend if still needed.\n— heimdall", h, t.Messages)}
}
