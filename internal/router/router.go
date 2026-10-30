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
	Access      core.Access
	Order       []string // default harness order
	MaxParallel int
	Accounts    Accounts
	Log         *log.Logger
	TypingEvery time.Duration // how often Typing repeats while a turn runs; default 4s
	AlbumWait   time.Duration // how long a grouped message's parts are held after the last; default 1.5s
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
	if opt.TypingEvery <= 0 {
		opt.TypingEvery = 4 * time.Second
	}
	if opt.AlbumWait <= 0 {
		opt.AlbumWait = 1500 * time.Millisecond
	}
	r := &Router{opt: opt, store: st, ifaces: map[string]Interface{}, sem: make(chan struct{}, opt.MaxParallel), log: opt.Log, convs: map[core.ConvKey]*conv{}, wakeOut: map[string]chan struct{}{}}
	for _, i := range ifaces {
		r.ifaces[i.Name()] = i
		r.wakeOut[i.Name()] = make(chan struct{}, 1)
	}
	return r
}

// Run recovers interrupted work, then routes until ctx ends or an
// interface fails, which stops everything else and is returned. Turns still
// running at shutdown stay recorded as running, so the next start reports
// them exactly like a crash.
func (r *Router) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
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
	var wg sync.WaitGroup
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
	return err
}

// accept persists one verified message and hands it to its conversation.
// Senders who cannot be identified are ignored without storing their text.
func (r *Router) accept(_ context.Context, in core.Inbound) error {
	if in.Origin == "" {
		in.Origin = core.OriginLive
	}
	if err := in.Validate(); err != nil {
		return err
	}
	if _, ok := r.opt.Access.Role(in.From); !ok {
		r.log.Printf("ignored message from an unidentified %s caller in %s", in.From.Platform, in.Conv)
		return nil
	}
	if in.Ambient {
		r.keepAmbient(in)
		return nil
	}
	row, dup, err := r.store.AddInbound(in)
	if err != nil || dup {
		return err
	}
	if g, ok := r.ifaces[in.Conv.Interface].(GroupContext); ok && g.GroupContext() && !in.Private && in.Origin == core.OriginLive && parseCommand(in.Text).Kind == cmdMessage {
		// Addressed group messages are context for later turns too, as
		// in the hub; commands are not.
		r.keepAmbient(in)
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
	waiting bool // a goroutine is waiting for a parallelism slot
	albums  map[string]*album
}

// album is a caller's parts of one grouped message, held until none has
// arrived for AlbumWait.
type album struct {
	rows []store.InboundRow
	gen  int
}

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
		c.busy.Store(c.turn != nil || len(c.queue) > 0 || c.waiting || len(c.inbox) > 0 || len(c.albums) > 0)
		c.mu.Unlock()
	}
}

// hold keeps one part of a grouped message until the caller's group has
// been quiet for AlbumWait (the hub's 1.5 s media group merge). The parts
// are then handled together, so they start one turn, or steer the running
// one once.
func (c *conv) hold(ctx context.Context, row store.InboundRow) {
	key := row.From.Key() + "\x00" + row.Group
	if c.albums == nil {
		c.albums = map[string]*album{}
	}
	a := c.albums[key]
	if a == nil {
		a = &album{}
		c.albums[key] = a
	}
	a.rows = append(a.rows, row)
	a.gen++
	ev := albumEvent{key: key, gen: a.gen}
	time.AfterFunc(c.r.opt.AlbumWait, func() { c.send(ctx, ev) })
}

func (c *conv) reply(row store.InboundRow, text string) {
	c.r.enqueue(core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: scrub(text)})
}

// answer settles a ! command and its reply together. If that cannot be
// recorded the message stays pending and is answered after a restart.
func (c *conv) answer(row store.InboundRow, e store.CommandEnd, text string) {
	e.Reply = core.Outbound{Conv: c.key, ReplyTo: row.MsgID, Text: scrub(text) + "\n— heimdall"}
	if err := c.r.store.AnswerCommand(row.ID, c.key, e); err != nil {
		c.r.log.Printf("message %d: command %s not recorded: %v", row.ID, e.Detail, err)
		return
	}
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
		c.answer(row, store.CommandEnd{Detail: "stop"}, c.stop(role, row.From.Key()))
		return false
	case cmdCancel:
		n := c.cancelQueued(role, row.From.Key(), "cancelled by cancel")
		text := fmt.Sprintf("Cancelled %d queued message(s).", n)
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
	it := item{row: row, role: role, harness: cmd.Harness, next: cmd.Next, text: cmd.Text}
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
		if n := c.cancelQueued(role, who, "cancelled by stop"); n > 0 {
			return fmt.Sprintf("Nothing is running in this conversation. Cleared %d queued message(s).", n)
		}
		return "Nothing is running in this conversation."
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

func (c *conv) cancelTurn(t *turn) {
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
		if a.rows = rows; len(rows) == 0 {
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
	t := &turn{id: id, role: first.role, from: first.row.From, who: who, private: first.row.Private, replyTo: first.row.MsgID, started: time.Now()}
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
	var parts []string
	var images []harness.Image
	if p, ok := c.r.ifaces[c.key.Interface].(Prompter); ok {
		if note := strings.TrimSpace(p.PromptNote()); note != "" {
			parts = append(parts, note)
		}
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
		caller := harness.Caller{Principal: t.from, Role: t.role, Conv: c.key, Private: t.private}
		run, err := harness.Start(ctx, harness.Spec{Harness: h, Prompt: prompt, Images: images, Session: sess, Launch: launch, Caller: caller})
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
			if err := c.r.store.BindTurn(t.id, c.key, t.who, h, run.SessionID()); err != nil {
				// The session is already recorded as in progress; only a new
				// session's native ID is missing, so a crash now means the
				// next turn starts a fresh one.
				c.r.log.Printf("turn %d: native session ID not recorded: %v", t.id, err)
			}
			c.send(ctx, readyEvent{t: t, harness: h})
			c.send(ctx, doneEvent{t: t, harness: h, res: run.Result(), notes: notes})
			return
		default:
		}
		if ctx.Err() != nil {
			// Shutdown: the binding stays, as after a crash, so the next
			// start reports the turn and never resumes the session.
			break
		}
		c.unbind(t, h, prev, clean)
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

// unbind restores h's session binding after h failed before its turn was
// ready. If that cannot be recorded the session stays not resumable, which
// only costs a fresh session.
func (c *conv) unbind(t *turn, h, prev string, clean bool) {
	if err := c.r.store.UnbindTurn(t.id, c.key, t.who, h, prev, clean); err != nil {
		c.r.log.Printf("turn %d: %s session binding not restored: %v", t.id, h, err)
	}
}

// steerNext hands the oldest waiting steer to the running turn, one at a
// time and in order, once the harness is ready.
func (c *conv) steerNext(ctx context.Context, t *turn) {
	if !t.ready || t.steering || t.done != nil || len(t.steerQ) == 0 {
		return
	}
	n := 1
	for g := t.steerQ[0].row.Group; g != "" && n < len(t.steerQ) && t.steerQ[n].row.Group == g; n++ {
	}
	items := slices.Clone(t.steerQ[:n])
	t.steerQ = t.steerQ[n:]
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
		t.steered++
		for _, it := range ev.items {
			if err := c.r.store.Steered(t.id, it.row.ID); err != nil {
				c.r.log.Printf("message %d: steer not recorded: %v", it.row.ID, err)
			}
		}
		if t.done == nil {
			c.typedOnce(ctx)
		}
	case harness.SteerUnknown:
		for _, it := range ev.items {
			_ = c.r.store.SetInbound(it.row.ID, store.InUnconfirmed, "steer acceptance unknown; not replayed")
		}
		c.reply(ev.items[0].row, "⚠ This message may or may not have reached the running "+t.harness+" turn. It will not be re-sent automatically; resend it if the answer does not cover it.\n— heimdall")
	default:
		// Not handed over: run it as a follow-up turn.
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
		text = "⚠ " + d.harness + " failed: " + failureReason(res.Failure) + ". Please retry, or send reset if this keeps happening."
	default:
		e.State, e.ItemState, e.Clean = store.TurnDone, store.InDone, true
		text = strings.TrimSpace(res.Answer)
	}
	e.Reply = &core.Outbound{Conv: c.key, ReplyTo: t.replyTo, Formatted: e.State == store.TurnDone}
	if r, ok := c.r.ifaces[c.key.Interface].(Reactor); ok && e.State == store.TurnDone {
		t.mu.Lock()
		var directives bool
		text, directives, e.Reply.Reactions = reactions(text, t.refs, r.Reaction)
		t.mu.Unlock()
		switch {
		case text != "":
		case len(e.Reply.Reactions) > 0:
			footer = "" // a reaction-only answer (the hub's)
		case directives:
			text, e.Reply.Formatted = "Could not confirm reaction delivery.", false
		}
	}
	if text+footer != "" {
		e.Reply.Text = scrub(text + footer)
	}
	if _, err := c.r.store.EndTurn(t.id, c.key, t.who, e); err != nil {
		c.r.log.Printf("turn %d: end not recorded: %v", t.id, err)
	}
	c.r.wake(c.key.Interface)
	c.r.log.Printf("turn %d in %s ended: %s (harness %q, %s)", t.id, c.key, e.State, d.harness, time.Since(t.started).Round(time.Millisecond))
	c.turn = nil
	<-c.r.sem
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
