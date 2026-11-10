package router

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

// One bug must not take down every conversation: panics are recovered
// where Heimdall's own goroutines run, and only that work fails. Logs name
// the panic's type and a fixed site, never its value (it may hold message
// text).

// safely runs fn and reports whether it panicked.
func (r *Router) safely(site string, fn func()) (panicked bool) {
	defer func() {
		if p := recover(); p != nil {
			panicked = true
			r.log.Printf("recovered panic (%T) in %s", p, site)
		}
	}()
	fn()
	return false
}

// guarded wraps a callback handed to a harness or interface code: a panic
// in it is recovered and logged, and the call does nothing.
func guarded[T any](r *Router, site string, f func(T)) func(T) {
	if f == nil {
		return nil
	}
	return func(v T) { r.safely(site, func() { f(v) }) }
}

// panicNotice is the one plain line a conversation gets when Heimdall's own
// code failed while handling it.
const panicNotice = "⚠ Something went wrong in Heimdall while handling this chat. Any turn running here was stopped and not re-run; resend if still needed.\n— heimdall"

// afterPanic restores the conversation loop after a recovered panic: the
// running turn is stopped, recorded as failed and forgotten (its later
// events are ignored), the message being routed is failed, the slot is
// given back, and the chat is told once.
func (c *conv) afterPanic(ctx context.Context) {
	replyTo := ""
	if t := c.turn; t != nil && !t.detached {
		c.turn = nil
		t.abandoned = true
		t.stopped.Store(true)
		if t.startCancel != nil {
			t.startCancel()
		}
		c.stopTyping(t)
		t.mu.Lock()
		run := t.run
		t.mu.Unlock()
		if run != nil {
			run.Cancel()
		}
		c.r.warm.Invalidate(c.key, "") // never reuse a worker of the failed turn
		replyTo = t.replyTo
		if _, err := c.r.store.EndTurn(t.id, c.key, t.who, store.TurnEnd{State: store.TurnFailed, ItemState: store.InFailed, Detail: "internal error"}); err != nil {
			c.r.log.Printf("turn %d: failed end not recorded: %v", t.id, err)
		}
		if t.job != nil {
			c.r.mu.Lock()
			delete(c.r.jobs, t.job.Job)
			c.r.mu.Unlock()
		}
	}
	if c.routing != 0 {
		if err := c.r.store.SetInbound(c.routing, store.InFailed, "internal error"); err != nil {
			c.r.log.Printf("message %d: failure not recorded: %v", c.routing, err)
		}
		c.routing = 0
	}
	c.releaseSlot()
	c.r.enqueue(core.Outbound{Conv: c.key, ReplyTo: replyTo, Text: panicNotice})
}

// stopTyping ends a turn's typing indicator, once.
func (c *conv) stopTyping(t *turn) {
	if t.typing != nil && !t.typingStopped {
		t.typingStopped = true
		close(t.typing)
	}
}

// sendDone hands a turn's end to the loop, at most once per turn.
func (c *conv) sendDone(ctx context.Context, ev doneEvent) {
	if ev.t.doneSent.Swap(true) {
		return
	}
	c.send(ctx, ev)
}

// recoverTurn ends a turn whose runner panicked: its harness is stopped
// and the turn fails, unless it already ended.
func (c *conv) recoverTurn(ctx context.Context, t *turn) {
	p := recover()
	if p == nil {
		return
	}
	c.r.log.Printf("recovered panic (%T) in turn %d runner", p, t.id)
	t.mu.Lock()
	run := t.run
	t.mu.Unlock()
	if run != nil {
		run.Cancel()
	}
	c.sendDone(ctx, doneEvent{t: t, res: harness.Result{Failure: "internal error"}, panicked: true})
}

// sendSafely is i.Send with a panic reported as a transient failure: the
// reply is retried with backoff, as after any other failed send.
func (r *Router) sendSafely(ctx context.Context, i Interface, out core.Outbound) (err error) {
	if r.safely("delivery to "+i.Name(), func() { err = i.Send(ctx, out) }) {
		err = errors.New("internal error while sending")
	}
	return err
}

// Restartable is an optional Interface feature: its Run may be called again
// after it returned, so a failure after start-up restarts only it.
type Restartable interface {
	Restartable() bool
}

// runInterface runs one interface. A failure (error or panic) within
// ifaceGrace of its first start stays fatal, as does any failure of an
// interface that cannot be restarted; later ones restart only it, with
// backoff from ifaceBackoff to five minutes.
func (r *Router) runInterface(ctx context.Context, i Interface, errs chan<- error) {
	first := time.Now()
	backoff := r.ifaceBackoff
	for {
		started := time.Now()
		var err error
		if r.safely("interface "+i.Name(), func() { err = i.Run(ctx, r.accept) }) {
			err = errors.New("internal error (recovered panic)")
		}
		if ctx.Err() != nil || err == nil {
			return
		}
		restart, ok := i.(Restartable)
		if time.Since(first) < r.ifaceGrace || !ok || !restart.Restartable() {
			errs <- fmt.Errorf("interface %s: %w", i.Name(), err)
			return
		}
		if time.Since(started) >= 5*time.Minute {
			backoff = r.ifaceBackoff
		}
		r.log.Printf("interface %s stopped: %v; restarting in %s", i.Name(), err, backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, 5*time.Minute)
	}
}

// keepRunning runs a router worker loop, restarting it after a panic.
func (r *Router) keepRunning(ctx context.Context, site string, fn func()) {
	for ctx.Err() == nil {
		if !r.safely(site, fn) {
			return
		}
		sleep(ctx, time.Second)
	}
}
