package router

import (
	"context"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

type continuationKey struct {
	source *turn
	id     uint64
}
type continuationEvent struct {
	accepted chan bool

	source  *turn
	harness string
	native  harness.Continuation
}

// continuation runs only on the conversation loop. Task starts are durable
// immediately; only publication waits for the originating person's reply.
func (c *conv) continuation(ctx context.Context, e continuationEvent) {
	accepted := false
	if e.accepted != nil {
		defer func() { e.accepted <- accepted }()
	}
	source := e.source
	key := continuationKey{source, e.native.ID}
	if c.nativeTurns == nil {
		c.nativeTurns = map[continuationKey]*turn{}
	}
	t := c.nativeTurns[key]
	role, _, admitted := c.r.opt.Access.Decide(source.from, c.key)
	valid := source.generation == c.generation[source.who] && !source.reset && !source.stopped.Load() && admitted && role == source.role
	if !valid {
		// A start already accepted must still end durably and stop typing even
		// when authority/session changes before its result. Never publish its text.
		if t != nil {
			if !admitted || role != source.role {
				source.mu.Lock()
				run := source.run
				source.mu.Unlock()
				if run != nil {
					run.Cancel()
				}
			}
			delete(c.nativeTurns, key)
			c.discardNative(ctx, t, e.native.Result.SessionID)
		}
		return
	}
	if e.native.Phase != "start" && !source.published {
		c.continuations = append(c.continuations, e)
		return
	}
	if e.native.Phase == "notice" {
		if e.native.Interrupted {
			source.mu.Lock()
			run := source.run
			source.mu.Unlock()
			if run != nil {
				if err := c.r.store.InvalidateNativeSession(c.key, source.who, e.harness, run.SessionID()); err != nil {
					c.r.log.Printf("background session invalidation failed: %v", err)
				}
			}
		}
		c.r.enqueue(core.Outbound{Conv: c.key, ReplyTo: source.replyTo, Text: c.r.scrub(e.native.Notice)})
		return
	}
	if e.native.Phase == "start" {
		if t != nil {
			accepted = true
			return
		}
		id, err := c.r.store.StartContinuation(c.key, source.role, source.who, source.replyTo, e.harness, e.native.Result.SessionID)
		if err != nil {
			c.r.log.Printf("native continuation transaction refused: %v", err)
			notice := continuationEvent{source: source, harness: e.harness, native: harness.Continuation{Phase: "notice", Notice: "Background work could not be recorded. Its native process is being stopped; check existing results before resending.\n— heimdall"}}
			if source.published {
				c.continuation(ctx, notice)
			} else {
				c.continuations = append(c.continuations, notice)
			}
			return
		}
		source.mu.Lock()
		refs := map[string]string{}
		for k, v := range source.refs {
			refs[k] = v
		}
		t = &turn{id: id, role: source.role, from: source.from, who: source.who, private: source.private, replyTo: source.replyTo, started: time.Now(), harness: e.harness, dir: source.dir, launch: source.launch, refs: refs, detached: true}
		source.mu.Unlock()
		c.nativeTurns[key] = t
		accepted = true
		c.startTyping(ctx, t)
		c.observe(ctx, t, "progress", harness.Progress{Phase: "running"})
		return
	}
	if t == nil {
		return
	}
	delete(c.nativeTurns, key)
	d := doneEvent{t: t, harness: e.harness, res: e.native.Result}
	if e.native.Interrupted {
		t.stopped.Store(true)
	}
	if d.res.Failure == "" && d.res.NeedsHuman == "" && !d.res.Cancelled {
		d.files, d.skipped = c.collect(t)
	}
	t.done = &d
	c.finish(ctx, t)
}
func (c *conv) discardNative(ctx context.Context, t *turn, session string) {
	t.stopped.Store(true)
	t.done = &doneEvent{t: t, harness: t.harness, res: harness.Result{Cancelled: true, Unclean: true, SessionID: session}}
	c.finish(ctx, t)
}

// cancelNative settles accepted detached turns without a second stop reply.
// Native processes remain owned by their original caller; another employee
// may stop only their own, even while someone else's person turn is running.
func (c *conv) cancelNative(ctx context.Context, who string, all, stopProcess bool) int {
	count := 0
	for key, t := range c.nativeTurns {
		if !all && key.source.who != who {
			continue
		}
		delete(c.nativeTurns, key)
		count++
		source := key.source
		source.mu.Lock()
		run := source.run
		source.mu.Unlock()
		session := ""
		if run != nil {
			session = run.SessionID()
		}
		c.discardNative(ctx, t, session)
		if stopProcess {
			source.stopped.Store(true)
			if run != nil {
				run.Cancel()
			}
		}
	}
	return count
}

func (c *conv) flushContinuations(ctx context.Context) {
	waiting := c.continuations
	c.continuations = nil
	for _, e := range waiting {
		c.continuation(ctx, e)
	}
}
