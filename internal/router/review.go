package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

// reviewTimeout bounds one supervisor review turn (a variable for tests).
var reviewTimeout = 30 * time.Minute

// Review runs a durable commitment's supervisor review as one turn of the
// creator's harness choice through the ordinary turn machinery, in the
// commitment's own conversation (core.ReviewChat): a fresh native session,
// no scheduler or MCP control, nothing sent to any chat. The creator and
// the attempt are revalidated as the turn starts (conv.start). It is keyed
// by req.Key: a repeated key never starts a second turn, it waits for or
// returns the first one's persisted outcome, so a restart before the
// runtime checkpointed the result neither loses nor reapplies it.
func (r *Router) Review(ctx context.Context, q durable.ReviewRequest) (durable.ReviewResult, error) {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return durable.ReviewResult{}, ctx.Err()
	}
	if _, ok := r.ifaces[q.Route.Interface]; !ok || q.Owner.Platform == "" || q.Owner.ID == "" {
		return durable.ReviewResult{}, durable.Permanent{Err: fmt.Errorf("review %s: no verified creator or route interface", q.Key)}
	}
	in := core.Inbound{Conv: core.ConvKey{Interface: q.Route.Interface, Chat: core.ReviewChat(q.Issue)}, MsgID: q.Key, From: q.Owner,
		Origin: core.OriginCommitment, Text: q.Prompt, At: time.Now(),
		Review: &core.Review{Issue: q.Issue, Attempt: q.Attempt, Key: q.Key, Target: q.Route, Private: q.Private}}
	row, dup, err := r.store.AddInbound(in)
	if err != nil {
		return durable.ReviewResult{}, err
	}
	if !dup {
		r.dispatch(row)
	}
	pause := 200 * time.Millisecond
	for {
		o, ok, err := r.store.InboundOutcome(in.Conv, in.MsgID)
		if err != nil {
			return durable.ReviewResult{}, err
		}
		if !ok {
			return durable.ReviewResult{}, errors.New("review " + q.Key + " not stored")
		}
		if res, done := reviewOutcome(o); done {
			return res, nil
		}
		sleep(ctx, pause)
		if ctx.Err() != nil {
			return durable.ReviewResult{}, ctx.Err()
		}
		pause = min(pause*2, 2*time.Second)
	}
}

// OwnsRoute revalidates a durable commitment's creator before a dispatch:
// still an admitted owner on the commitment's original route, on an
// interface this router serves. It is durable.Authorizer.
func (r *Router) OwnsRoute(p core.Principal, route core.ConvKey) bool {
	if _, ok := r.ifaces[route.Interface]; !ok || p.Platform == "" || p.ID == "" {
		return false
	}
	role, _, ok := r.opt.Access.Decide(p, route)
	return ok && role == core.RoleOwner
}

const (
	verdictAccept = "verdict accept: "
	verdictReject = "verdict reject: "
	reviewHeld    = "held: " // never re-invoked without an explicit !work retry
)

// reviewOutcome reads a stored review's result; done=false while it is
// still queued or running.
func reviewOutcome(o store.InboundOutcome) (durable.ReviewResult, bool) {
	switch o.State {
	case store.InPending, store.InRunning, store.InSteering, store.InSteered:
		return durable.ReviewResult{}, false
	}
	res := durable.ReviewResult{Turn: o.Turn}
	switch {
	case o.TurnState == store.TurnDone && strings.HasPrefix(o.TurnDetail, verdictAccept):
		res.Verdict, res.Reason = "accept", strings.TrimPrefix(o.TurnDetail, verdictAccept)
	case o.TurnState == store.TurnDone && strings.HasPrefix(o.TurnDetail, verdictReject):
		res.Verdict, res.Reason = "reject", strings.TrimPrefix(o.TurnDetail, verdictReject)
	case strings.HasPrefix(o.TurnDetail, reviewHeld):
		res.Held = strings.TrimPrefix(o.TurnDetail, reviewHeld)
	case o.TurnState == "" && strings.HasPrefix(o.Detail, reviewHeld):
		res.Held = strings.TrimPrefix(o.Detail, reviewHeld)
	default:
		res.Failure = o.State + ": " + or(o.TurnDetail, o.Detail)
	}
	return res, true
}

// reviewEnd ends a review turn: no reply, no files, no notices. The
// outcome is recorded with the turn (its detail) in the same transaction
// that ends it, so Review reads one persisted outcome. A native approval
// request, a refused creator, a stop, or a commitment that is no longer
// current (paused, cancelled, moved on) is held; a time limit, a harness
// failure or a malformed answer is a technical failure. A restart during
// the turn is recorded by store.Recover as interrupted (technical).
func (c *conv) reviewEnd(t *turn, d *doneEvent, res harness.Result) store.TurnEnd {
	removeFiles(d.files)
	e := store.TurnEnd{Harness: d.harness, Session: res.SessionID}
	current := c.r.opt.Work != nil && c.r.opt.Work.Reviewable(t.review.Issue, t.review.Attempt)
	switch {
	case d.refused:
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, reviewHeld+"creator admission or role changed; review refused"
	case res.Cancelled && t.stopped.Load():
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, reviewHeld+"review stopped"
	case res.NeedsHuman != "":
		e.State, e.ItemState, e.Detail = store.TurnNeedsHuman, store.InNeedsHuman, reviewHeld+"the harness asked for a native approval or input; nothing was approved"
	case res.Cancelled:
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "cancelled: "+or(string(res.Cause), "no cause")
	case d.harness == "":
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, "no harness could start: "+strings.Join(d.notes, "; ")
	case res.Failure != "":
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, failureReason(c.r.scrub(res.Failure))
	default:
		e.State, e.ItemState, e.Clean = store.TurnDone, store.InDone, !res.Unclean
		e.Detail = parseVerdict(c.r.scrub(stripHarnessTag(res.Answer, d.harness)))
	}
	if !current && !strings.HasPrefix(e.Detail, reviewHeld) {
		e.Detail = reviewHeld + "the commitment was no longer current when the review ended (paused, cancelled or moved on); outcome not applied: " + e.Detail
	}
	return e
}

// parseVerdict takes the verdict from the whole answer: the first line is
// exactly ACCEPT or REJECT (markdown emphasis aside), the rest is the
// reason. Anything else, or no reason, is not a verdict.
func parseVerdict(answer string) string {
	answer = strings.TrimSpace(answer)
	first, rest, _ := strings.Cut(answer, "\n")
	word := strings.Trim(first, " \t*_`#")
	reason := strings.Join(strings.Fields(rest), " ")
	if len(reason) > 1000 {
		reason = reason[:1000] + "…"
	}
	switch {
	case answer == "":
		return "empty answer: no verdict"
	case reason == "" && (word == "ACCEPT" || word == "REJECT"):
		return "verdict without a reason: no verdict"
	case word == "ACCEPT":
		return verdictAccept + reason
	case word == "REJECT":
		return verdictReject + reason
	}
	return "malformed answer: no verdict"
}
