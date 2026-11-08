package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
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
	o, err := r.commitmentTurn(ctx, in, true)
	if err != nil {
		return durable.ReviewResult{}, err
	}
	res, _ := reviewOutcome(o)
	return res, nil
}

// commitmentTurn stores in (once per key) and waits for its persisted
// outcome. With start false it never stores: it only waits for a turn
// already stored under the key (ok=false: there is none).
func (r *Router) commitmentTurn(ctx context.Context, in core.Inbound, start bool) (store.InboundOutcome, error) {
	if start {
		row, dup, err := r.store.AddInbound(in)
		if err != nil {
			return store.InboundOutcome{}, err
		}
		if !dup {
			r.dispatch(row)
		}
	}
	pause := 200 * time.Millisecond
	for {
		o, ok, err := r.store.InboundOutcome(in.Conv, in.MsgID)
		if err != nil {
			return o, err
		}
		if !ok {
			return o, errNoTurn
		}
		switch o.State {
		case store.InPending, store.InRunning, store.InSteering, store.InSteered:
		default:
			return o, nil
		}
		sleep(ctx, pause)
		if ctx.Err() != nil {
			return o, ctx.Err()
		}
		pause = min(pause*2, 2*time.Second)
	}
}

var errNoTurn = errors.New("commitment turn not stored")

// current applies a commitment turn's fence: a review needs its attempt
// reported, a dispatch its attempt dispatching, in a running workflow.
func current(w *durable.Runtime, b *core.Review) bool {
	if b.Run != "" {
		return w.Waitable(b.Issue, b.Attempt)
	}
	if b.Worker != "" {
		return w.Dispatchable(b.Issue, b.Attempt)
	}
	return w.Reviewable(b.Issue, b.Attempt)
}

// Dispatch sends one attempt's brief through a server-initiated turn of the
// creator's harness choice, in the commitment's own conversation, with the
// creator's verified native OAuth actor from admission: the harness uses its
// own native T3 tool (t3_thread_send) and native permissions, unchanged. It
// is durable.Dispatcher. The turn is keyed by q.Key: a repeated key never
// starts a second turn. Only a well-formed receipt for the selected thread
// counts; any other outcome (native denial or approval request, no bound
// native call, timeout, interruption) is held, never
// replayed; !work retry starts a new turn under a new key with the same
// clientRequestId. Nothing is sent to any chat. T3 dedupes that
// clientRequestId only within the creator's T3 sign-in (one OAuth client
// session). The receipt is the engine's decoded native tool result
// (harness.NativeCalls), never the model's answer.
func (r *Router) Dispatch(ctx context.Context, q durable.DispatchRequest) (durable.Receipt, error) {
	in, thread, err := r.dispatchInbound(ctx, q, "", false)
	if err != nil {
		return durable.Receipt{}, err
	}
	o, err := r.commitmentTurn(ctx, in, true)
	if err != nil {
		return durable.Receipt{}, err
	}
	return dispatchOutcome(o, thread)
}

// Reconcile returns a dispatch turn's persisted outcome without starting
// one: found=false when no turn was stored under the key.
func (r *Router) Reconcile(ctx context.Context, q durable.DispatchRequest) (durable.Receipt, bool, error) {
	in, thread, err := r.dispatchInbound(ctx, q, "", false)
	if err != nil {
		return durable.Receipt{}, false, err
	}
	o, err := r.commitmentTurn(ctx, in, false)
	if errors.Is(err, errNoTurn) {
		return durable.Receipt{}, false, nil
	}
	if err != nil {
		return durable.Receipt{}, false, err
	}
	rc, err := dispatchOutcome(o, thread)
	return rc, err == nil, err
}

// Wait is durable.Waiter: one server-initiated turn of the creator's
// harness waits, read-only, for run with its native t3_thread_wait, under
// the same fences and binding as the dispatch. Only the engine's decoded
// tool result counts; anything else holds.
func (r *Router) Wait(ctx context.Context, q durable.DispatchRequest, run string) (string, bool, error) {
	if run == "" {
		return "", false, durable.Permanent{Err: errors.New("wait " + q.Key + ": no dispatched run")}
	}
	in, _, err := r.dispatchInbound(ctx, q, run, false)
	if err != nil {
		return "", false, err
	}
	o, err := r.commitmentTurn(ctx, in, true)
	if err != nil {
		return "", false, err
	}
	var v struct {
		Status   string `json:"status"`
		Finished bool   `json:"finished"`
	}
	if o.TurnState == store.TurnDone && strings.HasPrefix(o.TurnDetail, waitResult) && json.Unmarshal([]byte(strings.TrimPrefix(o.TurnDetail, waitResult)), &v) == nil {
		return v.Status, v.Finished, nil
	}
	return "", false, durable.Permanent{Err: fmt.Errorf("wait turn %d %s: %s; not replayed (!work retry waits again)", o.Turn, o.State, or(strings.TrimPrefix(or(o.TurnDetail, o.Detail), reviewHeld), "no result"))}
}

// Read is durable.Waiter's result read: one server-initiated turn reads the
// finished run's own assistant result with the native t3_thread_read,
// under the same fences and binding. Only the engine's decoded tool
// results count; no bound body holds.
func (r *Router) Read(ctx context.Context, q durable.DispatchRequest, run string) (string, error) {
	if run == "" {
		return "", durable.Permanent{Err: errors.New("read " + q.Key + ": no dispatched run")}
	}
	in, _, err := r.dispatchInbound(ctx, q, run, true)
	if err != nil {
		return "", err
	}
	o, err := r.commitmentTurn(ctx, in, true)
	if err != nil {
		return "", err
	}
	if o.TurnState == store.TurnDone && strings.HasPrefix(o.TurnDetail, readResult) {
		return strings.TrimPrefix(o.TurnDetail, readResult), nil
	}
	return "", durable.Permanent{Err: fmt.Errorf("read turn %d %s: %s; not replayed (!work retry reads again)", o.Turn, o.State, or(strings.TrimPrefix(or(o.TurnDetail, o.Detail), reviewHeld), "no result"))}
}

func (r *Router) dispatchInbound(ctx context.Context, q durable.DispatchRequest, run string, read bool) (core.Inbound, string, error) {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return core.Inbound{}, "", ctx.Err()
	}
	ref, ok := strings.CutPrefix(q.Worker, "t3:")
	thread, env, _ := strings.Cut(ref, "@")
	if _, served := r.ifaces[q.Route.Interface]; !served || q.Owner.Platform == "" || q.Owner.ID == "" || !ok || thread == "" || strings.ContainsAny(thread+env, " \t\n") {
		return core.Inbound{}, "", durable.Permanent{Err: fmt.Errorf("dispatch %s: no verified creator, route interface or t3:<thread>[@<environment>] worker", q.Key)}
	}
	in := core.Inbound{Conv: core.ConvKey{Interface: q.Route.Interface, Chat: core.ReviewChat(q.Issue)}, MsgID: q.Key, From: q.Owner,
		Origin: core.OriginCommitment, Text: dispatchPrompt(q, thread, env), At: time.Now(), OAuthActor: q.OAuthActor, OAuthInstallation: q.OAuthInstallation,
		Review: &core.Review{Issue: q.Issue, Attempt: q.Attempt, Key: q.Key, Target: q.Route, Private: q.Private, Worker: q.Worker, Request: q.ClientRequestID, Run: run}}
	if run == "" {
		in.Review.Message = q.Message
	}
	if run != "" {
		in.Text = waitPrompt(q, thread, env, run)
	}
	if read {
		in.Review.Read, in.Text = true, readPrompt(q, thread, env, run)
	}
	return in, thread, nil
}

// dispatchPrompt asks for exactly one native send and its receipt.
func dispatchPrompt(q durable.DispatchRequest, thread, env string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Dispatch for %s, attempt %d. Call your native T3 tool t3_thread_send exactly once with:\n", q.Issue, q.Attempt)
	fmt.Fprintf(&b, "threadId: %s\n", thread)
	if env != "" {
		fmt.Fprintf(&b, "environmentId: %s\n", env)
	}
	fmt.Fprintf(&b, "mode: queue\nclientRequestId: %s\nmessage: the text between BEGIN MESSAGE and END MESSAGE below, verbatim.\n", q.ClientRequestID)
	b.WriteString("Do nothing else: no other tool call, message or retry, and never another clientRequestId. ")
	b.WriteString("Then answer in one line. Heimdall reads the tool's own result, not your answer.\n")
	fmt.Fprintf(&b, "BEGIN MESSAGE\n%s\nEND MESSAGE", q.Message)
	return b.String()
}

const (
	dispatchReceipt = "receipt: "
	waitResult      = "wait: "
	t3WaitTool      = "mcp__t3__t3_thread_wait"
	waitMs          = 5000 // a bounded status check; DBOS owns the waiting between checks
)

// waitPrompt asks for exactly one native, read-only wait for run.
func waitPrompt(q durable.DispatchRequest, thread, env, run string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Wait for %s, attempt %d. Call your native T3 tool t3_thread_wait exactly once with:\nthreadId: %s\n", q.Issue, q.Attempt, thread)
	if env != "" {
		fmt.Fprintf(&b, "environmentId: %s\n", env)
	}
	fmt.Fprintf(&b, "runId: %s\ntimeoutMs: %d\n", run, waitMs)
	b.WriteString("Do nothing else: no other tool call or message. Then answer in one line. Heimdall reads the tool's own result, not your answer.")
	return b.String()
}

const (
	readResult = "result: "
	t3ReadTool = "mcp__t3__t3_thread_read"
	readChars  = 50000 // t3_thread_read's per-item maximum
)

// readPrompt asks for native, read-only reads of the thread to its end.
func readPrompt(q durable.DispatchRequest, thread, env, run string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Result of %s, attempt %d. Call your native T3 tool t3_thread_read with:\nthreadId: %s\n", q.Issue, q.Attempt, thread)
	if env != "" {
		fmt.Fprintf(&b, "environmentId: %s\n", env)
	}
	fmt.Fprintf(&b, "view: messages\nlimit: 100\nmaxCharsPerItem: %d\n", readChars)
	b.WriteString("While a result has hasMore true, call it again adding afterPosition: its nextPosition. ")
	fmt.Fprintf(&b, "For an item of run %s with textTruncated true, call it again adding itemId: its itemId and textOffset: its nextTextOffset. ", run)
	b.WriteString("Do nothing else: no other tool call or message. Then answer in one line. Heimdall reads the tool's own results, not your answer.")
	return b.String()
}

// nativeRead is a read turn's outcome from the engine's decoded
// t3_thread_read calls bound to the thread and environment: the last
// assistant message of the dispatched run, in this thread, its text
// complete (every cut-off page read on) and the timeline read to its end.
// Anything else holds: no body is taken from the model or another run.
func nativeRead(calls []harness.NativeCall, b *core.Review) string {
	ref, _ := strings.CutPrefix(b.Worker, "t3:")
	thread, env, _ := strings.Cut(ref, "@")
	if len(calls) == 0 {
		return reviewHeld + "no native " + t3ReadTool + " call in the read turn (its answer is not evidence)"
	}
	type item struct {
		Position       int     `json:"position"`
		SourceThreadID string  `json:"sourceThreadId"`
		ItemID         string  `json:"itemId"`
		RunID          string  `json:"runId"`
		Type           string  `json:"type"`
		Text           *string `json:"text"`
		TextTruncated  bool    `json:"textTruncated"`
		NextTextOffset *int    `json:"nextTextOffset"`
	}
	type chunk struct {
		text string
		next *int // T3's own offset of the rest; nil: complete
	}
	chunks := map[string]map[int]chunk{} // itemId → textOffset → page
	// The plain (non-itemId) reads must be one chain from the start:
	// the first omits afterPosition, each next one's afterPosition is the
	// page before's nextPosition, strictly advancing, and only that chain's
	// hasMore=false page is the end. itemId reads never extend or end it.
	last, ended, pages, at := item{Position: -1}, false, 0, -1
	for _, n := range calls {
		var a struct {
			ThreadID      string `json:"threadId"`
			EnvironmentID string `json:"environmentId"`
			View          string `json:"view"`
			ItemID        string `json:"itemId"`
			TextOffset    int    `json:"textOffset"`
			AfterPosition *int   `json:"afterPosition"`
		}
		if json.Unmarshal(n.Args, &a) != nil || a.ThreadID != thread || a.EnvironmentID != env || a.View != "" && a.View != "messages" {
			return reviewHeld + "a native " + t3ReadTool + " call was not bound to this worker thread"
		}
		if n.Error {
			return reviewHeld + "the native " + t3ReadTool + " call failed or was denied"
		}
		var v struct {
			Items        []item `json:"items"`
			HasMore      *bool  `json:"hasMore"`
			NextPosition *int   `json:"nextPosition"`
		}
		if json.Unmarshal(toolJSON(n.Result), &v) != nil || v.HasMore == nil {
			return reviewHeld + "the native " + t3ReadTool + " result is not a thread read"
		}
		if a.ItemID == "" {
			start := pages == 0 && a.AfterPosition == nil
			follows := pages > 0 && a.AfterPosition != nil && *a.AfterPosition == at
			if ended || !start && !follows {
				return reviewHeld + "the native " + t3ReadTool + " pages are not one continuous read of the thread (skipped, repeated or out of order); the run's last result is unknown"
			}
			if *v.HasMore {
				if v.NextPosition == nil || *v.NextPosition <= at {
					return reviewHeld + "the native " + t3ReadTool + " page did not advance; the run's last result is unknown"
				}
				at = *v.NextPosition
			}
			pages, ended = pages+1, !*v.HasMore
		}
		for _, it := range v.Items {
			if it.SourceThreadID != thread || it.RunID != b.Run || it.Type != "assistant_message" || it.Text == nil {
				continue
			}
			if chunks[it.ItemID] == nil {
				chunks[it.ItemID] = map[int]chunk{}
			}
			var next *int
			if it.TextTruncated {
				next = it.NextTextOffset
				if next == nil || *next <= a.TextOffset {
					return reviewHeld + "run " + b.Run + "'s result " + it.ItemID + " is cut off without a next offset; incomplete"
				}
			}
			chunks[it.ItemID][a.TextOffset] = chunk{*it.Text, next}
			if a.ItemID == "" && it.Position > last.Position {
				last = it
			}
		}
	}
	if last.Position < 0 {
		return reviewHeld + "no assistant result of run " + b.Run + " in the native " + t3ReadTool + " results; never another run's"
	}
	if !ended {
		return reviewHeld + "the native " + t3ReadTool + " results did not reach the thread's end (hasMore); the run's last result is unknown"
	}
	var body strings.Builder
	for off := 0; ; {
		c, ok := chunks[last.ItemID][off]
		if !ok {
			return reviewHeld + "run " + b.Run + "'s result " + last.ItemID + " is cut off and its rest was not read; incomplete"
		}
		body.WriteString(c.text)
		if c.next == nil {
			break
		}
		off = *c.next
	}
	if strings.TrimSpace(body.String()) == "" {
		return reviewHeld + "run " + b.Run + "'s result is empty"
	}
	return readResult + body.String()
}

// nativeWait is a wait turn's outcome from the engine's decoded
// t3_thread_wait call bound to the thread, environment and run.
func nativeWait(calls []harness.NativeCall, b *core.Review) string {
	ref, _ := strings.CutPrefix(b.Worker, "t3:")
	thread, env, _ := strings.Cut(ref, "@")
	if len(calls) == 0 {
		return reviewHeld + "no native " + t3WaitTool + " call in the wait turn (its answer is not evidence)"
	}
	n := calls[0]
	var a struct {
		ThreadID      string `json:"threadId"`
		EnvironmentID string `json:"environmentId"`
		RunID         string `json:"runId"`
	}
	if json.Unmarshal(n.Args, &a) != nil || a.ThreadID != thread || a.EnvironmentID != env || a.RunID != b.Run {
		return reviewHeld + "the native " + t3WaitTool + " call was not bound to this run"
	}
	if n.Error {
		return reviewHeld + "the native " + t3WaitTool + " call failed or was denied"
	}
	var v struct {
		RunID    string `json:"runId"`
		Status   string `json:"status"`
		TimedOut bool   `json:"timedOut"`
	}
	if json.Unmarshal(toolJSON(n.Result), &v) != nil || v.RunID != b.Run || v.Status == "" {
		return reviewHeld + "the native " + t3WaitTool + " result does not name run " + b.Run
	}
	// T3's own run statuses (isTerminalRunStatus, app 0.1.7): only these
	// end a run; the rest are still going. Anything else holds.
	terminal := slices.Contains([]string{"completed", "failed", "cancelled", "interrupted", "rolled_back"}, v.Status)
	if !terminal && !slices.Contains([]string{"preparing", "queued", "starting", "running", "waiting"}, v.Status) {
		return reviewHeld + "the native " + t3WaitTool + " result has an unknown run status " + strconv.Quote(v.Status)
	}
	out, _ := json.Marshal(map[string]any{"status": v.Status, "finished": terminal && !v.TimedOut})
	return waitResult + string(out)
}

// dispatchOutcome maps a finished dispatch turn: only a stored receipt is
// a dispatch; anything else holds.
func dispatchOutcome(o store.InboundOutcome, thread string) (durable.Receipt, error) {
	if o.TurnState == store.TurnDone && strings.HasPrefix(o.TurnDetail, dispatchReceipt) {
		var rc durable.Receipt
		if err := json.Unmarshal([]byte(strings.TrimPrefix(o.TurnDetail, dispatchReceipt)), &rc); err == nil && rc.Thread == thread && rc.Run != "" {
			return rc, nil
		}
	}
	why := strings.TrimPrefix(or(o.TurnDetail, o.Detail), reviewHeld)
	return durable.Receipt{}, durable.Permanent{Err: fmt.Errorf("dispatch turn %d %s: %s; not replayed (check the worker thread, then !work retry: same clientRequestId, deduped by T3 only within the creator's T3 sign-in)", o.Turn, o.State, or(why, "no receipt"))}
}

const t3SendTool = "mcp__t3__t3_thread_send"

// nativeReceipt is a dispatch turn's outcome from the engine's own decoded
// T3 tool calls; the model's answer is never read. A call bound to this
// dispatch (thread, environment, clientRequestId, queue mode, the message
// with this turn's marker) whose native result is a send receipt for the
// thread is the dispatch. No bound call, a call to anything else, a native
// error or a result without a receipt holds: a send may have happened.
func nativeReceipt(calls []harness.NativeCall, b *core.Review) string {
	ref, _ := strings.CutPrefix(b.Worker, "t3:")
	thread, env, _ := strings.Cut(ref, "@")
	if len(calls) == 0 || b.Message == "" {
		return reviewHeld + "no native " + t3SendTool + " call in the dispatch turn (its answer is not evidence)"
	}
	for _, n := range calls {
		var a struct {
			ThreadID        string `json:"threadId"`
			EnvironmentID   string `json:"environmentId"`
			ClientRequestID string `json:"clientRequestId"`
			Mode            string `json:"mode"`
			Message         string `json:"message"`
		}
		if json.Unmarshal(n.Args, &a) != nil || a.ThreadID != thread || a.EnvironmentID != env || a.ClientRequestID != b.Request || a.Mode != "queue" || a.Message != b.Message {
			return reviewHeld + "a native " + t3SendTool + " call was not bound to this dispatch (thread, environment, clientRequestId, mode or message); not replayed"
		}
	}
	n := calls[0]
	if n.Error {
		return reviewHeld + "the native " + t3SendTool + " call failed or was denied"
	}
	var v struct {
		ThreadID  string `json:"threadId"`
		MessageID string `json:"messageId"`
		RunID     string `json:"runId"`
		Delivery  string `json:"delivery"`
	}
	if json.Unmarshal(toolJSON(n.Result), &v) != nil || v.ThreadID != thread || v.RunID == "" || !strings.Contains(v.MessageID, ":thread-send:") {
		return reviewHeld + "the native " + t3SendTool + " result has no send receipt for " + thread + "; the send may have happened"
	}
	out, _ := json.Marshal(durable.Receipt{Thread: v.ThreadID, Run: v.RunID, Message: v.MessageID, Delivery: v.Delivery})
	return dispatchReceipt + string(out)
}

// toolJSON is an MCP tool result's JSON object: structuredContent, or the
// JSON text of its first text block (Claude content, Pi/Codex result).
func toolJSON(raw json.RawMessage) json.RawMessage {
	var r struct {
		StructuredContent json.RawMessage `json:"structuredContent"`
		Content           json.RawMessage `json:"content"`
	}
	if json.Unmarshal(raw, &r) == nil {
		if len(r.StructuredContent) > 0 && string(r.StructuredContent) != "null" {
			return r.StructuredContent
		}
		if len(r.Content) > 0 {
			raw = r.Content
		}
	}
	var blocks []struct{ Type, Text string }
	if json.Unmarshal(raw, &blocks) == nil {
		for _, b := range blocks {
			if b.Type == "text" {
				return json.RawMessage(b.Text)
			}
		}
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return json.RawMessage(text)
	}
	return raw
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
	live := c.r.opt.Work != nil && current(c.r.opt.Work, t.review)
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
		if t.review.Worker != "" {
			t.mu.Lock()
			calls := slices.Clone(t.native)
			t.mu.Unlock()
			if t.review.Read {
				e.Detail = nativeRead(calls, t.review)
			} else if t.review.Run != "" {
				e.Detail = nativeWait(calls, t.review)
			} else {
				e.Detail = nativeReceipt(calls, t.review)
			}
		} else {
			e.Detail = parseVerdict(c.r.scrub(stripHarnessTag(res.Answer, d.harness)))
		}
	}
	if !live && !strings.HasPrefix(e.Detail, reviewHeld) {
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
