// Package scheduler runs jobs as their creators (docs/scheduler.md): the due
// loop, one run per job and slot, grace and missed slots, overlap and caps,
// retries before the ready signal, results, notices, commands and drafts.
// A job's run is an ordinary router turn with Origin scheduled in the job's
// own conversation; the router keeps doing identity, harness choice,
// sessions, delivery and crash recovery.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

// Meta keys in the store.
const (
	MetaTick = "tick_at"    // the due loop's last wake, Unix ms
	MetaHold = "hold_until" // heimdall drain: no run starts before then
)

// backoff spaces retries of a run that failed before its turn was ready.
var backoff = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// Config is the scheduler's part of Heimdall's configuration.
type Config struct {
	DefaultTZ         string
	MaxConcurrent     int // scheduled turns running at once, inside the router's max_parallel
	PerCreator        int // running runs per creator
	GraceRecurring    time.Duration
	GraceOneOff       time.Duration
	PauseAfter        int      // failed or unverified runs in a row that pause a job
	EmployeeHarnesses []string // harnesses employee jobs may name
	Interfaces        []string // configured interfaces, for targets
}

// Verified is a Slack creator verified live, with the OAuth scope of their
// Slack identity as the Slack interface sets it on live messages.
type Verified struct {
	Principal                     core.Principal
	OAuthActor, OAuthInstallation string
}

// Scheduler is the due loop and the job logic the router calls.
type Scheduler struct {
	cfg    Config
	st     *store.Store
	access core.Access
	log    *log.Logger
	now    func() time.Time
	wake   chan struct{}
	pruned time.Time

	// Reverify re-checks a Slack creator live at every fire (§4): ok false
	// refuses for good, an error is temporary. Nil (no Slack): refused.
	Reverify func(context.Context, core.Principal) (Verified, bool, error)

	mu       sync.Mutex
	dispatch func(store.InboundRow)
	deliver  func(iface string)
	stop     func(job string) bool
}

func New(cfg Config, st *store.Store, access core.Access, logger *log.Logger) *Scheduler {
	if logger == nil {
		logger = log.Default()
	}
	return &Scheduler{cfg: cfg, st: st, access: access, log: logger, now: time.Now, wake: make(chan struct{}, 1)}
}

// Attach connects the router: dispatch hands it a run's message, deliver
// wakes an interface's delivery, stop interrupts a job's running turn.
func (s *Scheduler) Attach(dispatch func(store.InboundRow), deliver func(string), stop func(string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatch, s.deliver, s.stop = dispatch, deliver, stop
}

// Wake makes the due loop look again now: a job changed or a run ended.
func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) notify(outs ...core.Outbound) {
	s.mu.Lock()
	deliver := s.deliver
	s.mu.Unlock()
	for _, o := range outs {
		if deliver != nil {
			deliver(o.Conv.Interface)
		}
	}
}

// Run is the due loop: it sleeps until the earliest due slot or retry, at
// most a minute, and wakes early on any change.
func (s *Scheduler) Run(ctx context.Context) {
	for ctx.Err() == nil {
		next := s.tick(ctx)
		timer := time.NewTimer(max(time.Until(next), 50*time.Millisecond))
		select {
		case <-ctx.Done():
		case <-s.wake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// tick creates the runs of due slots, starts waiting runs and returns when
// to look again.
func (s *Scheduler) tick(ctx context.Context) time.Time {
	now := s.now()
	next := now.Add(time.Minute)
	soonest := func(t time.Time) {
		if !t.IsZero() && t.Before(next) {
			next = t
		}
	}
	if err := s.st.SetMeta(MetaTick, now.UnixMilli()); err != nil {
		s.log.Printf("scheduler: tick not recorded: %v", err)
	}
	jobs, err := s.st.Jobs(store.JobFilter{States: []string{store.JobActive}})
	if err != nil {
		s.log.Printf("scheduler: jobs not read: %v", err)
	}
	for _, j := range jobs {
		d, sched, err := parse(j)
		if err != nil {
			s.setState(j, store.JobPaused, "unreadable definition")
			continue
		}
		if until, err := time.Parse(time.RFC3339, d.Until); err == nil {
			if !now.Before(until) {
				s.setState(j, store.JobDone, "expired", s.notices(j, d, false, s.text(j, d, "expired and was stopped", time.Time{}, "", 0))...)
				continue
			}
			soonest(until)
		}
		switch {
		case j.NextDue.IsZero():
		case j.NextDue.After(now):
			soonest(j.NextDue)
		default:
			soonest(s.fire(j, d, sched, now))
		}
	}
	if !s.held(now) {
		soonest(s.start(ctx, now))
	}
	if now.Sub(s.pruned) > time.Hour {
		s.pruned = now
		if err := s.st.Prune(now, 90*24*time.Hour, 30*24*time.Hour); err != nil {
			s.log.Printf("scheduler: prune: %v", err)
		}
	}
	return next
}

// held reports a drain's hold: no run starts.
func (s *Scheduler) held(now time.Time) bool {
	hold, _ := s.st.Meta(MetaHold)
	return hold > now.UnixMilli()
}

func parse(j store.Job) (Def, *schedule.Schedule, error) {
	var d Def
	if err := json.Unmarshal([]byte(j.Def), &d); err != nil {
		return d, nil, err
	}
	sched, err := schedule.Compile(d.Schedule, d.TZ, j.Created)
	return d, sched, err
}

// fire records the run of a job's newest due slot and moves the job on.
// After downtime only the newest slot is considered; the older ones, and
// the newest when it is later than grace, are missed with one notice.
func (s *Scheduler) fire(j store.Job, d Def, sched *schedule.Schedule, now time.Time) time.Time {
	first := schedule.Slot{Key: j.NextSlot, At: j.NextDue}
	newest, n := first, 1
	next, more := sched.Next(first.At)
	for more && !next.At.After(now) && n < 100000 {
		newest, n = next, n+1
		next, more = sched.Next(next.At)
	}
	if !more {
		next = schedule.Slot{}
	}
	j.NextDue = next.At
	run := store.Run{Job: j.ID, Version: j.Version, Slot: newest.Key, Due: newest.At, Trigger: "schedule", State: store.RunPending}
	missed := n - 1
	var outs []core.Outbound
	if now.Sub(newest.At) > s.grace(d, sched) {
		run.State, run.Cause, missed = store.RunMissed, "not started within grace", n
	} else if busy, _ := s.st.Runs(store.RunFilter{Job: j.ID, States: []string{store.RunPending, store.RunRunning}}); len(busy) > 0 &&
		(d.Overlap != "queue_one" || slices.ContainsFunc(busy, func(r store.Run) bool { return r.State == store.RunPending })) {
		run.State, run.Cause = store.RunSkipped, "previous run still pending or running"
		if j.Skips+1 == 3 {
			outs = s.notices(j, d, false, s.text(j, d, "skipped 3 slots in a row: its previous run is still going", newest.At, "overlap", 0))
		}
	}
	if missed > 0 {
		outs = append(outs, s.notices(j, d, true, s.text(j, d, fmt.Sprintf("missed %d run(s) since %s", missed, when(d, first.At)), newest.At, "Heimdall was down, held or busy", 0))...)
	}
	if _, err := s.st.CreateRun(run, next.Key, next.At, outs...); err != nil {
		s.log.Printf("scheduler: job %s slot %s not recorded: %v", j.ID, newest.Key, err)
		return now.Add(time.Minute)
	}
	if !more && run.State != store.RunPending {
		s.setState(j, store.JobDone, "one-off "+run.State)
	}
	s.log.Printf("scheduler: job %s slot %s: %s (late %s)", j.ID, newest.Key, run.State, now.Sub(newest.At).Round(time.Second))
	s.notify(outs...)
	return next.At
}

// start hands waiting runs to the router in due order, within the caps:
// max_concurrent scheduled turns, per_creator runs per creator, one per
// job. It returns the earliest retry time still ahead.
func (s *Scheduler) start(ctx context.Context, now time.Time) (wake time.Time) {
	runs, err := s.st.Runs(store.RunFilter{States: []string{store.RunPending, store.RunRunning}})
	if err != nil {
		s.log.Printf("scheduler: runs not read: %v", err)
		return time.Time{}
	}
	jobs := map[string]store.Job{}
	load := func(id string) (store.Job, bool) {
		j, ok := jobs[id]
		if !ok {
			j, ok, _ = s.st.Job(id)
			jobs[id] = j
		}
		return j, ok && j.ID != ""
	}
	total, byJob, byCreator := 0, map[string]int{}, map[string]int{}
	for _, r := range runs {
		if j, _ := load(r.Job); r.State == store.RunRunning {
			total, byJob[r.Job], byCreator[j.Creator.Key()] = total+1, byJob[r.Job]+1, byCreator[j.Creator.Key()]+1
		}
	}
	for _, r := range runs {
		if r.State != store.RunPending {
			continue
		}
		if r.NotBefore.After(now) {
			if wake.IsZero() || r.NotBefore.Before(wake) {
				wake = r.NotBefore
			}
			continue
		}
		j, ok := load(r.Job)
		d, sched, err := parse(j)
		switch {
		case !ok || err != nil || j.State == store.JobDeleted || j.State == store.JobDone || j.State == store.JobPaused && r.Trigger != "manual":
			s.endRun(j, d, store.RunEnd{ID: r.ID, Job: r.Job, State: store.RunCancelled, Cause: "job " + j.State})
			continue
		case now.Sub(r.Due) > s.grace(d, sched):
			s.endRun(j, d, store.RunEnd{ID: r.ID, Job: r.Job, State: store.RunMissed, Cause: "not started within grace"},
				s.notices(j, d, true, s.text(j, d, "missed a run", r.Due, "not started within grace", r.Attempt))...)
			continue
		case total >= s.cfg.MaxConcurrent:
			return wake
		case byJob[j.ID] > 0 || byCreator[j.Creator.Key()] >= s.cfg.PerCreator:
			continue
		}
		if s.launch(ctx, j, d, sched, r, now) {
			total, byJob[j.ID], byCreator[j.Creator.Key()] = total+1, byJob[j.ID]+1, byCreator[j.Creator.Key()]+1
		}
	}
	return wake
}

// launch evaluates the creator now (Slack live re-verification, admission,
// role rules) and runs r: a notify job's text is enqueued, an agent job's
// prompt is handed to the router. It reports a turn handed over.
func (s *Scheduler) launch(ctx context.Context, j store.Job, d Def, sched *schedule.Schedule, r store.Run, now time.Time) bool {
	p, v := j.Creator, Verified{}
	if p.Platform == core.PlatformSlack {
		if s.Reverify == nil {
			s.refuse(j, d, r, "Slack is not configured, so the creator cannot be re-verified")
			return false
		}
		vctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		var ok bool
		var err error
		v, ok, err = s.Reverify(vctx, p)
		cancel()
		switch {
		case err != nil:
			s.endRun(j, d, s.retry(j, d, sched, r, now, "temporary Slack identity error"))
			return false
		case !ok:
			s.refuse(j, d, r, "Slack identity no longer verified")
			return false
		}
		p = v.Principal
	}
	tgt, err := s.admit(j, d, p, now)
	if err != nil {
		s.refuse(j, d, r, err.Error())
		return false
	}
	if d.Kind == KindNotify {
		end := store.RunEnd{ID: r.ID, Job: j.ID, State: store.RunOK}
		if sched.OneOff() && r.Trigger != "manual" {
			end.JobState, end.JobReason = store.JobDone, "one-off ran"
		}
		s.endRun(j, d, end, core.Outbound{Conv: tgt.Conv, Text: d.Text})
		return false
	}
	in := core.Inbound{Conv: core.ConvKey{Interface: tgt.Conv.Interface, Chat: core.SchedChat(j.ID)}, MsgID: fmt.Sprintf("run:%d:%d", r.ID, r.Attempt),
		From: p, Origin: core.OriginScheduled, Text: d.Prompt, At: now, OAuthActor: v.OAuthActor, OAuthInstallation: v.OAuthInstallation,
		Run: &core.ScheduledRun{Job: j.ID, Run: r.ID, Attempt: r.Attempt, Target: tgt.Conv}}
	row, err := s.st.StartRun(r, in)
	if err != nil {
		s.log.Printf("scheduler: run %d of job %s not started: %v", r.ID, j.ID, err)
		return false
	}
	s.log.Printf("scheduler: run %d of job %s started (attempt %d)", r.ID, j.ID, r.Attempt)
	s.mu.Lock()
	dispatch := s.dispatch
	s.mu.Unlock()
	if dispatch != nil {
		dispatch(row)
	}
	return true
}

// admit is the creator's admission now: Access.Decide against the target
// conversation, and the job still valid under the creator's current role.
func (s *Scheduler) admit(j store.Job, d Def, p core.Principal, now time.Time) (Target, error) {
	c := Caller{Principal: p, Conv: j.CreatedIn, Private: j.CreatedPrivate}
	tgt, err := s.resolve(d.To, c)
	if err != nil {
		return tgt, err
	}
	role, _, ok := s.access.Decide(p, tgt.Conv)
	if !ok {
		return tgt, fmt.Errorf("creator %s is no longer admitted", p.Key())
	}
	c.Role = role
	if _, _, err := s.check(&d, c, j.Created, now); err != nil {
		return tgt, fmt.Errorf("no longer allowed for the creator's role: %v", err)
	}
	return tgt, nil
}

// refuse fails a run whose creator cannot be verified or admitted: the job
// is paused, and the owners (and the creator, where reachable) are told.
func (s *Scheduler) refuse(j store.Job, d Def, r store.Run, why string) {
	end, outs := s.refusal(j, d, r, why)
	s.endRun(j, d, end, outs...)
}

func (s *Scheduler) refusal(j store.Job, d Def, r store.Run, why string) (store.RunEnd, []core.Outbound) {
	text := s.text(j, d, "was paused: its run was refused", r.Due, "identity: "+why, r.Attempt+1)
	outs := s.notices(j, d, false, text)
	for _, key := range s.access.OwnerKeys() {
		if p, err := core.ParsePrincipalKey(key); err == nil {
			if k, ok, _ := s.st.PrivateChat(p); ok && !slices.ContainsFunc(outs, func(o core.Outbound) bool { return o.Conv == k }) {
				outs = append(outs, core.Outbound{Conv: k, Text: text})
			}
		}
	}
	return store.RunEnd{ID: r.ID, Job: j.ID, State: store.RunFailed, Cause: "identity", JobState: store.JobPaused, JobReason: "creator refused: " + why}, outs
}

// retry is a run's next attempt after a failure before its turn was ready,
// or its failure when no attempt or grace is left.
func (s *Scheduler) retry(j store.Job, d Def, sched *schedule.Schedule, r store.Run, now time.Time, cause string) store.RunEnd {
	if r.Attempt < len(backoff) {
		wait := backoff[r.Attempt]
		at := now.Add(wait + time.Duration((rand.Float64()*0.4-0.2)*float64(wait)))
		if !at.After(r.Due.Add(s.grace(d, sched))) {
			return store.RunEnd{ID: r.ID, Job: j.ID, Retry: at, Cause: cause}
		}
	}
	return s.failed(j, r, cause)
}

// failed ends a run as failed (endNotices adds the fixed failure notice);
// pause_after failures in a row pause the job.
func (s *Scheduler) failed(j store.Job, r store.Run, cause string) store.RunEnd {
	end := store.RunEnd{ID: r.ID, Job: j.ID, State: store.RunFailed, Cause: cause}
	s.pauseAfter(j, &end)
	return end
}

// pauseAfter pauses the job when this run is its pause_after-th failed or
// unverified run in a row.
func (s *Scheduler) pauseAfter(j store.Job, end *store.RunEnd) {
	if (end.State == store.RunFailed || end.State == store.RunUnverified) && end.JobState == "" && j.Fails+1 >= s.cfg.PauseAfter {
		end.JobState, end.JobReason = store.JobPaused, fmt.Sprintf("%d failed or unverified runs in a row", j.Fails+1)
	}
}

// endRun settles a run outside a turn.
func (s *Scheduler) endRun(j store.Job, d Def, end store.RunEnd, outs ...core.Outbound) {
	outs = append(outs, s.endNotices(j, d, end)...)
	if err := s.st.EndRun(end, outs...); err != nil {
		s.log.Printf("scheduler: run %d of job %s not settled: %v", end.ID, end.Job, err)
		return
	}
	s.log.Printf("scheduler: run %d of job %s: %s", end.ID, end.Job, or(end.State, "retry"))
	s.notify(outs...)
	s.Wake()
}

// endNotices are the notices a run's end needs beyond its own: a failure
// notice, and a pause notice.
func (s *Scheduler) endNotices(j store.Job, d Def, end store.RunEnd) []core.Outbound {
	var outs []core.Outbound
	if end.State == store.RunFailed && end.Cause != "identity" {
		run, _ := s.st.Run(end.ID)
		outs = s.notices(j, d, true, s.text(j, d, "failed", run.Due, end.Cause, run.Attempt+1))
	}
	if end.JobState == store.JobPaused && end.Cause != "identity" {
		hint := ""
		if len(d.Requires) > 0 {
			hint = fmt.Sprintf(" If it needs a sign-in (%s): send mcp login in your private chat, then !job resume %s.", strings.Join(d.Requires, ", "), j.ID)
		}
		outs = append(outs, s.notices(j, d, false, s.text(j, d, "was paused: "+end.JobReason+"."+hint, time.Time{}, "", 0))...)
	}
	return outs
}

func (s *Scheduler) setState(j store.Job, state, reason string, outs ...core.Outbound) {
	if err := s.st.UpdateJob(j.ID, store.Set{"state": state, "reason": reason}, outs...); err != nil {
		s.log.Printf("scheduler: job %s not set %s: %v", j.ID, state, err)
		return
	}
	s.notify(outs...)
}

// Recover settles runs a restart cut off, before the router replays pending
// messages; it also clears a drain's hold. A run whose message never
// started is left to the router (Begin applies grace). A turn interrupted
// before its harness was ready is queued again, as is an idempotent job's
// run, once, within grace; any other is interrupted, with a notice, and
// never re-run.
func (s *Scheduler) Recover() error {
	if err := s.st.SetMeta(MetaHold, 0); err != nil {
		return err
	}
	held, err := s.st.HeldRuns()
	if err != nil {
		return err
	}
	now := s.now()
	for _, h := range held {
		j, _, _ := s.st.Job(h.Job)
		d, sched, _ := parse(j)
		switch h.InState {
		case store.InPending:
		case store.InInterrupted:
			if !h.Ready || d.Idempotent && !h.Replayed && now.Sub(h.Due) <= s.grace(d, sched) {
				why := "queued again: interrupted before the harness was ready"
				if h.Ready {
					why = "re-run after restart"
				}
				s.endRun(j, d, store.RunEnd{ID: h.ID, Job: h.Job, Retry: now, Replay: h.Ready, Cause: why})
				continue
			}
			s.endRun(j, d, store.RunEnd{ID: h.ID, Job: h.Job, State: store.RunInterrupted, Cause: "Heimdall restarted during the run"},
				s.notices(j, d, true, s.text(j, d, "was interrupted by a restart and was not re-run", h.Due, "restart", h.Attempt+1))...)
		default:
			s.endRun(j, d, store.RunEnd{ID: h.ID, Job: h.Job, State: store.RunFailed, Cause: "run lost: message " + or(h.InState, "missing")})
		}
	}
	return nil
}

// Turn is a scheduled turn's job, as the router runs it.
type Turn struct {
	Job, Name string
	Run       int64
	Attempt   int
	Due       time.Time
	Def       Def
	Target    Target
	Timeout   time.Duration // 0: the harness default
	Notes     []string      // Heimdall's instructions for this run
	History   []string      // the job's last delivered replies, untrusted
}

// Begin is called as a scheduled message's turn starts. ok false: the run
// may not start now. It was settled (job paused or removed, later than
// grace), its message cancelled; or a drain holds it, and it waits again
// with its message dropped.
func (s *Scheduler) Begin(row store.InboundRow) (*Turn, bool) {
	ref := row.Run
	r, rerr := s.st.Run(ref.Run)
	j, ok, _ := s.st.Job(ref.Job)
	d, sched, err := parse(j)
	now := s.now()
	end := store.RunEnd{ID: ref.Run, Job: ref.Job, Inbound: row.ID}
	switch {
	case rerr != nil || r.State != store.RunRunning || r.Inbound != row.ID:
		// Not the run's current message: only the message goes.
		_ = s.st.SetInbound(row.ID, store.InCancelled, "run no longer current")
		return nil, false
	case !ok || err != nil:
		end.State, end.Cause = store.RunCancelled, "job missing or unreadable"
	case j.State == store.JobDeleted || j.State == store.JobDone || j.State == store.JobPaused && r.Trigger != "manual":
		end.State, end.Cause = store.RunCancelled, "job "+j.State
	case now.Sub(r.Due) > s.grace(d, sched):
		end.State, end.Cause = store.RunMissed, "not started within grace"
	case s.held(now):
		end.Retry, end.Held, end.Cause = now, true, "held by heimdall drain"
	default:
		t := &Turn{Job: j.ID, Name: j.Name, Run: r.ID, Attempt: r.Attempt, Due: r.Due, Def: d}
		t.Target, _ = s.resolve(d.To, Caller{Principal: j.Creator, Conv: j.CreatedIn, Private: j.CreatedPrivate})
		t.Target.Conv = ref.Target
		t.Timeout, _ = span(d.Timeout)
		if d.Quiet {
			t.Notes = append(t.Notes, "[Scheduled job: if there is nothing new to report, answer exactly "+QuietToken+" and nothing else.]")
		}
		if t.Target.AgentNet != "" {
			t.Notes = append(t.Notes, fmt.Sprintf("[Scheduled job: deliver the result with the `agentnet` CLI to `%s`. End your answer with `AGENTNET_SENT <message-id>`.]", t.Target.AgentNet))
		}
		if d.HistoryCtx {
			replies, _ := s.st.JobReplies(j.ID, 3)
			size := 0
			for _, text := range replies {
				if size += len(text); size > 8<<10 {
					break
				}
				t.History = append(t.History, text)
			}
			slices.Reverse(t.History)
		}
		return t, true
	}
	var outs []core.Outbound
	if end.State == store.RunMissed {
		outs = s.notices(j, d, true, s.text(j, d, "missed a run", r.Due, end.Cause, r.Attempt))
	}
	s.endRun(j, d, end, outs...)
	return nil, false
}

// Outcome is how a scheduled turn ended, as the router saw it.
type Outcome struct {
	Turn    *Turn
	TurnID  int64
	Harness string // "" when no harness became ready
	Result  harness.Result
	Stopped bool     // !job stop or !job rm
	Refused bool     // the creator's admission or role changed as the turn started
	Notes   []string // why candidates failed before starting
	Files   []core.File
	Tools   []string // native tool names seen
}

var (
	doneLine   = regexp.MustCompile(`(?m)^[ \t]*\[job: done\][ \t]*$`)
	agentnetOK = regexp.MustCompile(`(?m)^[ \t]*AGENTNET_SENT [^\s]+[ \t]*\z`)
)

// NeedsHumanCause is a run's cause when the harness asked for interactive
// input (a question tool, an approval or a sign-in): nobody is there to
// answer, so the turn ends at once, nothing approved.
const NeedsHumanCause = "needs_human: the harness asked for interactive input (a question, approval or sign-in) that a scheduled run cannot answer"

// End decides a scheduled turn's outcome (§3.8–3.10): the run's state, what
// is delivered where, and the notices; the router stores it with the turn.
func (s *Scheduler) End(o Outcome) store.TurnEnd {
	t, res := o.Turn, o.Result
	j, _, _ := s.st.Job(t.Job)
	_, sched, _ := parse(j)
	r := store.Run{ID: t.Run, Job: t.Job, Due: t.Due, Attempt: t.Attempt}
	e := store.TurnEnd{State: store.TurnDone, ItemState: store.InDone}
	run := store.RunEnd{ID: t.Run, Job: t.Job}
	fail := func(cause string) {
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, cause
		run = s.failed(j, r, cause)
	}
	text := strings.TrimSpace(res.Answer)
	done := doneLine.MatchString(text)
	text = strings.TrimSpace(doneLine.ReplaceAllString(text, ""))
	text, _ = cutBlocks(text) // jobs never create jobs
	reply := func(k core.ConvKey, body string) {
		e.Reply = &core.Outbound{TurnID: o.TurnID, Conv: k, Text: body, Formatted: true}
		for _, f := range o.Files {
			e.Files = append(e.Files, core.Outbound{TurnID: o.TurnID, Conv: k, File: &f})
		}
	}
	switch {
	case o.Stopped:
		e.State, e.ItemState, e.Detail = store.TurnCancelled, store.InCancelled, "stopped by command"
		run.State, run.Cause = store.RunCancelled, "stopped by command"
	case o.Refused:
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, "creator refused"
		run, e.Notices = s.refusal(j, t.Def, r, "admission or role changed")
	case o.Harness == "":
		e.State, e.ItemState, e.Detail = store.TurnFailed, store.InFailed, "no harness could start"
		cause := "harness did not start"
		if len(o.Notes) > 0 {
			cause += ": " + category(strings.Join(o.Notes, "; "))
		}
		run = s.retry(j, t.Def, sched, r, s.now(), cause)
	case res.NeedsHuman != "":
		fail(NeedsHumanCause)
	case res.Cancelled:
		fail("cancelled or timed out")
	case res.Failure != "":
		fail(category(res.Failure))
	case t.Target.AgentNet != "":
		if agentnetOK.MatchString(text) {
			run.State = store.RunOK
		} else {
			run.State, run.Cause = store.RunUnverified, "AGENTNET_SENT marker missing"
		}
	case t.Def.Quiet && text == QuietToken:
		run.State = store.RunQuiet
	case text == "" && len(o.Files) == 0:
		fail("empty answer")
	default:
		check := expectFail(t.Def.Expect, text, len(o.Files), o.Tools)
		switch {
		case check == "":
			run.State = store.RunOK
			reply(t.Target.Conv, text)
		case t.Target.Private:
			run.State, run.Cause = store.RunUnverified, check
			reply(t.Target.Conv, "⚠ unverified: "+check+"\n\n"+text)
		default:
			run.State, run.Cause = store.RunUnverified, check
			if k, private := s.noticeChat(j); private {
				reply(k, fmt.Sprintf("⚠ unverified: %s — job %q (%s); withheld from its target:\n\n%s", check, j.Name, j.ID, text))
			}
		}
	}
	if run.State == store.RunUnverified {
		if !t.Target.Private {
			e.Notices = s.notices(j, t.Def, t.Target.AgentNet == "", s.text(j, t.Def, "result is unverified", t.Due, run.Cause, t.Attempt+1))
		}
		s.pauseAfter(j, &run)
	}
	if run.Retry.IsZero() && run.JobState == "" && (done || sched != nil && sched.OneOff() && j.NextDue.IsZero()) {
		run.JobState, run.JobReason = store.JobDone, "finished"
		if done {
			run.JobReason = "the run answered [job: done]"
		}
	}
	e.Notices = append(e.Notices, s.endNotices(j, t.Def, run)...)
	e.Run = &run
	return e
}

func expectFail(e Expect, text string, files int, tools []string) string {
	for _, p := range e.Tools {
		if !slices.ContainsFunc(tools, func(t string) bool { ok, _ := path.Match(p, t); return ok }) {
			return "no tool matched " + p
		}
	}
	if files < e.Files {
		return fmt.Sprintf("%d file(s) expected, %d sent", e.Files, files)
	}
	if e.Prefix != "" && !strings.HasPrefix(text, e.Prefix) {
		return "the answer lacks the expected prefix"
	}
	return ""
}

// category is a failure's fixed cause label: native error text never
// reaches a notice.
func category(why string) string {
	w := strings.ToLower(why)
	for _, c := range [][2]string{
		{`timeout|timed out|deadline`, "timeout"},
		{`login|logged in|sign.?in|credential|unauthori|401|403|oauth`, "sign-in or credentials"},
		{`overload|unavailable|rate limit|429`, "provider unavailable"},
		{`server.error|5\d\d`, "provider server error"},
		{`context window|context length|too many tokens|token limit`, "context too large"},
		{`enoent|eacces|eperm|no such file|not found on the path|account|config`, "harness or runtime configuration"},
		{`connection|network|econn|socket`, "connection error"},
	} {
		if regexp.MustCompile(c[0]).MatchString(w) {
			return c[1]
		}
	}
	return "harness error"
}

// noticeChat is where a job's creator gets notices: the private chat the
// job was created in, else the creator's private chat on record, else the
// conversation it was created in (private false: fixed text only).
func (s *Scheduler) noticeChat(j store.Job) (core.ConvKey, bool) {
	if j.CreatedPrivate {
		return j.CreatedIn, true
	}
	if k, ok, _ := s.st.PrivateChat(j.Creator); ok {
		return k, true
	}
	return j.CreatedIn, false
}

// notices sends fixed text to the creator's notice chat, and to the job's
// target when toTarget, once per conversation.
func (s *Scheduler) notices(j store.Job, d Def, toTarget bool, text string) []core.Outbound {
	var outs []core.Outbound
	add := func(k core.ConvKey) {
		if k.Interface != "" && !slices.ContainsFunc(outs, func(o core.Outbound) bool { return o.Conv == k }) {
			outs = append(outs, core.Outbound{Conv: k, Text: text})
		}
	}
	if toTarget {
		if t, err := s.resolve(d.To, Caller{Principal: j.Creator, Conv: j.CreatedIn, Private: j.CreatedPrivate}); err == nil && t.AgentNet == "" {
			add(t.Conv)
		}
	}
	k, _ := s.noticeChat(j)
	add(k)
	return outs
}

// text is a notice: fixed template, Heimdall's own words only.
func (s *Scheduler) text(j store.Job, d Def, what string, slot time.Time, cause string, attempts int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "⚠ Job %q (%s) %s", j.Name, j.ID, strings.TrimSuffix(what, "."))
	if !slot.IsZero() {
		fmt.Fprintf(&b, "\nSlot: %s", when(d, slot))
	}
	if cause != "" {
		fmt.Fprintf(&b, "\nCause: %s", cause)
	}
	if attempts > 0 {
		fmt.Fprintf(&b, "\nAttempts: %d", attempts)
	}
	if j.State == store.JobActive && !j.NextDue.IsZero() {
		fmt.Fprintf(&b, "\nNext run: %s", when(d, j.NextDue))
	}
	fmt.Fprintf(&b, "\n!job show %s\n— heimdall", j.ID)
	return b.String()
}

// when is a time in the job's zone and in UTC.
func when(d Def, t time.Time) string {
	loc, err := schedule.Zone(d.TZ)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format("2006-01-02 15:04 MST") + " (" + t.UTC().Format("15:04") + " UTC)"
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
