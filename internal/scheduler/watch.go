package scheduler

// Watch jobs (docs/scheduler.md §6): an owner-installed checker runs on the
// job's schedule with no AI turn, and the job speaks only when the checker
// prints something. Checks run in their own goroutines, outside the due
// loop and the router; the store and the notices are the same as for
// every other job.

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

// launchWatch starts a pending watch run: outside its delivery window it is
// skipped and the checker does not run; under a drain's hold it stays
// pending. It reports a check started.
func (s *Scheduler) launchWatch(ctx context.Context, j store.Job, d Def, sched *schedule.Schedule, r store.Run, now time.Time) bool {
	oneOff := sched.OneOff() && r.Trigger != "manual"
	if d.DeliveryWindow.Next(now).After(now) {
		end := store.RunEnd{ID: r.ID, Job: j.ID, State: store.RunSkipped, Cause: "outside delivery window"}
		if oneOff {
			end.JobState, end.JobReason = store.JobDone, "one-off skipped"
		}
		s.endRun(j, d, end)
		return false
	}
	held, ok, err := s.st.StartLocalRun(MetaHold, now, r.ID)
	switch {
	case err != nil:
		s.log.Printf("scheduler: run %d of job %s not started: %v", r.ID, j.ID, err)
		return false
	case held:
		s.log.Printf("scheduler: run %d of job %s held by drain", r.ID, j.ID)
		return false
	case !ok:
		return false
	}
	cctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.checks[j.ID] = &check{run: r.ID, cancel: cancel}
	s.mu.Unlock()
	s.log.Printf("scheduler: run %d of job %s: check started (attempt %d)", r.ID, j.ID, r.Attempt)
	s.running.Add(1)
	go func() {
		defer s.running.Done()
		defer func() {
			s.mu.Lock()
			if c := s.checks[j.ID]; c != nil && c.run == r.ID {
				delete(s.checks, j.ID)
			}
			s.mu.Unlock()
			cancel()
		}()
		s.watch(ctx, cctx, j, d, sched, r, now, oneOff)
	}()
	return true
}

// watch runs one check and settles its run. parent ending (shutdown) writes
// nothing: the run stays running for Recover, which interrupts it.
func (s *Scheduler) watch(parent, ctx context.Context, j store.Job, d Def, sched *schedule.Schedule, r store.Run, now time.Time, oneOff bool) {
	settle := func(end store.RunEnd, outs ...core.Outbound) {
		if parent.Err() != nil {
			return
		}
		if end.JobState == "" && end.Retry.IsZero() && oneOff {
			end.JobState, end.JobReason = store.JobDone, "one-off ran"
		}
		fresh, _, _ := s.st.Job(j.ID)
		outs = append(outs, s.endNotices(fresh, d, end)...)
		outcome, err := s.st.SettleLocalRun(end, outs...)
		switch {
		case err != nil:
			s.log.Printf("scheduler: run %d of job %s not settled: %v", r.ID, j.ID, err)
		case outcome == store.LocalSettled:
			s.log.Printf("scheduler: run %d of job %s: %s", r.ID, j.ID, or(end.State, "retry"))
			s.notify(outs...)
		default:
			s.log.Printf("scheduler: run %d of job %s: %s, nothing sent", r.ID, j.ID, outcome)
		}
		s.Wake()
	}
	v, tgt, err := s.verify(ctx, j, d, now)
	switch {
	case parent.Err() != nil:
		return
	case ctx.Err() != nil:
		settle(store.RunEnd{ID: r.ID, Job: j.ID, State: store.RunCancelled, Cause: "stopped by command"})
		return
	case errors.Is(err, ErrTemporary):
		settle(s.retry(j, d, sched, r, now, err.Error()))
		return
	case err != nil:
		end, outs := s.refusal(j, d, r, err.Error())
		settle(end, outs...)
		return
	}
	res := s.runCheck(ctx, d, v, tgt)
	if parent.Err() != nil {
		return
	}
	fresh, _, _ := s.st.Job(j.ID) // its failures in a row now, for pause_after
	end := store.RunEnd{ID: r.ID, Job: j.ID}
	var outs []core.Outbound
	switch {
	case res.Stopped:
		end.State, end.Cause = store.RunCancelled, "stopped by command"
	case res.Cause != "":
		cause := res.Cause
		if res.Detail != "" {
			cause += " — " + res.Detail
		}
		end = s.failed(fresh, r, cause)
	case res.Output == "":
		end.State = store.RunQuiet
	default:
		end.State = store.RunOK
		outs = []core.Outbound{{Conv: tgt.Conv, Text: cutOutput(res.Output), Window: d.DeliveryWindow}}
	}
	settle(end, outs...)
}

// runCheck resolves the job's checker now (one added since the start
// needs no restart) and runs it as run_as.owner for the verified creator.
func (s *Scheduler) runCheck(ctx context.Context, d Def, v Verified, tgt Target) harness.CheckResult {
	path, err := s.cfg.Checkers.Resolve(d.Check.Checker)
	if err != nil {
		return harness.CheckResult{Cause: "checker unusable: " + err.Error()}
	}
	l, err := s.cfg.Checkers.Prepare(harness.Caller{Principal: v.Principal, Role: core.RoleOwner, Conv: tgt.Conv, OAuthActor: v.OAuthActor, OAuthInstallation: v.OAuthInstallation})
	if err != nil {
		return harness.CheckResult{Cause: "checker cannot run as run_as.owner", Detail: core.SanitizeNativeError(err.Error(), "")}
	}
	timeout := harness.CheckerTimeout
	if t, err := span(d.Timeout); d.Timeout != "" && err == nil {
		timeout = t
	}
	return s.runChecker(ctx, path, l, timeout)
}

// cancelStale stops checks whose run is no longer running: a !job stop or
// !job rm marked it cancelled in the command's own transaction.
func (s *Scheduler) cancelStale() {
	s.mu.Lock()
	running := make([]*check, 0, len(s.checks))
	for _, c := range s.checks {
		running = append(running, c)
	}
	s.mu.Unlock()
	for _, c := range running {
		if r, err := s.st.Run(c.run); err == nil && r.State != store.RunRunning {
			c.cancel()
		}
	}
}

// cutOutput is a checker's output as delivered: at most watchTextMax
// bytes, cut at a UTF-8 boundary, with a last line saying so. A chatty
// script never loses its alert.
func cutOutput(text string) string {
	if len(text) <= watchTextMax {
		return text
	}
	text = text[:watchTextMax]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text + "\n… (cut: output too long)"
}
