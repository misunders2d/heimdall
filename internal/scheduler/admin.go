package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// The operator's commands (heimdall sched, heimdall drain): they read and
// write the store while Heimdall runs, never through the router.

// Admin answers heimdall sched list and show, as the owner sees them in a
// private chat.
func (s *Scheduler) Admin(sub, id string) string {
	c := Caller{Role: core.RoleOwner, Private: true}
	if sub == "show" {
		return s.show(c, id)
	}
	return s.list(c, true)
}

// Health is heimdall sched health's report.
type Health struct {
	OK     bool           `json:"ok"`
	TickAt string         `json:"tick_at,omitempty"`
	Issues []string       `json:"issues"`
	Paused []PausedJob    `json:"paused"`
	Counts map[string]int `json:"jobs"`
}

type PausedJob struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Health reports what needs the operator (docs/scheduler.md §7.3): a stale
// due loop, runs past their timeout, active jobs overdue beyond grace, jobs
// the scheduler paused, and failures open for more than a day. runTimeout
// is the harness default for runs without their own.
func (s *Scheduler) Health(runTimeout time.Duration) Health {
	now := s.now()
	h := Health{OK: true, Issues: []string{}, Paused: []PausedJob{}, Counts: map[string]int{}}
	issue := func(format string, args ...any) {
		h.OK = false
		h.Issues = append(h.Issues, fmt.Sprintf(format, args...))
	}
	tick, err := s.st.Meta(MetaTick)
	switch {
	case err != nil:
		issue("store: %v", err)
		return h
	case tick == 0:
		issue("the due loop has never run")
	default:
		at := time.UnixMilli(tick)
		h.TickAt = at.UTC().Format(time.RFC3339)
		if now.Sub(at) > 3*time.Minute {
			issue("the due loop last ran %s ago", now.Sub(at).Round(time.Second))
		}
	}
	jobs, err := s.st.Jobs(store.JobFilter{States: []string{store.JobActive, store.JobPaused, store.JobDone}})
	if err != nil {
		issue("store: %v", err)
		return h
	}
	for _, j := range jobs {
		h.Counts[j.State]++
		d, sched, err := parse(j)
		if err != nil {
			issue("job %s: unreadable definition", j.ID)
			continue
		}
		if j.State == store.JobPaused {
			h.Paused = append(h.Paused, PausedJob{j.ID, j.Name, j.Reason})
			if !strings.HasPrefix(j.Reason, "paused by ") {
				issue("job %s paused by the scheduler: %s", j.ID, j.Reason)
			}
		}
		if j.State == store.JobActive && !j.NextDue.IsZero() && now.Sub(j.NextDue) > s.grace(d, sched) {
			issue("job %s is overdue since %s", j.ID, when(d, j.NextDue))
		}
		runs, _ := s.st.Runs(store.RunFilter{Job: j.ID, Limit: 50})
		for _, r := range runs {
			limit := runTimeout
			if t, err := span(d.Timeout); err == nil && d.Timeout != "" {
				limit = t
			}
			if r.State == store.RunRunning && !r.Started.IsZero() && now.Sub(r.Started) > limit+time.Minute {
				issue("job %s: run %d is past its timeout", j.ID, r.ID)
			}
		}
		// The current streak of failed or unverified runs, oldest last.
		i := slices.IndexFunc(runs, func(r store.Run) bool {
			return r.State != store.RunFailed && r.State != store.RunUnverified && store.Ended(r.State) && r.State != store.RunSkipped
		})
		if i < 0 {
			i = len(runs)
		}
		for _, r := range slices.Backward(runs[:i]) {
			if (r.State == store.RunFailed || r.State == store.RunUnverified) && now.Sub(r.Ended) > 24*time.Hour {
				issue("job %s has failed since %s", j.ID, when(d, r.Ended))
			}
			break
		}
	}
	return h
}

// Export is every job's definition, for backup and rollback: identities and
// schedules, no tokens or other secrets (there are none in the store).
func (s *Scheduler) Export() ([]byte, error) {
	jobs, err := s.st.Jobs(store.JobFilter{States: []string{store.JobActive, store.JobPaused, store.JobDone}})
	if err != nil {
		return nil, err
	}
	type entry struct {
		ID        string          `json:"id"`
		Name      string          `json:"name"`
		State     string          `json:"state"`
		Reason    string          `json:"reason,omitempty"`
		Creator   string          `json:"creator"`
		CreatedIn core.ConvKey    `json:"created_in"`
		Version   int             `json:"version"`
		Def       json.RawMessage `json:"def"`
	}
	out := []entry{}
	for _, j := range jobs {
		out = append(out, entry{j.ID, j.Name, j.State, j.Reason, j.Creator.Key(), j.CreatedIn, j.Version, json.RawMessage(j.Def)})
	}
	return json.MarshalIndent(out, "", "  ")
}

// Drain holds new scheduled runs until timeout and waits for every running
// turn to end, every run handed to the router to start or be held at its
// turn's start (Begin), and every reply to be delivered: idle true then.
// Live chat is not held. The hold clears when Heimdall starts.
func Drain(ctx context.Context, st *store.Store, timeout, poll time.Duration) (idle bool, err error) {
	deadline := time.Now().Add(timeout)
	if err := st.SetMeta(MetaHold, deadline.UnixMilli()); err != nil {
		return false, err
	}
	for {
		turns, replies, err := st.Busy()
		if err != nil {
			return false, err
		}
		if turns == 0 && replies == 0 {
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(min(poll, time.Until(deadline))):
		}
	}
}
