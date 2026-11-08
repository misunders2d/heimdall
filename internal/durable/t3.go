package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Observation is a worker's state as its own host reports it.
type Observation struct {
	Status    string `json:"status"`
	LatestRun string `json:"latest_run"`
	ActiveRun string `json:"active_run"` // "" when no run is in progress
	Pending   int    `json:"pending"`    // requests waiting for an answer
	RunCount  int    `json:"run_count"`  // the host's run order: runs started so far
	Reply     string `json:"reply,omitempty"`
	// Replies is the last assistant text of each run among the latest
	// messages, so a dispatched run's result is read by its own id.
	Replies map[string]string `json:"replies,omitempty"`
}

// Observer reads a worker's state. It is read-only: it never sends,
// approves or answers anything, and never chooses a provider or model.
type Observer interface {
	// Observable reports whether worker is a reference this observer reads.
	Observable(worker string) bool
	Observe(ctx context.Context, worker string) (Observation, error)
}

// T3 observes T3 Code threads ("t3:<thread id>") of its own host through
// the host CLI's read-only `thread show --json`. Command is the installed
// CLI, configured explicitly; there is no default path. Qualified only where
// that CLI runs as the same user as the T3 server on the same host
// (docs/durable.md). A worker in another environment ("t3:<thread>@<env>")
// is not observable here; the router's native wait turn reads it.
type T3 struct {
	Command []string
	Timeout time.Duration
}

func (t T3) Observable(worker string) bool {
	id, ok := strings.CutPrefix(worker, "t3:")
	return ok && id != "" && !strings.Contains(id, "@") && len(t.Command) > 0
}

func (t T3) Observe(ctx context.Context, worker string) (Observation, error) {
	id, err := t.thread(worker)
	if err != nil {
		return Observation{}, err
	}
	v, err := t.show(ctx, id, 20)
	if err != nil {
		return Observation{}, err
	}
	o := Observation{Status: v.Thread.Status, Pending: v.Thread.PendingRequestCount, RunCount: v.Thread.RunCount}
	if v.Thread.LatestRunID != nil {
		o.LatestRun = *v.Thread.LatestRunID
	}
	if v.Thread.ActiveRunID != nil {
		o.ActiveRun = *v.Thread.ActiveRunID
	}
	for _, m := range v.Messages {
		if m.Role == "assistant" && m.RunID != "" {
			if o.Replies == nil {
				o.Replies = map[string]string{}
			}
			o.Replies[m.RunID] = clip(m.Text, 1500)
		}
	}
	o.Reply = o.Replies[o.LatestRun]
	return o, nil
}

type shown struct {
	Thread *struct {
		Status              string  `json:"status"`
		LatestRunID         *string `json:"latestRunId"`
		ActiveRunID         *string `json:"activeRunId"`
		PendingRequestCount int     `json:"pendingRequestCount"`
		RunCount            int     `json:"runCount"`
	} `json:"thread"`
	Messages []struct {
		Role  string `json:"role"`
		RunID string `json:"runId"`
		Text  string `json:"text"`
	} `json:"messages"`
}

func (t T3) run(ctx context.Context, stdin string, args ...string) ([]byte, error) {
	d := t.Timeout
	if d <= 0 {
		d = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	cmd := exec.CommandContext(ctx, t.Command[0], append(append([]string{}, t.Command[1:]...), args...)...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("%w: %s", err, clip(strings.TrimSpace(stderr.String()), 300))
	}
	return out, nil
}

func (t T3) show(ctx context.Context, id string, messages int) (shown, error) {
	var v shown
	out, err := t.run(ctx, "", "thread", "show", id, "--json", "--messages", strconv.Itoa(messages))
	if err != nil {
		return v, fmt.Errorf("t3 thread show: %w", err)
	}
	if err := json.Unmarshal(out, &v); err != nil || v.Thread == nil {
		return v, errors.New("t3 thread show: unexpected output")
	}
	return v, nil
}

func (t T3) thread(worker string) (string, error) {
	id, _ := strings.CutPrefix(worker, "t3:")
	if !t.Observable(worker) || strings.HasPrefix(id, "-") {
		return "", errors.New("not a t3 thread reference")
	}
	return id, nil
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
