package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Observation is a worker's state as its own host reports it.
type Observation struct {
	Status    string `json:"status"`
	LatestRun string `json:"latest_run"`
	ActiveRun string `json:"active_run"` // "" when no run is in progress
	Pending   int    `json:"pending"`    // requests waiting for an answer
	Reply     string `json:"reply,omitempty"`
}

// Observer reads a worker's state. It is read-only: it never sends,
// approves or answers anything, and never chooses a provider or model.
type Observer interface {
	// Observable reports whether worker is a reference this observer reads.
	Observable(worker string) bool
	Observe(ctx context.Context, worker string) (Observation, error)
}

// T3 observes T3 Code threads ("t3:<thread id>") through the host CLI's
// read-only `thread show --json`. Command is the installed CLI, configured
// explicitly; there is no default path. Qualified only where that CLI runs
// as the same user as the T3 server on the same host (docs/durable.md).
type T3 struct {
	Command []string
	Timeout time.Duration
}

func (t T3) Observable(worker string) bool {
	id, ok := strings.CutPrefix(worker, "t3:")
	return ok && id != "" && len(t.Command) > 0
}

func (t T3) Observe(ctx context.Context, worker string) (Observation, error) {
	id, _ := strings.CutPrefix(worker, "t3:")
	if !t.Observable(worker) || strings.HasPrefix(id, "-") {
		return Observation{}, errors.New("not a t3 thread reference")
	}
	d := t.Timeout
	if d <= 0 {
		d = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	args := append(append([]string{}, t.Command[1:]...), "thread", "show", id, "--json", "--messages", "1")
	cmd := exec.CommandContext(ctx, t.Command[0], args...)
	out, err := cmd.Output()
	if err != nil {
		return Observation{}, fmt.Errorf("t3 thread show: %w", err)
	}
	var v struct {
		Thread *struct {
			Status              string  `json:"status"`
			LatestRunID         *string `json:"latestRunId"`
			ActiveRunID         *string `json:"activeRunId"`
			PendingRequestCount int     `json:"pendingRequestCount"`
		} `json:"thread"`
		Messages []struct {
			Role  string `json:"role"`
			RunID string `json:"runId"`
			Text  string `json:"text"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &v); err != nil || v.Thread == nil {
		return Observation{}, errors.New("t3 thread show: unexpected output")
	}
	o := Observation{Status: v.Thread.Status, Pending: v.Thread.PendingRequestCount}
	if v.Thread.LatestRunID != nil {
		o.LatestRun = *v.Thread.LatestRunID
	}
	if v.Thread.ActiveRunID != nil {
		o.ActiveRun = *v.Thread.ActiveRunID
	}
	for _, m := range v.Messages {
		if m.Role == "assistant" && m.RunID == o.LatestRun {
			o.Reply = clip(m.Text, 1500)
		}
	}
	return o, nil
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
