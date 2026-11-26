package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"time"
)

// ErrClaudeSignedOut means the native status command rejected authentication
// before a Claude turn process was launched or given its prompt.
var ErrClaudeSignedOut = errors.New("Claude authentication status reports signed out")

// errClaudeLoginExpired means a stored Claude.ai login reached the latest
// expiry reported by native status before the job process was launched.
var errClaudeLoginExpired = errors.New("Claude authentication status reports the subscription login deadline has elapsed")

// ErrClaudeAuthStatusPending means the caller stopped waiting for the native
// status command. No Claude turn was launched and auth is still unconfirmed.
var ErrClaudeAuthStatusPending = errors.New("Claude authentication status did not settle within the startup budget")

var claudeAuthProbe = make(chan struct{}, 1)

func checkClaudeAuth(ctx context.Context, launch Launch, env []string) error {
	select {
	case claudeAuthProbe <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-claudeAuthProbe
		return err
	}
	// Claude can refresh and save credentials in this command. Let it finish
	// natively even if this turn stops waiting. Keep the probe slot until exit.
	cmd := exec.Command(launch.Path, "auth", "status")
	cmd.Dir, cmd.Env = launch.Dir, env
	setProcAttr(cmd, launch.Cred)
	cmd.Cancel = nil
	// Status can include account details. Decode only the verdict and keep no
	// output in logs, errors or notes.
	var stdout limitedBuffer
	stdout.max = 16 << 10
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		<-claudeAuthProbe
		return nil
	}
	done := make(chan error, 1)
	go func() {
		defer func() { <-claudeAuthProbe }()
		err := cmd.Wait()
		done <- claudeStatusRejection(err, &stdout, time.Now())
	}()
	select {
	case rejection := <-done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return rejection
	case <-ctx.Done():
		return ctx.Err()
	}
}

func claudeStatusRejection(waitErr error, stdout *limitedBuffer, now time.Time) error {
	if stdout.truncated {
		return nil
	}
	var status struct {
		LoggedIn       *bool           `json:"loggedIn"`
		AuthMethod     json.RawMessage `json:"authMethod"`
		LoginExpiresAt json.RawMessage `json:"loginExpiresAt"`
	}
	if json.Unmarshal([]byte(stdout.String()), &status) != nil || status.LoggedIn == nil {
		return nil
	}
	if !*status.LoggedIn {
		var exit *exec.ExitError
		if errors.As(waitErr, &exit) && exit.ExitCode() == 1 {
			return ErrClaudeSignedOut
		}
		return nil
	}
	var method string
	if waitErr != nil || json.Unmarshal(status.AuthMethod, &method) != nil || method != "claude.ai" {
		return nil
	}
	// Anthropic reports the later expected end date, not the first date in
	// the warning window. Only reject once that deadline is reached.
	var expiry time.Time
	if json.Unmarshal(status.LoginExpiresAt, &expiry) == nil && !expiry.IsZero() && !expiry.After(now) {
		return errClaudeLoginExpired
	}
	return nil
}
