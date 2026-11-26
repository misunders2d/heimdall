package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
)

// ErrClaudeSignedOut means the native status command rejected authentication
// before a Claude turn process was launched or given its prompt.
var ErrClaudeSignedOut = errors.New("Claude authentication status reports signed out")

// ErrClaudeAuthStatusPending means the caller stopped waiting for the native
// status command. No Claude turn was launched and auth is still unconfirmed.
var ErrClaudeAuthStatusPending = errors.New("Claude authentication status did not settle within the startup budget")

var claudeAuthProbe = make(chan struct{}, 1)

func claudeSignedOut(ctx context.Context, launch Launch, env []string) (bool, error) {
	select {
	case claudeAuthProbe <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-claudeAuthProbe
		return false, err
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
		return false, nil
	}
	done := make(chan bool, 1)
	go func() {
		defer func() { <-claudeAuthProbe }()
		err := cmd.Wait()
		done <- claudeStatusSignedOut(err, &stdout)
	}()
	select {
	case signedOut := <-done:
		return signedOut, ctx.Err()
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func claudeStatusSignedOut(err error, stdout *limitedBuffer) bool {
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || stdout.truncated {
		return false
	}
	var status struct {
		LoggedIn *bool `json:"loggedIn"`
	}
	return json.Unmarshal([]byte(stdout.String()), &status) == nil && status.LoggedIn != nil && !*status.LoggedIn
}
