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

func claudeSignedOut(ctx context.Context, launch Launch, env []string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, launch.Path, "auth", "status")
	cmd.Dir, cmd.Env = launch.Dir, env
	cmd.WaitDelay = 250 * time.Millisecond
	setProcAttr(cmd, launch.Cred)
	// Status can include account details. Decode only the verdict and keep no
	// output in logs, errors or notes.
	var stdout limitedBuffer
	stdout.max = 16 << 10
	cmd.Stdout = &stdout
	err := cmd.Run()
	stopGroup(cmd)
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || stdout.truncated {
		return false
	}
	var status struct {
		LoggedIn *bool `json:"loggedIn"`
	}
	return json.Unmarshal([]byte(stdout.String()), &status) == nil && status.LoggedIn != nil && !*status.LoggedIn
}
