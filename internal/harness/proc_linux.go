package harness

// Ported from AgentNet internal/client/proc_unix.go and proc_linux.go.

import (
	"os/exec"
	"syscall"
)

// setProcAttr starts cmd in its own process group as cred (nil: Heimdall's
// own user) and makes cancellation kill the whole group, so helpers the
// harness started stop too. The harness also dies with Heimdall (Pdeathsig).
// A descendant that starts its own session or group escapes this.
func setProcAttr(cmd *exec.Cmd, cred *syscall.Credential) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL, Credential: cred}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}

// termGroup asks cmd's process group to exit.
func termGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
}

// stopGroup kills what is left of cmd's process group once cmd has exited.
func stopGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // ESRCH: nothing left
	}
}
