package harness

// Ported from AgentNet internal/client/proc_unix.go and proc_linux.go.

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// setProcAttr starts cmd in its own process group as cred (nil: Heimdall's
// own user) and makes cancellation kill the whole group, so helpers the
// harness started stop too. The harness also dies with Heimdall (Pdeathsig).
//
// A descendant that started its own session or group gets no group signal.
// Cancellation (an explicit stop, the turn's time limit, or SIGKILL after a
// stop's TERM grace ran out) kills those too, from a /proc snapshot taken
// while the harness still runs. A graceful retirement (termGroup: idle,
// lifetime, engine switch, drain) signals only the group: harnesses stay
// unconstrained (owner, 2026-11-10), and an agent may deliberately detach a
// long job (nohup/setsid) to outlive its turn. Detached processes end with
// the service's cgroup (KillMode=control-group) at unit stop.
func setProcAttr(cmd *exec.Cmd, cred *syscall.Credential) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL, Credential: cred}
	cmd.Cancel = func() error {
		escaped := escapedDescendants(cmd.Process.Pid)
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		killAll(escaped)
		return err
	}
}

// termGroup asks cmd's process group to exit. Descendants that left the
// group are left alone (see setProcAttr).
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

func killAll(pids []int) {
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// escapedDescendants is a snapshot of the live descendants of root (a
// process group leader) that are no longer in root's group, from /proc.
// Taken just before signalling, while root still runs, so the tree is
// still linked to it.
func escapedDescendants(root int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	type proc struct{ ppid, pgrp int }
	procs := map[int]proc{}
	children := map[int][]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		ppid, pgrp, ok := procStat(pid)
		if !ok {
			continue
		}
		procs[pid] = proc{ppid, pgrp}
		children[ppid] = append(children[ppid], pid)
	}
	var out []int
	queue := append([]int(nil), children[root]...)
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		if procs[pid].pgrp != root {
			out = append(out, pid)
		}
		queue = append(queue, children[pid]...)
	}
	return out
}

// procStat reads a process's parent and process group from /proc/PID/stat.
// The command name may hold spaces and parentheses: fields start after the
// last ')'.
func procStat(pid int) (ppid, pgrp int, ok bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, false
	}
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, 0, false
	}
	f := bytes.Fields(b[i+1:]) // state ppid pgrp ...
	if len(f) < 3 {
		return 0, 0, false
	}
	ppid, err1 := strconv.Atoi(string(f[1]))
	pgrp, err2 := strconv.Atoi(string(f[2]))
	return ppid, pgrp, err1 == nil && err2 == nil
}
