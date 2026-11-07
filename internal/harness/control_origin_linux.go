package harness

import (
	"golang.org/x/sys/unix"
	"os"
	"strconv"
	"strings"
)

// ProcessAncestors reads kernel process ancestry, never model-supplied identity.
// A bounded walk fails closed on missing/unreadable procfs or malformed entries.
func ProcessAncestors(pid int) map[int]bool {
	out := map[int]bool{}
	for i := 0; pid > 1 && i < 128 && !out[pid]; i++ {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			return nil
		}
		end := strings.LastIndexByte(string(data), ')')
		if end < 0 {
			return nil
		}
		fields := strings.Fields(string(data)[end+1:])
		if len(fields) < 2 {
			return nil
		}
		parent, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil
		}
		out[pid] = true
		pid = parent
	}
	return out
}

// ControlCaller resolves the current native turn, not the process's first turn.
// Idle workers, ambiguous background/foreground overlap and passive continuations
// have no interactive scheduler authority. A worker never changes caller/chat.
func (r *Run) ControlCaller(ancestors map[int]bool) (Caller, bool) {
	c, turn := r.ControlGeneration(ancestors)
	return c, turn != nil
}

// ControlGeneration identifies the exact native person input that grants access.
func (r *Run) ControlGeneration(ancestors map[int]bool) (Caller, *Run) {
	if r.cmd == nil || r.cmd.Process == nil || !ancestors[r.cmd.Process.Pid] {
		return Caller{}, nil
	}
	if !r.Process().controlProcessAlive() {
		return Caller{}, nil
	}
	if r.worker != nil {
		w := r.worker
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.dead || w.retiring || w.pending != nil || w.background != nil || len(w.continuing) != 0 || bridgeDone(w.current.bridge) {
			return Caller{}, nil
		}
		r = w.current
	}
	select {
	case <-r.exited:
		return Caller{}, nil
	default:
	}
	select {
	case <-r.done:
		return Caller{}, nil
	default:
	}
	if !r.schedulerControl || r.cancelled.Load() || r.stopRequested.Load() {
		return Caller{}, nil
	}
	return r.caller, r
}

// Called before starting Wait: the child's PID cannot be recycled before this
// pidfd is acquired, even if it already exited. A failure disables control only.
func (r *Run) bindControlProcess() {
	fd, err := unix.PidfdOpen(r.cmd.Process.Pid, 0)
	if err == nil {
		r.controlProcess = os.NewFile(uintptr(fd), "scheduler-harness-pidfd")
	}
}
func (r *Run) closeControlProcess() {
	if r.controlProcess != nil {
		_ = r.controlProcess.Close()
	}
}
func (r *Run) controlProcessAlive() bool {
	if r.controlProcess == nil {
		return false
	}
	raw, err := r.controlProcess.SyscallConn()
	if err != nil {
		return false
	}
	alive := false
	err = raw.Control(func(fd uintptr) {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, e := unix.Poll(fds, 0)
		alive = e == nil && n == 0
	})
	return err == nil && alive
}
