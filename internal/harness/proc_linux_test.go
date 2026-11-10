package harness

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A harness child that starts its own session leaves the process group, so
// the group signal alone never reached it: stopping the turn must kill it.
func TestStopKillsChildThatLeftTheGroup(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid not installed")
	}
	for name, stop := range map[string]func(*exec.Cmd, context.CancelFunc){
		"term":   func(cmd *exec.Cmd, _ context.CancelFunc) { termGroup(cmd) },
		"cancel": func(_ *exec.Cmd, cancel context.CancelFunc) { cancel() },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// The shell stays in the group and waits; its child setsid()s away.
			cmd := exec.CommandContext(ctx, "sh", "-c", `setsid sleep 300 & echo $!; trap '' TERM; wait`)
			setProcAttr(cmd, nil)
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(out).ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			child, err := strconv.Atoi(strings.TrimSpace(line))
			if err != nil {
				t.Fatal(err)
			}
			defer syscall.Kill(child, syscall.SIGKILL)
			// Wait until the child has its own session (setsid ran).
			deadline := time.Now().Add(5 * time.Second)
			for {
				_, pgrp, ok := procStat(child)
				if ok && pgrp == child {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("child %d never left the group", child)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := escapedDescendants(cmd.Process.Pid); len(got) != 1 || got[0] != child {
				t.Fatalf("escaped descendants %v, want [%d]", got, child)
			}
			stop(cmd, cancel)
			if name == "term" {
				// The shell ignores TERM; the escaped child must already be gone.
				defer syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			for deadline := time.Now().Add(5 * time.Second); alive(child); {
				if time.Now().After(deadline) {
					t.Fatalf("escaped child %d survived stop", child)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if name == "term" {
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			cmd.Wait()
		})
	}
}

// alive: the process exists and is not a zombie waiting to be reaped.
func alive(pid int) bool {
	b, err := readStat(pid)
	if err != nil {
		return false
	}
	i := strings.LastIndexByte(b, ')')
	return i >= 0 && !strings.HasPrefix(strings.TrimSpace(b[i+1:]), "Z")
}

func readStat(pid int) (string, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	return string(b), err
}
