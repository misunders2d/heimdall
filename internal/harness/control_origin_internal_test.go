package harness

import (
	"github.com/misunders2d/heimdall/internal/core"
	"os/exec"
	"testing"
)

func TestControlOriginRejectsDeadRunBeforeExitPublication(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r := &Run{cmd: cmd, done: make(chan struct{}), exited: make(chan struct{}), schedulerControl: true, caller: Caller{Role: core.RoleOwner}}
	r.bindControlProcess()
	defer r.closeControlProcess()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	// Even a stale cached ancestry match cannot reuse this spawn-time process.
	if _, ok := r.ControlCaller(map[int]bool{cmd.Process.Pid: true}); ok {
		t.Fatal("dead Run accepted before exited channel closes")
	}
}
