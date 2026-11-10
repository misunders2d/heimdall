package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// Modelled kernel view for task_export peers.
const (
	adminPeer   = 920001 // the dashboard exporter's CLI
	adminScript = 920002 // its root oneshot script, a child of init
	turnPeer    = 920011 // a CLI under a Heimdall-started process
	mixedPeer   = 920021 // runs as the admin user under another user's process
	mixedParent = 920022
)

func exportRig(t *testing.T) (*rig, *Control, uint32) {
	t.Helper()
	r, c := controlRig(t)
	me := uint32(os.Geteuid())
	c.uid = me + 1 // run_as.owner is another user, as on the hub
	self := os.Getpid()
	stat := map[int]procEntry{
		adminPeer:   {adminPeer, adminScript, 50},
		adminScript: {adminScript, 1, 40},
		turnPeer:    {turnPeer, self, 30},
		self:        {self, 1, 5},
		mixedPeer:   {mixedPeer, mixedParent, 20},
		mixedParent: {mixedParent, 1, 10},
	}
	uid := map[int]uint32{adminPeer: me, adminScript: me, turnPeer: me, self: me, mixedPeer: me, mixedParent: me + 7}
	var mu sync.Mutex
	oldStat, oldUIDs := procStat, procUIDs
	t.Cleanup(func() { procStat, procUIDs = oldStat, oldUIDs })
	procStat = func(pid int) (int, uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		e, ok := stat[pid]
		if !ok {
			return 0, 0, errors.New("no such process")
		}
		return e.ppid, e.start, nil
	}
	procUIDs = func(pid int) ([]uint32, error) {
		mu.Lock()
		defer mu.Unlock()
		u, ok := uid[pid]
		if !ok {
			return nil, errors.New("no such process")
		}
		return []uint32{u, u, u, u}, nil
	}
	r.s.TaskExport = func() ([]byte, error) { return []byte(`{"tasks":[]}`), nil }
	return r, c, me
}

// task_export answers only a local admin process: Heimdall's own uid in
// the whole ancestry, outside Heimdall's process tree, with no turn,
// launcher or session bound to it, asking with no arguments. Anything
// else is refused, and so is a deployment where such a peer could not be
// told apart from an owner turn or a launcher's session.
func TestControlTaskExportGate(t *testing.T) {
	r, c, me := exportRig(t)
	live := func() bool { return true }
	req := ControlRequest{Action: "task_export"}
	if got := c.export(me, adminPeer, req, false, live); got.Error != "" || got.Reply != `{"tasks":[]}` {
		t.Fatalf("admin refused: %+v", got)
	}
	refused := func(name string, got ControlResult) {
		t.Helper()
		if got.Error == "" || got.Reply != "" || !strings.HasPrefix(got.Error, "task_export refused") {
			t.Fatalf("%s: %+v", name, got)
		}
	}
	refused("owner uid", c.export(c.uid, adminPeer, req, false, live))
	refused("bound turn or session", c.export(me, adminPeer, req, true, live))
	refused("Heimdall-started process", c.export(me, turnPeer, req, false, live))
	refused("Heimdall itself", c.export(me, os.Getpid(), req, false, live))
	refused("another user's ancestor", c.export(me, mixedPeer, req, false, live))
	refused("unreadable ancestry", c.export(me, 929999, req, false, live))
	refused("peer gone", c.export(me, adminPeer, req, false, func() bool { return false }))
	for _, extra := range []ControlRequest{{Action: "task_export", ID: "MEL-1"}, {Action: "task_export", RequestID: "r"}, {Action: "task_export", Title: "x"}, {Action: "task_export", Definition: []byte(`{}`)}} {
		refused("arguments", c.export(me, adminPeer, extra, false, live))
	}

	// Fail closed where an admin peer cannot be told apart.
	c.uid = me
	refused("owner turns as Heimdall's uid", c.export(me, adminPeer, req, false, live))
	c.uid = me + 1
	r.s.TrustedT3 = &TrustedLauncher{Path: "/nonexistent/t3", UID: me}
	refused("T3 launcher as Heimdall's uid", c.export(me, adminPeer, req, false, live))
	r.s.TrustedT3 = &TrustedLauncher{Path: "/nonexistent/t3", UID: me + 7}
	if got := c.export(me, adminPeer, req, false, live); got.Error != "" {
		t.Fatalf("T3 launcher as another user: %+v", got)
	}
	r.s.External = []ExternalOrigin{{Name: "agentnet", UID: me}}
	refused("external launcher as Heimdall's uid", c.export(me, adminPeer, req, false, live))
	r.s.External = nil

	r.s.TaskExport = nil
	if got := c.export(me, adminPeer, req, false, live); got.Error != "durable tasks are not enabled here" {
		t.Fatalf("%+v", got)
	}

	// A bound caller asking through the ordinary path is refused there too.
	r.s.Tasks = func(Caller, string, string, string, string) (string, bool) { return "unexpected", false }
	got := c.execute(context.Background(), c.uid, req, Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true})
	if !strings.HasPrefix(got.Error, "unsupported task action") {
		t.Fatalf("%+v", got)
	}
}

// Over the real socket a Heimdall turn (bound through ResolveControl) and
// a process Heimdall started (here: the test process itself, the
// listener's own process) are refused task_export.
func TestControlTaskExportSocketRefusesHeimdallProcesses(t *testing.T) {
	r, _ := controlRig(t)
	r.s.TaskExport = func() ([]byte, error) { return []byte(`{"tasks":[]}`), nil }
	var turn atomic.Bool
	r.s.ResolveControl = func(int) (Caller, func() bool, error) {
		if turn.Load() {
			return asBoss, func() bool { return true }, nil
		}
		return Caller{}, nil, errors.New("no active person turn for scheduler peer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := filepath.Join(t.TempDir(), "ctl")
	l, err := r.s.ListenControl(ctx, boss.Key(), uint32(os.Geteuid()), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	socket := filepath.Join(dir, "control.sock")
	result, err := CallControl(ctx, socket, ControlRequest{Action: "task_export"})
	if err == nil || result.Reply != "" || !strings.Contains(result.Error, "process Heimdall started") {
		t.Fatalf("unbound Heimdall process: %+v %v", result, err)
	}
	turn.Store(true)
	result, err = CallControl(ctx, socket, ControlRequest{Action: "task_export"})
	if err == nil || result.Reply != "" || !strings.Contains(result.Error, "bound") {
		t.Fatalf("bound turn: %+v %v", result, err)
	}
}
