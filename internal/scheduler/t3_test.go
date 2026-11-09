package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/store"
)

// Modelled process tree: the control CLI (peer) under a harness under the
// current T3 server under the T3 launcher, as on the hub.
const (
	t3Peer     = 910001
	t3Harness  = 910002
	t3Server   = 910003
	t3Launcher = 910004
)

type t3Tree struct {
	mu   sync.Mutex
	stat map[int]procEntry
	exe  map[int]string
	uid  map[int]uint32
}

func (f *t3Tree) set(fn func()) { f.mu.Lock(); fn(); f.mu.Unlock() }

// t3Layout builds the packaged layout: a stable launcher symlink into a
// private (0700) version directory holding the executables.
func t3Layout(t *testing.T) (root, launcher, real, server, spoof string) {
	t.Helper()
	root = t.TempDir()
	for _, d := range []string{"bin", "versions", "versions/a", "versions/b", "other"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{"", "bin", "versions", "other"} {
		os.Chmod(filepath.Join(root, d), 0o755)
	}
	for _, f := range []string{"versions/a/t3", "versions/b/t3", "other/t3"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("synthetic"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	os.Chmod(filepath.Join(root, "versions/a"), 0o700)
	os.Chmod(filepath.Join(root, "versions/b"), 0o700)
	launcher = filepath.Join(root, "bin/t3")
	if err := os.Symlink(filepath.Join(root, "versions/a/t3"), launcher); err != nil {
		t.Fatal(err)
	}
	real, err := filepath.EvalSymlinks(launcher)
	if err != nil {
		t.Fatal(err)
	}
	return root, launcher, real, filepath.Join(filepath.Dir(real), "../b/t3"), filepath.Join(root, "other/t3")
}

// t3Rig: a control with no Heimdall turn owning the peer and the trusted T3
// launcher configured; the kernel's process view is the modelled tree.
func t3Rig(t *testing.T) (*rig, *Control, *t3Tree, string) {
	t.Helper()
	r, c := controlRig(t)
	r.s.ResolveControl = func(int) (Caller, func() bool, error) {
		return Caller{}, nil, errors.New("no active person turn for scheduler peer")
	}
	root, launcher, real, server, spoof := t3Layout(t)
	me := uint32(os.Getuid())
	f := &t3Tree{
		stat: map[int]procEntry{
			t3Peer:     {t3Peer, t3Harness, 40},
			t3Harness:  {t3Harness, t3Server, 30},
			t3Server:   {t3Server, t3Launcher, 20},
			t3Launcher: {t3Launcher, 1, 10},
		},
		exe: map[int]string{t3Peer: "/usr/bin/heimdall", t3Harness: "/usr/bin/claude", t3Server: filepath.Clean(server), t3Launcher: real},
		uid: map[int]uint32{t3Peer: me, t3Harness: me, t3Server: me, t3Launcher: me},
	}
	oldStat, oldExe, oldUIDs := procStat, procExe, procUIDs
	t.Cleanup(func() { procStat, procExe, procUIDs = oldStat, oldExe, oldUIDs })
	procStat = func(pid int) (int, uint64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		e, ok := f.stat[pid]
		if !ok {
			return 0, 0, errors.New("no such process")
		}
		return e.ppid, e.start, nil
	}
	procExe = func(pid int) (string, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if e, ok := f.exe[pid]; ok {
			return e, nil
		}
		return "", errors.New("no such process")
	}
	procUIDs = func(pid int) ([]uint32, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		u, ok := f.uid[pid]
		if !ok {
			return nil, errors.New("no such process")
		}
		return []uint32{u, u, u, u}, nil
	}
	r.s.TrustedT3 = &TrustedLauncher{Path: launcher, UID: me, root: root}
	return r, c, f, spoof
}

// foreignOwner makes path report an owner that is neither root nor the
// launcher user, as the packaged hub executable does.
func foreignOwner(t *testing.T, path string) {
	t.Helper()
	old := pathLstat
	t.Cleanup(func() { pathLstat = old })
	pathLstat = func(p string) (os.FileInfo, error) {
		info, err := old(p)
		if err != nil || p != path {
			return info, err
		}
		st := *info.Sys().(*syscall.Stat_t)
		st.Uid = uint32(os.Getuid()) + 1
		return ownedInfo{info, st}, nil
	}
}

type ownedInfo struct {
	os.FileInfo
	st syscall.Stat_t
}

func (o ownedInfo) Sys() any { return &o.st }

// The owner trusts T3 as a task registration source ("just make T3 auto
// accepted"): a peer under the trusted launcher admits one durable task for
// the deployment owner's registered private conversation, labelled as a T3
// registration in its wakes across a restart, and can do nothing else.
func TestTrustedT3TaskAdmission(t *testing.T) {
	r, c, _, _ := t3Rig(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "durable.db")
	open := func(a durable.Assistant) *durable.Runtime {
		t.Helper()
		work, err := durable.Open(durable.Options{Path: path, Outbox: r.st, Assistant: a, Silence: -1})
		if err != nil {
			t.Fatal(err)
		}
		if err := work.Launch(); err != nil {
			t.Fatal(err)
		}
		r.s.Tasks = func(who Caller, action, issue, title string) (string, bool) {
			return work.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, OAuthActor: who.OAuthActor, OAuthInstallation: who.OAuthInstallation, Origin: who.Registered}, action, issue, title)
		}
		return work
	}
	first := &waitAssistant{wait: 2 * time.Second}
	work := open(first)
	op := &syscall.Ucred{Uid: 1234}

	who, valid, err := c.resolvePeer(t3Peer, alwaysAlive)
	if err != nil {
		t.Fatal(err)
	}
	if who.Principal.Key() != boss.Key() || who.Conv != bossDM || !who.Private || who.Registered != trustedT3 || who.Job != "" {
		t.Fatalf("wrong binding: %+v", who)
	}
	add := ControlRequest{Action: "task_add", ID: "MEL-400", Title: "Confirm the supplier PO", RequestID: "t3-task-1"}
	got := c.executePeer(ctx, op, add, who, valid)
	if got.Error != "" || !got.Changed || !strings.HasPrefix(got.Reply, "Tracking MEL-400") {
		t.Fatal(got)
	}
	if again := c.executePeer(ctx, op, add, who, valid); again != got {
		t.Fatal("request_id replay changed result", again)
	}
	changed := add
	changed.Title = "Something else"
	if got := c.executePeer(ctx, op, changed, who, valid); got.Error == "" {
		t.Fatal("changed request under the same request_id accepted", got)
	}
	dup := add
	dup.RequestID = "t3-task-2"
	if got := c.executePeer(ctx, op, dup, who, valid); got.Changed || !strings.Contains(got.Reply, "already tracked") {
		t.Fatal("same issue admitted twice", got)
	}
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_list"}, who, valid); strings.Count(got.Reply, "MEL-400") != 1 {
		t.Fatal("list", got)
	}
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_show", ID: "MEL-400"}, who, valid); got.Error != "" || !strings.Contains(got.Reply, "MEL-400") {
		t.Fatal("show", got)
	}
	q := first.next(t, "MEL-400")
	if q.Owner.Key() != boss.Key() || q.Route != bossDM || !q.Private || !strings.Contains(q.Prompt, "Registered through trusted T3, not a live chat message.") {
		t.Fatalf("wake not bound and labelled: %+v", q)
	}

	// Task-only: no scheduler operation, read or write, and no job.
	for _, req := range []ControlRequest{daily, {Action: "list"}, {Action: "confirm", ID: "x", RequestID: "c"}, {Action: "help"}} {
		if got := c.executePeer(ctx, op, req, who, valid); !strings.Contains(got.Error, "may only task_add") {
			t.Fatalf("%s: %+v", req.Action, got)
		}
	}
	if jobs, _ := r.st.Jobs(store.JobFilter{}); len(jobs) != 0 {
		t.Fatal("T3 registration created a job", jobs)
	}
	if got := c.executePeer(ctx, &syscall.Ucred{Uid: 9999}, add, who, valid); got.Error != "operator not authorized" {
		t.Fatal("foreign peer UID", got)
	}
	if outs := r.sent(); len(outs) != 0 {
		t.Fatal("task registration posted chat output", outs)
	}

	// Restart: the commitment, its next action and its origin survive.
	work.Close()
	second := &waitAssistant{}
	work = open(second)
	defer work.Close()
	q = second.next(t, "MEL-400")
	if !strings.Contains(q.Prompt, "Registered through trusted T3") || !strings.Contains(q.Prompt, "read the supplier reply") || q.Route != bossDM {
		t.Fatalf("wake after restart: %+v", q)
	}

	// Only a current owner: the deployment owner losing the role refuses.
	delete(r.s.access.Owners, boss.Key())
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_list"}, who, valid); got.Error == "" {
		t.Fatal("former owner admitted", got)
	}
	if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), "not a current owner") {
		t.Fatal(err)
	}
}

// waitAssistant answers the first wake with WAIT and later ones with REPORT.
type waitAssistant struct {
	wait time.Duration
	mu   sync.Mutex
	reqs []durable.WorkRequest
}

func (a *waitAssistant) Work(_ context.Context, q durable.WorkRequest) (durable.WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reqs = append(a.reqs, q)
	if a.wait > 0 {
		return durable.WorkResult{Turn: 1, Wait: a.wait, Next: "read the supplier reply"}, nil
	}
	return durable.WorkResult{Turn: 2, Report: "Supplier confirmed PO 551 by email."}, nil
}

func (a *waitAssistant) next(t *testing.T, issue string) durable.WorkRequest {
	t.Helper()
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		a.mu.Lock()
		for _, q := range a.reqs {
			if q.Issue == issue {
				a.mu.Unlock()
				return q
			}
		}
		a.mu.Unlock()
	}
	t.Fatalf("no wake for %s", issue)
	return durable.WorkRequest{}
}

// Refusals: only the real running launcher process, in a protected path,
// as the launcher user, with an unchanged lineage; never a Heimdall turn,
// a launcher that refused, or a stale turn.
func TestTrustedT3Refusals(t *testing.T) {
	refused := func(t *testing.T, c *Control, want string) {
		t.Helper()
		if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("want refusal %q, got %v", want, err)
		}
	}
	t.Run("off by default", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		r.s.TrustedT3 = nil
		refused(t, c, "no external origin is configured")
	})
	t.Run("name spoof: another t3 executable", func(t *testing.T) {
		_, c, f, spoof := t3Rig(t)
		f.set(func() { f.exe[t3Launcher] = spoof })
		refused(t, c, "trusted T3 launcher started this process")
	})
	t.Run("server version alone is not the launcher", func(t *testing.T) {
		_, c, f, _ := t3Rig(t)
		f.set(func() { f.stat[t3Server] = procEntry{t3Server, 1, 20} })
		refused(t, c, "trusted T3 launcher started this process")
	})
	t.Run("launcher as another user", func(t *testing.T) {
		_, c, f, _ := t3Rig(t)
		f.set(func() { f.uid[t3Launcher] = uint32(os.Getuid()) + 1 })
		refused(t, c, "does not run as the configured launcher user")
	})
	t.Run("inside Heimdall's own tree", func(t *testing.T) {
		_, c, f, _ := t3Rig(t)
		self := os.Getpid()
		f.set(func() {
			f.stat[t3Harness] = procEntry{t3Harness, self, 30}
			f.stat[self] = procEntry{self, t3Launcher, 25}
		})
		refused(t, c, "a process Heimdall started")
	})
	t.Run("stale Heimdall turn never falls through", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		r.s.ResolveControl = func(int) (Caller, func() bool, error) {
			return Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true}, func() bool { return false }, nil
		}
		refused(t, c, "no active origin")
	})
	t.Run("a launcher that refused never falls through", func(t *testing.T) {
		r, c, f, _ := t3Rig(t)
		self := os.Getpid() // the fake launcher serves from this process
		f.set(func() {
			f.stat[t3Harness] = procEntry{t3Harness, self, 30}
			f.stat[self] = procEntry{self, t3Launcher, 25}
		})
		l := newFakeLauncher(t, func(q externalQuery) externalAnswer {
			return externalAnswer{V: 1, Run: "req-1", Person: "person:sergey", Author: "agent"}
		})
		r.s.External = []ExternalOrigin{l.origin()}
		refused(t, c, "not requested by the person themself")
	})
	t.Run("an unavailable launcher does not block T3", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		r.s.External = []ExternalOrigin{{Name: "agentnet", Socket: filepath.Join(t.TempDir(), "gone.sock"), UID: uint32(os.Getuid())}}
		if who, _, err := c.resolvePeer(t3Peer, alwaysAlive); err != nil || who.Registered != trustedT3 {
			t.Fatal(who, err)
		}
	})
	t.Run("stale lineage after binding", func(t *testing.T) {
		_, c, f, _ := t3Rig(t)
		who, valid, err := c.resolvePeer(t3Peer, alwaysAlive)
		if err != nil {
			t.Fatal(err)
		}
		f.set(func() { f.stat[t3Server] = procEntry{t3Server, t3Launcher, 21} }) // reused pid
		if valid() {
			t.Fatal("changed ancestor still valid")
		}
		if got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, ControlRequest{Action: "task_list"}, who, valid); got.Error == "" {
			t.Fatal(got)
		}
	})
	t.Run("launcher retargeted after binding", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		_, valid, err := c.resolvePeer(t3Peer, alwaysAlive)
		if err != nil {
			t.Fatal(err)
		}
		l := r.s.TrustedT3.Path
		os.Remove(l)
		os.Symlink(filepath.Join(r.s.TrustedT3.root, "versions/b/t3"), l)
		if valid() {
			t.Fatal("binding survived a changed launcher")
		}
		// Resolved again per request, no version pin: the running process of
		// the newly configured executable (here the server) now matches.
		if who, _, err := c.resolvePeer(t3Peer, alwaysAlive); err != nil || who.Registered != trustedT3 {
			t.Fatal(who, err)
		}
	})
	t.Run("dead peer", func(t *testing.T) {
		_, c, _, _ := t3Rig(t)
		if _, _, err := c.resolvePeer(t3Peer, func() bool { return false }); err == nil {
			t.Fatal("dead peer bound")
		}
	})
}

// Path protection follows the packaged layout: a private version directory
// owned by the launcher user protects an executable another user owns; the
// same file is refused once anyone else could reach or change the path.
func TestTrustedT3PathProtection(t *testing.T) {
	t.Run("hub layout: foreign-owned executable in a private directory", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		real, _ := filepath.EvalSymlinks(r.s.TrustedT3.Path)
		foreignOwner(t, real)
		if who, _, err := c.resolvePeer(t3Peer, alwaysAlive); err != nil || who.Registered != trustedT3 {
			t.Fatal(who, err)
		}
	})
	t.Run("foreign-owned executable in a searchable directory", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		real, _ := filepath.EvalSymlinks(r.s.TrustedT3.Path)
		os.Chmod(filepath.Dir(real), 0o755)
		foreignOwner(t, real)
		if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), "not root or the launcher user") {
			t.Fatal(err)
		}
	})
	t.Run("foreign-owned parent directory", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		foreignOwner(t, filepath.Join(r.s.TrustedT3.root, "versions"))
		if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), "not root or the launcher user") {
			t.Fatal(err)
		}
	})
	t.Run("group-writable launcher directory", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		os.Chmod(filepath.Join(r.s.TrustedT3.root, "bin"), 0o775)
		if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), "writable by group or others") {
			t.Fatal(err)
		}
	})
	t.Run("missing launcher", func(t *testing.T) {
		r, c, _, _ := t3Rig(t)
		os.Remove(r.s.TrustedT3.Path)
		if _, _, err := c.resolvePeer(t3Peer, alwaysAlive); err == nil || !strings.Contains(err.Error(), "launcher unusable") {
			t.Fatal(err)
		}
	})
}
