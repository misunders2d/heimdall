package scheduler

import (
	"context"
	"encoding/json"
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
		r.s.Tasks = func(who Caller, action, issue, title, requestID string) (string, bool) {
			return work.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, OAuthActor: who.OAuthActor, OAuthInstallation: who.OAuthInstallation, Origin: who.Registered}, action, issue, title, requestID)
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

// Management (owner opt-in "manage"): off, trusted T3 stays task-only; on,
// the same bound deployment owner creates, edits (applied at once, no card),
// lists, reads, runs, pauses, resumes and stops only their own jobs and
// pauses, resumes and closes only their own tasks, with request_id replay;
// never remove, checkpoint, acknowledge or confirm.
func TestTrustedT3Management(t *testing.T) {
	r, c, f, _ := t3Rig(t)
	ctx := context.Background()
	work, err := durable.Open(durable.Options{Path: filepath.Join(t.TempDir(), "durable.db"), Outbox: r.st, Assistant: &waitAssistant{wait: time.Hour}, Silence: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := work.Launch(); err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	r.s.Tasks = func(who Caller, action, issue, title, requestID string) (string, bool) {
		return work.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, OAuthActor: who.OAuthActor, OAuthInstallation: who.OAuthInstallation, Origin: who.Registered}, action, issue, title, requestID)
	}
	other := core.Principal{Platform: core.PlatformTelegram, ID: "3", Name: "other owner"}
	otherDM := core.ConvKey{Interface: core.PlatformTelegram, Chat: "3"}
	r.s.access.Owners[other.Key()] = true
	mine := r.add(asBoss, `{"name":"slack monitor","schedule":{"every":"1h"},"prompt":"synthetic"}`)
	theirs := r.add(Caller{Principal: other, Role: core.RoleOwner, Conv: otherDM, Private: true}, `{"name":"their job","schedule":{"every":"1h"},"prompt":"synthetic"}`)
	if _, ok := work.Task(durable.Caller{Principal: other, Role: core.RoleOwner, Conv: otherDM, Live: true, Private: true, MsgID: "m1"}, "add", "MEL-401", "Their task", ""); !ok {
		t.Fatal("other owner's task not admitted")
	}
	op := &syscall.Ucred{Uid: 1234}
	who, valid, err := c.resolvePeer(t3Peer, alwaysAlive)
	if err != nil {
		t.Fatal(err)
	}
	do := func(req ControlRequest) ControlResult { t.Helper(); return c.executePeer(ctx, op, req, who, valid) }
	ok := func(req ControlRequest) ControlResult {
		t.Helper()
		got := do(req)
		if got.Error != "" || got.Reply == "" {
			t.Fatalf("%s refused: %+v", req.Action, got)
		}
		return got
	}
	refused := func(req ControlRequest, want string) {
		t.Helper()
		if got := do(req); !strings.Contains(got.Error, want) {
			t.Fatalf("%s: got %+v, want %q", req.Action, got, want)
		}
	}
	if got := ok(ControlRequest{Action: "task_add", ID: "MEL-755", Title: "Activate the Slack monitor", RequestID: "add-755"}); !got.Changed {
		t.Fatal(got)
	}

	// Baseline: without the opt-in, management is the existing refusal.
	manage := []ControlRequest{
		{Action: "list"}, {Action: "show", ID: mine}, {Action: "history", ID: mine},
		{Action: "pause", ID: mine, RequestID: "off-p"}, {Action: "run", ID: mine, RequestID: "off-r"},
		daily, {Action: "edit", ID: mine, RequestID: "off-e", Definition: json.RawMessage(`{"prompt":"changed"}`)},
		{Action: "task_pause", ID: "MEL-755", RequestID: "off-tp"}, {Action: "task_close", ID: "MEL-755", RequestID: "off-tc"},
	}
	for _, req := range manage {
		refused(req, "may only task_add, task_list and task_show")
	}
	if r.job(mine).State != store.JobActive {
		t.Fatal("refused pause changed the job")
	}

	r.s.TrustedT3.Manage = true
	// Jobs: the bound owner's own, exact creator, with request_id replay.
	if got := ok(ControlRequest{Action: "list"}); !strings.Contains(got.Reply, "slack monitor") || strings.Contains(got.Reply, "their job") {
		t.Fatal("list", got)
	}
	ok(ControlRequest{Action: "show", ID: mine})
	ok(ControlRequest{Action: "history", ID: mine})
	pause := ControlRequest{Action: "pause", ID: mine, RequestID: "pause-1"}
	got := ok(pause)
	if !got.Changed || r.job(mine).State != store.JobPaused {
		t.Fatal("pause", got, r.job(mine).State)
	}
	if again := do(pause); again != got {
		t.Fatal("pause replay changed result", again)
	}
	if got := ok(ControlRequest{Action: "resume", ID: mine, RequestID: "resume-1"}); !got.Changed || r.job(mine).State != store.JobActive {
		t.Fatal("resume", got)
	}
	if got := ok(ControlRequest{Action: "run", ID: mine, RequestID: "run-1"}); !got.Changed || len(r.runs(mine)) != 1 {
		t.Fatal("run", got, r.runs(mine))
	}
	ok(ControlRequest{Action: "stop", ID: mine, RequestID: "stop-1"})
	for _, req := range []ControlRequest{{Action: "show", ID: theirs}, {Action: "pause", ID: theirs, RequestID: "theirs-p"}, {Action: "stop", ID: theirs, RequestID: "theirs-s"}} {
		refused(req, "no job of the bound creator")
	}
	if r.job(theirs).State != store.JobActive {
		t.Fatal("other owner's job changed")
	}
	refused(ControlRequest{Action: "pause", ID: mine}, "stable request_id")
	// Create applies at once (owner, 2026-11-11: "T3 is a trusted source of
	// scheduling"), in the owner's private chat, and is listed; its replay
	// is the recorded answer.
	r.sent()
	created := ok(daily)
	if created.JobID == "" || created.DraftID != "" || !created.Changed || !strings.Contains(created.Reply, "Scheduled task created") {
		t.Fatal("create", created)
	}
	if j := r.job(created.JobID); j.State != store.JobActive || j.Creator.Key() != boss.Key() || j.CreatedIn != bossDM {
		t.Fatal("created job", j)
	}
	if out := r.sent(); len(out) != 1 || out[0].Conv != bossDM || !strings.Contains(out[0].Text, "Scheduled task created") {
		t.Fatal("create notice", out)
	}
	if again := do(daily); again != created {
		t.Fatal("create replay", again)
	}
	if got := ok(ControlRequest{Action: "list"}); !strings.Contains(got.Reply, "daily check") {
		t.Fatal("created job not listed", got)
	}
	// Edit applies at once to the owner's own job only.
	edited := ok(ControlRequest{Action: "edit", ID: mine, RequestID: "e1", Definition: json.RawMessage(`{"prompt":"changed"}`)})
	if d, _, _ := parse(r.job(mine)); !edited.Changed || d.Prompt != "changed" || r.job(mine).Version != 2 {
		t.Fatal("edit", edited, d)
	}
	refused(ControlRequest{Action: "edit", ID: theirs, RequestID: "e2", Definition: json.RawMessage(`{"prompt":"changed"}`)}, "no job of the bound creator")
	// Validation stays: a schedule without a future run is refused.
	refused(ControlRequest{Action: "create", RequestID: "past", Definition: json.RawMessage(`{"name":"past","schedule":{"at":"2020-01-01T09:00"},"prompt":"p"}`)}, "no future run")
	// rerun_on_timeout keeps its human confirmation: a draft and a card in
	// the owner's private chat, where a plain yes confirms it.
	rerun := ok(ControlRequest{Action: "create", RequestID: "rr", Definition: json.RawMessage(`{"name":"repeat-safe","schedule":{"every":"1h"},"prompt":"p","idempotent":true,"rerun_on_timeout":true,"timeout":"1m"}`)})
	if rerun.DraftID == "" || r.job(rerun.DraftID).State != store.JobDraft {
		t.Fatal("rerun_on_timeout applied without confirmation", rerun)
	}
	r.cmd(asBoss, "job discard "+rerun.DraftID)
	// Never remove, checkpoint, acknowledge, confirm or discover.
	for _, req := range []ControlRequest{
		{Action: "remove", ID: mine, RequestID: "x1"},
		{Action: "checkpoint", ID: mine},
		{Action: "acknowledge", ID: mine, RequestID: "a1", Key: "k", Revision: "v", Outbound: 1},
		{Action: "confirm", ID: mine, RequestID: "c1"},
		{Action: "discover"},
	} {
		refused(req, "may only task_add, task_list and task_show; with manage: list, show, history, create, edit, run, pause, resume, stop of the owner's jobs and task_pause, task_resume, task_close (never remove, checkpoint or acknowledge)")
	}
	if jobs, _ := r.st.Jobs(store.JobFilter{States: []string{store.JobActive}}); len(jobs) != 3 || r.job(mine).State != store.JobActive {
		t.Fatal("management removed a job", jobs)
	}

	// Tasks: the creator's own pause, resume and close; close is one event
	// per request_id and its replay is the recorded answer.
	tp := ok(ControlRequest{Action: "task_pause", ID: "MEL-755", RequestID: "tp-1"})
	if !tp.Changed || !strings.Contains(tp.Reply, "paused by owner") {
		t.Fatal("task_pause", tp)
	}
	if again := do(ControlRequest{Action: "task_pause", ID: "MEL-755", RequestID: "tp-1"}); again != tp {
		t.Fatal("task_pause replay", again)
	}
	if got := ok(ControlRequest{Action: "task_resume", ID: "MEL-755", RequestID: "tr-1"}); !got.Changed || got.Reply != "MEL-755 resumed." {
		t.Fatal("task_resume", got)
	}
	refused(ControlRequest{Action: "task_close", ID: "MEL-755"}, "stable request_id")
	refused(ControlRequest{Action: "task_close", ID: "MEL-755", Title: "done", RequestID: "tc-x"}, "takes only an issue id and request_id")
	closeReq := ControlRequest{Action: "task_close", ID: "MEL-755", RequestID: "tc-1"}
	tc := ok(closeReq)
	if !tc.Changed || tc.Reply != "Sent close to MEL-755." {
		t.Fatal("task_close", tc)
	}
	if again := do(closeReq); again != tc {
		t.Fatal("task_close replay", again)
	}
	status := func(issue string) string {
		vs, _ := work.Views()
		for _, v := range vs {
			if v.Issue == issue {
				return v.Status
			}
		}
		return ""
	}
	var notices []core.Outbound
	for end := time.Now().Add(20 * time.Second); status("MEL-755") != "SUCCESS"; time.Sleep(20 * time.Millisecond) {
		notices = append(notices, r.sent()...) // the closed notice's receipt ends it
		if time.Now().After(end) {
			t.Fatal("close did not end the task", status("MEL-755"))
		}
	}
	closedNotices := 0
	for _, o := range notices {
		if strings.Contains(o.Text, "MEL-755 closed (closed by "+boss.Key()+")") {
			closedNotices++
			if o.Conv != bossDM {
				t.Fatal("closed notice routed elsewhere", o)
			}
		}
	}
	if closedNotices != 1 {
		t.Fatal("closed notices", notices)
	}
	if got := do(ControlRequest{Action: "task_close", ID: "MEL-755", RequestID: "tc-2"}); got.Error != "" || status("MEL-755") != "SUCCESS" {
		t.Fatal("second close of a closed task", got)
	}
	// Another owner's task: not visible, never changed.
	for _, a := range []string{"task_pause", "task_close"} {
		if got := do(ControlRequest{Action: a, ID: "MEL-401", RequestID: "theirs-" + a}); got.Changed || !strings.Contains(got.Reply, "not tracked here") {
			t.Fatal(a, got)
		}
	}
	if s := status("MEL-401"); s == "SUCCESS" || s == "CANCELLED" {
		t.Fatal("other owner's task changed", s)
	}

	// Binding negatives stay: foreign UID, unknown registration origin,
	// employee and scheduled callers, stale lineage.
	if got := c.executePeer(ctx, &syscall.Ucred{Uid: 9999}, ControlRequest{Action: "pause", ID: mine, RequestID: "uid"}, who, valid); got.Error != "operator not authorized" {
		t.Fatal("foreign peer UID", got)
	}
	unknown := who
	unknown.Registered = "other registrar"
	if got := c.executePeer(ctx, op, ControlRequest{Action: "pause", ID: mine, RequestID: "unk"}, unknown, valid); !strings.Contains(got.Error, "may only task_add") {
		t.Fatal("unknown registration origin managed", got)
	}
	emp := who
	emp.Role = core.RoleEmployee
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_close", ID: "MEL-401", RequestID: "emp"}, emp, valid); !strings.Contains(got.Error, "origin required") {
		t.Fatal("employee managed", got)
	}
	sched := who
	sched.Job, sched.Run = mine, 1
	if got := c.task(1234, ControlRequest{Action: "task_close", ID: "MEL-401", RequestID: "sched"}, sched, nil); !strings.Contains(got.Error, "scheduled runs cannot") {
		t.Fatal("scheduled run managed a task", got)
	}
	if got := c.executePeer(ctx, op, ControlRequest{Action: "pause", ID: mine, RequestID: "sched-p"}, sched, valid); got.Error == "" {
		t.Fatal("scheduled origin managed a job", got)
	}
	// Employee and scheduled origins never create or edit.
	for _, bad := range []Caller{emp, sched, unknown} {
		for _, req := range []ControlRequest{
			{Action: "create", RequestID: "bad-c", Definition: daily.Definition},
			{Action: "edit", ID: mine, RequestID: "bad-e", Definition: json.RawMessage(`{"prompt":"bad"}`)},
		} {
			if got := c.executePeer(ctx, op, req, bad, valid); got.Error == "" || got.Changed {
				t.Fatal("non-owner origin changed a job", bad, req.Action, got)
			}
		}
	}
	if d, _, _ := parse(r.job(mine)); d.Prompt != "changed" {
		t.Fatal("refused edit changed the job", d)
	}
	f.set(func() { f.stat[t3Server] = procEntry{t3Server, t3Launcher, 21} }) // reused pid
	if got := do(ControlRequest{Action: "pause", ID: mine, RequestID: "stale"}); got.Error == "" || r.job(mine).State != store.JobActive {
		t.Fatal("stale lineage managed a job", got)
	}
}
