package scheduler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// fakeLauncher serves the external origin contract from this test process,
// so this process's direct children are "its" runs.
type fakeLauncher struct {
	socket string
	mu     sync.Mutex
	answer func(q externalQuery) externalAnswer
	asked  []externalQuery
}

func newFakeLauncher(t *testing.T, answer func(externalQuery) externalAnswer) *fakeLauncher {
	t.Helper()
	dir, err := os.MkdirTemp("", "hxo")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	f := &fakeLauncher{socket: filepath.Join(dir, "run-origin.sock"), answer: answer}
	l, err := net.Listen("unix", f.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	_ = os.Chmod(f.socket, 0o600)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				scan := bufio.NewScanner(conn)
				var q externalQuery
				if !scan.Scan() || strictJSON(scan.Bytes(), &q) != nil {
					return
				}
				f.mu.Lock()
				f.asked = append(f.asked, q)
				a := f.answer(q)
				f.mu.Unlock()
				raw, _ := json.Marshal(a)
				conn.Write(append(raw, '\n'))
			}()
		}
	}()
	return f
}

func (f *fakeLauncher) set(answer func(externalQuery) externalAnswer) {
	f.mu.Lock()
	f.answer = answer
	f.mu.Unlock()
}

func (f *fakeLauncher) origin() ExternalOrigin {
	return ExternalOrigin{Name: "agentnet", Socket: f.socket, UID: uint32(os.Getuid()), People: map[string]ExternalPerson{
		"person:sergey": {Principal: boss.Key()},
		"person:worker": {Principal: worker.Key()},
	}}
}

// vouch answers for exactly one run process (a direct child of this test).
func vouch(run *exec.Cmd, person, author string) func(externalQuery) externalAnswer {
	return func(q externalQuery) externalAnswer {
		_, start, err := procStat(run.Process.Pid)
		if q.V != 1 || q.PID != run.Process.Pid || err != nil || q.Start != start {
			return externalAnswer{V: 1, Error: "not a run of mine"}
		}
		return externalAnswer{V: 1, Run: "req-1", Person: person, Author: author}
	}
}

func externalRig(t *testing.T) (*rig, *Control, *fakeLauncher, *exec.Cmd) {
	t.Helper()
	r, c := controlRig(t)
	r.s.ResolveControl = func(int) (Caller, func() bool, error) {
		return Caller{}, nil, errors.New("no active person turn for scheduler peer")
	}
	run := exec.Command("sleep", "60") // the launcher's run: this test's direct child
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { run.Process.Kill(); run.Wait() })
	f := newFakeLauncher(t, vouch(run, "person:sergey", "human"))
	r.s.External = []ExternalOrigin{f.origin()}
	return r, c, f, run
}

var daily = ControlRequest{Action: "create", RequestID: "daily-1", Definition: json.RawMessage(`{"name":"daily check","schedule":{"cron":["0 9 * * *"]},"prompt":"check the thing","summary":"daily check"}`)}

func alwaysAlive() bool { return true }

func TestExternalOwnerRunCreatesScheduleAtOnce(t *testing.T) {
	r, c, f, run := externalRig(t)
	who, valid, err := c.resolvePeer(run.Process.Pid, alwaysAlive)
	if err != nil {
		t.Fatal(err)
	}
	if who.Principal.Key() != boss.Key() || who.Conv != bossDM || !who.Private || who.External != "agentnet req-1" {
		t.Fatal("wrong binding", who)
	}
	got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, daily, who, valid)
	if got.Error != "" || !got.Changed || got.DraftID != "" || got.JobID == "" || !strings.Contains(got.Reply, "created") {
		t.Fatal(got)
	}
	if !strings.Contains(got.Reply, "Your private Telegram conversation") || strings.Contains(got.Reply, "This private conversation") {
		t.Fatal("external receipt hides the actual delivery platform", got.Reply)
	}
	jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key()})
	if len(jobs) != 1 || jobs[0].State != store.JobActive || jobs[0].CreatedIn != bossDM || jobs[0].NextDue.IsZero() {
		t.Fatal("schedule not created active for the mapped owner", jobs)
	}
	if outs := r.sent(); len(outs) != 1 || outs[0].Conv != bossDM {
		t.Fatal("audit notice not in the owner's private chat", outs)
	}
	if again := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, daily, who, valid); again != got {
		t.Fatal("request_id replay changed result", again)
	}
	if jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key()}); len(jobs) != 1 {
		t.Fatal("replay created a second job")
	}
	if len(f.asked) < 2 {
		t.Fatal("launcher not asked again before commit", f.asked)
	}
	// Edits apply at once too; removal keeps human confirmation.
	edit := ControlRequest{Action: "edit", ID: jobs[0].ID, RequestID: "edit-1", Definition: json.RawMessage(`{"prompt":"check it twice"}`)}
	if got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, edit, who, valid); got.Error != "" || !strings.Contains(got.Reply, "updated") {
		t.Fatal(got)
	}
	if j := r.job(jobs[0].ID); j.Version != 2 || !strings.Contains(j.Def, "check it twice") {
		t.Fatal("edit not applied", j)
	}
	rm := ControlRequest{Action: "remove", ID: jobs[0].ID, RequestID: "rm-1"}
	if got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, rm, who, valid); got.Error != "" || got.DraftID == "" {
		t.Fatal("removal must stay a draft", got)
	}
	if r.job(jobs[0].ID).State != store.JobActive {
		t.Fatal("removal applied without human confirmation")
	}
	task := ControlRequest{Action: "task_add", ID: "MEL-1", Title: "t", RequestID: "task-1"}
	r.s.Tasks = func(Caller, string, string, string) (string, bool) { return "admitted", true }
	if got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, task, who, valid); !strings.Contains(got.Error, "live Heimdall chat turn") {
		t.Fatal("external origin admitted a durable task", got)
	}
}

func TestExternalOriginRefusals(t *testing.T) {
	ctx := context.Background()
	refused := func(t *testing.T, c *Control, pid int, want string) {
		t.Helper()
		_, _, err := c.resolvePeer(pid, alwaysAlive)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("want refusal %q, got %v", want, err)
		}
	}
	t.Run("not launched", func(t *testing.T) {
		_, c, _, _ := externalRig(t)
		os.Setenv("AGENTNET_REQUESTER", "person:sergey") // environment is never identity
		defer os.Unsetenv("AGENTNET_REQUESTER")
		refused(t, c, os.Getpid(), "no configured launcher started this process")
	})
	t.Run("not configured", func(t *testing.T) {
		r, c, _, run := externalRig(t)
		r.s.External = nil
		refused(t, c, run.Process.Pid, "no external origin is configured")
	})
	t.Run("detached from the run", func(t *testing.T) {
		_, c, _, _ := externalRig(t)
		out, err := exec.Command("sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!").Output()
		if err != nil {
			t.Fatal(err)
		}
		var orphan int
		json.Unmarshal([]byte(strings.TrimSpace(string(out))), &orphan)
		defer syscall.Kill(orphan, syscall.SIGKILL)
		refused(t, c, orphan, "no configured launcher started this process")
	})
	t.Run("launcher does not vouch", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		f.set(func(externalQuery) externalAnswer { return externalAnswer{V: 1, Error: "request finished"} })
		refused(t, c, run.Process.Pid, "agentnet does not vouch for this process: request finished")
	})
	t.Run("other person", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		f.set(vouch(run, "person:alice", "human"))
		refused(t, c, run.Process.Pid, `agentnet person "person:alice" has no Heimdall identity mapping`)
	})
	t.Run("agent authored", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		f.set(vouch(run, "person:sergey", "agent"))
		refused(t, c, run.Process.Pid, "was not requested by the person themself")
	})
	t.Run("mapped non-owner", func(t *testing.T) {
		r, c, f, run := externalRig(t)
		if _, _, err := r.st.AddInbound(core.Inbound{Conv: workDM, From: worker, Origin: core.OriginLive, Private: true, MsgID: "w"}); err != nil {
			t.Fatal(err)
		}
		f.set(vouch(run, "person:worker", "human"))
		refused(t, c, run.Process.Pid, `maps to telegram:2, which is not a current owner`)
	})
	t.Run("revoked owner", func(t *testing.T) {
		r, c, _, run := externalRig(t)
		who, valid, err := c.resolvePeer(run.Process.Pid, alwaysAlive)
		if err != nil {
			t.Fatal(err)
		}
		r.s.access.Owners = map[string]bool{}
		if got := c.executePeer(ctx, &syscall.Ucred{Uid: 1234}, daily, who, valid); !strings.Contains(got.Error, "not a current owner") {
			t.Fatal(got)
		}
	})
	t.Run("run ended before commit", func(t *testing.T) {
		r, c, _, run := externalRig(t)
		who, valid, err := c.resolvePeer(run.Process.Pid, alwaysAlive)
		if err != nil {
			t.Fatal(err)
		}
		run.Process.Kill()
		run.Wait()
		if got := c.executePeer(ctx, &syscall.Ucred{Uid: 1234}, daily, who, valid); got.Error == "" {
			t.Fatal("ended run admitted", got)
		}
		if jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key()}); len(jobs) != 0 {
			t.Fatal("job created for an ended run")
		}
	})
	t.Run("answer changes before commit", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		who, valid, err := c.resolvePeer(run.Process.Pid, alwaysAlive)
		if err != nil {
			t.Fatal(err)
		}
		f.set(vouch(run, "person:worker", "human"))
		if got := c.executePeer(ctx, &syscall.Ucred{Uid: 1234}, daily, who, valid); got.Error == "" {
			t.Fatal("changed vouching admitted", got)
		}
	})
	t.Run("pid reuse", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		real := procStat
		defer func() { procStat = real }()
		var answered atomic.Bool
		honest := vouch(run, "person:sergey", "human")
		f.set(func(q externalQuery) externalAnswer { a := honest(q); answered.Store(true); return a })
		procStat = func(pid int) (int, uint64, error) {
			ppid, start, err := real(pid)
			if pid == run.Process.Pid && answered.Load() {
				start++ // the same PID, another process, after the answer
			}
			return ppid, start, err
		}
		refused(t, c, run.Process.Pid, "run ended during verification")
	})
	t.Run("unsafe socket directory", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		os.Chmod(filepath.Dir(f.socket), 0o777)
		defer os.Chmod(filepath.Dir(f.socket), 0o700)
		refused(t, c, run.Process.Pid, "origin socket unusable")
	})
	t.Run("socket served by another user", func(t *testing.T) {
		r, c, _, run := externalRig(t)
		r.s.External[0].UID = uint32(os.Getuid()) + 1
		refused(t, c, run.Process.Pid, "origin socket unusable")
	})
	t.Run("heimdall turn stays authoritative", func(t *testing.T) {
		r, c, _, run := externalRig(t)
		r.s.ResolveControl = func(int) (Caller, func() bool, error) { return asBoss, func() bool { return false }, nil }
		refused(t, c, run.Process.Pid, "no active origin")
	})
}

// TestExternalOriginHelper is the "run" process of the end-to-end test: it
// calls the real control socket the way a harness's MCP adapter does.
func TestExternalOriginHelper(t *testing.T) {
	socket := os.Getenv("HEIMDALL_TEST_CONTROL_SOCKET")
	if socket == "" {
		t.Skip("helper process only")
	}
	got, _ := CallControl(context.Background(), socket, daily)
	raw, _ := json.Marshal(got)
	os.Stdout.Write(append(append([]byte("RESULT "), raw...), '\n'))
}

func TestExternalOriginEndToEndOverControlSocket(t *testing.T) {
	r, _ := controlRig(t)
	r.s.ResolveControl = func(int) (Caller, func() bool, error) {
		return Caller{}, nil, errors.New("no active person turn for scheduler peer")
	}
	dir, err := os.MkdirTemp("", "hxc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := r.s.ListenControl(ctx, boss.Key(), uint32(os.Getuid()), filepath.Join(dir, "ctl"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	helper := func(author string) ControlResult {
		run := exec.Command(os.Args[0], "-test.run=^TestExternalOriginHelper$")
		run.Env = append(os.Environ(), "HEIMDALL_TEST_CONTROL_SOCKET="+filepath.Join(dir, "ctl", "control.sock"))
		var out strings.Builder
		run.Stdout = &out
		started := make(chan struct{})
		f := newFakeLauncher(t, func(q externalQuery) externalAnswer {
			<-started
			return vouch(run, "person:sergey", author)(q)
		})
		r.s.External = []ExternalOrigin{f.origin()}
		if err := run.Start(); err != nil {
			t.Fatal(err)
		}
		close(started)
		done := make(chan error, 1)
		go func() { done <- run.Wait() }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			run.Process.Kill()
			t.Fatal("helper timed out")
		}
		_, line, ok := strings.Cut(out.String(), "RESULT ")
		var got ControlResult
		if !ok || json.Unmarshal([]byte(strings.SplitN(line, "\n", 2)[0]), &got) != nil {
			t.Fatal("helper output", out.String())
		}
		return got
	}
	if got := helper("agent"); !strings.Contains(got.Error, "trusted active request origin unavailable: agentnet run req-1 was not requested by the person themself") {
		t.Fatal("agent-authored run admitted", got)
	}
	got := helper("human")
	if got.Error != "" || !strings.Contains(got.Reply, "created") {
		t.Fatal(got)
	}
	jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key()})
	if len(jobs) != 1 || jobs[0].State != store.JobActive {
		t.Fatal("schedule not created", jobs)
	}
}
