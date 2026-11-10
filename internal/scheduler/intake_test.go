package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// fakeT3 is a T3 CLI whose `thread show --json` prints, after a
// multi-line log block like the real v0.1.5 CLI's, the state the test
// sets (optionally slowly).
type fakeT3 struct {
	t     *testing.T
	dir   string
	state string
	by    string // the thread's createdBy mark; "" leaves it out
}

func newFakeT3(t *testing.T) *fakeT3 {
	dir := t.TempDir()
	f := &fakeT3{t: t, dir: dir, state: filepath.Join(dir, "state.json"), by: "user"}
	script := `#!/bin/sh
[ -f "` + dir + `/delay" ] && sleep "$(cat "` + dir + `/delay")"
printf '%s\n' "$@" >> "` + dir + `/argv"
printf '[13:04:35.205] WARN (#20): Database migration history diverges {\n  divergent: [\n    x\n  ]\n}\n'
cat "` + f.state + `"
`
	if err := os.WriteFile(filepath.Join(dir, "t3"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return f
}

type msg struct {
	Role      string `json:"role"`
	CreatedBy string `json:"createdBy,omitempty"`
	RunID     string `json:"runId"`
	Text      string `json:"text"`
}

func (f *fakeT3) set(status, latest, active string, msgs ...msg) {
	f.t.Helper()
	th := map[string]any{"threadId": "thread-A", "title": "Supplier work", "status": status, "latestRunId": latest, "activeRunId": nil, "runCount": len(msgs)}
	if f.by != "" {
		th["createdBy"] = f.by
	}
	if active != "" {
		th["activeRunId"] = active
	}
	b, _ := json.Marshal(map[string]any{"thread": th, "messages": msgs})
	if err := os.WriteFile(f.state+".tmp", b, 0o600); err != nil {
		f.t.Fatal(err)
	}
	os.Rename(f.state+".tmp", f.state)
}

func (f *fakeT3) delay(s string) {
	if s == "" {
		os.Remove(filepath.Join(f.dir, "delay"))
		return
	}
	os.WriteFile(filepath.Join(f.dir, "delay"), []byte(s), 0o600)
}

// Item 9: every owner request made in a T3 session lands in the durable
// queue through the session's hooks, with no model involved: the trusted
// T3 lineage binds the owner, T3's own createdBy=user mark is the owner
// check, read through the configured CLI after the hook has its answer.
// One prompt gives one tracking task (T3-<n>), a repeat gives none, an
// agent's prompt gives none. T3 v0.1.5 marks only the thread: an
// owner-created thread's latest message gives one tracking task that
// closes quietly when its run completes and, when its run is interrupted,
// becomes an active task whose brief says the attribution is the
// thread's only; an agent-created or unmarked thread gives none and one
// log line; message marks still win over the thread's. An untrusted caller is
// refused, the stop hook closes it quietly, a newer prompt supersedes an
// older one, and a run that ends interrupted becomes an active task that
// the owner's assistant continues. Nothing reaches the owner's chat
// unless the owner must act.
func TestT3IntakeEndToEnd(t *testing.T) {
	defer durable.SetIntakePause(20 * time.Millisecond)()
	r, c, _, _ := t3Rig(t)
	ctx := context.Background()
	cli := newFakeT3(t)
	logs := &syncBuf{}
	a := &planWake{}
	work, err := durable.Open(durable.Options{Path: filepath.Join(t.TempDir(), "durable.db"), Outbox: r.st, Assistant: a, Silence: -1,
		Observer: durable.T3{Command: []string{filepath.Join(cli.dir, "t3")}}, Observe: 30 * time.Millisecond, Log: log.New(logs, "", 0)})
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
	op := &syscall.Ucred{Uid: 1234}
	who, valid, err := c.resolvePeer(t3Peer, alwaysAlive)
	if err != nil || who.Registered != trustedT3 {
		t.Fatal(who, err)
	}
	prompt := ControlRequest{Action: "task_intake", ID: "thread-A", RequestID: "prompt_submit"}
	stop := ControlRequest{Action: "task_intake_done", ID: "thread-A", RequestID: "stop"}
	call := func(req ControlRequest) ControlResult {
		t.Helper()
		got := c.executePeer(ctx, op, req, who, valid)
		if got.Error != "" {
			t.Fatalf("%s: %+v", req.Action, got)
		}
		return got
	}
	var chat []core.Outbound
	phase := func(issue, want string) durable.State {
		t.Helper()
		var st durable.State
		end := time.Now().Add(20 * time.Second)
		for time.Now().Before(end) {
			chat = append(chat, r.sent()...) // deliver anything queued, as the interface would
			if st, err = work.State(issue); err == nil && st.Phase == want {
				return st
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s never %s: %+v %v\n%s", issue, want, st, err, logs.String())
		return st
	}
	list := func() string {
		reply, _ := work.Task(durable.Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Live: true, Private: true}, "list", "", "", "")
		return reply
	}
	owner1 := msg{Role: "user", CreatedBy: "user", RunID: "run-1", Text: "Confirm the supplier PO\nand tell me"}

	// One prompt: one tracking task, nothing woken or sent.
	cli.set("running", "run-1", "run-1", owner1)
	call(prompt)
	st := phase("T3-1", durable.PhaseTracking)
	if !strings.Contains(st.Next, "run-1") {
		t.Fatalf("%+v", st)
	}
	show, _ := work.Task(durable.Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Live: true, Private: true}, "show", "T3-1", "", "")
	if !strings.Contains(show, "T3-1 Confirm the supplier PO and tell me") || !strings.Contains(show, "Linear: none (T3 thread thread-A, run run-1)") {
		t.Fatal(show)
	}
	// A repeat of the same request gives none.
	call(prompt)
	time.Sleep(700 * time.Millisecond)
	if _, err := work.State("T3-2"); err == nil {
		t.Fatal("repeat admitted a second task")
	}
	// The stop hook: the run completed, the task closes quietly and leaves
	// the list.
	cli.set("completed", "run-1", "", owner1)
	call(stop)
	st = phase("T3-1", durable.PhaseClosed)
	if !strings.Contains(st.Verdict, "finished in T3 (completed)") {
		t.Fatalf("%+v", st)
	}
	if l := list(); strings.Contains(l, "T3-1") {
		t.Fatal("closed intake listed:", l)
	}

	// A newer owner prompt supersedes an older tracking one.
	owner2 := msg{Role: "user", CreatedBy: "user", RunID: "run-2", Text: "Draft the invoice"}
	owner3 := msg{Role: "user", CreatedBy: "user", RunID: "run-3", Text: "Actually send it to Bob"}
	cli.set("running", "run-2", "run-2", owner1, owner2)
	call(prompt)
	phase("T3-2", durable.PhaseTracking)
	// T3 shows run-3 active while run-2 is still the latest: T3-2's own
	// watch cannot close it ("a later run followed" would race the
	// supersede), so only the newer request's supersede can.
	cli.set("running", "run-2", "run-3", owner1, owner2, owner3)
	call(prompt)
	phase("T3-3", durable.PhaseTracking)
	if st := phase("T3-2", durable.PhaseClosed); !strings.Contains(st.Verdict, "superseded") {
		t.Fatalf("%+v", st)
	}

	// The run ends interrupted (a restart of the host): the request
	// becomes an active task the owner's assistant continues.
	cli.set("interrupted", "run-3", "", owner1, owner2, owner3)
	phase("T3-3", durable.PhaseReported)
	q := a.first(t)
	for _, want := range []string{"T3-3", "Actually send it to Bob", "T3 thread thread-A", "ended interrupted", "do not repeat what was done", "Registered through trusted T3"} {
		if !strings.Contains(q.Prompt, want) {
			t.Fatalf("wake prompt lacks %q: %s", want, q.Prompt)
		}
	}
	if q.Owner.Key() != boss.Key() || q.Route != bossDM {
		t.Fatalf("%+v", q)
	}

	// An agent's prompt (not createdBy=user) is not an owner request.
	agent := msg{Role: "user", CreatedBy: "agent", RunID: "run-4", Text: "delegated work"}
	cli.set("running", "run-4", "run-4", owner1, owner2, owner3, agent)
	call(prompt)
	time.Sleep(400 * time.Millisecond)
	if _, err := work.State("T3-4"); err == nil {
		t.Fatal("agent prompt admitted")
	}
	// T3 v0.1.5: messages carry no createdBy, only the thread does. An
	// owner-created thread's latest user message gives exactly one T3-<n>;
	// a completed run closes it quietly, no wake.
	weak := msg{Role: "user", RunID: "run-5", Text: "Order more sheets"}
	cli.set("running", "run-5", "run-5", msg{Role: "user", RunID: "run-0", Text: "older"}, weak)
	call(prompt)
	phase("T3-4", durable.PhaseTracking)
	call(prompt)
	time.Sleep(700 * time.Millisecond)
	if _, err := work.State("T3-5"); err == nil {
		t.Fatal("repeat of a thread-marked request admitted a second task")
	}
	cli.set("completed", "run-5", "", weak)
	call(stop)
	if st := phase("T3-4", durable.PhaseClosed); !strings.Contains(st.Verdict, "finished in T3 (completed)") {
		t.Fatalf("%+v", st)
	}
	// An interrupted run of a thread-marked request becomes an active task
	// like a marked one; its brief says who wrote it is the thread's mark.
	weak2 := msg{Role: "user", RunID: "run-5b", Text: "Reorder the towels"}
	cli.set("running", "run-5b", "run-5b", weak, weak2)
	call(prompt)
	phase("T3-5", durable.PhaseTracking)
	cli.set("interrupted", "run-5b", "", weak, weak2)
	call(stop)
	phase("T3-5", durable.PhaseReported)
	q = a.nth(t, 1, 2)
	for _, want := range []string{"T3-5", "Reorder the towels", "T3 thread thread-A", "ended interrupted", "do not repeat what was done",
		"T3 does not mark who wrote this message; it is the latest request in a thread the owner created. Read the thread first and confirm what was asked before acting."} {
		if !strings.Contains(q.Prompt, want) {
			t.Fatalf("wake prompt lacks %q: %s", want, q.Prompt)
		}
	}
	if q.Owner.Key() != boss.Key() || q.Route != bossDM {
		t.Fatalf("%+v", q)
	}
	// An agent-created thread, then a thread with no createdBy anywhere:
	// none, and one metadata-only log line each.
	refused := func(n int) {
		t.Helper()
		end := time.Now().Add(10 * time.Second)
		for strings.Count(logs.String(), "T3 intake refused (created_by), thread thread-A") < n && time.Now().Before(end) {
			time.Sleep(20 * time.Millisecond)
		}
		if got := strings.Count(logs.String(), "T3 intake refused"); got != n {
			t.Fatalf("refusal lines %d, want %d: %q", got, n, logs.String())
		}
	}
	cli.by = "agent"
	cli.set("running", "run-6", "run-6", msg{Role: "user", RunID: "run-6", Text: "SECRET-AGENT-TEXT"})
	call(prompt)
	refused(1)
	cli.by = ""
	cli.set("running", "run-7", "run-7", msg{Role: "user", RunID: "run-7", Text: "SECRET-REQUEST-TEXT"})
	call(prompt)
	refused(2)
	if strings.Contains(logs.String(), "SECRET-") {
		t.Fatalf("logs %q", logs.String())
	}
	if _, err := work.State("T3-6"); err == nil {
		t.Fatal("agent-created or unmarked thread admitted")
	}

	// A slow CLI never holds the hook: the answer is immediate, the task
	// comes after. Message marks win over the thread's: the owner's marked
	// message in an agent-created thread is admitted.
	cli.by = "agent"
	cli.delay("2")
	owner6 := msg{Role: "user", CreatedBy: "user", RunID: "run-8", Text: "Check the stock"}
	cli.set("running", "run-8", "run-8", owner1, owner6)
	began := time.Now()
	call(prompt)
	if d := time.Since(began); d > time.Second {
		t.Fatalf("control answer took %s", d)
	}
	cli.delay("")
	phase("T3-6", durable.PhaseTracking)

	// A delegated thread is skipped; untrusted callers are refused.
	if got := call(ControlRequest{Action: "task_intake", ID: "mcp:launch:abc", RequestID: "prompt_submit"}); !strings.Contains(got.Reply, "skipped") {
		t.Fatal(got)
	}
	live := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true}
	if got := c.executeLocked(ctx, 1234, prompt, []Caller{live}); !strings.Contains(got.Error, "only for trusted T3") {
		t.Fatal("live turn used the hook action", got)
	}
	if got := c.executeLocked(ctx, 1234, stop, []Caller{{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true, External: "agentnet"}}); !strings.Contains(got.Error, "only for trusted T3") {
		t.Fatal("external launcher used the hook action", got)
	}
	if got := c.executePeer(ctx, &syscall.Ucred{Uid: 9999}, prompt, who, valid); got.Error != "operator not authorized" {
		t.Fatal("foreign uid", got)
	}
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_intake", ID: "thread A", RequestID: "p"}, who, valid); got.Error == "" {
		t.Fatal("bad thread id", got)
	}
	// The event is an allowlisted word, never caller text in the log.
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_intake", ID: "thread-A", RequestID: "Email the PO"}, who, valid); !strings.Contains(got.Error, "prompt_submit") || strings.Contains(logs.String(), "Email the PO") {
		t.Fatal("free-text event", got)
	}
	// Owner chat: only the active tasks' unaccepted reports (the owner must
	// verify them); intake tracking, closes and supersedes sent nothing.
	time.Sleep(300 * time.Millisecond)
	chat = append(chat, r.sent()...)
	var texts []string
	for _, o := range chat {
		texts = append(texts, o.Text)
	}
	if len(texts) != 2 || !strings.Contains(texts[0], "T3-3 attempt 1: UNACCEPTED worker report") || !strings.Contains(texts[1], "T3-5 attempt 1: UNACCEPTED worker report") {
		t.Fatalf("owner chat %q", texts)
	}
}

// planWake records wakes and reports at once.
type planWake struct {
	mu   sync.Mutex
	reqs []durable.WorkRequest
}

func (a *planWake) Work(_ context.Context, q durable.WorkRequest) (durable.WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reqs = append(a.reqs, q)
	return durable.WorkResult{Turn: 1, Report: "sent to Bob once; checked the thread first"}, nil
}

func (a *planWake) first(t *testing.T) durable.WorkRequest { t.Helper(); return a.nth(t, 0, 1) }

// nth returns wake i when exactly n wakes were recorded.
func (a *planWake) nth(t *testing.T, i, n int) durable.WorkRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.reqs) != n {
		t.Fatalf("wakes %d, want %d", len(a.reqs), n)
	}
	return a.reqs[i]
}

// The hook script prints nothing, always exits 0, gives up after 3 s, and
// sends only the action, the thread pointer and the event.
func TestT3IntakeHookScript(t *testing.T) {
	script, err := filepath.Abs("../../deploy/t3-intake/heimdall-t3-intake.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "heimdall")
	os.WriteFile(fake, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> "+dir+"/argv\ncat >> "+dir+"/stdin\n[ -f "+dir+"/slow ] && sleep 10\necho '{\"reply\":\"noise\"}'\nexit 1\n"), 0o700)
	run := func(thread, event string) (string, time.Duration) {
		t.Helper()
		cmd := exec.Command(script, fake, "/run/heimdall/control.sock", event)
		cmd.Env = append(os.Environ(), "T3CODE_THREAD_ID="+thread)
		cmd.Stdin = strings.NewReader(`{"hook_event_name":"UserPromptSubmit","prompt":"secret"}`)
		began := time.Now()
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("exit: %v %s", err, out)
		}
		return string(out), time.Since(began)
	}
	if out, _ := run("thread-A", "prompt"); out != "" {
		t.Fatalf("printed %q", out)
	}
	run("thread-A", "stop")
	for _, skip := range [][2]string{{"mcp:launch:abc", "prompt"}, {"", "prompt"}, {"bad\"id", "prompt"}, {"thread-A", "other"}} {
		run(skip[0], skip[1])
	}
	got, _ := os.ReadFile(filepath.Join(dir, "stdin"))
	want := `{"action":"task_intake","id":"thread-A","request_id":"prompt_submit"}` + "\n" + `{"action":"task_intake_done","id":"thread-A","request_id":"stop"}` + "\n"
	if string(got) != want {
		t.Fatalf("requests %q", got)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
	if strings.Count(string(argv), "sched control -socket /run/heimdall/control.sock") != 2 {
		t.Fatalf("argv %q", argv)
	}
	os.WriteFile(filepath.Join(dir, "slow"), nil, 0o600)
	if _, d := run("thread-A", "prompt"); d > 3*time.Second {
		t.Fatalf("hook took %s", d)
	}
}
