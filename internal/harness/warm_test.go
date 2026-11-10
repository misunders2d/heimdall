package harness_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

func TestWarmMCPCallbackUsesCurrentRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, state := warmSpec(t, "pi")
	s.Caller.Role = core.RoleOwner
	s.Caller.Private = true
	s.MCPNotify = func(string) {}
	s.Prompt = "MCP_LOGIN_EXPIRE"
	p := harness.NewPool(harness.WarmConfig{})
	first := pooledTurn(t, p, ctx, s)
	firstResult := first.Result()
	s.Session = harness.Session{ID: firstResult.SessionID, Resume: true}
	s.Prompt = "MCP_LOGIN"
	second, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-second.Exited() }()
	if second == first || second.Process() != first.Process() {
		t.Fatal("login did not reuse the first process with a new Run")
	}
	root := first.Process() // Router keeps only this root in r.runs.
	wrong := "http://localhost:18765/callback?state=turn-1&code=synthetic"
	callback := "http://localhost:18765/callback?state=turn-2&code=synthetic"
	deadline := time.Now().Add(5 * time.Second)
	for {
		if root.RespondMCP(ctx, s.Caller.Principal, s.Caller.Conv, wrong) {
			t.Fatal("stale state accepted")
		}
		if root.RespondMCP(ctx, s.Caller.Principal, s.Caller.Conv, callback) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("matching callback refused on reused process")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-second.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("second login never received its native UI response")
	}
	if res := second.Result(); res.Failure != "" || res.Answer == "" {
		t.Fatalf("second login failed: %+v", res)
	}
	if root.RespondMCP(ctx, s.Caller.Principal, s.Caller.Conv, callback) {
		t.Fatal("callback replayed")
	}
	if first.Result().Answer != firstResult.Answer {
		t.Fatal("stale first turn changed")
	}
	data, err := os.ReadFile(filepath.Join(state, "pids"))
	if err != nil || len(strings.Fields(string(data))) != 1 {
		t.Fatalf("expected one warm process: %s, %v", data, err)
	}
}

func warmSpec(t *testing.T, name string) (harness.Spec, string) {
	bin, state := fakeharness.Install(t, name)
	l := launch(t, bin, name, true)
	l.Env = append(l.Env, "FAKE_WARM=1")
	return harness.Spec{Harness: name, Prompt: "hello", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: protocolCaller}, state
}
func pooledTurn(t *testing.T, p *harness.Pool, ctx context.Context, s harness.Spec) *harness.Run {
	t.Helper()
	r, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("turn timeout")
	}
	if res := r.Result(); res.Failure != "" || res.Answer == "" {
		t.Fatalf("result: %+v", res)
	}
	r.Warm()
	return r
}
func TestWarmReuseAndLatency(t *testing.T) {
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			p := harness.NewPool(harness.WarmConfig{})
			if name == "claude" {
				s.Launch.Env = append(s.Launch.Env, "FAKE_STARTUP_MS=3000")
			}
			start := time.Now()
			r := pooledTurn(t, p, ctx, s)
			first := time.Since(start)
			s.Session = harness.Session{ID: r.Result().SessionID, Resume: true}
			s.Prompt = "second"
			start = time.Now()
			r2 := pooledTurn(t, p, ctx, s)
			second := time.Since(start)
			b, _ := os.ReadFile(filepath.Join(state, "pids"))
			if len(strings.Fields(string(b))) != 1 || !strings.Contains(r2.Result().Answer, "turn=2") {
				t.Fatalf("not reused: %s %+v", b, r2.Result())
			}
			if name == "claude" {
				t.Logf("startup 3s: first %s second %s", first, second)
				if first < 3*time.Second || second > time.Second {
					t.Fatal("startup paid twice")
				}
			}
			cancel()
			<-r2.Exited()
		})
	}
}
func TestWarmFingerprintAndIsolation(t *testing.T) {
	for _, change := range []string{"env", "role", "private", "caller", "conversation", "session", "directory", "slack"} {
		t.Run(change, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, "claude")
			p := harness.NewPool(harness.WarmConfig{})
			r := pooledTurn(t, p, ctx, s)
			s.Session = harness.Session{ID: r.Result().SessionID, Resume: true}
			switch change {
			case "env":
				s.Launch.Env = append(s.Launch.Env, "MODEL=changed")
			case "role":
				s.Caller.Role = "employee"
			case "private":
				s.Caller.Private = true
			case "caller":
				s.Caller.Principal.ID = "43"
			case "conversation":
				s.Caller.Conv.Thread = "other"
			case "session":
				s.Session = harness.Session{ID: harness.NewSessionID("claude")}
			case "directory":
				s.Launch.Dir = t.TempDir()
			case "slack":
				s.OwnerSlack = &harness.OwnerSlack{}
			}
			pooledTurn(t, p, ctx, s)
			b, _ := os.ReadFile(filepath.Join(state, "pids"))
			if len(strings.Fields(string(b))) != 2 {
				t.Fatalf("launch reused after %s: %s", change, b)
			}
			cancel()
		})
	}
}
func TestWarmExpiryLRUResetAndStatus(t *testing.T) {
	for _, why := range []string{"idle", "lifetime", "lru", "reset"} {
		t.Run(why, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, _ := warmSpec(t, "pi")
			cfg := harness.WarmConfig{MaxWarm: 1}
			if why == "idle" {
				cfg.IdleTimeoutSeconds = 1
			}
			if why == "lifetime" {
				cfg.MaxLifetimeSeconds = 1
			}
			p := harness.NewPool(cfg)
			r := pooledTurn(t, p, ctx, s)
			status := p.Status(s.Caller.Conv)
			if len(status) != 1 || status[0].Harness != "pi" || status[0].Idle > time.Second {
				t.Fatalf("status %+v", status)
			}
			if why == "reset" {
				p.Invalidate(s.Caller.Conv, s.Caller.Principal.Key())
			}
			if why == "lru" {
				s.Caller.Conv.Thread = "other"
				s.Session.ID = harness.NewSessionID("pi")
				pooledTurn(t, p, ctx, s)
			}
			select {
			case <-r.Exited():
			case <-time.After(3 * time.Second):
				t.Fatal("expiry failed")
			}
		})
	}
}
func TestWarmIdleCrashResumesSilently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, state := warmSpec(t, "claude")
	p := harness.NewPool(harness.WarmConfig{})
	s.Prompt = "EXIT_IDLE"
	r := pooledTurn(t, p, ctx, s)
	<-r.Exited()
	s.Prompt = "after crash"
	s.Session = harness.Session{ID: r.Result().SessionID, Resume: true}
	pooledTurn(t, p, ctx, s)
	b, _ := os.ReadFile(filepath.Join(state, "pids"))
	if len(strings.Fields(string(b))) != 2 {
		t.Fatal("no replacement")
	}
}
func TestWarmInterruptAndRunTimeout(t *testing.T) {
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			s.Prompt = "HANG"
			p := harness.NewPool(harness.WarmConfig{RunTimeoutSeconds: 1})
			r, err := p.Start(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-r.Exited():
			case <-time.After(8 * time.Second):
				t.Fatal("timeout failed")
			}
			if !r.Result().Cancelled || r.Result().Cause != harness.CauseTimeout {
				t.Fatal("not cancelled")
			}
			if _, err := os.Stat(filepath.Join(state, "interrupt")); err != nil {
				t.Fatal("native interrupt missing", err)
			}
		})
	}
}

func TestWarmLRURefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := warmSpec(t, "pi")
	p := harness.NewPool(harness.WarmConfig{MaxWarm: 2})
	first := pooledTurn(t, p, ctx, s)
	firstSession := harness.Session{ID: first.Result().SessionID, Resume: true}
	s.Caller.Conv.Thread = "second"
	s.Session.ID = harness.NewSessionID("pi")
	second := pooledTurn(t, p, ctx, s)
	s.Caller.Conv.Thread = ""
	s.Session = firstSession
	fresh := pooledTurn(t, p, ctx, s)
	s.Caller.Conv.Thread = "third"
	s.Session = harness.Session{ID: harness.NewSessionID("pi")}
	pooledTurn(t, p, ctx, s)
	select {
	case <-second.Exited():
	case <-time.After(2 * time.Second):
		t.Fatal("LRU did not evict oldest")
	}
	select {
	case <-fresh.Exited():
		t.Fatal("recent worker evicted")
	default:
	}
}
func TestWarmInvalidationDuringTurnCannotRepool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := warmSpec(t, "claude")
	s.Launch.Env = append(s.Launch.Env, "FAKE_STARTUP_MS=100")
	p := harness.NewPool(harness.WarmConfig{})
	r, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	p.Invalidate(s.Caller.Conv, "")
	<-r.Done()
	if r.Warm() {
		t.Fatal("invalidated active worker retained")
	}
	select {
	case <-r.Exited():
	case <-time.After(2 * time.Second):
		t.Fatal("invalidated process alive")
	}
}

func TestWarmAgyRemainsPerTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bin, _ := fakeharness.Install(t, "agy")
	s := harness.Spec{Harness: "agy", Prompt: "hello", Launch: launch(t, bin, "agy", true), Caller: protocolCaller}
	p := harness.NewPool(harness.WarmConfig{})
	first, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if r := first.Result(); r.Failure != "" || r.Answer == "" {
		t.Fatalf("first %+v", r)
	}
	if first.Warm() {
		t.Fatal("AGY retained")
	}
	<-first.Exited()
	s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
	second, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if r := second.Result(); r.Failure != "" || !strings.HasPrefix(r.Answer, "agy#2 resumed=true") {
		t.Fatalf("second %+v", r)
	}
	if first.Process() == second.Process() {
		t.Fatal("AGY reused process")
	}
	<-second.Exited()
}

func TestOneShotIgnoresWarmLifetime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := warmSpec(t, "pi")
	s.OneShot = true
	s.Prompt = "HANG"
	s.Timeout = 1500 * time.Millisecond
	p := harness.NewPool(harness.WarmConfig{MaxLifetimeSeconds: 1})
	r, err := p.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	<-r.Exited()
	if got := r.Result(); got.Cause != harness.CauseTimeout {
		t.Fatalf("%+v", got)
	}
}

func TestWarmRequiresLifetimeHeadroom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, state := warmSpec(t, "pi")
	p := harness.NewPool(harness.WarmConfig{MaxLifetimeSeconds: 10, RunTimeoutSeconds: 20})
	first := pooledTurn(t, p, ctx, s)
	s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
	second := pooledTurn(t, p, ctx, s)
	second.Cancel()
	<-second.Exited()
	data, _ := os.ReadFile(filepath.Join(state, "pids"))
	if len(strings.Fields(string(data))) != 2 {
		t.Fatal("reused worker without lifetime headroom")
	}
}

func TestUncleanLiveWorkerLeaseCannotColdResume(t *testing.T) {
	for _, retire := range []string{"invalidate", "lru", "exit"} {
		t.Run(retire, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, _ := warmSpec(t, "claude")
			s.Prompt = "BACKGROUND_NEVER"
			p := harness.NewPool(harness.WarmConfig{MaxWarm: 1})
			first := pooledTurn(t, p, ctx, s)
			oldID, lease := p.NativeSession(s.Caller.Conv, s.Caller.Principal.Key(), s.Harness)
			if lease != first || oldID == "" {
				t.Fatal("missing exact live-worker lease")
			}
			switch retire {
			case "invalidate":
				p.Invalidate(s.Caller.Conv, s.Caller.Principal.Key())
			case "lru":
				other := s
				other.Prompt = "other"
				other.Caller.Conv.Chat = "4343"
				other.Session = harness.Session{ID: harness.NewSessionID("claude")}
				pooledTurn(t, p, ctx, other)
			case "exit":
				first.Stop("synthetic exit between lookup and claim")
			}
			<-first.Exited() // retirement consumes background bookkeeping before claim
			s.Prompt = "after lease lost"
			s.Session = harness.Session{ID: oldID, Resume: true}
			s.LiveWorkerOnly = lease
			next := pooledTurn(t, p, ctx, s)
			if next.Result().SessionID == oldID {
				t.Fatalf("cold launch resumed unclean session after %s: %q", retire, oldID)
			}
			cancel()
			<-next.Exited()
		})
	}
}

func TestGenericAutonomousTurnGatesNewInput(t *testing.T) {
	for _, name := range []string{"pi", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			s.Prompt = "AUTONOMOUS"
			events := make(chan harness.Continuation, 16)
			s.Continuation = func(e harness.Continuation) bool { events <- e; return true }
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			select {
			case e := <-events:
				if e.Phase != "start" {
					t.Fatalf("unexpected event: %+v", e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("native autonomous start missing")
			}
			s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
			s.Prompt = "SECOND_PERSON_INPUT"
			result := make(chan *harness.Run, 1)
			failure := make(chan error, 1)
			go func() {
				r, err := p.Start(ctx, s)
				if err != nil {
					failure <- err
				} else {
					result <- r
				}
			}()
			select {
			case <-result:
				t.Fatal("new input handed over before passive result")
			case err := <-failure:
				t.Fatal(err)
			case <-time.After(100 * time.Millisecond):
			}
			recorded, _ := os.ReadFile(filepath.Join(state, "inputs.jsonl"))
			if strings.Contains(string(recorded), "SECOND_PERSON_INPUT") {
				t.Fatal("person input reached unfinished native turn")
			}
			if err := os.WriteFile(filepath.Join(state, "release-autonomous"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case e := <-events:
				if e.Phase != "result" || e.Result.Answer != "AUTONOMOUS_ANSWER" {
					t.Fatalf("mixed passive result: %+v", e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("autonomous result missing")
			}
			var next *harness.Run
			select {
			case next = <-result:
			case err := <-failure:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("queued user input stayed blocked")
			}
			select {
			case <-next.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("person result missing")
			}
			if answer := next.Result().Answer; !strings.Contains(answer, "SECOND_PERSON_INPUT") || strings.Contains(answer, "AUTONOMOUS_ANSWER") {
				t.Fatalf("mixed person answer: %q", answer)
			}
			cancel()
			<-next.Exited()
		})
	}
}

func TestGenericWaitCancellationCannotLaunchPersonInput(t *testing.T) {
	for _, name := range []string{"pi", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			s.Prompt = "AUTONOMOUS"
			events := make(chan harness.Continuation, 16)
			s.Continuation = func(e harness.Continuation) bool { events <- e; return true }
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			<-events
			request, stopRequest := context.WithCancel(context.Background())
			s.StartContext = request
			s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
			s.Prompt = "MUST_NOT_START"
			finished := make(chan error, 1)
			go func() { _, err := p.Start(ctx, s); finished <- err }()
			deadline := time.Now().Add(5 * time.Second)
			for len(p.Status(s.Caller.Conv)) != 0 {
				if time.Now().After(deadline) {
					t.Fatal("pending request did not claim warm worker")
				}
				time.Sleep(time.Millisecond)
			}
			stopRequest()
			os.WriteFile(filepath.Join(state, "release-autonomous"), nil, 0600)
			select {
			case err := <-finished:
				if err != context.Canceled {
					t.Fatalf("cancelled request launched: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("request cancellation did not release warm wait")
			}
			recorded, _ := os.ReadFile(filepath.Join(state, "inputs.jsonl"))
			if strings.Contains(string(recorded), "MUST_NOT_START") {
				t.Fatal("cancelled input reached native stdin")
			}
			pids, _ := os.ReadFile(filepath.Join(state, "pids"))
			if len(strings.Fields(string(pids))) != 1 {
				t.Fatalf("cancelled request cold-launched a process: %s", pids)
			}
			cancel()
			<-first.Exited()
		})
	}
}

func TestFailedGenericWaitWithoutLeaseAlwaysStartsFresh(t *testing.T) {
	for _, name := range []string{"pi", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, _ := warmSpec(t, name)
			s.Prompt = "AUTONOMOUS"
			events := make(chan harness.Continuation, 16)
			s.Continuation = func(e harness.Continuation) bool { events <- e; return true }
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			<-events
			previous := first.Result().SessionID
			s.Session = harness.Session{ID: previous, Resume: true}
			s.Prompt = "AFTER_WAIT_LIMIT"
			s.Timeout = 200 * time.Millisecond
			s.LiveWorkerOnly = nil
			var logged []string
			var mu sync.Mutex
			s.Log = func(line string) { mu.Lock(); logged = append(logged, line); mu.Unlock() }
			next := pooledTurn(t, p, ctx, s)
			mu.Lock()
			reported := strings.Contains(strings.Join(logged, "\n"), "new "+name+" session: the native worker ended or changed")
			mu.Unlock()
			if !reported || strings.Contains(next.Result().Warning, "session") {
				t.Fatalf("fresh session logged %t, chat warning %q", reported, next.Result().Warning)
			}
			if next.Result().SessionID == previous {
				t.Fatal("failed no-lease reuse cold-resumed interrupted session")
			}
			cancel()
			<-next.Exited()
		})
	}
}

// A restricted OMP turn intentionally gets a new confined cwd/process while
// continuing only that employee's clean native session. Real lease loss and
// other scope/settings changes must retain the fresh-session fence.
func TestEmployeeOMPTurnDirectoryResume(t *testing.T) {
	for _, change := range []string{"cwd-only", "scope", "local-settings", "lost", "mismatched-lease"} {
		t.Run(change, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, _ := warmSpec(t, "omp")
			s.Caller.Role = core.RoleEmployee
			capture := filepath.Join(t.TempDir(), "launch.json")
			s.Launch.Env = append(s.Launch.Env, "FAKE_CAPTURE_LAUNCH="+capture)
			firstDir, err := harness.NewTurnDir(s.Launch, "restricted-one")
			if err != nil {
				t.Fatal(err)
			}
			s.Launch.Dir = firstDir.Path()
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			id, lease := p.RetainedSession(s.Caller.Conv, s.Caller.Principal.Key(), "omp")
			if lease != first || id == "" {
				t.Fatal("no exact retained employee worker")
			}
			nextDir, err := harness.NewTurnDir(s.Launch, "restricted-two")
			if err != nil {
				t.Fatal(err)
			}
			s.Launch.Dir = nextDir.Path()
			s.Session = harness.Session{ID: id, Resume: true}
			s.LiveWorkerOnly = lease
			s.Prompt = "employee next"
			switch change {
			case "scope":
				s.Caller.Private = !s.Caller.Private
			case "local-settings":
				if err = os.MkdirAll(filepath.Join(s.Launch.Dir, ".omp"), 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(s.Launch.Dir, ".omp", "settings.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "lost":
				first.Stop("synthetic lost employee worker")
				<-first.Exited()
			case "mismatched-lease":
				s.LiveWorkerOnly = &harness.Run{}
			}
			next := pooledTurn(t, p, ctx, s)
			if next.Process() == first.Process() {
				t.Fatal("restricted employee process reused across turn directories")
			}
			if got := next.Result().SessionID; (got == id) != (change == "cwd-only") {
				t.Fatalf("unsafe resume distinction %s: previous=%q next=%q", change, id, got)
			}
			data, err := os.ReadFile(capture)
			if err != nil {
				t.Fatal(err)
			}
			var actual struct {
				TurnDir            string `json:"turn_dir"`
				Memory, Restricted string
				Args               []string
			}
			if err = json.Unmarshal(data, &actual); err != nil {
				t.Fatal(err)
			}
			args := strings.Join(actual.Args, " ")
			if actual.TurnDir != nextDir.Path() || actual.Memory != "1" || actual.Restricted != "1" || !strings.Contains(args, "--no-mcp") || !strings.Contains(args, "--tools read") || !strings.Contains(args, "--no-extensions") {
				t.Fatalf("employee restrictions changed: %+v", actual)
			}
			cancel()
			<-next.Exited()
		})
	}
}

func TestWarmSchedulerOriginUsesCurrentTurn(t *testing.T) {
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, state := warmSpec(t, name)
			s.SchedulerControl = true
			s.Caller.Private = true
			p := harness.NewPool(harness.WarmConfig{})
			first := pooledTurn(t, p, ctx, s)
			defer func() { cancel(); <-first.Exited() }()
			data, err := os.ReadFile(filepath.Join(state, "pids"))
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(strings.Fields(string(data))[0])
			if err != nil {
				t.Fatal(err)
			}
			ancestors := harness.ProcessAncestors(pid)
			if _, ok := first.ControlCaller(ancestors); ok {
				t.Fatal("idle worker retained authority")
			}
			s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
			for _, eligible := range []bool{false, true} {
				s.SchedulerControl = eligible
				s.Prompt = "SCHEDULER_HOLD"
				_ = os.Remove(filepath.Join(state, "scheduler-release"))
				run, err := p.Start(ctx, s)
				if err != nil {
					t.Fatal(err)
				}
				if run.Process() != first.Process() {
					t.Fatal("expected same warm MCP parent")
				}
				select {
				case <-run.Ready():
				case <-time.After(5 * time.Second):
					t.Fatal("not ready")
				}
				// Claude's native replay, rather than startup timing, selects current turn.
				deadline := time.Now().Add(time.Second)
				for {
					c, ok := first.ControlCaller(ancestors)
					if ok == eligible {
						if ok && c.Conv != s.Caller.Conv {
							t.Fatal("wrong current caller")
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("stale turn authority", eligible)
					}
					time.Sleep(time.Millisecond)
				}
				if _, ok := first.ControlCaller(harness.ProcessAncestors(os.Getpid())); ok {
					t.Fatal("unrelated peer admitted")
				}
				if err := os.WriteFile(filepath.Join(state, "scheduler-release"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				select {
				case <-run.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("not done")
				}
				run.Warm()
				if _, ok := first.ControlCaller(ancestors); ok {
					t.Fatal("completed turn retained authority")
				}
				s.Session = harness.Session{ID: run.Result().SessionID, Resume: true}
			}
		})
	}
}

func TestWarmContextClaimAndAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, state := warmSpec(t, "claude")
	s.Context = ""
	calls, consumed := 0, 0
	s.WarmContext = func(process *harness.Run, born time.Time) (string, func()) {
		calls++
		if process == nil || born.IsZero() {
			t.Fatal("missing exact warm claim")
		}
		return "historical delivered data", func() { consumed++ }
	}
	p := harness.NewPool(harness.WarmConfig{})
	first := pooledTurn(t, p, ctx, s)
	if calls != 0 {
		t.Fatal("cold session imported historical delta")
	}
	s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
	second := pooledTurn(t, p, ctx, s)
	if calls != 1 || consumed != 0 {
		t.Fatal("speculative cursor consumed")
	}
	second.AcceptContext()
	second.AcceptContext()
	if consumed != 1 {
		t.Fatal("acknowledgement did not consume exactly once")
	}
	inputs := fakeharness.Inputs(state)
	if inputs[1].Context != "historical delivered data" {
		t.Fatal("warm context never handed over")
	}
	s.Launch.Env = append(s.Launch.Env, "CHANGED_ACCOUNT_SCOPE=1")
	third := pooledTurn(t, p, ctx, s)
	if third.Process() == first.Process() || calls != 1 {
		t.Fatal("changed launch imported old process delta")
	}
	inputs = fakeharness.Inputs(state)
	if inputs[len(inputs)-1].Context != "" {
		t.Fatal("old context leaked into replacement process")
	}
	cancel()
	<-third.Exited()
}

func TestWarmContextCancelledBeforeNativeInputDoesNotConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := warmSpec(t, "claude")
	s.Prompt = "first"
	start, stopStart := context.WithCancel(ctx)
	defer stopStart()
	consumed := 0
	s.WarmContext = func(*harness.Run, time.Time) (string, func()) {
		stopStart()
		return "historical data", func() { consumed++ }
	}
	p := harness.NewPool(harness.WarmConfig{})
	first := pooledTurn(t, p, ctx, s)
	s.StartContext = start
	s.Prompt = "must not enter native"
	s.Session = harness.Session{ID: first.Result().SessionID, Resume: true}
	run, err := p.Start(ctx, s)
	if err == nil {
		select {
		case <-run.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("cancelled launch did not settle")
		}
		if run.Result().Ready || run.Result().Answer != "" {
			t.Fatal("cancelled input reached native")
		}
	}
	if consumed != 0 {
		t.Fatal("failed start consumed delivered cursor")
	}
	cancel()
	<-first.Exited()
}
