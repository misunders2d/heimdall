package harness_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

func warmSpec(t *testing.T, name string) (harness.Spec, string) {
	bin, state := fakeharness.Install(t, name)
	l := launch(t, bin, name, true)
	l.Env = append(l.Env, "FAKE_WARM=1")
	return harness.Spec{Harness: name, Prompt: "hello", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: caller}, state
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
				s.Caller.Role = "owner"
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
			if !r.Result().Cancelled {
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
	s := harness.Spec{Harness: "agy", Prompt: "hello", Launch: launch(t, bin, "agy", true), Caller: caller}
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
