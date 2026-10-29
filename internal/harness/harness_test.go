package harness_test

import (
	"context"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

func TestMain(m *testing.M) {
	fakeharness.Main()
	os.Exit(m.Run())
}

func self(t *testing.T) string {
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	return u.Username
}

func launch(t *testing.T, bin, name string, memory bool) harness.Launch {
	t.Helper()
	l, err := harness.Prepare(core.RunAs{User: self(t), Dir: t.TempDir(), Path: bin}, memory, harness.Binary(name))
	if err != nil {
		t.Fatal(err)
	}
	l.Home = t.TempDir() // session directories stay inside the test
	return l
}

func turn(t *testing.T, l harness.Launch, name, prompt string, s harness.Session) harness.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: prompt, Session: s, Launch: l})
	if err != nil {
		t.Fatal(err)
	}
	return r.Result()
}

var all = []string{"pi", "omp", "claude", "codex", "agy"}

func TestSessionsCreateThenResume(t *testing.T) {
	bin, _ := fakeharness.Install(t, all...)
	for _, name := range all {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			first := turn(t, l, name, "hello", harness.Session{ID: harness.NewSessionID(name)})
			if first.Failure != "" || !strings.HasPrefix(first.Answer, name+"#1 resumed=false") || !first.Ready || first.SessionID == "" {
				t.Fatalf("first turn: %+v", first)
			}
			second := turn(t, l, name, "again", harness.Session{ID: first.SessionID, Resume: true})
			if second.Failure != "" || !strings.HasPrefix(second.Answer, name+"#2 resumed=true") || second.SessionID != first.SessionID {
				t.Fatalf("resumed turn: %+v", second)
			}
		})
	}
}

func TestSteerAcceptedOnNativeAcknowledgement(t *testing.T) {
	bin, _ := fakeharness.Install(t, all...)
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: "work WAIT_STEER", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l})
			if err != nil {
				t.Fatal(err)
			}
			<-r.Ready()
			if got := r.Steer(ctx, "s1", "also this"); got != harness.SteerAccepted {
				t.Fatalf("first steer: %s", got)
			}
			if got := r.Steer(ctx, "s2", "now FINISH"); got != harness.SteerAccepted {
				t.Fatalf("second steer: %s", got)
			}
			res := r.Result()
			if !strings.Contains(res.Answer, "steers=2") {
				t.Fatalf("result: %+v", res)
			}
			if got := r.Steer(ctx, "s3", "late"); got != harness.SteerQueued {
				t.Fatalf("steer after the turn: %s, want queued (not handed over)", got)
			}
		})
	}
}

func TestAgyHasNoSteering(t *testing.T) {
	bin, _ := fakeharness.Install(t, "agy")
	l := launch(t, bin, "agy", true)
	r, err := harness.Start(context.Background(), harness.Spec{Harness: "agy", Prompt: "hi", Launch: l})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Steer(context.Background(), "s1", "x"); got != harness.SteerUnsupported {
		t.Fatalf("agy steer: %s", got)
	}
	r.Result()
}

func TestFailureBeforeReadyIsNotReady(t *testing.T) {
	bin, _ := fakeharness.Install(t, all...)
	for _, name := range all {
		t.Run(name, func(t *testing.T) {
			res := turn(t, launch(t, bin, name, true), name, "x FAILPRE="+name, harness.Session{ID: harness.NewSessionID(name)})
			if res.Ready || res.Failure == "" || res.Answer != "" {
				t.Fatalf("result: %+v", res)
			}
		})
	}
}

func TestFailureAfterReadyIsReady(t *testing.T) {
	bin, _ := fakeharness.Install(t, all...)
	for _, name := range all {
		t.Run(name, func(t *testing.T) {
			res := turn(t, launch(t, bin, name, true), name, "x FAILPOST="+name, harness.Session{ID: harness.NewSessionID(name)})
			if !res.Ready || res.Failure == "" || res.Answer != "" {
				t.Fatalf("result: %+v", res)
			}
		})
	}
}

func TestNativeApprovalRequestNeedsHuman(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi", "omp", "claude", "codex")
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			res := turn(t, launch(t, bin, name, true), name, "x ASK_HUMAN", harness.Session{ID: harness.NewSessionID(name)})
			if res.NeedsHuman == "" || res.Answer != "" {
				t.Fatalf("result: %+v", res)
			}
		})
	}
}

func TestCancelStopsTheTurn(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi")
	l := launch(t, bin, "pi", true)
	r, err := harness.Start(context.Background(), harness.Spec{Harness: "pi", Prompt: "SLEEP", Session: harness.Session{ID: harness.NewSessionID("pi")}, Launch: l})
	if err != nil {
		t.Fatal(err)
	}
	<-r.Ready()
	start := time.Now()
	r.Cancel()
	res := r.Result()
	if !res.Cancelled || time.Since(start) > 5*time.Second {
		t.Fatalf("result: %+v after %s", res, time.Since(start))
	}
}

func TestMemoryOffSuppressesMementoHooks(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi")
	t.Setenv("MEMENTO_PROCESSING", "")
	on := turn(t, launch(t, bin, "pi", true), "pi", "x", harness.Session{ID: harness.NewSessionID("pi")})
	off := turn(t, launch(t, bin, "pi", false), "pi", "x", harness.Session{ID: harness.NewSessionID("pi")})
	if !strings.Contains(on.Answer, "mem= ") || !strings.Contains(off.Answer, "mem=1 ") {
		t.Fatalf("memory on: %q; off: %q", on.Answer, off.Answer)
	}
}

func TestPrepareStripsHeimdallEnvironment(t *testing.T) {
	t.Setenv("HEIMDALL_TELEGRAM_TOKEN", "secret")
	l, err := harness.Prepare(core.RunAs{User: self(t), Path: "/usr/bin:/bin"}, true, "sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range l.Env {
		if strings.HasPrefix(kv, "HEIMDALL_") {
			t.Fatalf("harness environment carries %s", strings.SplitN(kv, "=", 2)[0])
		}
	}
	if l.Path != "/usr/bin/sh" && l.Path != "/bin/sh" {
		t.Fatalf("sh resolved to %q", l.Path)
	}
}

func TestPrepareRefusesAnotherUserWithoutRoot(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root")
	}
	if _, err := harness.Prepare(core.RunAs{User: "root"}, true, "sh"); err == nil || !strings.Contains(err.Error(), "not root") {
		t.Fatalf("err = %v, want refusal", err)
	}
}

func TestPrepareDoesNotUseHeimdallPATH(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi")
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	if _, err := harness.Prepare(core.RunAs{User: self(t), Path: t.TempDir()}, true, "pi"); err == nil {
		t.Fatal("found pi on Heimdall's PATH instead of the run-as PATH")
	}
}
