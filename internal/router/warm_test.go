package router

import (
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDuplicateHarnessTag(t *testing.T) {
	for _, text := range []string{"answer\n\n— claude", "answer\n— claude\n— claude", "answer"} {
		if got := stripHarnessTag(text, "claude") + "\n\n— claude"; got != "answer\n\n— claude" {
			t.Fatalf("duplicate footer: %q", got)
		}
	}
	if got := stripHarnessTag("— claude is quoted\nanswer", "claude"); got != "— claude is quoted\nanswer" {
		t.Fatal("stripped body")
	}
}

// Same conversation reuses; a different conversation can run at max_parallel=1
// while the first process remains idle. Status names its retained harness.
func testRouterWarm(t *testing.T, other bool) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Accounts = warmAccounts{e.opt.Accounts}
	e.opt.MaxParallel = 1
	e.start()
	e.send(owner, "c1", "hello")
	want(t, e.reply(), "turn=1")
	e.send(owner, "c1", "!status")
	want(t, e.reply(), "Warm: omp, idle")
	chat := "c1"
	expected := "turn=2"
	if other {
		chat = "c2"
		expected = "turn=1"
	}
	e.send(owner, chat, "second")
	want(t, e.reply(), expected)
	b, _ := os.ReadFile(filepath.Join(e.state, "pids"))
	n := 1
	if other {
		n = 2
	}
	if len(strings.Fields(string(b))) != n {
		t.Fatalf("pids %s", b)
	}
}

type warmAccounts struct{ Accounts }

func (a warmAccounts) Prepare(role core.Role, memory bool, bin string) (harness.Launch, error) {
	l, e := a.Accounts.Prepare(role, memory, bin)
	l.Env = append(l.Env, "FAKE_WARM=1")
	return l, e
}

type namedFakeIface struct {
	*fakeIface
	platform string
}

func (f namedFakeIface) Name() string { return f.platform }

func TestStopExactlyOneOutboundAcrossInterfaces(t *testing.T) {
	for _, platform := range []string{"slack", "telegram"} {
		t.Run(platform, func(t *testing.T) {
			e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
			e.stop()
			stopOnce.Delete(e.st)
			e.platform = platform
			e.opt.Accounts = warmAccounts{e.opt.Accounts}
			e.start()
			conv := core.ConvKey{Interface: platform, Chat: "42", Thread: "root"}
			who := core.Principal{Platform: "fake", ID: "owner"}
			e.sendIn(who, conv, "HANG")
			for {
				e.sendIn(who, conv, "!status")
				if strings.Contains(e.reply().Text, "Running: claude turn") {
					break
				}
			}
			e.sendIn(who, conv, "!stop")
			want(t, e.reply(), "Stopping current run.")
			eventuallyTrue(t, "stopped", e.r.Idle)
			e.noReply(200 * time.Millisecond)
			e.sendIn(who, conv, "!stop")
			want(t, e.reply(), "No active run to stop.")
			e.noReply(100 * time.Millisecond)
		})
	}
}

func TestDuplicateHarnessTagDelivery(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Accounts = warmAccounts{e.opt.Accounts}
	e.start()
	e.send(owner, "c1", "DUPLICATE_TAG")
	out := e.reply()
	if strings.Count(out.Text, "— claude") != 1 {
		t.Fatalf("duplicate signature: %q", out.Text)
	}
}

func TestIdleExitRacingNextTurnRetriesSameHarness(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Accounts = warmAccounts{e.opt.Accounts}
	e.start()
	e.send(owner, "c1", "EXIT_ON_NEXT")
	want(t, e.reply(), "turn=1")
	e.send(owner, "c1", "after idle exit")
	out := e.reply()
	want(t, out, "after idle exit", "— omp")
	if strings.Contains(out.Text, "failed") {
		t.Fatalf("idle crash leaked to user: %s", out.Text)
	}
	b, _ := os.ReadFile(filepath.Join(e.state, "pids"))
	if len(strings.Fields(string(b))) != 2 {
		t.Fatalf("no cold replacement: %s", b)
	}
}

type changingWarmAccounts struct {
	Accounts
	change atomic.Bool
}

func (a *changingWarmAccounts) Prepare(role core.Role, memory bool, bin string) (harness.Launch, error) {
	l, err := a.Accounts.Prepare(role, memory, bin)
	l.Env = append(l.Env, "FAKE_WARM=1", "FAKE_IGNORE_TERM=1")
	if a.change.Swap(false) {
		l.Env = append(l.Env, "MODEL=changed")
	}
	return l, err
}
func TestWarmCleanupDoesNotBlockAnotherConversation(t *testing.T) {
	setGrace(t, 60*time.Second, 2*time.Second)
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	accounts := &changingWarmAccounts{Accounts: e.opt.Accounts}
	e.opt.Accounts = accounts
	e.opt.MaxParallel = 2
	e.start()
	e.send(owner, "c1", "first")
	want(t, e.reply(), "turn=1")
	accounts.change.Store(true)
	e.send(owner, "c1", "changed launch")
	eventuallyTrue(t, "cleanup began", func() bool { return strings.Contains(e.logs.String(), "launch changed or expired") })
	e.send(owner, "c2", "unrelated")
	select {
	case out := <-e.f.out:
		if out.Conv.Chat != "c2" {
			t.Fatalf("cleanup blocked new conversation: %+v", out)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("idle cleanup held global launch lock")
	}
	want(t, e.reply(), "changed launch")
}
