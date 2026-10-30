package harness_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"sync"
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

// owns runs the owner's turns as r.
func owns(r core.RunAs) harness.Accounts {
	return harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: r}}
}

func launch(t *testing.T, bin, name string, memory bool) harness.Launch {
	t.Helper()
	l, err := owns(core.RunAs{User: self(t), Dir: t.TempDir(), Path: bin}).Prepare(core.RoleOwner, memory, harness.Binary(name))
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
	r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: prompt, Session: s, Launch: l, Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	return r.Result()
}

var all = []string{"pi", "omp", "claude", "codex", "agy"}

var caller = harness.Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleEmployee, Conv: core.ConvKey{Interface: "telegram", Chat: "-100"}}

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
			r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: "work WAIT_STEER", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: caller})
			if err != nil {
				t.Fatal(err)
			}
			<-r.Ready()
			if got := r.Steer(ctx, "s1", "also this", nil); got != harness.SteerAccepted {
				t.Fatalf("first steer: %s", got)
			}
			if got := r.Steer(ctx, "s2", "now FINISH", nil); got != harness.SteerAccepted {
				t.Fatalf("second steer: %s", got)
			}
			res := r.Result()
			if !strings.Contains(res.Answer, "steers=2") {
				t.Fatalf("result: %+v", res)
			}
			if got := r.Steer(ctx, "s3", "late", nil); got != harness.SteerQueued {
				t.Fatalf("steer after the turn: %s, want queued (not handed over)", got)
			}
		})
	}
}

// OMP answers the prompt, then each accepted steer, with its own final
// text; the answer keeps every one (live: an image described before two
// steers was lost).
func TestAnswerKeepsTheReplyToEachInput(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi", "omp")
	for _, name := range []string{"pi", "omp"} {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: "work WAIT_STEER STEERREPLY", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: caller})
			if err != nil {
				t.Fatal(err)
			}
			<-r.Ready()
			for i, text := range []string{"what is on this image?", "and the forward? FINISH"} {
				if got := r.Steer(ctx, "s"+strconv.Itoa(i), text, nil); got != harness.SteerAccepted {
					t.Fatalf("steer %d: %s", i, got)
				}
			}
			res := r.Result()
			parts := strings.Split(res.Answer, "\n\n")
			if len(parts) != 3 || parts[0] != "reply 1: work WAIT_STEER STEERREPLY" || parts[1] != "reply 2: what is on this image?" || !strings.HasPrefix(parts[2], name+"#1 ") || !strings.Contains(parts[2], "steers=2") {
				t.Fatalf("answer %q", res.Answer)
			}
		})
	}
}

// A steer Codex accepts after it completed a final answer, before the turn
// completed, gets its own answer; the earlier one is kept (review of
// 5b3c62f, finding 3: the last final answer replaced it).
func TestCodexSteerAfterFinalAnswerKeepsBoth(t *testing.T) {
	bin, _ := fakeharness.Install(t, "codex")
	l := launch(t, bin, "codex", true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := harness.Start(ctx, harness.Spec{Harness: "codex", Prompt: "work WAIT_STEER FINALFIRST", Session: harness.Session{}, Launch: l, Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	<-r.Ready()
	if got := r.Steer(ctx, "s1", "and this FINISH", nil); got != harness.SteerAccepted {
		t.Fatalf("steer: %s", got)
	}
	res := r.Result()
	parts := strings.Split(res.Answer, "\n\n")
	if len(parts) != 2 || parts[0] != "early answer: work WAIT_STEER FINALFIRST" || !strings.HasPrefix(parts[1], "codex#1 ") || !strings.Contains(parts[1], "steers=1") {
		t.Fatalf("answer %q", res.Answer)
	}
}

// A steer Claude takes only after its result runs as its next turn in the
// same stream; that turn's result is part of the answer, not dropped.
func TestClaudeLateSteerIsAnsweredNotDropped(t *testing.T) {
	bin, _ := fakeharness.Install(t, "claude")
	l := launch(t, bin, "claude", true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := harness.Start(ctx, harness.Spec{Harness: "claude", Prompt: "work STEERLATE", Session: harness.Session{ID: harness.NewSessionID("claude")}, Launch: l, Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	<-r.Ready()
	if got := r.Steer(ctx, "s1", "also CHARLIE", nil); got != harness.SteerAccepted {
		t.Fatalf("late steer: %s, want accepted", got)
	}
	res := r.Result()
	if !strings.HasPrefix(res.Answer, "claude#1 ") || !strings.HasSuffix(res.Answer, "\n\nlate reply: also CHARLIE") {
		t.Fatalf("answer %q", res.Answer)
	}
}

// lateSteer starts a Claude turn whose steer runs as Claude's next turn
// (STEERLATE plus the late turn's ending in prompt) and returns it once the
// steer was accepted, while Claude drains.
func lateSteer(t *testing.T, prompt string) *harness.Run {
	t.Helper()
	bin, _ := fakeharness.Install(t, "claude")
	l := launch(t, bin, "claude", true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	r, err := harness.Start(ctx, harness.Spec{Harness: "claude", Prompt: "work STEERLATE " + prompt, Session: harness.Session{ID: harness.NewSessionID("claude")}, Launch: l, Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	<-r.Ready()
	if got := r.Steer(ctx, "s1", "also CHARLIE", nil); got != harness.SteerAccepted {
		t.Fatalf("late steer: %s, want accepted", got)
	}
	return r
}

// A turn stopped while Claude drains is cancelled, never an empty answer
// with a resumable session (review of 5b3c62f, finding 1).
func TestClaudeDrainCancelled(t *testing.T) {
	r := lateSteer(t, "LATEHANG")
	r.Cancel()
	select {
	case <-r.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("cancel did not end the turn")
	}
	if res := r.Result(); !res.Cancelled || res.Answer != "" || res.Failure != "" {
		t.Fatalf("result %+v, want cancelled", res)
	}
}

// When the turn Claude runs for a late steer ends badly, the answer it
// already gave stands, with a note, and the session is not resumed
// (finding 2): a crash, an error result, an approval request, an exit
// without a result (review of d05b748, finding 4: clean, with no note).
func TestClaudeDrainBadEndKeepsTheAnswer(t *testing.T) {
	for word, note := range map[string]string{
		"LATECRASH": "claude exited: exit status 4",
		"LATEFAIL":  "a later message was not answered: Claude reported an error: fake late failure",
		"LATEASK":   "a later message was not answered: Claude needs native approval or input",
		"LATEEXIT":  "a later message was not answered: Claude exited without answering it",
	} {
		t.Run(word, func(t *testing.T) {
			res := lateSteer(t, word).Result()
			if !strings.HasPrefix(res.Answer, "claude#1 ") || strings.Contains(res.Answer, "late reply") || res.Failure != "" || res.NeedsHuman != "" {
				t.Fatalf("result %+v, want the first answer", res)
			}
			if !res.Unclean || !strings.Contains(res.Warning, note) {
				t.Fatalf("unclean %v, warning %q, want %q", res.Unclean, res.Warning, note)
			}
		})
	}
}

// A late steer whose turn answers leaves a clean session.
func TestClaudeDrainCleanEnd(t *testing.T) {
	res := lateSteer(t, "").Result()
	if res.Unclean || res.Warning != "" || !strings.HasSuffix(res.Answer, "\n\nlate reply: also CHARLIE") {
		t.Fatalf("result %+v", res)
	}
}

// A turn ends at the harness's answer, as the hub resolves at agent_end,
// not when the process exits: live, OMP took up to 28 s more to exit.
func TestAnswerIsFinalBeforeTheProcessExits(t *testing.T) {
	bin, state := fakeharness.Install(t, "pi", "omp", "claude", "codex")
	for _, name := range []string{"pi", "omp", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			r, err := harness.Start(ctx, harness.Spec{Harness: name, Prompt: "hello LINGER=4", Session: harness.Session{ID: harness.NewSessionID(name)}, Launch: l, Caller: caller})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-r.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("answer waited for the process to exit")
			}
			if res := r.Result(); !strings.HasPrefix(res.Answer, name+"#1 ") || res.Failure != "" || !res.Ready {
				t.Fatalf("result %+v", res)
			}
			select {
			case <-r.Exited():
				t.Fatal("exited before the fake's linger")
			default:
			}
			<-r.Exited()
			if _, err := os.Stat(state + "/lingering"); err == nil {
				t.Fatal("Exited closed while the process still ran")
			}
		})
	}
}

// lingers starts an omp turn whose process keeps running after its answer
// (LINGER=30 plus words), logging what Heimdall logs about it.
func lingers(t *testing.T, words string) (*harness.Run, string, *logLines) {
	t.Helper()
	bin, state := fakeharness.Install(t, "omp")
	l := launch(t, bin, "omp", true)
	logs := &logLines{}
	r, err := harness.Start(context.Background(), harness.Spec{Harness: "omp", Prompt: "hello LINGER=30 " + words, Session: harness.Session{ID: harness.NewSessionID("omp")}, Launch: l, Caller: caller, Log: logs.add})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Cancel)
	if res := r.Result(); !strings.HasPrefix(res.Answer, "omp#1 ") || res.Failure != "" {
		t.Fatalf("result %+v", res)
	}
	return r, state, logs
}

type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) add(s string) { l.mu.Lock(); l.lines = append(l.lines, s); l.mu.Unlock() }

func (l *logLines) all() []string { l.mu.Lock(); defer l.mu.Unlock(); return slices.Clone(l.lines) }

func exitsWithin(t *testing.T, r *harness.Run, d time.Duration) {
	t.Helper()
	select {
	case <-r.Exited():
	case <-time.After(d):
		t.Fatalf("process still running after %s", d)
	}
}

func setGrace(t *testing.T, exit, term time.Duration) {
	oldExit, oldTerm := harness.ExitGrace, harness.TermGrace
	harness.ExitGrace, harness.TermGrace = exit, term
	t.Cleanup(func() { harness.ExitGrace, harness.TermGrace = oldExit, oldTerm })
}

// A process still running ExitGrace after its answer gets SIGTERM, so it
// can finish its shutdown, and the stop is logged (metadata only). Before,
// it was SIGKILLed at once, silently.
func TestLingeringProcessGetsSIGTERMAfterGrace(t *testing.T) {
	setGrace(t, 300*time.Millisecond, 10*time.Second)
	r, state, logs := lingers(t, "")
	exitsWithin(t, r, 5*time.Second)
	if _, err := os.Stat(state + "/terminated"); err != nil {
		t.Fatal("the process did not get SIGTERM")
	}
	if got := logs.all(); len(got) != 1 || got[0] != "omp process still running 300ms after its answer: SIGTERM to its process group" {
		t.Fatalf("log %q", got)
	}
}

// A process that ignores SIGTERM is killed TermGrace later.
func TestLingeringProcessIgnoringSIGTERMIsKilled(t *testing.T) {
	setGrace(t, 300*time.Millisecond, 300*time.Millisecond)
	r, state, logs := lingers(t, "NOTERM")
	exitsWithin(t, r, 5*time.Second)
	if _, err := os.Stat(state + "/terminated"); err == nil {
		t.Fatal("NOTERM fake handled SIGTERM")
	}
	if got := logs.all(); len(got) != 2 || !strings.HasSuffix(got[1], "still running 300ms after SIGTERM: SIGKILL") {
		t.Fatalf("log %q", got)
	}
	if res := r.Result(); !strings.HasPrefix(res.Answer, "omp#1 ") || res.Failure != "" {
		t.Fatalf("the kill changed the answer: %+v", res)
	}
}

// Stop ends the wait at once (the router's !stop after an answer).
func TestStopEndsTheExitGraceNow(t *testing.T) {
	setGrace(t, 30*time.Second, 10*time.Second)
	r, state, logs := lingers(t, "")
	r.Stop("after its answer, stopped by stop")
	exitsWithin(t, r, 3*time.Second)
	if _, err := os.Stat(state + "/terminated"); err != nil {
		t.Fatal("Stop did not send SIGTERM")
	}
	if got := logs.all(); len(got) != 1 || !strings.Contains(got[0], "stopped by stop: SIGTERM") {
		t.Fatalf("log %q", got)
	}
}

func TestAgyHasNoSteering(t *testing.T) {
	bin, _ := fakeharness.Install(t, "agy")
	l := launch(t, bin, "agy", true)
	r, err := harness.Start(context.Background(), harness.Spec{Harness: "agy", Prompt: "hi", Launch: l, Caller: caller})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Steer(context.Background(), "s1", "x", nil); got != harness.SteerUnsupported {
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
	bin, _ := fakeharness.Install(t, "pi", "omp", "claude", "codex", "agy")
	for _, name := range []string{"pi", "omp", "claude", "codex", "agy"} {
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
	r, err := harness.Start(context.Background(), harness.Spec{Harness: "pi", Prompt: "SLEEP", Session: harness.Session{ID: harness.NewSessionID("pi")}, Launch: l, Caller: caller})
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
	l, err := owns(core.RunAs{User: self(t), Path: "/usr/bin:/bin"}).Prepare(core.RoleOwner, true, "sh")
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
	if _, err := owns(core.RunAs{User: "root"}).Prepare(core.RoleOwner, true, "sh"); err == nil || !strings.Contains(err.Error(), "not root") {
		t.Fatalf("err = %v, want refusal", err)
	}
}

func TestPrepareDoesNotUseHeimdallPATH(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi")
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	if _, err := owns(core.RunAs{User: self(t), Path: t.TempDir()}).Prepare(core.RoleOwner, true, "pi"); err == nil {
		t.Fatal("found pi on Heimdall's PATH instead of the run-as PATH")
	}
}

func TestIdentityMapsToHubVariables(t *testing.T) {
	conv := core.ConvKey{Interface: "x", Chat: "C9", Thread: "t"}
	for _, tc := range []struct {
		name string
		c    harness.Caller
		want []string
	}{
		{"telegram owner", harness.Caller{Principal: core.Principal{Platform: "telegram", ID: "42", Name: "PI_EXTERNAL_USER_ID=tg_user_1"}, Role: core.RoleOwner, Conv: conv},
			[]string{"PI_EXTERNAL_PLATFORM=telegram", "PI_EXTERNAL_USER_ID=tg_user_42", "PI_EXTERNAL_CHAT_ID=tg_chat_C9", "PI_RESTRICTED_EXTERNAL_USER=0", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}},
		{"telegram employee", harness.Caller{Principal: core.Principal{Platform: "telegram", ID: "43"}, Role: core.RoleEmployee, Conv: conv},
			[]string{"PI_EXTERNAL_PLATFORM=telegram", "PI_EXTERNAL_USER_ID=tg_user_43", "PI_EXTERNAL_CHAT_ID=tg_chat_C9", "PI_RESTRICTED_EXTERNAL_USER=1", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}},
		{"telegram private chat", harness.Caller{Principal: core.Principal{Platform: "telegram", ID: "43"}, Role: core.RoleEmployee, Conv: core.ConvKey{Interface: "telegram", Chat: "43"}, Private: true},
			[]string{"PI_EXTERNAL_PLATFORM=telegram", "PI_EXTERNAL_USER_ID=tg_user_43", "PI_EXTERNAL_CHAT_ID=tg_chat_43", "PI_RESTRICTED_EXTERNAL_USER=1", "PI_EXTERNAL_MCP_PRIVATE=1", "PI_TRANSPORT_CHILD=1"}},
		{"slack verified email", harness.Caller{Principal: core.SlackPrincipal(" User@Example.com", "T0WORK", ""), Role: core.RoleEmployee, Conv: conv},
			[]string{"PI_EXTERNAL_PLATFORM=slack", "PI_EXTERNAL_USER_ID=user@example.com", "PI_EXTERNAL_CHAT_ID=sl_channel_C9", "PI_RESTRICTED_EXTERNAL_USER=1", "PI_EXTERNAL_USER_EMAIL=user@example.com", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}},
		{"slack owner in a DM", harness.Caller{Principal: core.SlackPrincipal("owner@example.com", "T0WORK", ""), Role: core.RoleOwner, Conv: conv, Private: true},
			[]string{"PI_EXTERNAL_PLATFORM=slack", "PI_EXTERNAL_USER_ID=owner@example.com", "PI_EXTERNAL_CHAT_ID=sl_channel_C9", "PI_RESTRICTED_EXTERNAL_USER=0", "PI_EXTERNAL_USER_EMAIL=owner@example.com", "PI_EXTERNAL_MCP_PRIVATE=1", "PI_TRANSPORT_CHILD=1"}},
		{"agentnet owner is still restricted", harness.Caller{Principal: core.Principal{Platform: "agentnet", ID: "a", KeyID: "k"}, Role: core.RoleOwner, Conv: conv},
			[]string{"PI_EXTERNAL_PLATFORM=a2a", "PI_RESTRICTED_EXTERNAL_USER=1", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}},
		{"local never looks like the hub's own admin", harness.Caller{Principal: core.Principal{Platform: "local", ID: "local"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "local", Chat: "system"}},
			[]string{"PI_EXTERNAL_PLATFORM=local", "PI_EXTERNAL_USER_ID=local_user_local", "PI_EXTERNAL_CHAT_ID=local_chat_system", "PI_RESTRICTED_EXTERNAL_USER=0", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := harness.WithIdentity(nil, tc.c)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("got %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestIdentityReplacesInheritedClaims(t *testing.T) {
	inherited := []string{"PATH=/bin", "PI_EXTERNAL_USER_ID=tg_user_1", "PI_EXTERNAL_TENANT_ID=x", "A2A_CALLER_PRINCIPAL=boss", "PI_RESTRICTED_EXTERNAL_USER=0", "BIGQUERY_CALLER_PRINCIPAL=boss", "PI_TRANSPORT_CHILD=0", "PI_EXTERNAL_MCP_PRIVATE=1"}
	got, err := harness.WithIdentity(inherited, caller)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"PATH=/bin", "PI_EXTERNAL_PLATFORM=telegram", "PI_EXTERNAL_USER_ID=tg_user_42", "PI_EXTERNAL_CHAT_ID=tg_chat_-100", "PI_RESTRICTED_EXTERNAL_USER=1", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_TRANSPORT_CHILD=1"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestIdentityRequiredAndClean(t *testing.T) {
	for _, c := range []harness.Caller{
		{},
		{Principal: core.Principal{Platform: "telegram", ID: "1"}, Role: core.RoleOwner},
		{Principal: core.Principal{Platform: "slack", ID: "T1/U2"}, Role: core.RoleEmployee, Conv: core.ConvKey{Interface: "slack", Chat: "C1"}},
		{Principal: core.Principal{Platform: "telegram", ID: "1\nPI_RESTRICTED_EXTERNAL_USER=0"}, Role: core.RoleEmployee, Conv: core.ConvKey{Interface: "telegram", Chat: "1"}},
	} {
		if _, err := harness.WithIdentity(nil, c); err == nil {
			t.Fatalf("caller %+v accepted", c)
		}
	}
	bin, _ := fakeharness.Install(t, "pi")
	if _, err := harness.Start(context.Background(), harness.Spec{Harness: "pi", Prompt: "x", Session: harness.Session{ID: harness.NewSessionID("pi")}, Launch: launch(t, bin, "pi", true)}); err == nil {
		t.Fatal("a turn started without a caller")
	}
}

func TestHarnessProcessSeesCallerIdentity(t *testing.T) {
	bin, _ := fakeharness.Install(t, all...)
	t.Setenv("PI_EXTERNAL_USER_ID", "tg_user_1")
	t.Setenv("A2A_CALLER_PRINCIPAL", "boss")
	t.Setenv("PI_TRANSPORT_CHILD", "0") // Heimdall's own value never reaches a turn
	for _, name := range all {
		t.Run(name, func(t *testing.T) {
			res := turn(t, launch(t, bin, name, true), name, "x", harness.Session{ID: harness.NewSessionID(name)})
			want := `ext="PI_EXTERNAL_CHAT_ID=tg_chat_-100 PI_EXTERNAL_MCP_PRIVATE=0 PI_EXTERNAL_PLATFORM=telegram PI_EXTERNAL_USER_ID=tg_user_42 PI_RESTRICTED_EXTERNAL_USER=1 PI_TRANSPORT_CHILD=1"`
			if !strings.Contains(res.Answer, want) {
				t.Fatalf("answer %q lacks %s", res.Answer, want)
			}
		})
	}
}

// stubAccounts resolves OS user names to the given uids and homes, as if
// Heimdall ran as uid self.
func stubAccounts(t *testing.T, self int, users map[string]int) (harness.Accounts, map[string]string) {
	homes := map[string]string{}
	for name := range users {
		homes[name] = t.TempDir()
	}
	runAs := map[core.Role]core.RunAs{}
	if _, ok := users["boss"]; ok {
		runAs[core.RoleOwner] = core.RunAs{User: "boss", Path: "/usr/bin:/bin"}
	}
	if _, ok := users["emp"]; ok {
		runAs[core.RoleEmployee] = core.RunAs{User: "emp", Path: "/usr/bin:/bin"}
	}
	return harness.Accounts{
		RunAs: runAs,
		Lookup: func(name string) (*user.User, error) {
			uid, ok := users[name]
			if !ok {
				return nil, user.UnknownUserError(name)
			}
			id := strconv.Itoa(uid)
			return &user.User{Uid: id, Gid: id, Username: name, HomeDir: homes[name]}, nil
		},
		Getuid: func() int { return self },
	}, homes
}

func TestEmployeeAccountNeverRootOrOwner(t *testing.T) {
	for name, users := range map[string]map[string]int{
		"root by another name":  {"boss": 1000, "emp": 0},
		"owner by another name": {"boss": 1000, "emp": 1000},
		"root owner's uid":      {"boss": 0, "emp": 0},
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := stubAccounts(t, 1000, users)
			if err := a.Check(); err == nil {
				t.Fatal("Check accepted the employee account")
			}
			_, err := a.Prepare(core.RoleEmployee, true, "sh")
			var acct *harness.AccountError
			if !errors.As(err, &acct) {
				t.Fatalf("Prepare: %v, want an account refusal", err)
			}
		})
	}
	a, _ := stubAccounts(t, 0, map[string]int{"boss": 0, "emp": 1001})
	if err := a.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestEmployeeGroupLookupFailureIsLogged(t *testing.T) {
	a, _ := stubAccounts(t, 0, map[string]int{"boss": 0, "emp": 1001})
	a.GroupIDs = func(*user.User) ([]string, error) { return []string{"1001", "27"}, nil }
	l, err := a.Prepare(core.RoleEmployee, true, "sh")
	if err != nil {
		t.Fatal(err)
	}
	if l.Cred == nil || !slices.Equal(l.Cred.Groups, []uint32{1001, 27}) {
		t.Fatalf("employee groups: %+v", l.Cred)
	}
	var logs bytes.Buffer
	a.Log = log.New(&logs, "", 0)
	a.GroupIDs = func(*user.User) ([]string, error) { return nil, errors.New("group database unreadable") }
	if l, err = a.Prepare(core.RoleEmployee, true, "sh"); err != nil {
		t.Fatal(err)
	}
	if l.Cred == nil || len(l.Cred.Groups) != 0 || !strings.Contains(logs.String(), `OS user "emp"`) || !strings.Contains(logs.String(), "group database unreadable") {
		t.Fatalf("groups %+v, log %q", l.Cred, logs.String())
	}
}

func TestEmployeeEnvironmentRebuiltByRole(t *testing.T) {
	t.Setenv("HEIMDALL_UNRELATED", "x")
	t.Setenv("OWNER_SECRET", "x")
	// Heimdall runs as the employee's uid and the owner is root: the employee
	// turn still gets an environment rebuilt from its own account.
	a, homes := stubAccounts(t, 1001, map[string]int{"boss": 0, "emp": 1001})
	l, err := a.Prepare(core.RoleEmployee, true, "sh")
	if err != nil {
		t.Fatal(err)
	}
	if l.Cred != nil || l.Home != homes["emp"] || !slices.Contains(l.Env, "HOME="+homes["emp"]) || slices.ContainsFunc(l.Env, func(kv string) bool { return strings.HasPrefix(kv, "OWNER_SECRET=") }) {
		t.Fatalf("employee launch on Heimdall's uid: cred %v home %q env %q", l.Cred, l.Home, l.Env)
	}
	if _, err := a.Prepare(core.RoleOwner, true, "sh"); err == nil || !strings.Contains(err.Error(), "not root") {
		t.Fatalf("owner turn as root from a non-root Heimdall: %v", err)
	}
	// As root: the employee switches uid with a rebuilt environment, the
	// owner (root) keeps Heimdall's.
	a, homes = stubAccounts(t, 0, map[string]int{"boss": 0, "emp": 1001})
	l, err = a.Prepare(core.RoleEmployee, true, "sh")
	if err != nil {
		t.Fatal(err)
	}
	if l.Cred == nil || l.Cred.Uid != 1001 || l.Cred.Gid != 1001 || slices.ContainsFunc(l.Env, func(kv string) bool { return strings.HasPrefix(kv, "OWNER_SECRET=") }) || !slices.Contains(l.Env, "HOME="+homes["emp"]) {
		t.Fatalf("employee launch as root: cred %+v env %q", l.Cred, l.Env)
	}
	l, err = a.Prepare(core.RoleOwner, true, "sh")
	if err != nil {
		t.Fatal(err)
	}
	if l.Cred != nil || !slices.Contains(l.Env, "OWNER_SECRET=x") || slices.Contains(l.Env, "HEIMDALL_UNRELATED=x") {
		t.Fatalf("owner launch as root: cred %+v", l.Cred)
	}
}
