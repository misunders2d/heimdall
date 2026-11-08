package router

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

func continuationEnv(t *testing.T, platform string) *env {
	e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
	e.stop()
	stopOnce.Delete(e.st)
	e.platform = platform
	e.opt.Accounts = warmAccounts{e.opt.Accounts}
	e.start()
	return e
}

func TestNativeBetweenTurnDelivery(t *testing.T) {
	for _, platform := range []string{"telegram", "slack"} {
		t.Run(platform, func(t *testing.T) {
			e := continuationEnv(t, platform)
			conv := core.ConvKey{Interface: platform, Chat: "42", Thread: "root"}
			e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND_SAME_WRITE")
			first := e.reply()
			want(t, first, "turn=1")
			notice := e.reply()
			want(t, notice, "background task completed")
			late := e.reply()
			want(t, late, "BACKGROUND_DELIVERED")
			if first.Conv != conv || notice.Conv != conv || late.Conv != conv || late.ReplyTo != first.ReplyTo {
				t.Fatalf("wrong destination: %+v %+v", first, late)
			}
			e.noReply(100 * time.Millisecond)
			e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "next question")
			want(t, e.reply(), "turn=2")
			pids, _ := os.ReadFile(filepath.Join(e.state, "pids"))
			if len(strings.Fields(string(pids))) != 1 {
				t.Fatalf("worker replaced: %s", pids)
			}
		})
	}
}

func TestNativeCompletionBeforeNewInputReplay(t *testing.T) {
	e := continuationEnv(t, "telegram")
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND_COLLISION")
	first := e.reply()
	want(t, first, "turn=1")
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "second person question")
	notice := e.reply()
	want(t, notice, "background task completed")
	late := e.reply()
	want(t, late, "BACKGROUND_DELIVERED")
	answer := e.reply()
	want(t, answer, "second person question")
	if strings.Contains(answer.Text, "BACKGROUND_DELIVERED") || late.ReplyTo != first.ReplyTo || answer.ReplyTo == first.ReplyTo {
		t.Fatalf("mixed responses: %+v %+v", late, answer)
	}
	e.noReply(100 * time.Millisecond)
}

func TestNativeCompletionWhileNewUserTurnRuns(t *testing.T) {
	e := continuationEnv(t, "slack")
	conv := core.ConvKey{Interface: "slack", Chat: "42", Thread: "root"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND BG_SLOW")
	first := e.reply()
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "HANG")
	want(t, e.reply(), "background task completed")
	late := e.reply()
	want(t, late, "BACKGROUND_DELIVERED")
	if late.ReplyTo != first.ReplyTo {
		t.Fatalf("old result delivered as new answer: %+v", late)
	}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "!status")
	want(t, e.reply(), "Running: claude turn")
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "!stop")
	want(t, e.reply(), "Stopping current run.")
}

func TestNativeBackgroundRetirementReported(t *testing.T) {
	for _, why := range []string{"lifetime", "lru", "exit", "launch"} {
		t.Run(why, func(t *testing.T) {
			e := continuationEnv(t, "telegram")
			e.stop()
			stopOnce.Delete(e.st)
			cfg := harness.WarmConfig{}
			switch why {
			case "idle":
				cfg.IdleTimeoutSeconds = 1
			case "lifetime":
				cfg.MaxLifetimeSeconds = 1
			case "lru":
				cfg.MaxWarm = 1
			case "launch":
				cfg.MaxLifetimeSeconds = 1201
			}
			e.opt.Harness = cfg
			e.start()
			conv := core.ConvKey{Interface: "telegram", Chat: "42"}
			prompt := "BACKGROUND_NEVER"
			if why == "exit" {
				prompt += " EXIT_IDLE"
			}
			e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, prompt)
			want(t, e.reply(), "turn=1")
			if why == "lru" {
				e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, core.ConvKey{Interface: "telegram", Chat: "43"}, "second")
			}
			if why == "launch" {
				time.Sleep(1100 * time.Millisecond)
				e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "next input")
			}
			a := e.reply()
			if !strings.Contains(a.Text, "Pending background work was interrupted") {
				a = e.reply()
			}
			want(t, a, "Pending background work was interrupted")
		})
	}
}

func TestNativeBackgroundResetSuppressesOldOutput(t *testing.T) {
	e := continuationEnv(t, "telegram")
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND BG_SLOW")
	e.reply()
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "!reset")
	e.reply()
	e.noReply(1700 * time.Millisecond)
}

func TestNativeContinuationKeepsCallerAndConversation(t *testing.T) {
	e := continuationEnv(t, "slack")
	conv := core.ConvKey{Interface: "slack", Chat: "42", Thread: "root"}
	boss := core.Principal{Platform: "fake", ID: "owner"}
	employee := core.Principal{Platform: "fake", ID: "employee"}
	e.sendIn(boss, conv, "BACKGROUND BG_SLOW")
	first := e.reply()
	e.sendIn(employee, conv, "employee question")
	want(t, e.reply(), "employee question")
	other := conv
	other.Thread = "other"
	e.sendIn(boss, other, "other conversation")
	want(t, e.reply(), "other conversation")
	notice := e.reply()
	late := e.reply()
	want(t, late, "BACKGROUND_DELIVERED")
	if notice.Conv != conv || late.Conv != conv || late.ReplyTo != first.ReplyTo {
		t.Fatalf("crossed caller/conversation: %+v %+v", notice, late)
	}
	pids, _ := os.ReadFile(filepath.Join(e.state, "pids"))
	if len(strings.Fields(string(pids))) != 3 {
		t.Fatalf("sessions shared: %s", pids)
	}
	e.noReply(100 * time.Millisecond)
}

func TestNativeContinuationFilesReactionsAndDuplicateResult(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.f.files.Store(true)
	e.f.react.Store(true)
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND BG_FILE BG_REACT BG_DUPLICATE")
	first := e.reply()
	want(t, first, "turn=1")
	want(t, e.reply(), "background task completed")
	late := e.reply()
	want(t, late, "BACKGROUND_DELIVERED")
	if len(late.Reactions) != 1 || late.Reactions[0].MsgID != first.ReplyTo {
		t.Fatalf("reaction target changed: %+v", late)
	}
	file := e.reply()
	if file.File == nil || file.File.Name != "late.txt" || file.ReplyTo != first.ReplyTo {
		t.Fatalf("wrong file: %+v", file)
	}
	if body, ok := e.f.fileSent(file.File.Path); !ok || body != "late artifact" {
		t.Fatalf("bad artifact: %q", body)
	}
	e.noReply(200 * time.Millisecond)
}

func TestNativeContinuationCannotProposeJobs(t *testing.T) {
	e := continuationEnv(t, "telegram")
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND BG_JOB")
	e.reply()
	e.reply()
	late := e.reply()
	want(t, late, "BACKGROUND_DELIVERED")
	if strings.Contains(late.Text, "heimdall-job") || strings.Contains(late.Text, "unsolicited") {
		t.Fatalf("autonomous control block leaked: %q", late.Text)
	}
	e.noReply(100 * time.Millisecond)
}

func TestNativeBackgroundSurvivesRestartAsInterruption(t *testing.T) {
	for _, stage := range []string{"running", "notification-gap"} {
		t.Run(stage, func(t *testing.T) {
			e := continuationEnv(t, "telegram")
			who := core.Principal{Platform: "fake", ID: "owner"}
			conv := core.ConvKey{Interface: "telegram", Chat: "42"}
			prompt := "BACKGROUND_NEVER"
			if stage == "notification-gap" {
				prompt = "BACKGROUND BG_NORESULT"
			}
			e.sendIn(who, conv, prompt)
			want(t, e.reply(), "turn=1")
			if stage == "notification-gap" {
				want(t, e.reply(), "background task completed")
			}
			e.sendIn(who, conv, "!status")
			want(t, e.reply(), "native background turn")
			id, clean, err := e.st.Session(conv, who.Key(), "claude")
			if err != nil || id == "" || clean {
				t.Fatalf("background binding was clean: %q %v %v", id, clean, err)
			}
			e.stop()
			stopOnce.Delete(e.st)
			e.start()
			want(t, e.reply(), "interrupted", "not re-run")
			e.sendIn(who, conv, "after restart")
			want(t, e.reply(), "turn=1")
			nextID, nextClean, nextErr := e.st.Session(conv, who.Key(), "claude")
			if nextErr != nil || !nextClean || nextID == id {
				t.Fatalf("unclean native session resumed after restart: old=%q next=%q clean=%v err=%v", id, nextID, nextClean, nextErr)
			}
			e.noReply(100 * time.Millisecond)
		})
	}
}

func TestIdleBackgroundStopUsesCallerAuthority(t *testing.T) {
	for _, caller := range []string{"owner", "employee", "other"} {
		t.Run(caller, func(t *testing.T) {
			e := continuationEnv(t, "telegram")
			conv := core.ConvKey{Interface: "telegram", Chat: "42"}
			employee := core.Principal{Platform: "fake", ID: "employee"}
			e.sendIn(employee, conv, "BACKGROUND_NEVER")
			e.reply()
			before, _, err := e.st.Session(conv, employee.Key(), "claude")
			if err != nil {
				t.Fatal(err)
			}
			e.sendIn(core.Principal{Platform: "fake", ID: caller}, conv, "!stop")
			reply := e.reply()
			if caller == "other" {
				want(t, reply, "No active run to stop")
				e.sendIn(employee, conv, "!status")
				want(t, e.reply(), "native background turn")
				e.sendIn(employee, conv, "!stop")
				want(t, e.reply(), "Stopping background work")
			} else {
				want(t, reply, "Stopping background work")
			}
			e.noReply(150 * time.Millisecond)
			eventuallyTrue(t, "background stopped", e.r.Idle)
			e.sendIn(employee, conv, "after stop")
			want(t, e.reply(), "turn=1")
			after, _, err := e.st.Session(conv, employee.Key(), "claude")
			if err != nil || before == after {
				t.Fatalf("stopped native session resumed: %q %q %v", before, after, err)
			}
		})
	}
}

func TestWarmPendingApprovalNeverFallsBack(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"claude", "codex"}, "claude", "codex")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Accounts = warmAccounts{e.opt.Accounts}
	e.start()
	e.send(owner, "c1", "hello")
	e.reply()
	e.send(owner, "c1", "ASK_PRE_REPLAY")
	want(t, e.reply(), "needs a native approval")
	if strings.Contains(e.logs.String(), `harness "codex"`) {
		t.Fatalf("approval fell back: %s", e.logs.String())
	}
	e.noReply(100 * time.Millisecond)
}

func assertNoRunningNative(t *testing.T, e *env) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+e.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM turns WHERE state='running'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("orphaned native turn: n=%d err=%v", n, err)
	}
}
func TestNativeInvalidationSettlesAcceptedTurn(t *testing.T) {
	for _, action := range []string{"reset", "use", "role", "revoke"} {
		t.Run(action, func(t *testing.T) {
			e := continuationEnv(t, "telegram")
			who := core.Principal{Platform: "fake", ID: "owner"}
			conv := core.ConvKey{Interface: "telegram", Chat: "42"}
			if action == "role" || action == "revoke" {
				e.stop()
				stopOnce.Delete(e.st)
				dir := t.TempDir()
				for name, value := range map[string]string{"whitelist.json": `{"users":["7"]}`, "blacklist.json": `{}`, "admins.json": `{}`, "identities.json": `{}`, "capabilities.json": `{}`} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
						t.Fatal(err)
					}
				}
				t.Setenv("ADMIN_USER_IDS", "7")
				e.opt.Access = core.Access{Dir: dir, Mode: core.ModeCorporate}
				e.start()
				who = core.Principal{Platform: "telegram", ID: "7"}
			}
			e.sendIn(who, conv, "BACKGROUND BG_SLOW")
			e.reply()
			switch action {
			case "reset":
				e.sendIn(who, conv, "!reset")
				e.reply()
			case "use":
				e.sendIn(who, conv, "!use default")
				e.reply()
			case "role":
				t.Setenv("ADMIN_USER_IDS", "")
			case "revoke":
				t.Setenv("ADMIN_USER_IDS", "")
				os.WriteFile(filepath.Join(e.opt.Access.Dir, "whitelist.json"), []byte(`{"users":[]}`), 0600)
			}
			e.noReply(1800 * time.Millisecond)
			eventuallyTrue(t, "invalidated background settled", e.r.Idle)
			assertNoRunningNative(t, e)
		})
	}
}

func TestNativeRetirementAfterBufferedAnswerNotifiesOnce(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Harness.MaxLifetimeSeconds = 1
	e.start()
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND BG_NORESULT")
	e.reply()
	e.reply()
	want(t, e.reply(), "Pending background work was interrupted")
	e.noReply(250 * time.Millisecond)
	eventuallyTrue(t, "retired background settled", e.r.Idle)
	assertNoRunningNative(t, e)
}

func TestNativeContinuationTransactionRefusalStopsWorker(t *testing.T) {
	e := continuationEnv(t, "telegram")
	db, err := sql.Open("sqlite", "file:"+e.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TRIGGER refuse_native BEFORE INSERT ON turns WHEN NEW.harness='claude' BEGIN SELECT RAISE(ABORT,'synthetic continuation storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, conv, "BACKGROUND_NEVER")
	saw := false
	for i := 0; i < 3; i++ {
		o := e.reply()
		if strings.Contains(o.Text, "Background work could not be recorded") {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatal("native transaction refusal not reported")
	}
	eventuallyTrue(t, "refused worker stopped", e.r.Idle)
	assertNoRunningNative(t, e)
}

func TestPendingGenericStartStopAndShutdown(t *testing.T) {
	for _, name := range []string{"pi", "codex"} {
		for _, action := range []string{"stop", "shutdown"} {
			t.Run(name+"/"+action, func(t *testing.T) {
				e := setup(t, core.ModeCorporate, []string{name}, name)
				e.stop()
				stopOnce.Delete(e.st)
				e.opt.Accounts = warmAccounts{e.opt.Accounts}
				e.platform = "telegram"
				e.start()
				who := core.Principal{Platform: "fake", ID: "owner"}
				conv := core.ConvKey{Interface: "telegram", Chat: "42"}
				e.sendIn(who, conv, "AUTONOMOUS")
				e.reply()
				for {
					e.sendIn(who, conv, "!status")
					if strings.Contains(e.reply().Text, "native background turn") {
						break
					}
				}
				e.sendIn(who, conv, "MUST_NOT_START")
				eventuallyTrue(t, "native start is gated", func() bool { return len(e.r.warm.Status(conv)) == 0 })
				if action == "stop" {
					e.sendIn(who, conv, "!stop")
					want(t, e.reply(), "Stopping current run")
					eventuallyTrue(t, "pending stop settles", e.r.Idle)
					e.noReply(150 * time.Millisecond)
				} else {
					done := make(chan struct{})
					go func() { e.stop(); close(done) }()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						os.WriteFile(filepath.Join(e.state, "release-autonomous"), nil, 0600)
						select {
						case <-done:
						case <-time.After(10 * time.Second):
							t.Fatal("shutdown still blocked after releasing native gate")
						}
						t.Fatal("shutdown waited for retained process context instead of request cancellation")
					}
				}
				recorded, _ := os.ReadFile(filepath.Join(e.state, "inputs.jsonl"))
				if strings.Contains(string(recorded), "MUST_NOT_START") {
					t.Fatalf("%s executed stopped input: %s", action, recorded)
				}
				pids, _ := os.ReadFile(filepath.Join(e.state, "pids"))
				if len(strings.Fields(string(pids))) != 1 {
					t.Fatalf("%s cold-launched stopped input: %s", action, pids)
				}
			})
		}
	}
}

func TestPendingBackgroundSurvivesIdle(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Harness = harness.WarmConfig{IdleTimeoutSeconds: 1}
	e.start()
	e.sendIn(core.Principal{Platform: "fake", ID: "owner"}, core.ConvKey{Interface: "telegram", Chat: "42"}, "BACKGROUND BG_SLOW")
	want(t, e.reply(), "turn=1")
	want(t, e.reply(), "background task completed")
	want(t, e.reply(), "BACKGROUND_DELIVERED")
}

func TestNativeLossDeduplicatesAndSettlesEverySource(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Harness = harness.WarmConfig{IdleTimeoutSeconds: 1, MaxLifetimeSeconds: 3, RunTimeoutSeconds: 1}
	e.start()
	who := core.Principal{Platform: "fake", ID: "owner"}
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	for i := 0; i < 2; i++ {
		e.sendIn(who, conv, "BACKGROUND_NEVER")
		want(t, e.reply(), "turn=")
	}
	want(t, e.reply(), "Pending background work was interrupted", "worker lifetime reached")
	e.noReply(150 * time.Millisecond)
	eventuallyTrue(t, "all native sources settled", e.r.Idle)
	assertNoRunningNative(t, e)
	db, err := sql.Open("sqlite", "file:"+e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var cancelled int
	if err := db.QueryRow(`SELECT count(*) FROM turns WHERE state='cancelled' AND detail LIKE 'pending background work interrupted:%lifetime%'`).Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != 2 {
		t.Fatalf("durable settlement count=%d, want 2", cancelled)
	}
	pids, _ := os.ReadFile(filepath.Join(e.state, "pids"))
	if len(strings.Fields(string(pids))) != 1 {
		t.Fatal("worker replaced or work replayed")
	}
}

func TestNativeTerminalOnlySettlesDurably(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Harness = harness.WarmConfig{IdleTimeoutSeconds: 1}
	e.start()
	who := core.Principal{Platform: "fake", ID: "owner"}
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	e.sendIn(who, conv, "BACKGROUND BG_TERMINAL_ONLY")
	want(t, e.reply(), "turn=1")
	eventuallyTrue(t, "terminal native task settles", e.r.Idle)
	e.noReply(1200 * time.Millisecond)
	db, err := sql.Open("sqlite", "file:"+e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM turns WHERE state='done' AND detail='native task completed'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("terminal settlement n=%d err=%v", n, err)
	}
}
func TestNativeLossKeepsDifferentCallersSeparate(t *testing.T) {
	e := continuationEnv(t, "telegram")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Harness = harness.WarmConfig{MaxLifetimeSeconds: 2}
	e.start()
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	for _, id := range []string{"owner", "employee"} {
		e.sendIn(core.Principal{Platform: "fake", ID: id}, conv, "BACKGROUND_NEVER")
		want(t, e.reply(), "turn=")
	}
	first := e.reply()
	second := e.reply()
	want(t, first, "Pending background work was interrupted")
	want(t, second, "Pending background work was interrupted")
	if first.ReplyTo == second.ReplyTo {
		t.Fatal("different callers merged into same reply")
	}
	e.noReply(100 * time.Millisecond)
	eventuallyTrue(t, "both callers settled", e.r.Idle)
	assertNoRunningNative(t, e)
}
