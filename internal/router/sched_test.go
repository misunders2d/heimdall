package router

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

// schedEnv is a router with the scheduler enabled, on an interface named
// telegram so jobs can target tg: chats.
func schedEnv(t *testing.T, order []string, installed ...string) *env {
	t.Helper()
	e := setup(t, core.ModeCorporate, order, installed...)
	e.stop()
	stopOnce.Delete(e.st)
	e.platform = "telegram"
	e.sched = &scheduler.Config{DefaultTZ: "Europe/Kyiv", MaxConcurrent: 2, PerCreator: 1, GraceRecurring: 2 * time.Hour, GraceOneOff: 24 * time.Hour, PauseAfter: 3, Interfaces: []string{"telegram"}}
	e.start()
	return e
}

var (
	tgOwner  = core.Principal{Platform: "fake", ID: "owner", Name: "owner"}
	ownerDM  = core.ConvKey{Interface: "telegram", Chat: "1"}
	group    = core.ConvKey{Interface: "telegram", Chat: "-100"}
	jobIDRE  = regexp.MustCompile(`\bj[a-z0-9]{6}\b`)
	sayRE    = regexp.MustCompile(`SAY=\S+`)
	frameSch = `scheduled job \"nightly\" created by this caller`
)

func say(text string) string { return "SAY=" + base64.StdEncoding.EncodeToString([]byte(text)) }

// ask sends text from the owner in conv, private when conv is the owner's
// DM, and returns Heimdall's reply.
func (e *env) ask(conv core.ConvKey, text string) core.Outbound {
	e.t.Helper()
	id := fmt.Sprintf("m%d", e.seq.Add(1))
	select {
	case e.f.in <- core.Inbound{Conv: conv, MsgID: id, From: tgOwner, Origin: core.OriginLive, Text: text, Private: conv == ownerDM}:
	case <-time.After(5 * time.Second):
		e.t.Fatal("interface did not accept")
	}
	return e.reply()
}

// addJob creates an owner job in the owner's DM and returns its ID.
func (e *env) addJob(def string) string {
	e.t.Helper()
	out := e.ask(ownerDM, "!job add "+def)
	id := jobIDRE.FindString(out.Text)
	if !strings.Contains(out.Text, "created") || id == "" {
		e.t.Fatalf("job not created: %q", out.Text)
	}
	return id
}

func (e *env) history(id string) string {
	e.t.Helper()
	return e.ask(ownerDM, "!job history "+id).Text
}

func TestScheduledRunDeliversToItsTarget(t *testing.T) {
	capture := filepath.Join(t.TempDir(), "launch.json")
	t.Setenv("FAKE_CAPTURE_LAUNCH", capture)
	t.Setenv("HEIMDALL_TELEGRAM_TOKEN", "never-in-a-harness")
	e := schedEnv(t, []string{"omp"}, "omp")
	id := e.addJob(`{"name":"nightly","schedule":{"cron":["0 3 * * *"]},"to":"tg:-100","prompt":"check things"}`)
	typed := e.f.typed.Load()
	want(t, e.ask(ownerDM, "!job run "+id), "runs now")
	out := e.reply()
	if out.Conv != group || out.ReplyTo != "" || !out.Formatted {
		t.Fatalf("delivered %+v, want a fresh message in the target group", out)
	}
	// Identity: the creator, in the target chat; no memory, no private
	// chat, no hub scheduler tools; framed as the job.
	want(t, out, "mem=1", "PI_EXTERNAL_CHAT_ID=tg_chat_-100", "PI_EXTERNAL_MCP_PRIVATE=0", "PI_SCHEDULER_TOOLS=0", "PI_EXTERNAL_USER_ID=fake_user_owner", frameSch,
		`PI_EXTERNAL_DEFAULT_DELIVERY={\"chatId\":-100,\"platform\":\"telegram\",\"sessionKey\":\"tg_chat_-100\"}`)
	if raw, err := os.ReadFile(capture); err != nil || !strings.Contains(string(raw), `"heimdall":[]`) {
		t.Fatalf("harness environment: %s %v", raw, err)
	}
	if e.f.typed.Load() != typed {
		t.Fatal("a scheduled run showed typing")
	}
	if h := e.history(id); !strings.Contains(h, " ok") {
		t.Fatalf("history %q", h)
	}
	// A fresh session each run, never a warm chat process.
	want(t, e.ask(ownerDM, "!job run "+id), "runs now")
	if out := e.reply(); !strings.Contains(out.Text, "resumed=false") || out.Conv != group {
		t.Fatalf("second run %q", out.Text)
	}
}

func TestScheduledCodexRequiresSuccessfulRealShapedMCPCompletion(t *testing.T) {
	for _, mode := range []string{"MCP_SCAN_OK", "MCP_SCAN_START_ONLY", "MCP_SCAN_ERROR", "MCP_SCAN_ISERROR"} {
		t.Run(mode, func(t *testing.T) {
			e := schedEnv(t, []string{"codex"}, "codex")
			u := store.CheckpointUpdate{Complete: true, Through: time.Now().UTC().Format(time.RFC3339Nano), State: json.RawMessage(`{"checked":true}`), Alerts: []store.Alert{{Key: "request", Revision: "v1", Text: "synthetic alert"}}}
			raw, _ := json.Marshal(u)
			definition, _ := json.Marshal(map[string]any{"name": "native-check", "schedule": map[string]string{"every": "1h"}, "harness": "codex", "quiet": true, "checkpoint": true, "expect": map[string]any{"tools": []string{"mcp__codex_apps__slack_slack_read_thread"}}, "prompt": mode + " " + say("```heimdall-checkpoint\n"+string(raw)+"\n```")})
			id := e.addJob(string(definition))
			want(t, e.ask(ownerDM, "!job run "+id), "runs now")
			out := e.reply()
			cp, err := e.st.Checkpoint(id, tgOwner.Key())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "MCP_SCAN_OK" {
				if cp.Version != 1 || len(cp.Receipts) != 1 || out.Text != "synthetic alert" {
					t.Fatal(cp, out)
				}
			} else {
				if cp.Version != 0 || len(cp.Receipts) != 0 || !strings.Contains(out.Text, "failed") {
					t.Fatal("start/error satisfied success", cp, out)
				}
			}
		})
	}
}

func TestScheduledCheckpointDeliveryAndRedaction(t *testing.T) {
	t.Setenv("HEIMDALL_TEST_TOKEN", "synthetic-checkpoint-secret")
	e := schedEnv(t, []string{"codex"}, "codex")
	through := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	u := store.CheckpointUpdate{Version: 0, Complete: true, Through: through, State: json.RawMessage(`{"reference":"synthetic-checkpoint-secret"}`), Alerts: []store.Alert{{Key: "request-1", Revision: "v1", Text: "Action: synthetic-checkpoint-secret"}}}
	raw, _ := json.Marshal(u)
	definition, _ := json.Marshal(map[string]any{"name": "checkpoint", "schedule": map[string]string{"every": "30m"}, "prompt": say("```heimdall-checkpoint\n" + string(raw) + "\n```"), "harness": "codex", "quiet": true, "checkpoint": true})
	id := e.addJob(string(definition))
	want(t, e.ask(ownerDM, "!job run "+id), "runs now")
	out := e.reply()
	if out.Conv != ownerDM || out.Text != "Action: [REDACTED]" {
		t.Fatal("structured result escaped output redaction or routing", out)
	}
	if history := e.history(id); !strings.Contains(history, " ok") {
		t.Fatal(history)
	}
	cp, err := e.st.Checkpoint(id, tgOwner.Key())
	if err != nil || cp.Version != 1 || cp.Through != through || len(cp.Receipts) != 1 || cp.Receipts[0].Status != store.OutSent || cp.Receipts[0].Outbound != out.ID || strings.Contains(string(cp.State), "synthetic-checkpoint-secret") {
		t.Fatal("checkpoint/outbox transaction or stored redaction failed", cp, err)
	}
	e.noReply(100 * time.Millisecond)
}

func TestScheduledQuietNotifyAndFiles(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	e.f.files.Store(true)
	quiet := e.addJob(`{"name":"quiet","schedule":{"every":"1h"},"quiet":true,"prompt":"` + say("NO_UPDATE") + `"}`)
	e.ask(ownerDM, "!job run "+quiet)
	e.noReply(1500 * time.Millisecond)
	if h := e.history(quiet); !strings.Contains(h, " quiet") {
		t.Fatalf("history %q", h)
	}
	notify := e.addJob(`{"name":"ping","kind":"notify","schedule":{"every":"1h"},"text":"stand up"}`)
	e.ask(ownerDM, "!job run "+notify)
	if out := e.reply(); out.Text != "stand up" || out.Conv != ownerDM {
		t.Fatalf("notify %+v", out)
	}
	files := e.addJob(`{"name":"files","schedule":{"every":"1h"},"prompt":"report WRITE=a.txt:hello","expect":{"files":1}}`)
	e.ask(ownerDM, "!job run "+files)
	text, file := e.reply(), e.reply()
	if text.Conv != ownerDM || file.File == nil || file.File.Name != "a.txt" || file.Conv != ownerDM {
		t.Fatalf("files: %+v %+v", text, file)
	}
}

// The quiet instruction follows the prompt in the native message, never
// only in Pi's turn context (live Pi ignored it there).
func TestScheduledNoteInNativeMessage(t *testing.T) {
	e := schedEnv(t, []string{"pi"}, "pi")
	id := e.addJob(`{"name":"quiet","schedule":{"every":"1h"},"quiet":true,"prompt":"check things"}`)
	e.ask(ownerDM, "!job run "+id)
	want(t, e.reply(), `last="[Scheduled job: if there is nothing new to report, answer exactly NO_UPDATE`, frameSch[:len(`scheduled job \"`)])
}

// Pi gets the jobs note after the prompt in the first message of a fresh
// native session, not as turn context (live Pi ignored it there); Claude
// keeps it as turn context. Later messages and scheduled turns never carry it.
func TestJobNoteInPiFirstMessage(t *testing.T) {
	for _, h := range []string{"pi", "claude"} {
		t.Run(h, func(t *testing.T) {
			e := schedEnv(t, []string{h}, h)
			e.ask(ownerDM, "hello")
			e.ask(ownerDM, "again")
			in := fakeharness.Inputs(e.state)
			if len(in) != 2 {
				t.Fatalf("inputs %+v", in)
			}
			inText, inContext := strings.Contains(in[0].Text, scheduler.OwnerNote), strings.Contains(in[0].Context, scheduler.OwnerNote)
			if h == "pi" && (!inText || inContext || !strings.HasSuffix(in[0].Text, scheduler.OwnerNote) || !strings.Contains(in[0].Text, "hello")) {
				t.Fatalf("pi first input text %q context %q", in[0].Text, in[0].Context)
			}
			if h == "claude" && (inText || !inContext) {
				t.Fatalf("claude first input text %q context %q", in[0].Text, in[0].Context)
			}
			if in[0].Context == "" {
				t.Fatalf("%s lost its turn context", h)
			}
			if strings.Contains(in[1].Text, "Heimdall jobs:") {
				t.Fatalf("note repeated: %q", in[1].Text)
			}
			id := e.addJob(`{"name":"nightly","schedule":{"every":"1h"},"prompt":"check things"}`)
			e.ask(ownerDM, "!job run "+id)
			e.reply()
			in = fakeharness.Inputs(e.state)
			if last := in[len(in)-1]; !strings.Contains(last.Text, "check things") || strings.Contains(last.Text+last.Context, "Heimdall jobs:") {
				t.Fatalf("scheduled input %+v", last)
			}
		})
	}
}

// A Pi extension asking for interactive input (the pack's ask_user_question,
// an approval, a sign-in) in a scheduled run ends the run at once: failed
// with the needs_human cause in history, and a notice to the creator.
func TestScheduledNeedsHumanFailsFast(t *testing.T) {
	e := schedEnv(t, []string{"pi"}, "pi")
	id := e.addJob(`{"name":"asker","schedule":{"every":"1h"},"prompt":"ASK_HUMAN"}`)
	e.ask(ownerDM, "!job run "+id)
	out := e.reply()
	want(t, out, "failed", "Cause: "+scheduler.NeedsHumanCause, "— heimdall")
	if out.Conv != ownerDM {
		t.Fatalf("notice to %s", out.Conv)
	}
	if h := e.history(id); !strings.Contains(h, " failed") || !strings.Contains(h, scheduler.NeedsHumanCause) {
		t.Fatalf("history %q", h)
	}
}

func TestScheduledFailureAndUnverifiedNotices(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	// An unmet check on a shared target withholds the answer: the target
	// gets the fixed notice, the creator's private chat the answer.
	id := e.addJob(`{"name":"checked","schedule":{"every":"1h"},"to":"tg:-100","prompt":"` + say("secret result") + `","expect":{"prefix":"OK:"}}`)
	e.ask(ownerDM, "!job run "+id)
	got := map[core.ConvKey]string{}
	for range 3 {
		out := e.reply()
		got[out.Conv] += out.Text + "\n"
	}
	if strings.Contains(got[group], "secret result") || !strings.Contains(got[group], "unverified") || !strings.Contains(got[group], "— heimdall") {
		t.Fatalf("group got %q", got[group])
	}
	if !strings.Contains(got[ownerDM], "secret result") || !strings.Contains(got[ownerDM], "⚠ unverified") {
		t.Fatalf("owner got %q", got[ownerDM])
	}
	// A harness failure after ready fails the run with a sanitized notice.
	failing := e.addJob(`{"name":"failing","schedule":{"every":"1h"},"prompt":"FAILPOST=omp"}`)
	e.ask(ownerDM, "!job run "+failing)
	out := e.reply()
	want(t, out, "failed", "Cause: ", "— heimdall")
	if out.Conv != ownerDM {
		t.Fatalf("failure notice to %s", out.Conv)
	}
}

func TestJobDraftConfirmAndNote(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	// The first message of a fresh session carries the note; later ones not.
	if out := e.ask(ownerDM, "hello"); !strings.Contains(out.Text, `head="[Heimdall jobs:`) {
		t.Fatalf("no note in a fresh session: %q", out.Text)
	}
	if out := e.ask(ownerDM, "again"); strings.Contains(out.Text, "Heimdall jobs:") {
		t.Fatalf("note repeated: %q", out.Text)
	}
	block := "Sure.\n```heimdall-job\n{\"name\":\"standup\",\"schedule\":{\"cron\":[\"0 9 * * MON-FRI\"]},\"prompt\":\"summarize\"}\n```"
	answer := e.ask(ownerDM, say(block))
	card := e.reply()
	if answer.Text != "Sure." || !strings.Contains(card.Text, " active:") || !strings.Contains(card.Text, "Next:") {
		t.Fatalf("answer %q card %q", answer.Text, card.Text)
	}
	id := jobIDRE.FindString(card.Text)
	if j, _, _ := e.st.Job(id); j.State != store.JobActive {
		t.Fatalf("draft %+v", j)
	}
	// An invalid block gets an error card; nothing is stored.
	// The answer was only the block: the card alone answers.
	if card := e.ask(ownerDM, say("```heimdall-job\n{\"name\":\"x\",\"schedule\":{\"every\":\"10s\"},\"prompt\":\"p\"}\n```")); !strings.Contains(card.Text, "not valid") {
		t.Fatalf("error card %q", card.Text)
	}
	// No drafts: a bare no is just a message.
	if out := e.ask(ownerDM, "no"); !strings.Contains(out.Text, "omp#") {
		t.Fatalf("no without drafts %q", out.Text)
	}
}

func TestJobStopAndRemove(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	id := e.addJob(`{"name":"slow","schedule":{"every":"1h"},"prompt":"SLEEP"}`)
	e.ask(ownerDM, "!job run "+id)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if strings.Contains(e.ask(ownerDM, "!job stop "+id).Text, "Stopping") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run never stoppable")
		}
	}
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(e.history(id), "cancelled"); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("history %q", e.history(id))
		}
	}
	want(t, e.ask(ownerDM, "!job rm "+id), "deleted")
	want(t, e.ask(ownerDM, "!jobs"), "No jobs")
	e.noReply(500 * time.Millisecond)
}

// A job command's change is stored with its answer, in one transaction: a
// crash between the two can neither lose the answer nor replay the change.
// Here the answer cannot be stored (every outbound insert fails), so the
// command stays pending and is answered after the restart, once: one job.
func TestJobCommandStoredWithItsAnswer(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	db, err := sql.Open("sqlite", "file:"+e.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER no_reply BEFORE INSERT ON outbound BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	e.f.in <- core.Inbound{Conv: ownerDM, MsgID: "add1", From: tgOwner, Origin: core.OriginLive, Private: true,
		Text: `!job add {"name":"once only","schedule":{"every":"1h"},"prompt":"p"}`}
	eventuallyTrue(t, "the answer failed", func() bool { return strings.Contains(e.logs.String(), "command job not recorded") })
	if _, err := db.Exec(`DROP TRIGGER no_reply`); err != nil {
		t.Fatal(err)
	}
	e.stop()
	stopOnce.Delete(e.st)
	e.start()
	want(t, e.reply(), "created")
	jobs, err := e.st.Jobs(store.JobFilter{States: []string{store.JobActive, store.JobPaused}})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("a replayed !job add left %d jobs (%v)", len(jobs), err)
	}
}

// A run is checked as its turn starts, not only when it was handed over: a
// Slack creator deactivated meanwhile (here across a restart, the run's
// message replayed) is refused and the job paused; no harness runs.
func TestScheduledCreatorCheckedAtTurnStart(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	var live atomic.Bool
	live.Store(true)
	e.stop()
	stopOnce.Delete(e.st)
	e.reverify = func(_ context.Context, p core.Principal) (scheduler.Verified, bool, error) {
		return scheduler.Verified{Principal: p, OAuthActor: "sl_user_U1"}, live.Load(), nil
	}
	e.start()
	boss := core.SlackPrincipal("boss@example.com", "T1", "boss")
	e.f.in <- core.Inbound{Conv: group, MsgID: "add1", From: boss, Origin: core.OriginLive, Text: `!job add {"name":"slack job","schedule":{"every":"1h"},"to":"tg:-100","prompt":"check things"}`}
	id := jobIDRE.FindString(e.reply().Text)
	if id == "" {
		t.Fatal("job not created")
	}
	// Handed over as the due loop does after its check, then a restart
	// before the turn started, and the creator deactivated meanwhile.
	e.stop()
	stopOnce.Delete(e.st)
	st, err := store.Open(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateRun(store.Run{Job: id, Version: 1, Slot: "manual:1", Due: time.Now(), Trigger: "manual", State: store.RunPending}, "", time.Time{}); err != nil {
		t.Fatal(err)
	}
	runs, _ := st.Runs(store.RunFilter{Job: id})
	if _, err := st.StartRun(runs[0], core.Inbound{Conv: core.ConvKey{Interface: "telegram", Chat: core.SchedChat(id)}, MsgID: fmt.Sprintf("run:%d:0", runs[0].ID), From: boss, Origin: core.OriginScheduled, Text: "check things", At: time.Now(),
		Run: &core.ScheduledRun{Job: id, Run: runs[0].ID, Target: group}}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	live.Store(false)
	e.start()
	want(t, e.reply(), "refused", "Slack identity no longer verified", "— heimdall")
	if j, _, _ := e.st.Job(id); j.State != store.JobPaused {
		t.Fatalf("job %s (%s)", j.State, j.Reason)
	}
	for _, in := range fakeharness.Inputs(e.state) {
		if strings.Contains(in.Text, "check things") {
			t.Fatal("the refused creator's run reached a harness")
		}
	}
}

// A tool that answers auth_needed (the employee Google runtime does, as an
// ordinary tool result) fails a scheduled run even though the model then
// explains it in a normal answer: the notice names the right sign-in, and
// pause_after such runs pause the job.
func TestScheduledSignInNeededFails(t *testing.T) {
	e := schedEnv(t, []string{"pi"}, "pi")
	id := e.addJob(`{"name":"mail","schedule":{"every":"1h"},"prompt":"SIGNIN_NEEDED check mail"}`)
	for i := range 3 {
		want(t, e.ask(ownerDM, "!job run "+id), "runs now")
		out := e.reply()
		want(t, out, "failed", "sign-in needed: google", "google login", "— heimdall")
		if strings.Contains(out.Text, "omp#") || strings.Contains(out.Text, "pi#") {
			t.Fatalf("run %d delivered the answer: %q", i, out.Text)
		}
		if i == 2 {
			want(t, e.reply(), "was paused", "google login")
		}
	}
	if j, _, _ := e.st.Job(id); j.State != store.JobPaused {
		t.Fatalf("job %s after 3 sign-in failures", j.State)
	}
}

func TestJobRemovalProposalNeedsHuman(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	id := e.addJob(`{"name":"standup","schedule":{"every":"1h"},"prompt":"p"}`)
	card := e.ask(ownerDM, say("```heimdall-job\n{\"remove\":\""+id+"\"}\n```"))
	want(t, card, "remove job "+id, "standup", "Schedule:", "Next:")
	draft := jobIDRE.FindString(card.Text)
	// Model output that looks like a confirmation is delivered, never executed.
	e.ask(ownerDM, say("!job confirm "+draft))
	if j, _, _ := e.st.Job(id); j.State != store.JobActive {
		t.Fatalf("agent confirmed: %+v", j)
	}
	want(t, e.ask(ownerDM, "yes"), "deleted")
	if j, _, _ := e.st.Job(id); j.State != store.JobDeleted {
		t.Fatalf("not removed: %+v", j)
	}
	want(t, e.ask(ownerDM, "!job confirm "+draft), "No draft")
}

func TestOwnerProposalEditAndScheduledBlock(t *testing.T) {
	e := schedEnv(t, []string{"omp"}, "omp")
	block := func(raw string) string { return "```heimdall-job\n" + raw + "\n```" }
	out := e.ask(ownerDM, say(block(`{"name":"new","schedule":{"every":"1h"},"prompt":"read"}`)))
	want(t, out, " active:", "Next:", "Stop: !job rm", "— heimdall")
	if strings.Contains(out.Text, "\n") || strings.Contains(out.Text, "confirm") {
		t.Fatal(out.Text)
	}
	id := jobIDRE.FindString(out.Text)
	if rows, _ := e.st.Jobs(store.JobFilter{States: []string{store.JobDraft}}); len(rows) != 0 {
		t.Fatalf("drafts: %+v", rows)
	}
	out = e.ask(ownerDM, say(block(`{"edit":"`+id+`","name":"edited"}`)))
	want(t, out, "Changed: name")
	if j, _, _ := e.st.Job(id); j.Name != "edited" || j.Version != 2 {
		t.Fatalf("edit: %+v", j)
	}
	// The scheduler runs the newly activated job without restarting.
	e.ask(ownerDM, "!job run "+id)
	out = e.reply()
	if out.Conv != ownerDM {
		t.Fatalf("target: %+v", out)
	}
	// A scheduled owner answer cannot create another job.
	nested := block(`{"name":"nested","schedule":{"every":"1h"},"prompt":"no"}`)
	e.ask(ownerDM, "!job edit "+id+` {"prompt":"`+say(nested)+`"}`)
	e.ask(ownerDM, "!job run "+id)
	e.reply()
	if jobs, _ := e.st.Jobs(store.JobFilter{}); len(jobs) != 1 {
		t.Fatalf("scheduled proposal applied: %+v", jobs)
	}
}

func TestMCPControlProposalNeverAutoApplies(t *testing.T) {
	e := schedEnv(t, []string{"pi"}, "pi")
	e.cancel()
	<-e.done
	stopOnce.Store(e.st, true)
	t.Cleanup(func() { e.st.Close() })
	row, _, err := e.st.AddInbound(core.Inbound{Conv: ownerDM, From: tgOwner, MsgID: "control", Origin: core.OriginLive, Private: true, MCPControl: true, Text: "/mcp login test"})
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.st.StartTurn(ownerDM, core.RoleOwner, tgOwner.Key(), "control", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	tr := &turn{id: id, role: core.RoleOwner, from: tgOwner, who: tgOwner.Key(), private: true, started: time.Now(), batch: []item{{row: row}}}
	tr.done = &doneEvent{harness: "pi", res: harness.Result{Answer: "```heimdall-job\n" + `{"name":"control","schedule":{"every":"1h"},"prompt":"p"}` + "\n```"}}
	c := &conv{r: e.r, key: ownerDM, turn: tr}
	e.r.sem <- struct{}{}
	c.finish(context.Background(), tr)
	jobs, err := e.st.Jobs(store.JobFilter{})
	if err != nil || len(jobs) != 1 || jobs[0].State != store.JobDraft {
		t.Fatalf("MCP job=%+v error=%v", jobs, err)
	}
}
