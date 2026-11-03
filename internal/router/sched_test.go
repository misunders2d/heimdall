package router

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
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
	// A harness failure after ready fails the run with the fixed notice
	// only: native error text never reaches a chat.
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
	if answer.Text != "Sure." || !strings.Contains(card.Text, "Job draft") || !strings.Contains(card.Text, "Next:") {
		t.Fatalf("answer %q card %q", answer.Text, card.Text)
	}
	id := jobIDRE.FindString(card.Text)
	if j, _, _ := e.st.Job(id); j.State != store.JobDraft {
		t.Fatalf("draft %+v", j)
	}
	// Someone else's yes is an ordinary message; the owner's confirms.
	want(t, e.ask(ownerDM, "yes"), "is active")
	if j, _, _ := e.st.Job(id); j.State != store.JobActive {
		t.Fatalf("job %+v", j)
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
