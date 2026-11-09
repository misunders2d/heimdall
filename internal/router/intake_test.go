package router

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

// tgBoss is a Telegram owner: the control binds only Telegram or Slack
// ingress.
var tgBoss = core.Principal{Platform: core.PlatformTelegram, ID: "42", Name: "boss"}

// askBoss sends text from tgBoss in his private chat and returns the reply.
func (e *env) askBoss(text string) core.Outbound {
	e.t.Helper()
	id := fmt.Sprintf("m%d", e.seq.Add(1))
	select {
	case e.f.in <- core.Inbound{Conv: ownerDM, MsgID: id, From: tgBoss, Origin: core.OriginLive, Text: text, Private: true}:
	case <-time.After(5 * time.Second):
		e.t.Fatal("interface did not accept")
	}
	return e.reply()
}

// intakeStart runs the router with the scheduler, its control socket and
// durable assistant wakes wired as cmd/heimdall does (TaskIntake on), over
// the given databases; stop ends it as a process exit would.
func intakeStart(t *testing.T, e *env, durablePath, ctlDir string) (rt *durable.Runtime, stop func()) {
	t.Helper()
	st, err := store.Open(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if rt, err = durable.Open(durable.Options{Path: durablePath, Outbox: st, Silence: -1}); err != nil {
		t.Fatal(err)
	}
	e.st, e.f = st, newFakeIface()
	jobs := scheduler.New(*e.sched, st, e.opt.Access, e.opt.Log)
	jobs.Tasks = func(who scheduler.Caller, action, issue, title, requestID string) (string, bool) {
		return rt.Task(durable.Caller{Principal: who.Principal, Role: who.Role, Conv: who.Conv, Live: who.Job == "", Private: who.Private, OAuthActor: who.OAuthActor, OAuthInstallation: who.OAuthInstallation}, action, issue, title, requestID)
	}
	jobs.TaskIntake = true
	e.opt.Jobs, e.opt.Work = jobs, rt
	r := New(e.opt, st, namedFakeIface{e.f, "telegram"})
	e.r = r
	rt.SetWake(r.Wake)
	rt.SetAssistant(r)
	if err := rt.Launch(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l, err := jobs.ListenControl(ctx, tgBoss.Key(), uint32(os.Geteuid()), ctlDir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	stopOnce.Store(st, true) // setup's own cleanup must not stop this router
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
		l.Close()
		rt.Close()
		st.Close()
	}
	t.Cleanup(stop)
	return rt, stop
}

// An ordinary owner request (no !work, no manual registration) that the
// assistant accepts as follow-through work enters durable.db through the
// assistant's own native task_add call: every owner request turn is told
// how, resumed sessions included; the record carries the goal and the
// owner's route, and the first wake persists its owned next action and a
// durable wake. A plain question creates nothing. After a restart the task
// is still there, still waiting (no new wake); a retried tool call under
// the same request_id is answered from the record, and the same issue under
// a new request_id is the existing task: no second admission.
func TestOwnerRequestAdmitsDurableTaskWithoutWork(t *testing.T) {
	ctlDir := filepath.Join(t.TempDir(), "ctl")
	t.Setenv("FAKE_CONTROL_SOCKET", filepath.Join(ctlDir, "control.sock"))
	durablePath := filepath.Join(t.TempDir(), "durable.db")
	e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
	e.stop()
	stopOnce.Delete(e.st)
	e.platform = "telegram"
	e.opt.Access.Owners = map[string]bool{tgBoss.Key(): true}
	e.sched = &scheduler.Config{DefaultTZ: "Europe/Kyiv", MaxConcurrent: 2, PerCreator: 1, GraceRecurring: 2 * time.Hour, GraceOneOff: 24 * time.Hour, PauseAfter: 3, Interfaces: []string{"telegram"}}
	rt, stop := intakeStart(t, e, durablePath, ctlDir)

	// A plain question: answered, told about tasks, nothing admitted.
	e.askBoss("what is the PO number?")
	in := fakeharness.Inputs(e.state)
	if len(in) != 1 || !strings.Contains(in[0].Context, scheduler.TaskNote) {
		t.Fatalf("owner turn not told about tasks: %+v", in)
	}
	if views, err := rt.Views(); err != nil || len(views) != 0 {
		t.Fatalf("a question became a task: %+v %v", views, err)
	}

	// The accepted request: the assistant tracks it itself. The title is
	// the goal its wakes get; the fake answers the first wake with WAIT.
	title := "Confirm the supplier PO " + say("WAIT 2h\nrecheck the supplier inbox")
	args, _ := json.Marshal(map[string]string{"action": "task_add", "id": "MEL-300", "title": title, "request_id": "req-300"})
	e.askBoss("please get the supplier to confirm the PO and tell me " + control(string(args)))
	recs := controlRecords(t, e.state)
	if len(recs) != 1 || recs[0].IsError || !strings.Contains(recs[0].Text, "Tracking MEL-300") {
		t.Fatalf("task_add %+v", recs)
	}
	st := phase(t, rt, "MEL-300", durable.PhaseWorking)
	for end := time.Now().Add(20 * time.Second); !strings.HasPrefix(st.Next, "next wake in 2h0m0s: recheck the supplier inbox"); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("no owned next wake: %+v", st)
		}
		st, _ = rt.State("MEL-300")
	}
	views, err := rt.Views()
	if err != nil || len(views) != 1 || views[0].Issue != "MEL-300" || views[0].Title != title || views[0].Worker != "" {
		t.Fatalf("durable record %+v %v", views, err)
	}
	ownerView := durable.Caller{Principal: tgBoss, Role: core.RoleOwner, Conv: ownerDM, Live: true, Private: true}
	if list, _ := rt.Task(ownerView, "list", "", "", ""); !strings.Contains(list, "MEL-300") {
		t.Fatalf("not visible to its creator: %q", list)
	}
	wakes := func() (n int) {
		for _, in := range fakeharness.Inputs(e.state) {
			if strings.Contains(in.Text+in.Context, "Goal: MEL-300: ") {
				n++
			}
		}
		return n
	}
	if n := wakes(); n != 1 {
		t.Fatalf("wakes %d", n)
	}

	// Restart: retained, still waiting, no duplicate admission or wake.
	stop()
	rt, _ = intakeStart(t, e, durablePath, ctlDir)
	if st, err := rt.State("MEL-300"); err != nil || st.Phase != durable.PhaseWorking || !strings.HasPrefix(st.Next, "next wake in 2h0m0s") {
		t.Fatalf("after restart %+v %v", st, err)
	}
	e.askBoss("did you track it? " + control(string(args)))
	recs = controlRecords(t, e.state)
	if len(recs) != 2 || recs[1].Text != recs[0].Text {
		t.Fatalf("retried request not answered from its record: %+v", recs)
	}
	if in := fakeharness.Inputs(e.state); !strings.Contains(in[len(in)-1].Context, scheduler.TaskNote) {
		t.Fatalf("resumed owner request not told about tasks: %+v", in[len(in)-1])
	}
	// The same commitment under a new request_id is the existing task, not
	// a second one.
	again, _ := json.Marshal(map[string]string{"action": "task_add", "id": "MEL-300", "title": "Confirm the supplier PO", "request_id": "req-301"})
	e.askBoss("track the PO confirmation " + control(string(again)))
	if recs = controlRecords(t, e.state); len(recs) != 3 || !strings.Contains(recs[2].Text, "already tracked") {
		t.Fatalf("same issue, new request_id: %+v", recs)
	}
	time.Sleep(300 * time.Millisecond)
	if n := wakes(); n != 1 {
		t.Fatalf("wakes after restart %d", n)
	}
	if views, err := rt.Views(); err != nil || len(views) != 1 {
		t.Fatalf("after restart %+v %v", views, err)
	}
}

// Every live owner request turn, resumed sessions included, is told about
// tasks, and only when task intake is wired: an owner without intake and
// employees never are.
func TestTaskNoteOnlyForOwnerIntake(t *testing.T) {
	e := schedEnv(t, []string{"claude"}, "claude")
	e.ask(ownerDM, "hello")
	if in := fakeharness.Inputs(e.state); len(in) != 1 || strings.Contains(in[0].Text+in[0].Context, "Heimdall tasks:") || !strings.Contains(in[0].Context, scheduler.OwnerNote) {
		t.Fatalf("note without intake: %+v", in)
	}

	ctlDir := filepath.Join(t.TempDir(), "ctl")
	e = setup(t, core.ModeCorporate, []string{"claude"}, "claude")
	e.stop()
	stopOnce.Delete(e.st)
	e.platform = "telegram"
	e.sched = &scheduler.Config{DefaultTZ: "Europe/Kyiv", MaxConcurrent: 2, PerCreator: 1, GraceRecurring: 2 * time.Hour, GraceOneOff: 24 * time.Hour, PauseAfter: 3, Interfaces: []string{"telegram"}}
	intakeStart(t, e, filepath.Join(t.TempDir(), "durable.db"), ctlDir)
	e.ask(group, "hello")
	e.sendIn(core.Principal{Platform: "fake", ID: "employee", Name: "employee"}, core.ConvKey{Interface: "telegram", Chat: "-200"}, "hello from staff")
	e.reply()
	e.ask(group, "and again")
	in := fakeharness.Inputs(e.state)
	if len(in) != 3 {
		t.Fatalf("inputs %+v", in)
	}
	if !strings.Contains(in[0].Context, scheduler.TaskNote) {
		t.Fatalf("owner new session not told: %+v", in[0])
	}
	if s := in[1].Text + in[1].Context; strings.Contains(s, "Heimdall tasks:") || !strings.Contains(s, "Heimdall jobs:") {
		t.Fatalf("employee told about tasks: %q", s)
	}
	// The same session's next owner request is told again (sessions from
	// before intake must not miss it); the jobs note stays first-only.
	if s := in[2].Text + in[2].Context; !strings.Contains(s, scheduler.TaskNote) || strings.Contains(s, "Heimdall jobs:") {
		t.Fatalf("resumed owner request: %q", s)
	}
}
