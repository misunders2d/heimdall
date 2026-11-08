package router

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

func control(args string) string {
	return "CONTROL=" + base64.StdEncoding.EncodeToString([]byte(args))
}

type controlRecord struct {
	Args    json.RawMessage `json:"args"`
	Text    string          `json:"text"`
	IsError bool            `json:"is_error"`
}

func controlRecords(t *testing.T, state string) []controlRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(state, "control.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out []controlRecord
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var r controlRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(line, err)
		}
		out = append(out, r)
	}
	return out
}

// A scheduled owner run reaches the scheduler through the actual shape: its
// native harness process starts the stdio MCP adapter as a child, which dials
// the control socket; the kernel peer and its process ancestry bind it to the
// router's running scheduled turn. It may read its own job (show, history,
// checkpoint) but never draft or change; the same request from a process that
// is not that run's descendant, or after the run ended, is refused. Its
// checkpoint alert reaches the creator's original private conversation with
// a receipt that is sent only after the interface delivered it.
func TestScheduledRunReadsSchedulerThroughNativeMCPProcess(t *testing.T) {
	ctlDir := filepath.Join(t.TempDir(), "ctl")
	socket := filepath.Join(ctlDir, "control.sock")
	t.Setenv("FAKE_CONTROL_SOCKET", socket)
	e := schedEnv(t, []string{"codex"}, "codex")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	l, err := e.opt.Jobs.ListenControl(ctx, tgOwner.Key(), uint32(os.Geteuid()), ctlDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	// The original request: a live private topic of the same owner.
	topic := core.ConvKey{Interface: "telegram", Chat: "1", Thread: "7"}
	e.f.in <- core.Inbound{Conv: topic, MsgID: "orig", From: tgOwner, Origin: core.OriginLive, Text: "!chatid", Private: true}
	e.reply()

	block := func(version int, alerts []store.Alert) string {
		u := store.CheckpointUpdate{Version: version, Complete: true, Through: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), State: json.RawMessage(`{"tasks":["MEL-753"]}`), Alerts: alerts}
		raw, _ := json.Marshal(u)
		return say("```heimdall-checkpoint\n" + string(raw) + "\n```")
	}
	def := func(prompt string) string {
		d, _ := json.Marshal(map[string]any{"name": "supervisor", "schedule": map[string]string{"every": "1h"}, "harness": "codex", "quiet": true, "checkpoint": true, "prompt": prompt})
		return string(d)
	}
	id := e.addJob(def(control(`{"action":"list"}`) + " " + block(0, []store.Alert{{Key: "MEL-753", Revision: "delivered", Text: "MEL-753 result (synthetic).", To: "tg:1/7"}})))
	want(t, e.ask(ownerDM, "!job run "+id), "Check queued")
	if out := e.reply(); out.Conv != topic || out.Text != "MEL-753 result (synthetic)." {
		t.Fatal("alert not delivered to the original conversation", out)
	}
	// The receipt turns sent only after the interface's delivery is recorded.
	var cp store.Checkpoint
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cp, err = e.st.Checkpoint(id, tgOwner.Key()); err != nil || len(cp.Receipts) == 1 && cp.Receipts[0].Status == store.OutSent {
			break
		}
	}
	if err != nil || cp.Version != 1 || len(cp.Receipts) != 1 || cp.Receipts[0].To != "tg:1/7" || cp.Receipts[0].Status != store.OutSent {
		t.Fatal("original-route receipt", cp, err)
	}
	if recs := controlRecords(t, e.state); len(recs) != 1 || recs[0].IsError || !strings.Contains(recs[0].Text, "supervisor") || strings.Contains(recs[0].Text, `"error"`) {
		t.Fatal("scheduled run could not list its jobs", recs)
	}

	// Its own job: show, history and checkpoint read; pause and create refused.
	reads := []string{
		`{"action":"show","id":"` + id + `"}`,
		`{"action":"history","id":"` + id + `"}`,
		`{"action":"checkpoint","id":"` + id + `"}`,
		`{"action":"pause","id":"` + id + `","request_id":"sched-pause"}`,
		`{"action":"create","request_id":"sched-create","definition":{"name":"x","schedule":{"every":"1h"},"prompt":"p"}}`,
	}
	prompt := ""
	for _, r := range reads {
		prompt += control(r) + " "
	}
	edit, _ := json.Marshal(map[string]string{"prompt": prompt + block(1, nil)})
	t.Logf("edit reply: %q", e.ask(ownerDM, "!job edit "+id+" "+string(edit)).Text)
	want(t, e.ask(ownerDM, "!job run "+id), "Check queued")
	// Quiet scan: no reply; wait for the run's stored end instead.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		runs, _ := e.st.Runs(store.RunFilter{Job: id, Limit: 1})
		if len(runs) == 1 && runs[0].State != store.RunRunning && runs[0].State != store.RunPending && len(controlRecords(t, e.state)) > 1 {
			if runs[0].State != store.RunQuiet {
				t.Fatal("second scan", runs[0].State, runs[0].Cause)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second scan did not end", runs)
		}
	}
	recs := controlRecords(t, e.state)[1:]
	if len(recs) != len(reads) {
		t.Fatal("control calls", recs)
	}
	for i, r := range recs[:3] {
		var res scheduler.ControlResult
		_ = json.Unmarshal([]byte(r.Text), &res)
		if res.Error != "" || res.Reply == "" {
			t.Fatalf("read %d refused: %s", i, r.Text)
		}
	}
	if !strings.Contains(recs[2].Text, `tg:1/7`) || !strings.Contains(recs[2].Text, `"status\":\"sent`) {
		t.Fatal("checkpoint read lacks sent original-route receipt", recs[2].Text)
	}
	for _, r := range recs[3:] {
		if !strings.Contains(r.Text, "scheduled runs may only read") {
			t.Fatal("scheduled run mutated", r.Text)
		}
	}
	if j, _, _ := e.st.Job(id); j.State != store.JobActive {
		t.Fatal("scheduled pause applied", j.State)
	}
	if drafts, _ := e.st.Jobs(store.JobFilter{}); len(drafts) != 1 {
		t.Fatal("scheduled create drafted", len(drafts))
	}

	// Wrong PID: this test process is no scheduled run's descendant; the run
	// has also ended, so no generation remains for anyone.
	res, err := scheduler.CallControl(ctx, socket, scheduler.ControlRequest{Action: "show", ID: id})
	if err == nil || res.Error != "trusted active request origin unavailable" {
		t.Fatal("non-descendant peer admitted", res, err)
	}
}
