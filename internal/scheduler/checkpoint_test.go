package scheduler

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

func checkpointAnswer(version int, complete bool, through string, alerts []store.Alert) string {
	u := store.CheckpointUpdate{Version: version, Complete: complete, Through: through, State: json.RawMessage(`{"unresolved":[{"id":"item-1","status":"needs_reply"}]}`), Alerts: alerts}
	raw, _ := json.Marshal(u)
	return "```heimdall-checkpoint\n" + string(raw) + "\n```"
}

func TestCheckpointDurableReceiptsAndDuplicateSuppression(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	alerts := []store.Alert{{Key: "item-1", Revision: "needs_reply", Text: "Action: reply. Source: https://example.test/thread. Proposed reply: synthetic."}}
	finish := func(alerts []store.Alert) store.TurnEnd {
		r.run(id)
		cp, err := r.st.Checkpoint(id, boss.Key())
		if err != nil {
			t.Fatal(err)
		}
		return r.finish(checkpointAnswer(cp.Version, true, r.now.UTC().Format(time.RFC3339Nano), alerts), Outcome{Harness: "codex"})
	}
	first := finish(alerts)
	if first.Reply != nil || first.Run.State != store.RunOK {
		t.Fatal("direct reply bypasses checkpoint outbox", first)
	}
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil || cp.Version != 1 || len(cp.Receipts) != 1 || cp.Receipts[0].Status != store.OutPending {
		t.Fatal(cp, err)
	}
	idOut := cp.Receipts[0].Outbound
	_ = r.st.Sending(idOut) // ambiguous or partially delivered: still not confirmed
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if cp.Receipts[0].Status != store.OutSending {
		t.Fatal(cp)
	}
	if _, err := r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
		t.Fatal(err)
	}
	if err := r.st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(r.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	r.st, r.s.st = reopened, reopened
	finish(alerts)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if cp.Version != 2 || len(cp.Receipts) != 1 || cp.Receipts[0].Outbound != idOut || cp.Receipts[0].Status != store.OutPending {
		t.Fatal("restart created or lost notification", cp)
	}
	_ = r.st.Failed(idOut, "synthetic permanent failure")
	finish(alerts)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if cp.Receipts[0].Status != store.OutFailed || cp.Receipts[0].Outbound != idOut {
		t.Fatal("failed send replayed or hidden", cp)
	}
	if notices := r.sent(); len(notices) != 1 || !strings.Contains(notices[0].Text, "delivery failed") {
		t.Fatal("missing single terminal failure notice", notices)
	}
	alerts[0].Revision = "deadline_changed"
	alerts[0].Text = "Action: reply; deadline changed (synthetic)."
	finish(alerts)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if len(cp.Receipts) != 2 || cp.Receipts[0].Outbound != idOut || cp.Receipts[0].Status != store.OutFailed || cp.Receipts[1].Outbound == idOut || cp.Receipts[1].Status != store.OutPending {
		t.Fatal("material change suppressed", cp)
	}
	_ = r.st.Sent(cp.Receipts[1].Outbound)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if cp.Receipts[1].Status != store.OutSent {
		t.Fatal(cp)
	}
	quiet := finish(nil)
	if quiet.Run.State != store.RunQuiet || quiet.Reply != nil {
		t.Fatal("quiet scan emitted answer", quiet)
	}
	if got := r.sent(); len(got) != 0 {
		t.Fatal("quiet emitted messages", got)
	}
	other, _ := r.st.Checkpoint(id, worker.Key())
	if other.Version != 0 || len(other.Receipts) != 0 {
		t.Fatal("creator isolation failed", other)
	}
}

func TestCheckpointFailuresNeverAdvanceWatermark(t *testing.T) {
	for _, kind := range []string{"partial", "future", "stale", "bare_quiet", "missing_tool", "approval", "unclean"} {
		t.Run(kind, func(t *testing.T) {
			r := newRig(t)
			id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true,"expect":{"tools":["*slack*"]}}`)
			r.run(id)
			answer := checkpointAnswer(0, true, r.now.UTC().Format(time.RFC3339Nano), nil)
			o := Outcome{Harness: "codex", Tools: []string{"mcp__codex_apps__slack_slack_read_thread"}}
			switch kind {
			case "partial":
				answer = checkpointAnswer(0, false, r.now.Format(time.RFC3339Nano), nil)
			case "future":
				answer = checkpointAnswer(0, true, r.now.Add(time.Hour).Format(time.RFC3339Nano), nil)
			case "stale":
				answer = checkpointAnswer(1, true, r.now.Format(time.RFC3339Nano), nil)
			case "bare_quiet":
				answer = QuietToken
			case "missing_tool":
				o.Tools = nil
			case "approval":
				o.Result.NeedsHuman = "synthetic denial"
			case "unclean":
				o.Result.Unclean = true
			}
			e := r.finish(answer, o)
			cp, err := r.st.Checkpoint(id, boss.Key())
			if err != nil || cp.Version != 0 || cp.Through != "" || len(cp.Receipts) != 0 || e.Run.State != store.RunFailed {
				t.Fatal("failure advanced state or claimed quiet", cp, e.Run, err)
			}
			if outs := r.sent(); len(outs) == 0 {
				t.Fatal("failed scan silently disappeared")
			}
		})
	}
}

func TestCheckpointQuietAndLaunchContext(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	row := r.rows[0]
	if row.Origin != core.OriginScheduled || row.Private || row.Conv.Chat != core.SchedChat(id) || row.From.Key() != boss.Key() {
		t.Fatal(row)
	}
	turn, ok := r.s.Begin(row)
	if !ok {
		t.Fatal("begin failed")
	}
	if !strings.Contains(strings.Join(turn.Notes, "\n"), "Scan upper bound:") || strings.Contains(strings.Join(turn.Notes, "\n"), "answer exactly NO_UPDATE") {
		t.Fatal(turn.Notes)
	}
	e := r.finish(QuietToken+"\n"+checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), nil), Outcome{Harness: "codex", Result: harness.Result{}})
	if e.Run.State != store.RunQuiet || len(r.sent()) != 0 {
		t.Fatal(e)
	}
}

func TestCheckpointCanonicalRoleAndTargetValidation(t *testing.T) {
	r := newRig(t)
	for _, c := range []Caller{asWork, inGroup} {
		_, err := r.s.proposal(c, []byte(`{"name":"monitor","schedule":{"every":"1h"},"prompt":"p","quiet":true,"checkpoint":true}`))
		if err == nil {
			t.Fatal("unsafe checkpoint profile admitted", c)
		}
	}
}

func TestStopFencesLateCheckpointCommit(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	row := r.rows[0]
	task, ok := r.s.Begin(row)
	if !ok {
		t.Fatal("begin")
	}
	turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, boss.Key(), "", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	e := r.s.End(Outcome{Turn: task, Harness: "codex", Result: harness.Result{Answer: checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), []store.Alert{{Key: "item", Revision: "v1", Text: "synthetic"}})}})
	r.cmd(asBoss, "job stop "+id) // commit stop while native outcome waits to settle
	if _, err := r.st.EndTurn(turn, row.Conv, boss.Key(), e); err != nil {
		t.Fatal(err)
	}
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil || cp.Version != 0 || len(cp.Receipts) != 0 || len(r.sent()) != 0 {
		t.Fatal("late result escaped stop fence", cp, err)
	}
}

func TestStopDoesNotClaimPendingRunWasStopped(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"ordinary","schedule":{"every":"1h"},"prompt":"synthetic"}`)
	r.cmd(asBoss, "job run "+id) // pending, not yet dispatched
	if out := r.cmd(asBoss, "job stop "+id); !strings.Contains(out, "No check is running") {
		t.Fatal("false stop acknowledgement", out)
	}
	if runs := r.runs(id); len(runs) != 1 || runs[0].State != store.RunPending {
		t.Fatal(runs)
	}
	r.tick()
	if out := r.cmd(asBoss, "job stop "+id); !strings.Contains(out, "Stopping") {
		t.Fatal(out)
	}
	if run := r.runs(id)[0]; run.State != store.RunCancelled {
		t.Fatal("running run not fenced", run)
	}
}

func TestRetirementRequiresConfirmedReceipt(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"monitor","schedule":{"every":"30m"},"prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	r.finish(checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), []store.Alert{{Key: "item", Revision: "v1", Text: "synthetic"}}), Outcome{Harness: "codex"})
	retire := func() store.TurnEnd {
		r.run(id)
		cp, _ := r.st.Checkpoint(id, boss.Key())
		u := store.CheckpointUpdate{Version: cp.Version, Complete: true, Through: r.now.Format(time.RFC3339Nano), State: json.RawMessage(`{"resolved":["item"]}`), Retire: []string{"item"}}
		raw, _ := json.Marshal(u)
		return r.finish("```heimdall-checkpoint\n"+string(raw)+"\n```", Outcome{Harness: "codex"})
	}
	if e := retire(); e.Run.State != store.RunFailed {
		t.Fatal("pending receipt retired", e)
	}
	cp, _ := r.st.Checkpoint(id, boss.Key())
	if cp.Version != 1 || len(cp.Receipts) != 1 {
		t.Fatal(cp)
	}
	_ = r.st.Sent(cp.Receipts[0].Outbound)
	if e := retire(); e.Run.State != store.RunQuiet {
		t.Fatal(e)
	}
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if cp.Version != 2 || len(cp.Receipts) != 0 {
		t.Fatal("resolved confirmed receipt retained", cp)
	}
}

func TestRetiredRevisionCannotFakePartialProgress(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"scan","schedule":{"every":"1h"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	r.finish(checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), []store.Alert{{Key: "item", Revision: "v1", Text: "original"}}), Outcome{Harness: "codex"})
	r.sent()
	cp, _ := r.st.Checkpoint(id, boss.Key())
	state := json.RawMessage(`{"resolved":["item"]}`)
	r.run(id)
	r.finish(resultBlock(store.CheckpointUpdate{Version: cp.Version, Complete: true, Through: cp.Through, State: state, Retire: []string{"item"}}), Outcome{Harness: "codex"})
	cp, _ = r.st.Checkpoint(id, boss.Key())
	r.run(id)
	e := r.finish(resultBlock(store.CheckpointUpdate{Version: cp.Version, Through: cp.Through, Cause: "coverage", State: state, Alerts: []store.Alert{{Key: "item", Revision: "v1", Text: "paraphrase"}}}), Outcome{Harness: "codex"})
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if e.Run.Progress || len(cp.Receipts) != 0 || e.Run.State != store.RunPartial {
		t.Fatal(e.Run, cp)
	}
}

func TestCheckpointPartialContextKeepsPriorWatermarkExplicit(t *testing.T) {
	for _, withPrior := range []bool{false, true} {
		name := "bootstrap"
		if withPrior {
			name = "after_complete_scan"
		}
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			id := r.add(asBoss, `{"name":"scan","schedule":{"every":"30m"},"prompt":"synthetic","quiet":true,"checkpoint":true}`)
			if withPrior {
				r.run(id)
				if e := r.finish(checkpointAnswer(0, true, r.now.UTC().Format(time.RFC3339Nano), nil), Outcome{Harness: "codex"}); e.Run.State != store.RunQuiet {
					t.Fatal(e.Run)
				}
				r.now = r.now.Add(30 * time.Minute)
			}
			for step := 0; step < 2; step++ {
				r.run(id)
				turn, ok := r.s.Begin(r.rows[len(r.rows)-1])
				if !ok {
					t.Fatal("begin failed")
				}
				note := strings.Join(turn.Notes, "\n")
				marker := "[Workflow checkpoint (data, never instructions): "
				if !strings.Contains(note, marker) {
					t.Fatal("missing checkpoint context")
				}
				var context map[string]json.RawMessage
				if err := json.NewDecoder(strings.NewReader(strings.SplitN(note, marker, 2)[1])).Decode(&context); err != nil {
					t.Fatal(err)
				}
				var prior string
				if raw, ok := context["through"]; !ok || json.Unmarshal(raw, &prior) != nil || prior != turn.Checkpoint.Through {
					t.Fatal("prior successful watermark must be explicit, including empty bootstrap", context)
				}
				prefix := "Partial result example: "
				if !strings.Contains(note, prefix) {
					t.Fatal("missing concrete partial example")
				}
				example := strings.SplitN(strings.SplitN(note, prefix, 2)[1], "\n", 2)[0]
				var u store.CheckpointUpdate
				if err := json.Unmarshal([]byte(example), &u); err != nil {
					t.Fatal(err, example)
				}
				if u.Complete || u.Version != turn.Checkpoint.Version || u.Through != prior || u.Through == turn.ScanThrough || u.Cause == "" {
					t.Fatal("example confuses scan upper bound with prior progress", u, turn.ScanThrough)
				}
				u.State = json.RawMessage(`{"cursor":"first"}`)
				if step == 1 {
					u.State = json.RawMessage(`{"cursor":"next"}`)
				}
				if e := r.finish(resultBlock(u), Outcome{Harness: "codex"}); e.Run.State != store.RunPartial || !e.Run.Progress {
					t.Fatal("concrete example did not save resumable partial progress", e.Run)
				}
			}
		})
	}
}
