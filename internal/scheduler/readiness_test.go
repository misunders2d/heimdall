package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

func reopenRig(t *testing.T, r *rig) {
	t.Helper()
	if err := r.st.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(r.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	r.st, r.s.st = s, s
}

func resultBlock(u store.CheckpointUpdate) string {
	b, _ := json.Marshal(u)
	return "```heimdall-checkpoint\n" + string(b) + "\n```"
}

func TestPartialBootstrapResumesAcrossReopenAndStalls(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"scan","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	partial := func(cursor string) store.TurnEnd {
		r.run(id)
		cp, _ := r.st.Checkpoint(id, boss.Key())
		return r.finish(resultBlock(store.CheckpointUpdate{Version: cp.Version, Complete: false, Through: cp.Through, Cause: "pagination", State: json.RawMessage(fmt.Sprintf(`{"cursor":%q,"range_start":"backlog"}`, cursor))}), Outcome{Harness: "codex"})
	}
	if e := partial("p1"); e.Run.State != store.RunPartial || !e.Run.Progress {
		t.Fatal(e.Run)
	}
	first, _ := r.st.Checkpoint(id, boss.Key())
	if first.Through != "" || first.ScanThrough == "" || !first.Partial {
		t.Fatal(first)
	}
	reopenRig(t, r)
	if e := partial("p2"); e.Run.State != store.RunPartial || r.job(id).Fails != 0 {
		t.Fatal(e.Run, r.job(id))
	}
	cp, _ := r.st.Checkpoint(id, boss.Key())
	if cp.Version != 2 || cp.ScanThrough != first.ScanThrough || cp.Through != "" {
		t.Fatal(cp)
	}
	for n := 1; n <= 3; n++ {
		e := partial("p2")
		if e.Run.Progress || r.job(id).Fails != n {
			t.Fatal(e.Run, r.job(id))
		}
	}
	if r.job(id).State != store.JobPaused || !strings.Contains(texts(r.sent()), "no scan progress") {
		t.Fatal("stall not visible/bounded", r.job(id))
	}
	r.cmd(asBoss, "job resume "+id)
	r.run(id)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	turn, ok := r.s.Begin(r.rows[len(r.rows)-1])
	if !ok || turn.ScanThrough != first.ScanThrough {
		t.Fatal("range moved", turn)
	}
	e := r.finish(resultBlock(store.CheckpointUpdate{Version: cp.Version, Complete: true, Through: turn.ScanThrough, State: json.RawMessage(`{"cursor":"done"}`)}), Outcome{Harness: "codex"})
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if e.Run.State != store.RunQuiet || cp.Through != first.ScanThrough || cp.Partial || cp.ScanThrough != "" {
		t.Fatal(e.Run, cp)
	}
}

func TestCheckpointOverflowParaphraseAndCrashBoundary(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"scan","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	row := r.rows[len(r.rows)-1]
	task, _ := r.s.Begin(row)
	alerts := make([]store.Alert, 23)
	for i := range alerts {
		alerts[i] = store.Alert{Key: fmt.Sprintf("k%02d", i), Revision: "v1", Text: fmt.Sprintf("original %d", i)}
	}
	u := store.CheckpointUpdate{Complete: false, Cause: "budget", State: json.RawMessage(`{"cursor":"next"}`), Alerts: alerts}
	turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, boss.Key(), "", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	e := r.s.End(Outcome{Turn: task, Harness: "codex", Result: harness.Result{Answer: resultBlock(u)}})
	// Before EndTurn's transaction commits, neither state nor outbox is visible.
	if cp, _ := r.st.Checkpoint(id, boss.Key()); cp.Version != 0 || len(cp.Receipts) != 0 {
		t.Fatal(cp)
	}
	if out, ok, _ := r.st.NextOutbound("telegram"); ok {
		t.Fatal("precommit publication", out)
	}
	reopenRig(t, r) // process loss before commit: no proposed effects become durable
	if cp, _ := r.st.Checkpoint(id, boss.Key()); cp.Version != 0 || len(cp.Receipts) != 0 {
		t.Fatal(cp)
	}
	if _, err = r.st.EndTurn(turn, row.Conv, boss.Key(), e); err != nil {
		t.Fatal(err)
	}
	cp, _ := r.st.Checkpoint(id, boss.Key())
	if cp.Version != 1 || len(cp.Receipts) != 23 {
		t.Fatal(cp)
	}
	original := append([]store.AlertReceipt(nil), cp.Receipts...)
	if out := r.sent(); len(out) != 20 {
		t.Fatal("wrong release batch", len(out))
	}
	reopenRig(t, r)
	cp, _ = r.st.Checkpoint(id, boss.Key())
	for i, a := range cp.Receipts {
		if a.Outbound != original[i].Outbound || a.ContentHash != original[i].ContentHash {
			t.Fatal("receipt changed after reopen", a)
		}
	}
	if out := r.sent(); len(out) != 0 {
		t.Fatal("overflow released on reopen", out)
	}
	for i := range alerts {
		alerts[i].Text = "new model paraphrase"
	}
	r.run(id)
	u.Version = cp.Version
	u.Alerts = alerts
	e = r.finish(resultBlock(u), Outcome{Harness: "codex"})
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if e.Run.Progress || len(cp.Receipts) != 23 {
		t.Fatal(e.Run, cp)
	}
	for i, a := range cp.Receipts {
		if a.Outbound != original[i].Outbound || a.ContentHash != original[i].ContentHash {
			t.Fatal("paraphrase replaced original", a)
		}
	}
	if out := r.sent(); len(out) != 4 || out[0].Text != "original 20" || !strings.Contains(out[3].Text, "no scan progress") {
		t.Fatal("overflow lost/rephrased", out)
	}
}

func TestLostControlReplyReopenAndCachedStopCannotKillNewRun(t *testing.T) {
	r, c := controlRig(t)
	id := r.add(asBoss, `{"name":"ordinary","schedule":{"every":"1h"},"prompt":"synthetic"}`)
	ctx := context.Background()
	run := ControlRequest{Action: "run", ID: id, RequestID: "stable-run"}
	first := c.execute(ctx, 1234, run)
	if first.Error != "" {
		t.Fatal(first)
	}
	reopenRig(t, r)
	if got := c.execute(ctx, 1234, run); got != first || len(r.runs(id)) != 1 {
		t.Fatal("lost reply duplicated admitted run", got, r.runs(id))
	}
	r.tick()
	stops := 0
	r.s.Attach(func(row store.InboundRow) { r.rows = append(r.rows, row) }, func(string) {}, func(string) bool { stops++; return true })
	stop := ControlRequest{Action: "stop", ID: id, RequestID: "stable-stop"}
	first = c.execute(ctx, 1234, stop)
	if first.Error != "" || stops != 1 {
		t.Fatal(first, stops)
	}
	r.now = r.now.Add(time.Second)
	run.RequestID = "new-run"
	if got := c.execute(ctx, 1234, run); got.Error != "" {
		t.Fatal(got)
	}
	r.tick()
	reopenRig(t, r)
	if got := c.execute(ctx, 1234, stop); got != first || stops != 1 || r.runs(id)[0].State != store.RunRunning {
		t.Fatal("cached stop affected newer run", got, stops, r.runs(id))
	}
}

func TestControlSocketStaleSafety(t *testing.T) {
	for _, kind := range []string{"stale", "live", "file", "symlink", "foreign", "public_directory"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := controlRig(t)
			dir := filepath.Join(t.TempDir(), "ctl")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "control.sock")
			var old *net.UnixListener
			switch kind {
			case "file":
				if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			case "public_directory":
				if err := os.Chmod(dir, 0755); err != nil {
					t.Fatal(err)
				}
			default:
				var err error
				old, err = net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				old.SetUnlinkOnClose(false)
				defer old.Close()
				if kind != "live" {
					old.Close()
				}
				if kind == "foreign" {
					if os.Geteuid() != 0 {
						t.Skip("foreign socket ownership requires root")
					}
					if err := os.Chown(path, 12345, -1); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l, err := r.s.ListenControl(ctx, boss.Key(), uint32(os.Geteuid()), dir)
			if kind == "stale" {
				if err != nil {
					t.Fatal(err)
				}
				l.Close()
				return
			}
			if err == nil {
				l.Close()
				t.Fatal("unsafe endpoint accepted", kind)
			}
			if kind != "public_directory" {
				if _, err := os.Lstat(path); err != nil {
					t.Fatal("endpoint removed", err)
				}
			}
		})
	}
}

func TestFailedReceiptNoticeAndExactAcknowledgement(t *testing.T) {
	r, c := controlRig(t)
	id := r.add(asBoss, `{"name":"scan","schedule":{"every":"1h"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	r.run(id)
	r.finish(checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), []store.Alert{{Key: "request", Revision: "v1", Text: "original alert"}}), Outcome{Harness: "codex"})
	cp, _ := r.st.Checkpoint(id, boss.Key())
	a := cp.Receipts[0]
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "acknowledge", ID: id, Key: a.Key, Revision: a.Revision, Outbound: a.Outbound, RequestID: "pending"}); got.Error == "" {
		t.Fatal("pending acknowledged", got)
	}
	for i := 0; i < 2; i++ {
		if err := r.st.Failed(a.Outbound, "synthetic failure"); err != nil {
			t.Fatal(err)
		}
	}
	notice, ok, err := r.st.NextOutbound("telegram")
	if err != nil || !ok || !strings.HasSuffix(notice.Text, "— heimdall") || strings.Contains(notice.Text, "original alert") {
		t.Fatal(notice, ok, err)
	}
	if err := r.st.Failed(notice.ID, "notice also failed"); err != nil {
		t.Fatal(err)
	}
	if again := r.sent(); len(again) != 0 {
		t.Fatal("recursive notice", again)
	}
	ack := ControlRequest{Action: "acknowledge", ID: id, Key: a.Key, Revision: a.Revision, Outbound: a.Outbound, RequestID: "ack"}
	wrong := ack
	wrong.Outbound++
	wrong.RequestID = "wrong"
	if got := c.execute(context.Background(), 1234, wrong); got.Error == "" {
		t.Fatal("wrong receipt acknowledged", got)
	}
	got := c.execute(context.Background(), 1234, ack)
	if got.Error != "" || !got.Changed {
		t.Fatal(got)
	}
	reopenRig(t, r)
	if again := c.execute(context.Background(), 1234, ack); again != got {
		t.Fatal("ack reply changed", again)
	}
	cp, _ = r.st.Checkpoint(id, boss.Key())
	if len(cp.Receipts) != 1 || cp.Receipts[0].Status != store.OutFailed || !cp.Receipts[0].Acknowledged {
		t.Fatal("failed history hidden", cp)
	}
	if again := r.sent(); len(again) != 0 {
		t.Fatal("ack replayed alert", again)
	}
}

func TestCheckpointJobVersionAndCreatorFences(t *testing.T) {
	for _, kind := range []string{"version", "creator"} {
		t.Run(kind, func(t *testing.T) {
			r := newRig(t)
			id := r.add(asBoss, `{"name":"scan","schedule":{"every":"1h"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
			r.run(id)
			row := r.rows[0]
			task, _ := r.s.Begin(row)
			turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, boss.Key(), "", []int64{row.ID})
			if err != nil {
				t.Fatal(err)
			}
			e := r.s.End(Outcome{Turn: task, Harness: "codex", Result: harness.Result{Answer: checkpointAnswer(0, true, r.now.Format(time.RFC3339Nano), []store.Alert{{Key: "item", Revision: "v1", Text: "synthetic"}})}})
			who := boss.Key()
			if kind == "version" {
				if err := r.st.UpdateJob(id, store.Set{"version": r.job(id).Version + 1}); err != nil {
					t.Fatal(err)
				}
			} else {
				who = worker.Key()
			}
			_, err = r.st.EndTurn(turn, row.Conv, who, e)
			if kind == "creator" && err == nil {
				t.Fatal("foreign creator committed")
			}
			cp, _ := r.st.Checkpoint(id, boss.Key())
			if cp.Version != 0 || len(cp.Receipts) != 0 {
				t.Fatal("stale result persisted", cp)
			}
		})
	}
}
