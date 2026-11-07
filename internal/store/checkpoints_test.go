package store

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestCheckpointOutboxPrecommitFailureRollsBackAndReopenKeepsReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	p := core.Principal{Platform: "telegram", ID: "1"}
	k := core.ConvKey{Interface: "telegram", Chat: "1"}
	j := Job{ID: "jtest", Creator: p, CreatedIn: k, CreatedPrivate: true, State: JobActive, Def: `{}`}
	if err := s.AnswerCommand(0, k, CommandEnd{Job: &JobChange{Add: &j}}); err != nil {
		t.Fatal(err)
	}
	r := Run{Job: j.ID, Version: 1, Slot: "slot", Due: time.Now(), State: RunPending}
	if _, err := s.CreateRun(r, "", time.Time{}); err != nil {
		t.Fatal(err)
	}
	runs, err := s.Runs(RunFilter{Job: j.ID})
	if err != nil || len(runs) != 1 {
		t.Fatal(runs, err)
	}
	r = runs[0]
	row, err := s.StartRun(r, core.Inbound{Conv: k, From: p, MsgID: "run", Origin: core.OriginScheduled})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.StartTurn(k, core.RoleOwner, p.Key(), "", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	w := &core.DeliveryWindow{TZ: "Europe/Kyiv", Days: []time.Weekday{1, 2, 3, 4, 5}, Start: "12:00", End: "21:00"}
	u := CheckpointUpdate{Complete: false, ScanThrough: time.Now().UTC().Format(time.RFC3339Nano), Cause: "pagination", State: json.RawMessage(`{"cursor":"p2"}`), Target: k, Window: w, Alerts: []Alert{{Key: "request", Revision: "v1", Text: "original"}}}
	e := TurnEnd{State: TurnDone, ItemState: InDone, Run: &RunEnd{ID: r.ID, Job: j.ID, State: RunPartial, Progress: true}, Checkpoint: &u}
	// Interrupt the last insert, after alert/outbox rows were written in the
	// transaction. Nothing from that transaction may escape a failed commit.
	if _, err := s.db.Exec(`CREATE TRIGGER stop_checkpoint BEFORE INSERT ON job_checkpoints BEGIN SELECT RAISE(ABORT,'synthetic precommit interruption'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EndTurn(turn, k, p.Key(), e); err == nil {
		t.Fatal("injected transaction failure ignored")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := s.Checkpoint(j.ID, p.Key())
	if err != nil || cp.Version != 0 || len(cp.Receipts) != 0 {
		t.Fatal("precommit state escaped", cp, err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM outbound`).Scan(&n); err != nil || n != 0 {
		t.Fatal("precommit outbox escaped", n, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER stop_checkpoint`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EndTurn(turn, k, p.Key(), e); err != nil {
		t.Fatal(err)
	}
	cp, _ = s.Checkpoint(j.ID, p.Key())
	if cp.Version != 1 || len(cp.Receipts) != 1 {
		t.Fatal(cp)
	}
	receipt := cp.Receipts[0]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cp, _ = s.Checkpoint(j.ID, p.Key())
	if cp.Version != 1 || cp.ScanThrough != u.ScanThrough || string(cp.State) != string(u.State) || len(cp.Receipts) != 1 || cp.Receipts[0] != receipt {
		t.Fatal("postcommit receipt/cursor changed", cp)
	}
	if err := s.Failed(receipt.Outbound, "synthetic terminal failure"); err != nil {
		t.Fatal(err)
	}
	if err := s.Failed(receipt.Outbound, "synthetic terminal failure"); err != nil {
		t.Fatal(err)
	}
	var extra string
	if err := s.db.QueryRow(`SELECT extra FROM outbound WHERE id=(SELECT failure_notice FROM job_alerts WHERE outbound_id=?)`, receipt.Outbound).Scan(&extra); err != nil {
		t.Fatal(err)
	}
	var notice outExtra
	if json.Unmarshal([]byte(extra), &notice) != nil || notice.Window == nil || notice.Window.TZ != w.TZ || notice.Window.End != w.End {
		t.Fatal("failure notice lost delivery window", extra)
	}
}
