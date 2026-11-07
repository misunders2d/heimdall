package store

import (
	"github.com/misunders2d/heimdall/internal/core"
	"testing"
)

func TestStartContinuationIsAtomicAndRecoverable(t *testing.T) {
	s := open(t)
	who := "x:u"
	id, err := s.StartTurn(conv, core.RoleOwner, who, "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindTurn(id, conv, who, "claude", "native"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EndTurn(id, conv, who, TurnEnd{State: TurnDone, ItemState: InDone, Harness: "claude", Session: "native", Clean: true}); err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`CREATE TRIGGER refuse_dirty BEFORE UPDATE ON sessions BEGIN SELECT RAISE(ABORT,'synthetic dirty-bind failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.StartContinuation(conv, core.RoleOwner, who, "1", "claude", "native"); err == nil {
		t.Fatal("dirty binding failure accepted")
	}
	var n int
	if err = s.db.QueryRow(`SELECT count(*) FROM turns WHERE state=?`, TurnRunning).Scan(&n); err != nil || n != 0 {
		t.Fatalf("half-created continuation: n=%d err=%v", n, err)
	}
	_, clean, err := s.Session(conv, who, "claude")
	if err != nil || !clean {
		t.Fatalf("rollback changed binding: clean=%v err=%v", clean, err)
	}
	if _, err = s.db.Exec(`DROP TRIGGER refuse_dirty`); err != nil {
		t.Fatal(err)
	}
	native, err := s.StartContinuation(conv, core.RoleOwner, who, "1", "claude", "native")
	if err != nil {
		t.Fatal(err)
	}
	var h string
	if err = s.db.QueryRow(`SELECT harness FROM turns WHERE id=?`, native).Scan(&h); err != nil || h != "claude" {
		t.Fatalf("missing native harness: %q %v", h, err)
	}
	_, clean, err = s.Session(conv, who, "claude")
	if err != nil || clean {
		t.Fatal("continuation session stayed clean")
	}
	rec, err := s.Recover(func(i Interrupted) core.Outbound { return core.Outbound{} }, func(InboundRow) core.Outbound { return core.Outbound{} })
	if err != nil || len(rec.Turns) != 1 || rec.Turns[0].Harness != "claude" || rec.Turns[0].Turn != native {
		t.Fatalf("recovery lost atomic continuation: %+v %v", rec, err)
	}
}
