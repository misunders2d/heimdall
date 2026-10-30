package store

import (
	"path/filepath"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

var conv = core.ConvKey{Interface: "x", Chat: "c", Thread: "t"}

func msg(id string) core.Inbound {
	return core.Inbound{Conv: conv, MsgID: id, From: core.Principal{Platform: "x", ID: "u"}, Origin: core.OriginLive, Text: "hi " + id}
}

func TestInboundDedupeAndPending(t *testing.T) {
	s := open(t)
	a, dup, err := s.AddInbound(msg("1"))
	if err != nil || dup {
		t.Fatal(err, dup)
	}
	if _, dup, _ = s.AddInbound(msg("1")); !dup {
		t.Fatal("duplicate stored twice")
	}
	if _, _, err = s.AddInbound(msg("2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartTurn(conv, core.RoleOwner, "x:u", "1", []int64{a.ID}); err != nil {
		t.Fatal(err)
	}
	p, err := s.Pending()
	if err != nil || len(p) != 1 || p[0].MsgID != "2" {
		t.Fatalf("pending = %+v, %v", p, err)
	}
	if _, err := s.StartTurn(conv, core.RoleOwner, "x:u", "1", []int64{a.ID}); err == nil {
		t.Fatal("a running message joined a second turn")
	}
}

func TestRecoverInterruptsRunningTurnsOnce(t *testing.T) {
	s := open(t)
	a, _, _ := s.AddInbound(msg("1"))
	b, _, _ := s.AddInbound(msg("2"))
	turn, _ := s.StartTurn(conv, core.RoleOwner, "x:u", "1", []int64{a.ID})
	if err := s.CommitTurn(turn, conv, "x:u", "omp", "sess"); err != nil {
		t.Fatal(err)
	}
	if err := s.Steered(turn, b.ID); err != nil {
		t.Fatal(err)
	}
	notices := 0
	cut, err := s.Recover(func(i Interrupted) core.Outbound {
		notices++
		if i.Harness != "omp" || i.Messages != 2 || i.ReplyTo != "1" {
			t.Errorf("interrupted = %+v", i)
		}
		return core.Outbound{Conv: i.Conv, Text: "restarted"}
	})
	if err != nil || len(cut) != 1 || notices != 1 {
		t.Fatalf("recover: %v %+v %d", err, cut, notices)
	}
	if id, clean, _ := s.Session(conv, "x:u", "omp"); id != "sess" || clean {
		t.Fatalf("session after restart = %q clean=%v, want not resumable", id, clean)
	}
	if p, _ := s.Pending(); len(p) != 0 {
		t.Fatalf("interrupted messages would re-run: %+v", p)
	}
	if again, _ := s.Recover(func(Interrupted) core.Outbound { t.Fatal("second notice"); return core.Outbound{} }); len(again) != 0 {
		t.Fatal("recovered twice")
	}
	if n, _ := s.Undelivered(); n != 1 {
		t.Fatalf("undelivered = %d, want the one notice", n)
	}
}

func TestOutboundInOrderPerChat(t *testing.T) {
	s := open(t)
	other := core.ConvKey{Interface: "x", Chat: "d"}
	a, _ := s.Enqueue(core.Outbound{Conv: conv, Text: "a1"})
	s.Enqueue(core.Outbound{Conv: conv, Text: "a2"})
	s.Enqueue(core.Outbound{Conv: other, Text: "d1"})
	next := func() string {
		r, ok, err := s.NextOutbound("x")
		if err != nil || !ok {
			t.Fatalf("next: %v %v", ok, err)
		}
		if err := s.Sending(r.ID); err != nil {
			t.Fatal(err)
		}
		return r.Text
	}
	if got := next(); got != "a1" {
		t.Fatalf("first = %s", got)
	}
	// a1 is in flight: a2 must wait, the other chat proceeds.
	if got := next(); got != "d1" {
		t.Fatalf("second = %s, want other chat", got)
	}
	if _, ok, _ := s.NextOutbound("x"); ok {
		t.Fatal("a2 offered while a1 undelivered")
	}
	// A restart returns in-flight replies to the queue (at-least-once).
	if _, err := s.Recover(func(Interrupted) core.Outbound { return core.Outbound{} }); err != nil {
		t.Fatal(err)
	}
	if got := next(); got != "a1" {
		t.Fatalf("after restart = %s, want a1 again", got)
	}
	if err := s.Sent(a); err != nil {
		t.Fatal(err)
	}
	if got := next(); got != "a2" {
		t.Fatalf("after a1 sent = %s", got)
	}
}

func TestChatHarness(t *testing.T) {
	s := open(t)
	if h, _ := s.ChatHarness(conv); h != "" {
		t.Fatal(h)
	}
	s.SetChatHarness(conv, "codex")
	if h, _ := s.ChatHarness(conv); h != "codex" {
		t.Fatal(h)
	}
	s.SetChatHarness(conv, "")
	if h, _ := s.ChatHarness(conv); h != "" {
		t.Fatal(h)
	}
}

func TestPendingKeepsVerifiedEmailAndKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	// A database from before the email/key step keeps its pending messages.
	all := steps
	steps = all[:2]
	old, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.Exec(`INSERT INTO inbound(iface,chat,thread,msg_id,platform,user_id,user_name,origin,text,received_at,state) VALUES('x','c','t','old','x','u','','live','hi',0,?)`, InPending); err != nil {
		t.Fatal(err)
	}
	old.Close()
	steps = all
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	in := msg("new")
	in.From = core.Principal{Platform: "slack", ID: "T1/U2", Email: "user@example.com", KeyID: "key-1"}
	if _, _, err := s.AddInbound(in); err != nil {
		t.Fatal(err)
	}
	p, err := s.Pending()
	if err != nil || len(p) != 2 {
		t.Fatalf("pending = %+v, %v", p, err)
	}
	if p[0].From.Email != "" || p[0].From.KeyID != "" {
		t.Fatalf("old message gained identity: %+v", p[0].From)
	}
	if p[1].From != in.From {
		t.Fatalf("from = %+v, want %+v", p[1].From, in.From)
	}
}
