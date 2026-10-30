package store

import (
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

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
	c, _, _ := s.AddInbound(msg("3"))
	d, _, _ := s.AddInbound(msg("4"))
	turn, _ := s.StartTurn(conv, core.RoleOwner, "x:u", "1", []int64{a.ID})
	if err := s.BindTurn(turn, conv, "x:u", "omp", "sess"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{b.ID, c.ID, d.ID} {
		if err := s.Steering(turn, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Steered(turn, b.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Unsteer(d.ID); err != nil { // not taken: a follow-up again
		t.Fatal(err)
	}
	turns, steers := 0, 0
	rec, err := s.Recover(func(i Interrupted) core.Outbound {
		turns++
		if i.Harness != "omp" || i.Messages != 2 || i.ReplyTo != "1" {
			t.Errorf("interrupted = %+v", i)
		}
		return core.Outbound{Conv: i.Conv, Text: "restarted"}
	}, func(m InboundRow) core.Outbound {
		steers++
		if m.MsgID != "3" || m.TurnID != turn {
			t.Errorf("unconfirmed = %+v", m)
		}
		return core.Outbound{Conv: m.Conv, ReplyTo: m.MsgID, Text: "maybe steered"}
	})
	if err != nil || len(rec.Turns) != 1 || len(rec.Unconfirmed) != 1 || turns != 1 || steers != 1 {
		t.Fatalf("recover: %v %+v %d %d", err, rec, turns, steers)
	}
	if id, clean, _ := s.Session(conv, "x:u", "omp"); id != "sess" || clean {
		t.Fatalf("session after restart = %q clean=%v, want not resumable", id, clean)
	}
	if p, _ := s.Pending(); len(p) != 1 || p[0].MsgID != "4" {
		t.Fatalf("pending after restart = %+v, want only the message never handed over", p)
	}
	fail := func(Interrupted) core.Outbound { t.Fatal("second notice"); return core.Outbound{} }
	if again, _ := s.Recover(fail, func(InboundRow) core.Outbound { t.Fatal("second notice"); return core.Outbound{} }); len(again.Turns)+len(again.Unconfirmed) != 0 {
		t.Fatal("recovered twice")
	}
	if n, _ := s.Undelivered(); n != 2 {
		t.Fatalf("undelivered = %d, want the two notices", n)
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
	if _, err := s.Recover(func(Interrupted) core.Outbound { return core.Outbound{} }, func(InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
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

func TestAnswerCommandIsAtomic(t *testing.T) {
	s := open(t)
	use := func(id string, h string) error {
		row, _, err := s.AddInbound(msg(id))
		if err != nil {
			t.Fatal(err)
		}
		return s.AnswerCommand(row.ID, conv, CommandEnd{Detail: "use", SetHarness: true, Harness: h, Reply: core.Outbound{Conv: conv, Text: "ok"}})
	}
	if err := use("1", "codex"); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.ChatHarness(conv); h != "codex" {
		t.Fatal(h)
	}
	// A reply that cannot be stored undoes the whole command.
	if _, err := s.db.Exec(`CREATE TRIGGER no_replies BEFORE INSERT ON outbound BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	if err := use("2", ""); err == nil {
		t.Fatal("command recorded without its reply")
	}
	if h, _ := s.ChatHarness(conv); h != "codex" {
		t.Fatalf("harness = %q after a failed command", h)
	}
	if p, _ := s.Pending(); len(p) != 1 || p[0].MsgID != "2" {
		t.Fatalf("pending = %+v, want the unanswered command", p)
	}
	if _, err := s.db.Exec(`DROP TRIGGER no_replies`); err != nil {
		t.Fatal(err)
	}
	p, _ := s.Pending()
	if err := s.AnswerCommand(p[0].ID, conv, CommandEnd{Detail: "use", SetHarness: true, Reply: core.Outbound{Conv: conv, Text: "ok"}}); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.ChatHarness(conv); h != "" {
		t.Fatal(h)
	}
	if n, _ := s.Undelivered(); n != 2 {
		t.Fatalf("undelivered = %d, want both replies", n)
	}
}

func TestPendingKeepsVerifiedCallerAndPrivacy(t *testing.T) {
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
	in.From = core.Principal{Platform: "slack", ID: "user@example.com", Email: "user@example.com", Team: "T0WORK", KeyID: "key-1"}
	in.Private = true
	if _, _, err := s.AddInbound(in); err != nil {
		t.Fatal(err)
	}
	p, err := s.Pending()
	if err != nil || len(p) != 2 {
		t.Fatalf("pending = %+v, %v", p, err)
	}
	if p[0].From.Email != "" || p[0].From.Team != "" || p[0].From.KeyID != "" || p[0].Private {
		t.Fatalf("old message gained identity: %+v", p[0])
	}
	if p[1].From != in.From || !p[1].Private {
		t.Fatalf("pending = %+v, want from %+v, private", p[1], in.From)
	}
}

func TestNextDueIgnoresRepliesWaitingInTheirChat(t *testing.T) {
	s := open(t)
	first, _ := s.Enqueue(core.Outbound{Conv: conv, Text: "1"})
	if err := s.Sending(first); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	if err := s.Retry(first, later, "429"); err != nil {
		t.Fatal(err)
	}
	s.Enqueue(core.Outbound{Conv: conv, Text: "2"}) // due now, but waits for 1
	if _, ok, _ := s.NextOutbound("x"); ok {
		t.Fatal("a reply was offered during its chat's backoff")
	}
	due, ok, err := s.NextDue("x")
	if err != nil || !ok || due.Before(later.Add(-time.Second)) {
		t.Fatalf("next due = %v, %v, %v; want the backoff end %v, not now", due, ok, err, later)
	}
	// While the first is being sent nothing in its chat is due.
	if err := s.Retry(first, time.Now(), "x"); err != nil {
		t.Fatal(err)
	}
	if err := s.Sending(first); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.NextDue("x"); ok {
		t.Fatal("a reply waiting behind one in flight counted as due")
	}
}

// Delivery details beyond the text survive the queue.
func TestOutboundKeepsFormatted(t *testing.T) {
	s := open(t)
	if _, err := s.Enqueue(core.Outbound{Conv: conv, Text: "plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(core.Outbound{Conv: conv, Text: "*rich*", Formatted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(core.Outbound{Conv: conv, Reactions: []core.Reaction{{MsgID: "7", Emoji: "👍"}}}); err != nil {
		t.Fatal(err)
	}
	for i, want := range []bool{false, true, false} {
		r, ok, err := s.NextOutbound(conv.Interface)
		if err != nil || !ok || r.Formatted != want || (i == 2) != (len(r.Reactions) == 1 && r.Reactions[0] == core.Reaction{MsgID: "7", Emoji: "👍"}) {
			t.Fatalf("next = %+v %v %v, want formatted %v", r, ok, err, want)
		}
		if err := s.Sent(r.ID); err != nil {
			t.Fatal(err)
		}
	}
}

// What an interface adds beyond the text survives a restart with the
// message.
func TestPendingKeepsContext(t *testing.T) {
	s := open(t)
	in := msg("1")
	in.Context = "[reply context]\npreview"
	in.Group = "album-1"
	if _, _, err := s.AddInbound(in); err != nil {
		t.Fatal(err)
	}
	p, err := s.Pending()
	if err != nil || len(p) != 1 || p[0].Context != in.Context || p[0].Group != in.Group {
		t.Fatalf("pending = %+v, %v", p, err)
	}
}

// A conversation keeps its newest AmbientKeep context messages, each once;
// other conversations, forum topics included, are separate.
func TestAmbient(t *testing.T) {
	s := open(t)
	topic := core.ConvKey{Interface: conv.Interface, Chat: conv.Chat, Thread: "7"}
	add := func(c core.ConvKey, id string) {
		t.Helper()
		if err := s.AddAmbient(Ambient{Conv: c, MsgID: id, At: time.UnixMilli(1000), Who: "fake:x", Text: "t" + id}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= AmbientKeep+5; i++ {
		add(conv, strconv.Itoa(i))
	}
	add(conv, "3000")
	add(conv, "3000")
	add(topic, "t1")
	got, err := s.RecentAmbient(conv, []string{"3000", "1004"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, a := range got {
		ids = append(ids, a.MsgID)
	}
	if !slices.Equal(ids, []string{"1002", "1003", "1005"}) || got[0].Text != "t1002" || got[0].Who != "fake:x" || !got[0].At.Equal(time.UnixMilli(1000)) {
		t.Fatalf("recent %v %+v", ids, got)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM ambient WHERE thread=?`, conv.Thread).Scan(&n); err != nil || n != AmbientKeep {
		t.Fatalf("kept %d, %v", n, err)
	}
	if all, _ := s.RecentAmbient(conv, nil, 2*AmbientKeep); len(all) != AmbientKeep || all[0].MsgID != "7" {
		t.Fatalf("oldest kept %+v", all[0])
	}
	if got, _ := s.RecentAmbient(topic, nil, 40); len(got) != 1 || got[0].MsgID != "t1" {
		t.Fatalf("topic %+v", got)
	}
}
