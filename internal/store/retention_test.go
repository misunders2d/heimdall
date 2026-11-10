package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// retentionFixture stores one of each row kind PruneText distinguishes and
// backdates the "old" ones past the cutoff.
type retentionFixture struct {
	s                                  *Store
	p                                  core.Principal
	k, review                          core.ConvKey
	job                                string
	sentOld, sentNew, pendingOld, held int64
	sendingOld, alertLive, alertDone   int64
	effect                             int64
	reviewTurn, plainTurn              int64
}

const day = 24 * time.Hour

func newRetentionFixture(t *testing.T, s *Store) *retentionFixture {
	t.Helper()
	f := &retentionFixture{s: s, p: core.Principal{Platform: "telegram", ID: "1"}, k: core.ConvKey{Interface: "telegram", Chat: "1"}}
	add := func(in core.Inbound) InboundRow {
		t.Helper()
		row, dup, err := s.AddInbound(in)
		if err != nil || dup {
			t.Fatal(in.MsgID, dup, err)
		}
		return row
	}
	live := func(id, text string) core.Inbound {
		return core.Inbound{Conv: f.k, From: f.p, MsgID: id, Origin: core.OriginLive, Private: true, Text: text}
	}
	// Old live message with context, run as a turn that answered.
	in := live("old-done", "old secret text")
	in.Context, in.OAuthActor = "old thread context", "U1"
	row := add(in)
	turn, err := s.StartTurn(f.k, core.RoleOwner, f.p.Key(), "old-done", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	f.plainTurn = turn
	if f.sentOld, err = s.EndTurn(turn, f.k, f.p.Key(), TurnEnd{State: TurnDone, ItemState: InDone, Detail: "native task old notice", Reply: &core.Outbound{Conv: f.k, ReplyTo: "old-done", Text: "old reply text"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sent(f.sentOld); err != nil {
		t.Fatal(err)
	}
	// Old, never started; old mcp control command; new answered message.
	add(live("old-pending", "old pending text"))
	ctl := live("old-control", "old control text")
	ctl.MCPControl = true
	if err := s.SetInbound(add(ctl).ID, InCommand, "status"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetInbound(add(live("new-done", "new text")).ID, InDone, ""); err != nil {
		t.Fatal(err)
	}
	// Outbound in every state.
	enq := func(text string) int64 {
		id, err := s.Enqueue(core.Outbound{Conv: f.k, Text: text})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	f.sentNew = enq("new reply text")
	if err := s.Sent(f.sentNew); err != nil {
		t.Fatal(err)
	}
	f.pendingOld = enq("old pending reply")
	f.held = enq("old held reply")
	f.sendingOld = enq("old sending reply")
	if err := s.Sending(f.sendingOld); err != nil {
		t.Fatal(err)
	}
	if f.effect, err = s.EnqueueEffect("task/1/effect", core.Outbound{Conv: f.k, Text: "old effect text"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Failed(f.effect, "gone"); err != nil {
		t.Fatal(err)
	}
	// A workflow job with one live and one retired alert receipt.
	f.job = "jret"
	j := Job{ID: f.job, Creator: f.p, CreatedIn: f.k, CreatedPrivate: true, State: JobActive, Def: `{}`}
	if err := s.AnswerCommand(0, f.k, CommandEnd{Job: &JobChange{Add: &j}}); err != nil {
		t.Fatal(err)
	}
	f.alertLive, f.alertDone = enq("old live alert text"), enq("old retired alert text")
	for _, id := range []int64{f.alertLive, f.alertDone} {
		if err := s.Sent(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO job_alerts(job_id,creator,key,revision,outbound_id,retired) VALUES(?,?,'k','r1',?,0),(?,?,'k','r0',?,1)`, f.job, f.p.Key(), f.alertLive, f.job, f.p.Key(), f.alertDone); err != nil {
		t.Fatal(err)
	}
	// An old durable review turn: its detail is the result durable reads back.
	f.review = core.ConvKey{Interface: "telegram", Chat: core.ReviewChat("MEL-1")}
	rin := core.Inbound{Conv: f.review, From: f.p, MsgID: "MEL-1/1/read/1", Origin: core.OriginCommitment, Text: "old review prompt",
		Review: &core.Review{Issue: "MEL-1", Attempt: 1, Key: "MEL-1/1/read/1", Target: f.k, Message: "old dispatch brief"}}
	rrow := add(rin)
	if f.reviewTurn, err = s.StartTurn(f.review, core.RoleOwner, f.p.Key(), "", []int64{rrow.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EndTurn(f.reviewTurn, f.review, f.p.Key(), TurnEnd{State: TurnDone, ItemState: InDone, Detail: "read result: old worker report"}); err != nil {
		t.Fatal(err)
	}
	// Ambient context, old and new.
	for _, a := range []Ambient{{Conv: f.k, MsgID: "a-old", At: time.Now().Add(-40 * day), Who: f.p.Key(), Text: "old ambient"}, {Conv: f.k, MsgID: "a-new", At: time.Now(), Who: f.p.Key(), Text: "new ambient"}} {
		if err := s.AddAmbient(a); err != nil {
			t.Fatal(err)
		}
	}
	// Backdate everything "old" to 40 days ago.
	old := time.Now().Add(-40 * day).UnixMilli()
	for _, q := range []string{
		`UPDATE inbound SET received_at=? WHERE msg_id LIKE 'old-%' OR msg_id LIKE 'MEL-1/%'`,
		`UPDATE outbound SET created_at=? WHERE text LIKE 'old %'`,
		`UPDATE turns SET ended_at=? WHERE ended_at IS NOT NULL`,
	} {
		if _, err := s.db.Exec(q, old); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`UPDATE outbound SET state=? WHERE id=?`, OutHeld, f.held); err != nil {
		t.Fatal(err)
	}
	return f
}

// lookups is every existing read that identity, authority, dedupe and
// receipts depend on, including each lookup in checkpoints.go.
func (f *retentionFixture) lookups(t *testing.T) map[string]any {
	t.Helper()
	s := f.s
	out := map[string]any{}
	errText := func(err error) string {
		if err != nil {
			return err.Error()
		}
		return ""
	}
	out["OwnerIngress"] = errText(s.OwnerIngress(f.k, []string{f.p.Key()}))
	bp, bk, err := s.ControlBinding(f.p.Key())
	out["ControlBinding"] = []any{bp, bk, errText(err)}
	bp, bk, err = s.ControlBinding(f.p.Key(), &f.k)
	out["ControlBinding/source"] = []any{bp, bk, errText(err)}
	out["ControlOrigin"] = errText(s.ControlOrigin(f.p, f.k, true))
	cp, err := s.Checkpoint(f.job, f.p.Key())
	out["Checkpoint"] = []any{cp, errText(err)}
	known, err := s.AlertKnown(f.job, "k", "r0")
	out["AlertKnown"] = []any{known, errText(err)}
	pk, ok, err := s.PrivateChat(f.p)
	out["PrivateChat"] = []any{pk, ok, errText(err)}
	for _, id := range []string{"old-done", "old-control", "old-pending"} {
		o, ok, err := s.InboundOutcome(f.k, id)
		// Only durable review turns read a turn's detail back
		// (router/review.go); a plain turn's detail is pruned text.
		o.TurnDetail = ""
		out["InboundOutcome/"+id] = []any{o, ok, errText(err)}
	}
	o, ok, err := s.InboundOutcome(f.review, "MEL-1/1/read/1")
	out["InboundOutcome/review"] = []any{o, ok, errText(err)}
	for _, id := range []int64{f.sentOld, f.effect, f.alertLive, f.pendingOld, f.held, f.sendingOld} {
		r, ok, err := s.OutboundReceipt(id)
		out["OutboundReceipt/"+time.Duration(id).String()] = []any{r, ok, errText(err)}
	}
	return out
}

func TestPruneTextRemovesOldTextKeepsEverythingElse(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f := newRetentionFixture(t, s)
	before := f.lookups(t)
	p, err := s.PruneText(context.Background(), time.Now().Add(-30*day))
	if err != nil {
		t.Fatal(err)
	}
	// old-done, old-control, review inbound; sentOld, effect, retired alert;
	// the old answered turn; the old ambient row.
	if want := (Pruned{Inbound: 3, Outbound: 3, Turns: 1, Ambient: 1}); p != want {
		t.Fatalf("pruned %+v, want %+v", p, want)
	}
	after := f.lookups(t)
	for k, v := range before {
		if !reflect.DeepEqual(v, after[k]) {
			t.Errorf("%s changed by pruning:\n before %+v\n after  %+v", k, v, after[k])
		}
	}

	text := func(q string, args ...any) string {
		t.Helper()
		var v string
		if err := s.db.QueryRow(q, args...).Scan(&v); err != nil {
			t.Fatal(q, err)
		}
		return v
	}
	for id, want := range map[string]string{"old-done": "", "old-control": "", "old-pending": "old pending text", "new-done": "new text"} {
		if got := text(`SELECT text FROM inbound WHERE msg_id=?`, id); got != want {
			t.Errorf("inbound %s text %q, want %q", id, got, want)
		}
	}
	if got := text(`SELECT text FROM inbound WHERE msg_id='MEL-1/1/read/1'`); got != "" {
		t.Errorf("review inbound text kept: %q", got)
	}
	if got := text(`SELECT extra FROM inbound WHERE msg_id='old-done'`); got != `{"oauth_actor":"U1"}` {
		t.Errorf("old-done extra %s: want context removed, oauth actor kept", got)
	}
	if got := text(`SELECT coalesce(json_extract(extra,'$.mcp_control'),0) FROM inbound WHERE msg_id='old-control'`); got != "1" {
		t.Errorf("mcp_control lost: %s", got)
	}
	if got := text(`SELECT json_extract(extra,'$.review.message') IS NULL AND json_extract(extra,'$.review.key')='MEL-1/1/read/1' FROM inbound WHERE msg_id='MEL-1/1/read/1'`); got != "1" {
		t.Errorf("review extra: want message removed, binding kept")
	}
	for id, want := range map[int64]string{f.sentOld: "", f.effect: "", f.alertDone: "", f.sentNew: "new reply text", f.pendingOld: "old pending reply", f.held: "old held reply", f.sendingOld: "old sending reply", f.alertLive: "old live alert text"} {
		if got := text(`SELECT text FROM outbound WHERE id=?`, id); got != want {
			t.Errorf("outbound %d text %q, want %q", id, got, want)
		}
	}
	if got := text(`SELECT detail FROM turns WHERE id=?`, f.plainTurn); got != "" {
		t.Errorf("old turn detail kept: %q", got)
	}
	if got := text(`SELECT detail FROM turns WHERE id=?`, f.reviewTurn); got != "read result: old worker report" {
		t.Errorf("review turn detail %q pruned", got)
	}
	amb, err := s.RecentAmbient(f.k, nil, 10)
	if err != nil || len(amb) != 1 || amb[0].MsgID != "a-new" {
		t.Fatalf("ambient after prune: %+v %v", amb, err)
	}
	// Dedupe still holds: the same message and effect are never stored twice.
	if _, dup, err := s.AddInbound(core.Inbound{Conv: f.k, From: f.p, MsgID: "old-done", Origin: core.OriginLive, Text: "again"}); err != nil || !dup {
		t.Fatalf("pruned message stored again: dup=%v %v", dup, err)
	}
	if id, err := s.EnqueueEffect("task/1/effect", core.Outbound{Conv: f.k, Text: "again"}); err != nil || id != f.effect {
		t.Fatalf("effect dedupe after prune: %d %v (want %d)", id, err, f.effect)
	}
	pending, err := s.Pending()
	if err != nil || len(pending) != 1 || pending[0].Text != "old pending text" {
		t.Fatalf("pending after prune: %+v %v", pending, err)
	}
	// A second pass finds nothing left to prune.
	if p, err := s.PruneText(context.Background(), time.Now().Add(-30*day)); err != nil || p.Total() != 0 {
		t.Fatalf("second prune %+v %v", p, err)
	}
}

// The production schema at this release's base opens unchanged and prunes:
// retention adds no schema step.
func TestPruneTextOnBaseSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	if err := CreateAtSchema(path, AdminMinSchema); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != AdminMinSchema || Schema() != AdminMinSchema {
		t.Fatalf("schema %d (build %d), want %d: %v", v, Schema(), AdminMinSchema, err)
	}
	f := newRetentionFixture(t, s)
	before := f.lookups(t)
	if p, err := s.PruneText(context.Background(), time.Now().Add(-30*day)); err != nil || p.Total() == 0 {
		t.Fatalf("prune %+v %v", p, err)
	}
	after := f.lookups(t)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("lookups changed:\n%+v\n%+v", before, after)
	}
}

// Many old rows are pruned in several short batches, none left behind.
func TestPruneTextBatches(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k := core.ConvKey{Interface: "telegram", Chat: "9"}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-40 * day)
	n := 2*pruneBatch + 7
	for i := 0; i < n; i++ {
		in := core.Inbound{Conv: k, From: core.Principal{Platform: "telegram", ID: "9"}, MsgID: time.Duration(i).String(), Origin: core.OriginLive, Text: "x"}
		if _, _, err := addInbound(tx, in, old, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`UPDATE inbound SET state=?`, InDone); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	p, err := s.PruneText(context.Background(), time.Now().Add(-30*day))
	if err != nil || p.Inbound != int64(n) {
		t.Fatalf("pruned %+v, want %d inbound: %v", p, n, err)
	}
}
