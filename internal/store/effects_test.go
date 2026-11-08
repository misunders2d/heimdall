package store

import (
	"path/filepath"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestEnqueueEffectOncePerKey(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k := core.ConvKey{Interface: "test", Chat: "c"}
	a, err := s.EnqueueEffect("MEL-1/accepted/1", core.Outbound{Conv: k, Text: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Sent(a); err != nil {
		t.Fatal(err)
	}
	// After an ambiguous crash the same effect returns the first reply, in any state.
	again, err := s.EnqueueEffect("MEL-1/accepted/1", core.Outbound{Conv: k, Text: "two"})
	if err != nil || again != a {
		t.Fatalf("repeat %d %v, want %d", again, err, a)
	}
	// A similar key is a different effect: identity is the exact key, not text.
	b, err := s.EnqueueEffect("MEL-1/accepted/11", core.Outbound{Conv: k, Text: "one"})
	if err != nil || b == a {
		t.Fatalf("distinct effect %d %v", b, err)
	}
	rc, ok, err := s.OutboundReceipt(a)
	if err != nil || !ok || rc.State != OutSent || rc.SentAt == 0 {
		t.Fatalf("receipt %+v %v %v", rc, ok, err)
	}
	row, ok, err := s.NextOutbound("test")
	if err != nil || !ok || row.ID != b || row.Text != "one" {
		t.Fatalf("delivery still reads effect rows: %+v %v %v", row, ok, err)
	}
	if _, err := s.EnqueueEffect("", core.Outbound{Conv: k}); err == nil {
		t.Fatal("empty key accepted")
	}
	// The same key on another destination is a collision, not a receipt.
	if _, err := s.EnqueueEffect("MEL-1/accepted/1", core.Outbound{Conv: core.ConvKey{Interface: "test", Chat: "other"}, Text: "one"}); err == nil {
		t.Fatal("effect key reused across routes")
	}
	if _, err := s.EnqueueEffect("MEL-1/accepted/1", core.Outbound{Conv: k, ReplyTo: "m9", Text: "one"}); err == nil {
		t.Fatal("effect key reused for another reply target")
	}
}

func TestEnqueueEffectSkipsLegacyExtra(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, extra := range []string{"legacy-not-json", "{", `{"effect":`} {
		if _, err := s.db.Exec(`INSERT INTO outbound(iface,chat,thread,reply_to,text,extra,state,created_at) VALUES('test','c','','','old',?,?,0)`, extra, OutSent); err != nil {
			t.Fatal(err)
		}
	}
	k := core.ConvKey{Interface: "test", Chat: "c"}
	a, err := s.EnqueueEffect("MEL-2/report/1", core.Outbound{Conv: k, Text: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if b, err := s.EnqueueEffect("MEL-2/report/1", core.Outbound{Conv: k, Text: "new"}); err != nil || b != a {
		t.Fatalf("lookup across legacy rows: %d %v want %d", b, err, a)
	}
}
