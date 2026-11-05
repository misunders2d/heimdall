package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestWindowDefersRetriesWithoutBlockingLiveReplies(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := &core.DeliveryWindow{TZ: "Europe/Kyiv", Days: []time.Weekday{1, 2, 3, 4, 5}, Start: "12:00", End: "21:00"}
	k := core.ConvKey{Interface: "slack", Chat: "Dtest"}
	id, err := s.Enqueue(core.Outbound{Conv: k, Text: "scheduled", Window: w})
	if err != nil {
		t.Fatal(err)
	}
	friday, _ := time.Parse(time.RFC3339, "2026-11-06T19:00:00Z")
	if _, ok, err := s.nextOutbound("slack", friday); err != nil || ok {
		t.Fatal("off-hours attempt", ok, err)
	}
	live, err := s.Enqueue(core.Outbound{Conv: k, Text: "live"})
	if err != nil {
		t.Fatal(err)
	}
	if row, ok, err := s.nextOutbound("slack", friday); err != nil || !ok || row.ID != live {
		t.Fatal("monitor blocked live conversation", row, ok, err)
	}
	_ = s.Sent(live)
	monday, _ := time.Parse(time.RFC3339, "2026-11-09T10:00:00Z")
	if row, ok, err := s.nextOutbound("slack", monday); err != nil || !ok || row.ID != id || row.Window == nil {
		t.Fatal("pending alert lost", row, ok, err)
	}
	_ = s.Sending(id)
	_ = s.Retry(id, friday, "synthetic timeout")
	if _, ok, err := s.nextOutbound("slack", friday); err != nil || ok {
		t.Fatal("retry escaped hours", ok, err)
	}
}
