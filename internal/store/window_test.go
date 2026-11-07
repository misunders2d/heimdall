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

func TestDrainCountsOnlyEligibleWindowedPendingAndAllInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	w := &core.DeliveryWindow{TZ: "Europe/Kyiv", Days: []time.Weekday{1, 2, 3, 4, 5}, Start: "12:00", End: "21:00"}
	k := core.ConvKey{Interface: "slack", Chat: "Dtest"}
	weekend, _ := time.Parse(time.RFC3339, "2026-11-07T12:00:00Z")
	monday, _ := time.Parse(time.RFC3339, "2026-11-09T10:00:00Z")
	id, err := s.Enqueue(core.Outbound{Conv: k, Text: "deferred", Window: w})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.nextOutbound("slack", weekend); ok || err != nil {
		t.Fatal(ok, err)
	}
	if n, err := s.drainReplies(weekend); n != 0 || err != nil {
		t.Fatal("closed window blocked drain", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if row, ok, err := s.nextOutbound("slack", monday); err != nil || !ok || row.ID != id {
		t.Fatal("deferred row lost on reopen", row, ok, err)
	}
	if err := s.Retry(id, monday.Add(time.Hour), "in-window backoff"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.drainReplies(monday); n != 1 || err != nil {
		t.Fatal("in-window retry excluded", n, err)
	}
	if err := s.Sending(id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.drainReplies(weekend); n != 1 || err != nil {
		t.Fatal("sending excluded", n, err)
	}
	ordinary, err := s.Enqueue(core.Outbound{Conv: k, Text: "ordinary"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ordinary, monday, "future retry"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.drainReplies(weekend); n != 2 || err != nil {
		t.Fatal("ordinary future retry excluded", n, err)
	}
}
