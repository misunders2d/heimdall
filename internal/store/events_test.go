package store

import (
	"path/filepath"
	"testing"
)

func TestInterfaceEventSurvivesRestartAndDedupesCompletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StageEvent("slack", "event1", []byte(`{"event_id":"event1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, ok, err := s.NextEvent("slack")
	if err != nil || !ok || string(e.Payload) != `{"event_id":"event1"}` {
		t.Fatalf("recovery %v %v %+v", err, ok, e)
	}
	if err = s.StageEvent("slack", "event1", []byte("different payload")); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteEvent(e.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.StageEvent("slack", "event1", []byte("redelivery")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = s.NextEvent("slack"); err != nil || ok {
		t.Fatalf("redelivered completed event %v %v", ok, err)
	}
	var payload []byte
	if err = s.db.QueryRow(`SELECT payload FROM interface_events WHERE id=?`, e.ID).Scan(&payload); err != nil || len(payload) != 0 {
		t.Fatal("completed raw payload retained")
	}
	if err = s.StageEvent("other", "event1", []byte("other")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = s.NextEvent("other"); err != nil || !ok {
		t.Fatal("interface keys not independent")
	}
}
