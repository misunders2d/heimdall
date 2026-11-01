package store

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestReviewCompletedEventsPrunedAfterDedupeWindow(t *testing.T) {
	s := open(t)
	for _, key := range []string{"old", "pending", "recent"} {
		if err := s.StageEvent("slack", key, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"old", "recent"} {
		e, _, _ := s.Event("slack", key)
		s.CompleteEvent(e.ID)
	}
	s.db.Exec(`UPDATE interface_events SET received_at=?,completed_at=? WHERE event_key IN ('old','pending')`, time.Now().Add(-96*time.Hour).UnixMilli(), time.Now().Add(-96*time.Hour).UnixMilli())
	if err := s.PruneEvents("slack", time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM interface_events`).Scan(&count)
	if count != 2 {
		t.Fatalf("remaining %d", count)
	}
	if _, ok, _ := s.Event("slack", "pending"); !ok {
		t.Fatal("pending lost")
	}
	e, _, _ := s.Event("slack", "pending")
	s.CompleteEvent(e.ID)
	if err := s.PruneEvents("slack", time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var delayed int
	s.db.QueryRow(`SELECT count(*) FROM interface_events WHERE event_key='pending'`).Scan(&delayed)
	if delayed != 1 {
		t.Fatal("completion did not start fresh dedupe window")
	}
	// Recent completed event still dedupes; expired one can be staged again.
	s.StageEvent("slack", "recent", []byte(`{}`))
	if _, ok, _ := s.Event("slack", "recent"); ok {
		t.Fatal("dedupe lost")
	}
	s.StageEvent("slack", "old", []byte(`{}`))
	if _, ok, _ := s.Event("slack", "old"); !ok {
		t.Fatal("expired event still deduped")
	}
}

func TestReviewEventCompletionMigration(t *testing.T) {
	path := t.TempDir() + "/old.db"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps[:len(steps)-1] {
		if _, err := db.Exec(step); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, len(steps)-1)); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-96 * time.Hour).UnixMilli()
	if _, err := db.Exec(`INSERT INTO interface_events(iface,event_key,payload,received_at,complete) VALUES('slack','old',NULL,?,1),('slack','pending','{}',?,0)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO interface_events(iface,event_key,payload,received_at,complete) VALUES('slack','recent',NULL,?,1)`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.PruneEvents("slack", time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM interface_events`).Scan(&count)
	if count != 2 {
		t.Fatal("migration retention", count)
	}
	if _, ok, _ := s.Event("slack", "pending"); !ok {
		t.Fatal("migration lost pending event")
	}
}
