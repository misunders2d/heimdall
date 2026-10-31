package store

import (
	"database/sql"
	"errors"
)

// InterfaceEvent is a raw platform event waiting for post-ack processing.
// It is never a verified caller or a runnable input. The transport verifies
// identity live before building core.Inbound and passing it to the router.
type InterfaceEvent struct {
	ID      int64
	Payload []byte
}

// StageEvent commits before the interface acknowledges the envelope. Stable
// platform event keys dedupe reconnect/redelivery, including completed work.
func (s *Store) StageEvent(iface, key string, payload []byte) error {
	if iface == "" || key == "" || len(payload) == 0 || len(payload) > 8<<20 {
		return errors.New("invalid interface event")
	}
	_, err := s.db.Exec(`INSERT INTO interface_events(iface,event_key,payload,received_at) VALUES(?,?,?,?) ON CONFLICT(iface,event_key) DO NOTHING`, iface, key, payload, now())
	return err
}

// NextEvent returns the oldest pending payload. One transport owns its
// processor; the caller must hold the data-directory lock to exclude
// competing instances.
func (s *Store) NextEvent(iface string) (InterfaceEvent, bool, error) {
	var e InterfaceEvent
	err := s.db.QueryRow(`SELECT id,payload FROM interface_events WHERE iface=? AND complete=0 ORDER BY id LIMIT 1`, iface).Scan(&e.ID, &e.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return e, false, nil
	}
	return e, err == nil, err
}

// CompleteEvent is called only after the verified input (or silent drop)
// has committed. A crash between acceptance and this update retries local
// processing; the router's independent message key dedupes commands/turns.
func (s *Store) CompleteEvent(id int64) error {
	_, err := s.db.Exec(`UPDATE interface_events SET complete=1,payload=NULL WHERE id=?`, id)
	return err
}

// PendingEvents is a startup snapshot, before the socket starts receiving.
func (s *Store) PendingEvents(iface string) ([]InterfaceEvent, error) {
	rows, err := s.db.Query(`SELECT id,payload FROM interface_events WHERE iface=? AND complete=0 ORDER BY id`, iface)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []InterfaceEvent
	for rows.Next() {
		var e InterfaceEvent
		if err = rows.Scan(&e.ID, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
func (s *Store) Event(iface, key string) (InterfaceEvent, bool, error) {
	var e InterfaceEvent
	err := s.db.QueryRow(`SELECT id,payload FROM interface_events WHERE iface=? AND event_key=? AND complete=0`, iface, key).Scan(&e.ID, &e.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return e, false, nil
	}
	return e, err == nil, err
}

// EventPending rechecks a queued payload against durable completion. The
// channel-ordered worker calls this immediately before processing a copy.
func (s *Store) EventPending(id int64) (bool, error) {
	var complete int
	err := s.db.QueryRow(`SELECT complete FROM interface_events WHERE id=?`, id).Scan(&complete)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return complete == 0, err
}
