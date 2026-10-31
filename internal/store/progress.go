package store

import (
	"database/sql"
	"errors"
	"github.com/misunders2d/heimdall/internal/core"
	"time"
)

type Progress struct {
	Turn                 int64
	Conv                 core.ConvKey
	TS, Phase, TurnState string
	Pending              bool
}

func (s *Store) SaveProgress(p Progress) error {
	_, err := s.db.Exec(`INSERT INTO interface_progress(turn_id,iface,chat,thread,ts,phase) VALUES(?,?,?,?,?,?) ON CONFLICT(turn_id) DO UPDATE SET ts=excluded.ts,phase=excluded.phase`, p.Turn, p.Conv.Interface, p.Conv.Chat, p.Conv.Thread, p.TS, p.Phase)
	return err
}
func (s *Store) Progress(turn int64) (Progress, bool, error) {
	var p Progress
	err := s.db.QueryRow(`SELECT turn_id,iface,chat,thread,ts,phase FROM interface_progress WHERE turn_id=?`, turn).Scan(&p.Turn, &p.Conv.Interface, &p.Conv.Chat, &p.Conv.Thread, &p.TS, &p.Phase)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	return p, err == nil, err
}
func (s *Store) Progresses(iface string) ([]Progress, error) {
	rows, err := s.db.Query(`SELECT p.turn_id,p.iface,p.chat,p.thread,p.ts,p.phase,COALESCE(t.state,'interrupted'),EXISTS(SELECT 1 FROM outbound o WHERE o.iface=p.iface AND o.state IN ('pending','sending') AND json_extract(CASE WHEN o.extra='' THEN '{}' ELSE o.extra END,'$.turn_id')=p.turn_id) FROM interface_progress p LEFT JOIN turns t ON t.id=p.turn_id WHERE p.iface=?`, iface)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ps []Progress
	for rows.Next() {
		var p Progress
		if err = rows.Scan(&p.Turn, &p.Conv.Interface, &p.Conv.Chat, &p.Conv.Thread, &p.TS, &p.Phase, &p.TurnState, &p.Pending); err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return ps, rows.Err()
}
func (s *Store) DeleteProgress(turn int64) error {
	_, err := s.db.Exec(`DELETE FROM interface_progress WHERE turn_id=?`, turn)
	return err
}

// RecentDeliveries includes successful assistant outputs when live history
// is unavailable, and future scheduler replies that use the same queue.
func (s *Store) RecentDeliveries(k core.ConvKey) ([]string, error) {
	rows, err := s.db.Query(`SELECT text FROM outbound WHERE iface=? AND chat=? AND thread=? AND state=? AND created_at>? AND text!='' ORDER BY id DESC LIMIT 5`, k.Interface, k.Chat, k.Thread, OutSent, time.Now().Add(-72*time.Hour).UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var text string
		if err = rows.Scan(&text); err != nil {
			return nil, err
		}
		out = append(out, text)
	}
	return out, rows.Err()
}
