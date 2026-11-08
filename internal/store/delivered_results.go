package store

import (
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// DeliveryCursor is process-local, not a durable transcript. IDs disambiguate
// receipts with the same millisecond, including an older outbound sent later.
type DeliveryCursor struct {
	At  int64
	IDs map[int64]bool
}
type DeliveredResult struct {
	ID   int64     `json:"delivery_id"`
	At   time.Time `json:"sent_at"`
	Text string    `json:"historical_result"`
}

// SentScheduledResults reads only confirmed text deliveries to this exact target,
// created by this principal and OAuth account. No job prompt or caller authority
// is selected. The person input's receipt time fences concurrently arriving sends.
func (s *Store) SentScheduledResults(in core.Inbound, after DeliveryCursor) ([]DeliveredResult, DeliveryCursor, error) {
	rows, err := s.db.Query(`SELECT o.id,o.sent_at,o.text FROM outbound o
 JOIN turns t ON t.id=json_extract(nullif(o.extra,''),'$.turn_id')
 JOIN inbound i ON i.turn_id=t.id
 JOIN job_runs r ON r.inbound_id=i.id
 JOIN jobs j ON j.id=r.job_id
 WHERE o.iface=? AND o.chat=? AND o.thread=? AND o.state=?
 AND t.principal=? AND t.role=? AND t.state=? AND j.creator=t.principal
 AND i.user_team=? AND i.user_key_id=?
 AND i.platform || ':' || i.user_id=t.principal AND i.origin=?
 AND COALESCE(json_extract(nullif(i.extra,''),'$.oauth_actor'),'')=?
 AND COALESCE(json_extract(nullif(i.extra,''),'$.oauth_installation'),'')=?
 AND o.sent_at>=? AND o.sent_at<=? AND length(CAST(o.text AS BLOB)) BETWEEN 1 AND 4000
 AND json_extract(nullif(o.extra,''),'$.file') IS NULL
 ORDER BY o.sent_at DESC,o.id DESC LIMIT 16`, in.Conv.Interface, in.Conv.Chat, in.Conv.Thread, OutSent, in.From.Key(), core.RoleOwner, TurnDone, in.From.Team, in.From.KeyID, core.OriginScheduled, in.OAuthActor, in.OAuthInstallation, after.At, in.At.UnixMilli())
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	next := DeliveryCursor{At: after.At, IDs: map[int64]bool{}}
	for id, seen := range after.IDs {
		next.IDs[id] = seen
	}
	var out []DeliveredResult
	for rows.Next() {
		var d DeliveredResult
		var at int64
		if err := rows.Scan(&d.ID, &at, &d.Text); err != nil {
			return nil, after, err
		}
		if at == after.At && after.IDs[d.ID] {
			continue
		}
		d.At = time.UnixMilli(at).UTC()
		out = append(out, d)
		if at > next.At {
			next.At = at
			next.IDs = map[int64]bool{}
		}
		if at == next.At {
			next.IDs[d.ID] = true
		}
	}
	// query is newest first; context is chronological and omissions remain whole.
	for a, b := 0, len(out)-1; a < b; a, b = a+1, b-1 {
		out[a], out[b] = out[b], out[a]
	}
	return out, next, rows.Err()
}
