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
// created by this principal/account or a currently authorized owner alias.
// Aliased results require a matching private source-account binding; its OAuth
// scope stays on its own platform. No job prompt or caller authority
// is selected. The person input's receipt time fences concurrently arriving sends.
func (s *Store) SentScheduledResults(in core.Inbound, after DeliveryCursor, owners ...func(core.Principal) bool) ([]DeliveredResult, DeliveryCursor, error) {
	rows, err := s.db.Query(`SELECT o.id,o.sent_at,o.text,i.platform,i.user_id,i.user_email,i.user_team,i.user_key_id,
 COALESCE(json_extract(nullif(i.extra,''),'$.oauth_actor'),''),
 COALESCE(json_extract(nullif(i.extra,''),'$.oauth_installation'),''),
 COALESCE(b.user_team,''),COALESCE(b.user_key_id,''),
 COALESCE(json_extract(nullif(b.extra,''),'$.oauth_actor'),''),
 COALESCE(json_extract(nullif(b.extra,''),'$.oauth_installation'),''),b.id IS NOT NULL
 FROM outbound o
 JOIN turns t ON t.id=json_extract(nullif(o.extra,''),'$.turn_id')
 JOIN inbound i ON i.turn_id=t.id
 JOIN job_runs r ON r.inbound_id=i.id
 JOIN jobs j ON j.id=r.job_id
 LEFT JOIN inbound b ON b.id=(SELECT MAX(v.id) FROM inbound v
 WHERE v.platform=i.platform AND v.user_id=i.user_id AND v.iface=v.platform
 AND v.private=1 AND v.origin='live' AND coalesce(json_extract(nullif(v.extra,''),'$.mcp_control'),0)=0
 AND json_extract(nullif(v.extra,''),'$.run') IS NULL)
 WHERE o.iface=? AND o.chat=? AND o.thread=? AND o.state=?
 AND t.role=? AND t.state=? AND j.creator=t.principal
 AND i.platform || ':' || i.user_id=t.principal AND i.origin=?
 AND o.sent_at>=? AND o.sent_at<=? AND length(CAST(o.text AS BLOB)) BETWEEN 1 AND 4000
 AND json_extract(nullif(o.extra,''),'$.file') IS NULL
 ORDER BY o.sent_at DESC,o.id DESC LIMIT 16`, in.Conv.Interface, in.Conv.Chat, in.Conv.Thread, OutSent, core.RoleOwner, TurnDone, core.OriginScheduled, after.At, in.At.UnixMilli())
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
		var source core.Principal
		var actor, installation, team, key, bActor, bInstallation string
		var bound bool
		if err := rows.Scan(&d.ID, &at, &d.Text, &source.Platform, &source.ID, &source.Email, &source.Team, &source.KeyID, &actor, &installation, &team, &key, &bActor, &bInstallation, &bound); err != nil {
			return nil, after, err
		}
		if len(owners) > 1 || len(owners) == 1 && (owners[0] == nil || !owners[0](source)) {
			continue
		}
		exact := source.Key() == in.From.Key()
		if exact {
			if source.Team != in.From.Team || source.KeyID != in.From.KeyID || actor != in.OAuthActor || installation != in.OAuthInstallation {
				continue
			}
		} else {
			// Cross-platform equivalence is server-owned policy, never a
			// claim in result text. Source account scope is checked on its
			// original platform, never compared with another platform's IDs.
			if !in.Private || len(owners) != 1 || owners[0] == nil ||
				source.Platform == in.From.Platform || !bound || source.Team != team || source.KeyID != key || actor != bActor || installation != bInstallation {
				continue
			}
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
