package store

import (
	"encoding/json"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

const OwnerInputLimit = 16
const OwnerInputBytes = 16 * 1024

// OwnerInput contains only the interface's original human text, never its
// context, attachments, native transcript, or a generated turn prompt.
type OwnerInput struct {
	MessageID string    `json:"message_id"`
	At        time.Time `json:"received_at"`
	Text      string    `json:"text"`
}

// FailedOwnerInputs selects a bounded suffix of this caller's consecutive
// failed/interrupted turns before current. Terminal turns and explicit control
// fences prevent old requests from crossing completion, stop, cancel or reset.
// The caller must also check the existing unclean native binding before use.
func (s *Store) FailedOwnerInputs(conv core.ConvKey, who string, current int64) ([]OwnerInput, error) {
	rows, err := s.db.Query(`WITH boundary AS (
 SELECT COALESCE(MAX(id),0) AS id FROM turns
 WHERE iface=? AND chat=? AND thread=? AND principal=? AND id<?
 AND state NOT IN (?,?)
), fence AS (
 SELECT COALESCE(MAX(id),0) AS id FROM inbound
 WHERE iface=? AND chat=? AND thread=? AND platform || ':' || user_id=?
 AND state=? AND detail IN ('reset','stop','cancel')
)
SELECT i.msg_id,i.received_at,i.text,i.extra FROM inbound i JOIN turns t ON i.turn_id=t.id
WHERE t.iface=? AND t.chat=? AND t.thread=? AND t.principal=? AND t.role=?
 AND t.id>(SELECT id FROM boundary) AND t.id<? AND t.state IN (?,?)
 AND i.iface=t.iface AND i.chat=t.chat AND i.thread=t.thread
 AND i.platform || ':' || i.user_id=t.principal AND i.origin=?
 AND i.state IN (?,?) AND i.id>(SELECT id FROM fence)
 AND length(CAST(i.text AS BLOB)) BETWEEN 1 AND ?
 AND length(CAST(i.msg_id AS BLOB))<=256
ORDER BY i.id DESC LIMIT ?`,
		conv.Interface, conv.Chat, conv.Thread, who, current, TurnFailed, TurnInterrupted,
		conv.Interface, conv.Chat, conv.Thread, who, InCommand,
		conv.Interface, conv.Chat, conv.Thread, who, core.RoleOwner, current, TurnFailed, TurnInterrupted,
		core.OriginLive, InFailed, InInterrupted, OwnerInputBytes, OwnerInputLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OwnerInput
	for rows.Next() {
		var in OwnerInput
		var at int64
		var extra string
		if err := rows.Scan(&in.MessageID, &at, &in.Text, &extra); err != nil {
			return nil, err
		}
		if extra != "" {
			var x inExtra
			if err := json.Unmarshal([]byte(extra), &x); err != nil {
				return nil, err
			}
			if x.MCPControl || x.Run != nil || x.ControlIngress != 0 || x.InterfaceReply != "" {
				continue
			}
		}
		in.At = time.UnixMilli(at).UTC()
		out = append(out, in)
	}
	return out, rows.Err()
}
