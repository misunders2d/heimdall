package store

import (
	"database/sql"
	"errors"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

const ChatRestartLimit = 3

// ChatResume continues the original running turn. Record and Actions are
// filled by the router's read-only native session check before dispatch.
type ChatResume struct {
	Interrupted
	Session string
	Started time.Time
	Inputs  []InboundRow
	Record  string
	Actions []string
}

func claimChat(tx *sql.Tx, t *Interrupted) (*ChatResume, error) {
	if t.Role != core.RoleOwner || t.Messages == 0 || t.Conv.IsSched() || t.Conv.IsReview() {
		return nil, nil
	}
	switch t.Harness {
	case "omp", "codex", "claude":
	default:
		return nil, nil
	}
	var started int64
	var session string
	err := tx.QueryRow("SELECT t.started_at,s.native_id FROM turns t JOIN sessions s USING(iface,chat,thread,principal,harness) WHERE t.id=? AND t.no_resume=0 AND s.native_id<>'' AND s.clean=0", t.Turn).Scan(&started, &session)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query("SELECT "+inboundColumns+" FROM inbound WHERE turn_id=? AND state IN (?,?,?) ORDER BY id", t.Turn, InRunning, InSteered, InUnconfirmed)
	if err != nil {
		return nil, err
	}
	inputs, err := readInbounds(rows)
	if err != nil {
		return nil, err
	}
	if len(inputs) == 0 || inputs[0].MsgID != t.ReplyTo || inputs[0].State != InRunning {
		return nil, nil
	}
	for _, row := range inputs {
		if row.Origin != core.OriginLive || row.Run != nil || row.Review != nil || row.MCPControl || row.From.Key() != t.Principal || row.Conv != t.Conv {
			return nil, nil
		}
	}
	t.Cutoffs++
	if _, err := tx.Exec("UPDATE turns SET restart_count=? WHERE id=?", t.Cutoffs, t.Turn); err != nil {
		return nil, err
	}
	if t.Cutoffs >= ChatRestartLimit {
		return nil, nil
	}
	return &ChatResume{Interrupted: *t, Session: session, Started: time.UnixMilli(started), Inputs: inputs}, nil
}

// FenceChatResume records a stop or reset even if the process crashes while
// the native turn is still winding down.
func (s *Store) FenceChatResume(turn int64) error {
	_, err := s.db.Exec("UPDATE turns SET no_resume=1 WHERE id=? AND state=?", turn, TurnRunning)
	return err
}
