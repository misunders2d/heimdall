package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/misunders2d/heimdall/internal/core"
)

// ErrEffectConflict: an effect key already names a reply on another route.
var ErrEffectConflict = errors.New("effect key conflict")

// EnqueueEffect stores out for delivery at most once per effect key: a
// repeated key returns the reply already stored under it, in any state. The
// key is a structured identity (internal/durable: task/attempt/effect),
// kept in the reply's extra JSON, so no schema version changes and an older
// Heimdall still opens the database. Lookup and insert share one transaction
// on the store's single connection.
func (s *Store) EnqueueEffect(effect string, out core.Outbound) (id int64, err error) {
	if effect == "" || len(effect) > 256 {
		return 0, errors.New("effect key must be 1-256 bytes")
	}
	err = s.tx(func(tx *sql.Tx) error {
		// Legacy rows may hold non-JSON extra: never parse those.
		var k core.ConvKey
		var replyTo string
		err := tx.QueryRow(`SELECT id,iface,chat,thread,reply_to FROM outbound WHERE CASE WHEN json_valid(extra) THEN json_extract(extra,'$.effect') END=? ORDER BY id LIMIT 1`, effect).Scan(&id, &k.Interface, &k.Chat, &k.Thread, &replyTo)
		if err == nil && (k != out.Conv || replyTo != out.ReplyTo) {
			// A key names one effect on one route; never adopt another
			// destination's receipt.
			return fmt.Errorf("%w: %q belongs to another route (outbound %d)", ErrEffectConflict, effect, id)
		}
		if err != sql.ErrNoRows {
			return err
		}
		if err := out.Window.Validate(); err != nil {
			return err
		}
		x := outExtra{Window: out.Window, TurnID: out.TurnID, Formatted: out.Formatted, Reactions: out.Reactions, File: out.File, Effect: effect}
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		res, err := tx.Exec(`INSERT INTO outbound(iface,chat,thread,reply_to,text,extra,state,created_at) VALUES(?,?,?,?,?,?,?,?)`,
			out.Conv.Interface, out.Conv.Chat, out.Conv.Thread, out.ReplyTo, out.Text, string(b), OutPending, now())
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	return id, err
}

// Receipt is a stored reply's delivery state: OutPending, OutSending,
// OutSent or OutFailed (with its error).
type Receipt struct {
	State  string
	Error  string
	SentAt int64 // unix ms; 0 until sent
}

// OutboundReceipt reads reply id's delivery state.
func (s *Store) OutboundReceipt(id int64) (Receipt, bool, error) {
	var r Receipt
	var sent sql.NullInt64
	err := s.db.QueryRow(`SELECT state,error,sent_at FROM outbound WHERE id=?`, id).Scan(&r.State, &r.Error, &sent)
	if err == sql.ErrNoRows {
		return r, false, nil
	}
	r.SentAt = sent.Int64
	return r, err == nil, err
}
