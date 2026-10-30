// Package store persists Heimdall's queues and routing state in SQLite:
// inbound messages, turns, outbound replies, native session bindings and
// per-chat harness choices. A message is stored before its interface
// acknowledges it upstream, and a reply before delivery is attempted.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	_ "modernc.org/sqlite"
)

// Inbound states.
const (
	InPending     = "pending"     // stored, not yet in a turn
	InRunning     = "running"     // part of a turn's initial input
	InSteering    = "steering"    // being handed to a running turn; outcome not yet recorded
	InSteered     = "steered"     // accepted into a running turn by the harness
	InDone        = "done"        // its turn completed with an answer
	InFailed      = "failed"      // its turn failed
	InNeedsHuman  = "needs_human" // its turn ended on a native approval/input request
	InCancelled   = "cancelled"   // its turn was stopped
	InInterrupted = "interrupted" // its turn was cut off by a restart; never re-run
	InUnconfirmed = "unconfirmed" // steer acceptance unknown; never replayed
	InCommand     = "command"     // a ! command, answered by Heimdall itself
	InRejected    = "rejected"    // sender no longer authorized when handled
)

// Turn states.
const (
	TurnRunning     = "running"
	TurnDone        = "done"
	TurnFailed      = "failed"
	TurnNeedsHuman  = "needs_human"
	TurnCancelled   = "cancelled"
	TurnInterrupted = "interrupted"
)

// Outbound states.
const (
	OutPending = "pending"
	OutSending = "sending"
	OutSent    = "sent"
	OutFailed  = "failed"
)

// Schema steps are append-only: never edit a shipped step.
var steps = []string{
	`CREATE TABLE inbound(
	id INTEGER PRIMARY KEY,
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, msg_id TEXT NOT NULL,
	platform TEXT NOT NULL, user_id TEXT NOT NULL, user_name TEXT NOT NULL,
	origin TEXT NOT NULL, text TEXT NOT NULL, received_at INTEGER NOT NULL,
	state TEXT NOT NULL, turn_id INTEGER, detail TEXT NOT NULL DEFAULT '',
	UNIQUE(iface, chat, msg_id));
CREATE INDEX inbound_state ON inbound(state);
CREATE INDEX inbound_turn ON inbound(turn_id) WHERE turn_id IS NOT NULL;
CREATE TABLE turns(
	id INTEGER PRIMARY KEY,
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, role TEXT NOT NULL,
	harness TEXT NOT NULL DEFAULT '', reply_to TEXT NOT NULL DEFAULT '',
	started_at INTEGER NOT NULL, ended_at INTEGER, state TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
CREATE INDEX turns_state ON turns(state);
CREATE TABLE sessions(
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, role TEXT NOT NULL, harness TEXT NOT NULL,
	native_id TEXT NOT NULL, clean INTEGER NOT NULL, updated_at INTEGER NOT NULL,
	PRIMARY KEY(iface, chat, thread, role, harness));
CREATE TABLE chat_settings(
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, harness TEXT NOT NULL,
	PRIMARY KEY(iface, chat, thread));
CREATE TABLE outbound(
	id INTEGER PRIMARY KEY,
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, reply_to TEXT NOT NULL, text TEXT NOT NULL,
	state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL, sent_at INTEGER, error TEXT NOT NULL DEFAULT '');
CREATE INDEX outbound_queue ON outbound(iface, state, next_at);`,
	// Native sessions belong to the verified caller, not to a role.
	`DROP TABLE sessions;
CREATE TABLE sessions(
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, principal TEXT NOT NULL, harness TEXT NOT NULL,
	native_id TEXT NOT NULL, clean INTEGER NOT NULL, updated_at INTEGER NOT NULL,
	PRIMARY KEY(iface, chat, thread, principal, harness));
ALTER TABLE turns ADD COLUMN principal TEXT NOT NULL DEFAULT '';`,
	// The caller's interface-verified email (Slack) and key ID (AgentNet),
	// passed to the harness process; never used for access.
	`ALTER TABLE inbound ADD COLUMN user_email TEXT NOT NULL DEFAULT '';
ALTER TABLE inbound ADD COLUMN user_key_id TEXT NOT NULL DEFAULT '';`,
	// The Slack workspace that verified the caller.
	`ALTER TABLE inbound ADD COLUMN user_team TEXT NOT NULL DEFAULT '';`,
}

type Store struct{ db *sql.DB }

// Open opens or creates the database at path and applies schema steps.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one writer; keeps transactions simple and serial
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v > len(steps) {
		return fmt.Errorf("database schema %d is newer than this Heimdall (%d)", v, len(steps))
	}
	for ; v < len(steps); v++ {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(steps[v]); err == nil {
			_, err = tx.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, v+1))
		}
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("schema step %d: %w", v+1, err)
		}
	}
	return nil
}

func now() int64 { return time.Now().UnixMilli() }

// InboundRow is a stored inbound message.
type InboundRow struct {
	ID int64
	core.Inbound
	State  string
	TurnID int64
}

// AddInbound stores a message as pending. dup reports a message already
// stored (same interface, chat and message ID); it is never stored twice.
func (s *Store) AddInbound(in core.Inbound) (row InboundRow, dup bool, err error) {
	if err := in.Validate(); err != nil {
		return row, false, err
	}
	at := in.At
	if at.IsZero() {
		at = time.Now()
	}
	res, err := s.db.Exec(`INSERT INTO inbound(iface,chat,thread,msg_id,platform,user_id,user_name,user_email,user_team,user_key_id,origin,text,received_at,state)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(iface,chat,msg_id) DO NOTHING`,
		in.Conv.Interface, in.Conv.Chat, in.Conv.Thread, in.MsgID, in.From.Platform, in.From.ID, in.From.Name, in.From.Email, in.From.Team, in.From.KeyID, in.Origin, in.Text, at.UnixMilli(), InPending)
	if err != nil {
		return row, false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return row, true, nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return row, false, err
	}
	in.At = at
	return InboundRow{ID: id, Inbound: in, State: InPending}, false, nil
}

// Pending lists messages never started, oldest first.
func (s *Store) Pending() ([]InboundRow, error) {
	rows, err := s.db.Query(`SELECT id,iface,chat,thread,msg_id,platform,user_id,user_name,user_email,user_team,user_key_id,origin,text,received_at FROM inbound WHERE state=? ORDER BY id`, InPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InboundRow
	for rows.Next() {
		var r InboundRow
		var at int64
		if err := rows.Scan(&r.ID, &r.Conv.Interface, &r.Conv.Chat, &r.Conv.Thread, &r.MsgID, &r.From.Platform, &r.From.ID, &r.From.Name, &r.From.Email, &r.From.Team, &r.From.KeyID, &r.Origin, &r.Text, &at); err != nil {
			return nil, err
		}
		r.At, r.State = time.UnixMilli(at), InPending
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetInbound records a terminal or command state for messages.
func (s *Store) SetInbound(id int64, state, detail string) error {
	_, err := s.db.Exec(`UPDATE inbound SET state=?,detail=? WHERE id=?`, state, detail, id)
	return err
}

// StartTurn records a running turn for conv and moves its initial messages
// from pending to running in one transaction.
func (s *Store) StartTurn(conv core.ConvKey, role core.Role, who, replyTo string, items []int64) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO turns(iface,chat,thread,role,principal,reply_to,started_at,state) VALUES(?,?,?,?,?,?,?,?)`,
		conv.Interface, conv.Chat, conv.Thread, string(role), who, replyTo, now(), TurnRunning)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		res, err := tx.Exec(`UPDATE inbound SET state=?,turn_id=? WHERE id=? AND state=?`, InRunning, id, item, InPending)
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return 0, fmt.Errorf("message %d is no longer pending", item)
		}
	}
	return id, tx.Commit()
}

// BindTurn records the harness taking a turn and marks its native session
// in progress (not clean) until the turn ends. It runs before the harness
// gets any input, so a crash from then on never resumes that session, and
// again once the turn is ready, when a new session's native ID is known.
func (s *Store) BindTurn(turn int64, conv core.ConvKey, who, harness, session string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE turns SET harness=? WHERE id=?`, harness, turn); err != nil {
		return err
	}
	if err := saveSession(tx, conv, who, harness, session, false); err != nil {
		return err
	}
	return tx.Commit()
}

// UnbindTurn undoes BindTurn for a harness that failed before its turn
// was ready, so nothing can have happened: the session gets back its
// previous binding.
func (s *Store) UnbindTurn(turn int64, conv core.ConvKey, who, harness, prevSession string, prevClean bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE turns SET harness='' WHERE id=? AND harness=?`, turn, harness); err != nil {
		return err
	}
	if err := saveSession(tx, conv, who, harness, prevSession, prevClean); err != nil {
		return err
	}
	return tx.Commit()
}

// Steering records that a pending message is about to be handed to a
// running turn. From here on a crash reports it as unconfirmed and never
// replays it.
func (s *Store) Steering(turn, item int64) error {
	res, err := s.db.Exec(`UPDATE inbound SET state=?,turn_id=? WHERE id=? AND state=?`, InSteering, turn, item, InPending)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("message %d is no longer pending", item)
	}
	return nil
}

// Steered records a message accepted into a running turn.
func (s *Store) Steered(turn, item int64) error {
	_, err := s.db.Exec(`UPDATE inbound SET state=?,turn_id=? WHERE id=? AND state=?`, InSteered, turn, item, InSteering)
	return err
}

// Unsteer returns a message the running turn did not take to pending, to
// run as a follow-up.
func (s *Store) Unsteer(item int64) error {
	_, err := s.db.Exec(`UPDATE inbound SET state=?,turn_id=NULL WHERE id=? AND state=?`, InPending, item, InSteering)
	return err
}

// TurnEnd is how a turn ended.
type TurnEnd struct {
	State     string // a Turn* state
	Detail    string
	ItemState string // state for the turn's running and steered messages
	Harness   string // harness that ran it ("" if none became ready)
	Session   string // native session to keep for Harness, if known
	Clean     bool   // the session may be resumed
	Reply     *core.Outbound
}

// EndTurn settles a turn, its messages, its session binding and its reply
// in one transaction.
func (s *Store) EndTurn(turn int64, conv core.ConvKey, who string, e TurnEnd) (replyID int64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`UPDATE turns SET state=?,detail=?,ended_at=? WHERE id=? AND state=?`, e.State, e.Detail, now(), turn, TurnRunning); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(`UPDATE inbound SET state=? WHERE turn_id=? AND state IN (?,?)`, e.ItemState, turn, InRunning, InSteered); err != nil {
		return 0, err
	}
	if e.Harness != "" {
		if err = saveSession(tx, conv, who, e.Harness, e.Session, e.Clean && e.Session != ""); err != nil {
			return 0, err
		}
	}
	if e.Reply != nil {
		if replyID, err = enqueue(tx, *e.Reply); err != nil {
			return 0, err
		}
	}
	return replyID, tx.Commit()
}

func saveSession(tx *sql.Tx, conv core.ConvKey, who, harness, id string, clean bool) error {
	_, err := tx.Exec(`INSERT INTO sessions(iface,chat,thread,principal,harness,native_id,clean,updated_at) VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(iface,chat,thread,principal,harness) DO UPDATE SET native_id=excluded.native_id,clean=excluded.clean,updated_at=excluded.updated_at`,
		conv.Interface, conv.Chat, conv.Thread, who, harness, id, clean, now())
	return err
}

// Session returns the native session bound to (conv, caller, harness) and
// whether its last turn ended cleanly.
func (s *Store) Session(conv core.ConvKey, who, harness string) (id string, clean bool, err error) {
	err = s.db.QueryRow(`SELECT native_id,clean FROM sessions WHERE iface=? AND chat=? AND thread=? AND principal=? AND harness=?`,
		conv.Interface, conv.Chat, conv.Thread, who, harness).Scan(&id, &clean)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return id, clean, err
}

// CommandEnd is how a ! command was answered.
type CommandEnd struct {
	Detail     string
	SetHarness bool   // !use: make Harness sticky for the conversation
	Harness    string // "" restores the default order
	Reply      core.Outbound
}

// AnswerCommand settles a ! command: its message, any harness choice and
// its reply in one transaction. If it fails nothing is recorded and the
// message stays pending, to be answered after a restart.
func (s *Store) AnswerCommand(item int64, conv core.ConvKey, e CommandEnd) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE inbound SET state=?,detail=? WHERE id=?`, InCommand, e.Detail, item); err != nil {
		return err
	}
	if e.SetHarness {
		if err := setChatHarness(tx, conv, e.Harness); err != nil {
			return err
		}
	}
	if _, err := enqueue(tx, e.Reply); err != nil {
		return err
	}
	return tx.Commit()
}

// ChatHarness is the conversation's sticky harness, or "" for the default order.
func (s *Store) ChatHarness(conv core.ConvKey) (string, error) {
	var h string
	err := s.db.QueryRow(`SELECT harness FROM chat_settings WHERE iface=? AND chat=? AND thread=?`, conv.Interface, conv.Chat, conv.Thread).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return h, err
}

// setChatHarness makes harness sticky for conv; "" restores the default order.
func setChatHarness(tx *sql.Tx, conv core.ConvKey, harness string) error {
	if harness == "" {
		_, err := tx.Exec(`DELETE FROM chat_settings WHERE iface=? AND chat=? AND thread=?`, conv.Interface, conv.Chat, conv.Thread)
		return err
	}
	_, err := tx.Exec(`INSERT INTO chat_settings(iface,chat,thread,harness) VALUES(?,?,?,?) ON CONFLICT(iface,chat,thread) DO UPDATE SET harness=excluded.harness`,
		conv.Interface, conv.Chat, conv.Thread, harness)
	return err
}

// Enqueue stores a reply for delivery.
func (s *Store) Enqueue(out core.Outbound) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	id, err := enqueue(tx, out)
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func enqueue(tx *sql.Tx, out core.Outbound) (int64, error) {
	res, err := tx.Exec(`INSERT INTO outbound(iface,chat,thread,reply_to,text,state,created_at) VALUES(?,?,?,?,?,?,?)`,
		out.Conv.Interface, out.Conv.Chat, out.Conv.Thread, out.ReplyTo, out.Text, OutPending, now())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// OutboundRow is a stored reply.
type OutboundRow struct {
	ID int64
	core.Outbound
	Attempts int
}

// NextOutbound returns the oldest due reply of iface whose chat has no older
// undelivered reply, so each chat receives replies in order while other
// chats proceed.
func (s *Store) NextOutbound(iface string) (OutboundRow, bool, error) {
	var r OutboundRow
	err := s.db.QueryRow(`SELECT o.id,o.chat,o.thread,o.reply_to,o.text,o.attempts FROM outbound o
WHERE o.iface=? AND o.state=? AND o.next_at<=?
AND NOT EXISTS(SELECT 1 FROM outbound p WHERE p.iface=o.iface AND p.chat=o.chat AND p.id<o.id AND p.state IN (?,?))
ORDER BY o.id LIMIT 1`, iface, OutPending, now(), OutPending, OutSending).
		Scan(&r.ID, &r.Conv.Chat, &r.Conv.Thread, &r.ReplyTo, &r.Text, &r.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	r.Conv.Interface = iface
	return r, err == nil, err
}

// NextDue is when the earliest reply NextOutbound could offer for iface
// becomes due, if any. Replies queued behind an older undelivered one in
// their chat do not count: they wait for it, not for their own time.
func (s *Store) NextDue(iface string) (time.Time, bool, error) {
	var at sql.NullInt64
	err := s.db.QueryRow(`SELECT min(o.next_at) FROM outbound o
WHERE o.iface=? AND o.state=?
AND NOT EXISTS(SELECT 1 FROM outbound p WHERE p.iface=o.iface AND p.chat=o.chat AND p.id<o.id AND p.state IN (?,?))`, iface, OutPending, OutPending, OutSending).Scan(&at)
	if err != nil || !at.Valid {
		return time.Time{}, false, err
	}
	return time.UnixMilli(at.Int64), true, nil
}

// Sending marks a reply as being handed to its interface. A restart turns
// sending back into pending: delivery is at-least-once.
func (s *Store) Sending(id int64) error {
	_, err := s.db.Exec(`UPDATE outbound SET state=?,attempts=attempts+1 WHERE id=? AND state=?`, OutSending, id, OutPending)
	return err
}

func (s *Store) Sent(id int64) error {
	_, err := s.db.Exec(`UPDATE outbound SET state=?,sent_at=?,error='' WHERE id=?`, OutSent, now(), id)
	return err
}

// Retry returns a reply to the queue, due at next.
func (s *Store) Retry(id int64, next time.Time, why string) error {
	_, err := s.db.Exec(`UPDATE outbound SET state=?,next_at=?,error=? WHERE id=?`, OutPending, next.UnixMilli(), clip(why), id)
	return err
}

// Failed records a permanent delivery failure.
func (s *Store) Failed(id int64, why string) error {
	_, err := s.db.Exec(`UPDATE outbound SET state=?,error=? WHERE id=?`, OutFailed, clip(why), id)
	return err
}

// Undelivered counts replies not yet delivered or failed.
func (s *Store) Undelivered() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM outbound WHERE state IN (?,?)`, OutPending, OutSending).Scan(&n)
	return n, err
}

// Interrupted is a turn a restart cut off.
type Interrupted struct {
	Turn      int64
	Conv      core.ConvKey
	Role      core.Role
	Principal string
	Harness   string
	ReplyTo   string
	Messages  int
}

// Recovered is what a restart cut off.
type Recovered struct {
	Turns       []Interrupted
	Unconfirmed []InboundRow // messages whose hand-over to a running turn was never recorded
}

// Recover runs once at startup, before any interface: every turn still
// running was cut off by the restart. It is marked interrupted with its
// messages, its session is not resumed, and turnNotice builds the chat
// notice. A message caught mid-hand-over to a running turn is marked
// unconfirmed and steerNotice tells its sender. All in one transaction;
// nothing is re-run. Replies caught mid-send go back to the queue.
func (s *Store) Recover(turnNotice func(Interrupted) core.Outbound, steerNotice func(InboundRow) core.Outbound) (Recovered, error) {
	var rec Recovered
	tx, err := s.db.Begin()
	if err != nil {
		return rec, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT t.id,t.iface,t.chat,t.thread,t.role,t.principal,t.harness,t.reply_to,(SELECT count(*) FROM inbound i WHERE i.turn_id=t.id AND i.state IN (?,?)) FROM turns t WHERE t.state=? ORDER BY t.id`, InRunning, InSteered, TurnRunning)
	if err != nil {
		return rec, err
	}
	var cut []Interrupted
	for rows.Next() {
		var t Interrupted
		var role string
		if err := rows.Scan(&t.Turn, &t.Conv.Interface, &t.Conv.Chat, &t.Conv.Thread, &role, &t.Principal, &t.Harness, &t.ReplyTo, &t.Messages); err != nil {
			rows.Close()
			return rec, err
		}
		t.Role = core.Role(role)
		cut = append(cut, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rec, err
	}
	for _, t := range cut {
		if _, err := tx.Exec(`UPDATE turns SET state=?,detail=?,ended_at=? WHERE id=?`, TurnInterrupted, "Heimdall restarted during this turn; not re-run", now(), t.Turn); err != nil {
			return rec, err
		}
		if _, err := tx.Exec(`UPDATE inbound SET state=?,detail=? WHERE turn_id=? AND state IN (?,?)`, InInterrupted, "turn interrupted by restart; not re-run", t.Turn, InRunning, InSteered); err != nil {
			return rec, err
		}
		if t.Harness != "" {
			if _, err := tx.Exec(`UPDATE sessions SET clean=0 WHERE iface=? AND chat=? AND thread=? AND principal=? AND harness=?`, t.Conv.Interface, t.Conv.Chat, t.Conv.Thread, t.Principal, t.Harness); err != nil {
				return rec, err
			}
		}
		if _, err := enqueue(tx, turnNotice(t)); err != nil {
			return rec, err
		}
	}
	rows, err = tx.Query(`SELECT id,iface,chat,thread,msg_id,platform,user_id,turn_id FROM inbound WHERE state=? ORDER BY id`, InSteering)
	if err != nil {
		return rec, err
	}
	var held []InboundRow
	for rows.Next() {
		var r InboundRow
		var turn sql.NullInt64
		if err := rows.Scan(&r.ID, &r.Conv.Interface, &r.Conv.Chat, &r.Conv.Thread, &r.MsgID, &r.From.Platform, &r.From.ID, &turn); err != nil {
			rows.Close()
			return rec, err
		}
		r.State, r.TurnID = InUnconfirmed, turn.Int64
		held = append(held, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rec, err
	}
	for _, r := range held {
		if _, err := tx.Exec(`UPDATE inbound SET state=?,detail=? WHERE id=?`, InUnconfirmed, "steer interrupted by restart; not replayed", r.ID); err != nil {
			return rec, err
		}
		if _, err := enqueue(tx, steerNotice(r)); err != nil {
			return rec, err
		}
	}
	if _, err := tx.Exec(`UPDATE outbound SET state=? WHERE state=?`, OutPending, OutSending); err != nil {
		return rec, err
	}
	rec.Turns, rec.Unconfirmed = cut, held
	return rec, tx.Commit()
}

func clip(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}
