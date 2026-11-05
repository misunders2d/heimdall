// Package store persists Heimdall's queues and routing state in SQLite:
// inbound messages, turns, outbound replies, native session bindings and
// per-chat harness choices. A message is stored before its interface
// acknowledges it upstream, and a reply before delivery is attempted.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	OutHeld    = "held"
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
	// The interface's one-to-one chat flag (core.Inbound.Private).
	`ALTER TABLE inbound ADD COLUMN private INTEGER NOT NULL DEFAULT 0;`,
	// How a reply is delivered beyond its text (outExtra, JSON).
	`ALTER TABLE outbound ADD COLUMN extra TEXT NOT NULL DEFAULT '';`,
	// What an interface adds to a message beyond its text (inExtra, JSON).
	`ALTER TABLE inbound ADD COLUMN extra TEXT NOT NULL DEFAULT '';`,
	// Recent group messages, kept as context for later turns (Ambient).
	`CREATE TABLE ambient(
	id INTEGER PRIMARY KEY,
	iface TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, msg_id TEXT NOT NULL,
	at INTEGER NOT NULL, who TEXT NOT NULL, name TEXT NOT NULL, text TEXT NOT NULL, file TEXT NOT NULL,
	UNIQUE(iface, chat, msg_id));
CREATE INDEX ambient_conv ON ambient(iface, chat, thread, id);`,
	// Callers seen, for the owners' one-time new-caller notice.
	// Callers who wrote before this step are not new.
	`CREATE TABLE callers(principal TEXT PRIMARY KEY, first_at INTEGER NOT NULL);
INSERT INTO callers SELECT platform || ':' || user_id, MIN(received_at) FROM inbound GROUP BY platform, user_id;`,
	// Interface payloads committed before upstream ack. Slow verification,
	// history and downloads happen after this boundary; only verified callers
	// are admitted into inbound. Completed keys remain for redelivery dedupe,
	// with payload erased once router acceptance has committed.
	`CREATE TABLE interface_events(
 id INTEGER PRIMARY KEY,
 iface TEXT NOT NULL, event_key TEXT NOT NULL, payload BLOB,
 received_at INTEGER NOT NULL, complete INTEGER NOT NULL DEFAULT 0,
 UNIQUE(iface,event_key));
 CREATE INDEX interface_events_pending ON interface_events(iface,complete,id);`,
	`CREATE TABLE interface_progress(turn_id INTEGER PRIMARY KEY,iface TEXT NOT NULL,chat TEXT NOT NULL,thread TEXT NOT NULL,ts TEXT NOT NULL,phase TEXT NOT NULL);`,
	// Dedupe age starts at completion, so delayed pending work gets a full window.
	`ALTER TABLE interface_events ADD COLUMN completed_at INTEGER NOT NULL DEFAULT 0;
UPDATE interface_events SET completed_at=received_at WHERE complete=1;
CREATE INDEX interface_events_expiry ON interface_events(iface,completed_at) WHERE complete=1;`,
	`CREATE TABLE caller_notices(principal TEXT NOT NULL,owner TEXT NOT NULL,PRIMARY KEY(principal,owner));
 INSERT INTO caller_notices(principal,owner) SELECT principal,'*' FROM callers;`,
	// Scheduler (docs/scheduler.md §8): jobs, one run per job and slot, the
	// due loop's tick and hold, and the ready marker that decides whether an
	// interrupted run may be queued again.
	`CREATE TABLE jobs(
	id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
	creator TEXT NOT NULL, principal TEXT NOT NULL,
	created_iface TEXT NOT NULL, created_chat TEXT NOT NULL, created_thread TEXT NOT NULL, created_private INTEGER NOT NULL,
	def TEXT NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', edit_of TEXT NOT NULL DEFAULT '',
	draft_expires_at INTEGER NOT NULL DEFAULT 0, next_slot TEXT NOT NULL DEFAULT '', next_due INTEGER NOT NULL DEFAULT 0,
	version INTEGER NOT NULL DEFAULT 1, fails INTEGER NOT NULL DEFAULT 0, skips INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE INDEX jobs_state ON jobs(state, next_due);
CREATE TABLE job_runs(
	id INTEGER PRIMARY KEY, job_id TEXT NOT NULL, version INTEGER NOT NULL,
	slot TEXT NOT NULL, due_at INTEGER NOT NULL, trigger TEXT NOT NULL,
	state TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0, not_before INTEGER NOT NULL DEFAULT 0,
	inbound_id INTEGER, cause TEXT NOT NULL DEFAULT '', started_at INTEGER NOT NULL DEFAULT 0, ended_at INTEGER NOT NULL DEFAULT 0,
	UNIQUE(job_id, slot));
CREATE INDEX job_runs_state ON job_runs(state, due_at);
CREATE TABLE sched_meta(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
ALTER TABLE turns ADD COLUMN ready INTEGER NOT NULL DEFAULT 0;`,
	// An idempotent run's one re-run after a restart, recorded apart from
	// its cause (docs/scheduler.md §7.2).
	`ALTER TABLE job_runs ADD COLUMN replayed INTEGER NOT NULL DEFAULT 0;`,
	`CREATE TABLE job_checkpoints(job_id TEXT PRIMARY KEY,creator TEXT NOT NULL,version INTEGER NOT NULL,through_at TEXT NOT NULL,state TEXT NOT NULL,run_id INTEGER NOT NULL);
CREATE TABLE job_alerts(job_id TEXT NOT NULL,creator TEXT NOT NULL,key TEXT NOT NULL,revision TEXT NOT NULL,outbound_id INTEGER NOT NULL,PRIMARY KEY(job_id,creator,key));
CREATE TABLE scheduler_controls(creator TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,operator_uid INTEGER NOT NULL,action TEXT NOT NULL,job_id TEXT NOT NULL,response TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(creator,request_id));`,
	`ALTER TABLE job_checkpoints ADD COLUMN scan_through TEXT NOT NULL DEFAULT '';
ALTER TABLE job_checkpoints ADD COLUMN partial INTEGER NOT NULL DEFAULT 0;
ALTER TABLE job_checkpoints ADD COLUMN cause TEXT NOT NULL DEFAULT '';
ALTER TABLE job_alerts RENAME TO job_alerts_old;
CREATE TABLE job_alerts(job_id TEXT NOT NULL,creator TEXT NOT NULL,key TEXT NOT NULL,revision TEXT NOT NULL,outbound_id INTEGER NOT NULL,retired INTEGER NOT NULL DEFAULT 0,acknowledged_at INTEGER NOT NULL DEFAULT 0,failure_notice INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(job_id,creator,key,revision));
INSERT INTO job_alerts(job_id,creator,key,revision,outbound_id) SELECT job_id,creator,key,revision,outbound_id FROM job_alerts_old;
DROP TABLE job_alerts_old;`,
}

// inExtra is the inbound extra column: what the interface added beyond the
// text.
type inExtra struct {
	MCPControl        bool               `json:"mcp_control,omitempty"`
	SlackReadVerified bool               `json:"slack_read_verified,omitempty"`
	OAuthActor        string             `json:"oauth_actor,omitempty"`
	OAuthInstallation string             `json:"oauth_installation,omitempty"`
	Context           string             `json:"context,omitempty"`
	ControlIngress    int                `json:"control_ingress,omitempty"`
	InterfaceReply    string             `json:"interface_reply,omitempty"`
	Attachments       []core.Attachment  `json:"attachments,omitempty"`
	Group             string             `json:"group,omitempty"`
	Run               *core.ScheduledRun `json:"run,omitempty"`
}

func inExtraOf(in core.Inbound) (string, error) {
	x := inExtra{MCPControl: in.MCPControl, SlackReadVerified: in.SlackReadVerified, OAuthActor: in.OAuthActor, OAuthInstallation: in.OAuthInstallation, ControlIngress: in.ControlIngress, InterfaceReply: in.InterfaceReply, Context: in.Context, Attachments: in.Attachments, Group: in.Group, Run: in.Run}
	if !x.MCPControl && !x.SlackReadVerified && x.OAuthActor == "" && x.OAuthInstallation == "" && x.ControlIngress == 0 && x.InterfaceReply == "" && x.Context == "" && len(x.Attachments) == 0 && x.Group == "" && x.Run == nil {
		return "", nil
	}
	b, err := json.Marshal(x)
	return string(b), err
}

func (x inExtra) apply(in *core.Inbound) {
	in.MCPControl, in.SlackReadVerified = x.MCPControl, x.SlackReadVerified
	in.OAuthActor, in.OAuthInstallation = x.OAuthActor, x.OAuthInstallation
	in.ControlIngress = x.ControlIngress
	in.Context, in.Attachments, in.Group, in.InterfaceReply = x.Context, x.Attachments, x.Group, x.InterfaceReply
	in.Run = x.Run
}

// outExtra is the outbound extra column: delivery details beyond the text.
type outExtra struct {
	Window    *core.DeliveryWindow `json:"delivery_window,omitempty"`
	TurnID    int64                `json:"turn_id,omitempty"`
	Formatted bool                 `json:"formatted,omitempty"`
	Reactions []core.Reaction      `json:"reactions,omitempty"`
	File      *core.File           `json:"file,omitempty"`
}

func outExtraOf(out core.Outbound) (string, error) {
	x := outExtra{Window: out.Window, TurnID: out.TurnID, Formatted: out.Formatted, Reactions: out.Reactions, File: out.File}
	if x.Window == nil && x.TurnID == 0 && !x.Formatted && len(x.Reactions) == 0 && x.File == nil {
		return "", nil
	}
	b, err := json.Marshal(x)
	return string(b), err
}

func (x outExtra) apply(out *core.Outbound) {
	out.Window = x.Window
	out.Formatted, out.Reactions, out.File, out.TurnID = x.Formatted, x.Reactions, x.File, x.TurnID
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
	extra, err := inExtraOf(in)
	if err != nil {
		return row, false, err
	}
	return addInbound(s.db, in, at, extra)
}

func addInbound(db interface {
	Exec(string, ...any) (sql.Result, error)
}, in core.Inbound, at time.Time, extra string) (row InboundRow, dup bool, err error) {
	res, err := db.Exec(`INSERT INTO inbound(iface,chat,thread,msg_id,platform,user_id,user_name,user_email,user_team,user_key_id,origin,text,received_at,state,private,extra)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(iface,chat,msg_id) DO NOTHING`,
		in.Conv.Interface, in.Conv.Chat, in.Conv.Thread, in.MsgID, in.From.Platform, in.From.ID, in.From.Name, in.From.Email, in.From.Team, in.From.KeyID, in.Origin, in.Text, at.UnixMilli(), InPending, in.Private, extra)
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
	rows, err := s.db.Query(`SELECT id,iface,chat,thread,msg_id,platform,user_id,user_name,user_email,user_team,user_key_id,origin,text,received_at,private,extra FROM inbound WHERE state=? ORDER BY id`, InPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InboundRow
	for rows.Next() {
		var r InboundRow
		var at int64
		var extra string
		if err := rows.Scan(&r.ID, &r.Conv.Interface, &r.Conv.Chat, &r.Conv.Thread, &r.MsgID, &r.From.Platform, &r.From.ID, &r.From.Name, &r.From.Email, &r.From.Team, &r.From.KeyID, &r.Origin, &r.Text, &at, &r.Private, &extra); err != nil {
			return nil, err
		}
		if extra != "" {
			var x inExtra
			if err := json.Unmarshal([]byte(extra), &x); err != nil {
				return nil, fmt.Errorf("inbound %d: unreadable details: %w", r.ID, err)
			}
			x.apply(&r.Inbound)
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
	Checkpoint *CheckpointUpdate // scheduled workflow state and its alerts, committed with the run
	State      string            // a Turn* state
	Detail     string
	ItemState  string // state for the turn's running and steered messages
	Harness    string // harness that ran it ("" if none became ready)
	Session    string // native session to keep for Harness, if known
	Clean      bool   // the session may be resumed
	Forget     bool   // the caller reset their sessions: keep none for this conversation
	Reply      *core.Outbound
	Files      []core.Outbound // sent after Reply
	Notices    []core.Outbound // Heimdall's own messages (job notices, draft cards), after Files
	Run        *RunEnd         // a scheduled turn's run
	Draft      *Job            // a job draft the answer proposed
	Job        *JobChange      // activation of that draft, in this same transaction
}

// EndTurn settles a turn, its messages, its session binding and its reply
// in one transaction.
func (s *Store) EndTurn(turn int64, conv core.ConvKey, who string, e TurnEnd) (replyID int64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if e.Run != nil {
		var state string
		var current bool
		if err = tx.QueryRow(`SELECT r.state,j.version=r.version FROM job_runs r JOIN jobs j ON j.id=r.job_id WHERE r.id=?`, e.Run.ID).Scan(&state, &current); err != nil {
			return 0, err
		}
		if e.Checkpoint != nil && !current && state != RunCancelled {
			e.Checkpoint = nil
			e.State, e.ItemState, e.Clean = TurnFailed, InFailed, false
			e.Reply, e.Files, e.Notices = nil, nil, nil
			e.Run = &RunEnd{ID: e.Run.ID, Job: e.Run.Job, State: RunFailed, Cause: "workflow checkpoint: job version changed"}
		}
		if state == RunCancelled {
			e.Checkpoint = nil
			e.State, e.ItemState, e.Clean = TurnCancelled, InCancelled, false
			e.Reply, e.Files, e.Notices = nil, nil, nil
			e.Run = &RunEnd{ID: e.Run.ID, Job: e.Run.Job, State: RunCancelled, Cause: "stopped by command"}
		}
	}
	if _, err = tx.Exec(`UPDATE turns SET state=?,detail=?,ended_at=? WHERE id=? AND state=?`, e.State, e.Detail, now(), turn, TurnRunning); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(`UPDATE inbound SET state=? WHERE turn_id=? AND state IN (?,?)`, e.ItemState, turn, InRunning, InSteered); err != nil {
		return 0, err
	}
	switch {
	case e.Forget:
		if err = forgetSessions(tx, conv, who); err != nil {
			return 0, err
		}
	case e.Harness != "":
		if err = saveSession(tx, conv, who, e.Harness, e.Session, e.Clean && e.Session != ""); err != nil {
			return 0, err
		}
	}
	if e.Reply != nil {
		if replyID, err = enqueue(tx, *e.Reply); err != nil {
			return 0, err
		}
	}
	if e.Checkpoint != nil {
		if err = saveCheckpoint(tx, turn, who, e.Run, *e.Checkpoint); err != nil {
			return 0, err
		}
	}
	if e.Run != nil {
		if err = endRun(tx, *e.Run); err != nil {
			return 0, err
		}
	}
	if err = enqueueAll(tx, e.Files); err != nil {
		return 0, err
	}
	if e.Run != nil {
		err = enqueueRunNotices(tx, *e.Run, e.Notices)
	} else {
		err = enqueueAll(tx, e.Notices)
	}
	if err != nil {
		return 0, err
	}
	if e.Draft != nil {
		if err = insertJob(tx, *e.Draft); err != nil {
			return 0, err
		}
	}
	if err = e.Job.apply(tx); err != nil {
		return 0, err
	}
	return replyID, tx.Commit()
}

func saveSession(tx *sql.Tx, conv core.ConvKey, who, harness, id string, clean bool) error {
	_, err := tx.Exec(`INSERT INTO sessions(iface,chat,thread,principal,harness,native_id,clean,updated_at) VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(iface,chat,thread,principal,harness) DO UPDATE SET native_id=excluded.native_id,clean=excluded.clean,updated_at=excluded.updated_at`,
		conv.Interface, conv.Chat, conv.Thread, who, harness, id, clean, now())
	return err
}

// forgetSessions drops every native session binding of the caller in conv,
// so the caller's next turn there starts fresh on every harness.
func forgetSessions(tx *sql.Tx, conv core.ConvKey, who string) error {
	_, err := tx.Exec(`DELETE FROM sessions WHERE iface=? AND chat=? AND thread=? AND principal=?`, conv.Interface, conv.Chat, conv.Thread, who)
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

// EarlierSession finds the newest binding for this caller only. A resumed
// session needs a pointer only when another harness was updated more recently.
func (s *Store) EarlierSession(conv core.ConvKey, who, chosen string, resume bool) (name, id string, err error) {
	err = s.db.QueryRow(`SELECT harness,native_id FROM sessions
 WHERE iface=? AND chat=? AND thread=? AND principal=? AND native_id<>''
 AND (?=0 OR (harness<>? AND updated_at>(SELECT updated_at FROM sessions WHERE iface=? AND chat=? AND thread=? AND principal=? AND harness=?)))
 ORDER BY updated_at DESC,harness LIMIT 1`, conv.Interface, conv.Chat, conv.Thread, who, resume, chosen, conv.Interface, conv.Chat, conv.Thread, who, chosen).Scan(&name, &id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return
}

// CommandEnd is how a ! command was answered.
type CommandEnd struct {
	Detail     string
	SetHarness bool          // !use: make Harness sticky for the conversation
	Harness    string        // "" restores the default order
	Forget     string        // reset: forget this caller's sessions in the conversation
	Job        *JobChange    // a job command's change
	Reply      core.Outbound // none without a conversation
}

// AnswerCommand settles a ! command: its message, any harness choice or job
// change, and its reply in one transaction. If it fails nothing is recorded
// and the message stays pending, to be answered after a restart.
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
	if e.Forget != "" {
		if err := forgetSessions(tx, conv, e.Forget); err != nil {
			return err
		}
	}
	if err := e.Job.apply(tx); err != nil {
		return err
	}
	if e.Reply.Conv.Interface != "" {
		if _, err := enqueue(tx, e.Reply); err != nil {
			return err
		}
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

// LastTurnHarness is the harness that ran conv's newest ended turn ("" if
// none). Candidates that failed before starting are unbound, so never count.
func (s *Store) LastTurnHarness(conv core.ConvKey) (string, error) {
	var h string
	err := s.db.QueryRow(`SELECT harness FROM turns WHERE iface=? AND chat=? AND thread=? AND harness!='' AND ended_at IS NOT NULL ORDER BY id DESC LIMIT 1`, conv.Interface, conv.Chat, conv.Thread).Scan(&h)
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
	if err := out.Window.Validate(); err != nil {
		return 0, err
	}
	extra, err := outExtraOf(out)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT INTO outbound(iface,chat,thread,reply_to,text,extra,state,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		out.Conv.Interface, out.Conv.Chat, out.Conv.Thread, out.ReplyTo, out.Text, extra, OutPending, now())
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
	return s.nextOutbound(iface, time.Now())
}

func (s *Store) nextOutbound(iface string, at time.Time) (OutboundRow, bool, error) {
	for {
		var r OutboundRow
		var extra string
		var created int64
		err := s.db.QueryRow(`SELECT o.id,o.chat,o.thread,o.reply_to,o.text,o.extra,o.attempts,o.created_at FROM outbound o
WHERE o.iface=? AND o.state=? AND o.next_at<=?
AND NOT EXISTS(SELECT 1 FROM outbound p WHERE p.iface=o.iface AND p.chat=o.chat AND p.id<o.id AND p.state IN (?,?)
 AND (p.state=? OR p.next_at<=? OR json_extract(nullif(p.extra,''),'$.delivery_window') IS NULL OR json_extract(nullif(o.extra,''),'$.delivery_window') IS NOT NULL))
ORDER BY o.id LIMIT 1`, iface, OutPending, at.UnixMilli(), OutPending, OutSending, OutSending, at.UnixMilli()).
			Scan(&r.ID, &r.Conv.Chat, &r.Conv.Thread, &r.ReplyTo, &r.Text, &extra, &r.Attempts, &created)
		if errors.Is(err, sql.ErrNoRows) {
			return r, false, nil
		}
		if err != nil {
			return r, false, err
		}
		r.Conv.Interface = iface
		r.CreatedAt = time.UnixMilli(created)
		if extra != "" {
			var x outExtra
			if err := json.Unmarshal([]byte(extra), &x); err != nil {
				return r, false, fmt.Errorf("outbound %d: unreadable delivery details: %w", r.ID, err)
			}
			x.apply(&r.Outbound)
		}
		if r.Window != nil {
			if err := r.Window.Validate(); err != nil {
				return r, false, err
			}
			if next := r.Window.Next(at); next.After(at) {
				if _, err := s.db.Exec(`UPDATE outbound SET next_at=? WHERE id=? AND state=?`, next.UnixMilli(), r.ID, OutPending); err != nil {
					return r, false, err
				}
				continue
			}
		}
		return r, true, nil
	}
}

// NextDue is when the earliest reply NextOutbound could offer for iface
// becomes due, if any. Replies queued behind an older undelivered one in
// their chat do not count: they wait for it, not for their own time.
func (s *Store) NextDue(iface string) (time.Time, bool, error) {
	var at sql.NullInt64
	err := s.db.QueryRow(`SELECT min(o.next_at) FROM outbound o
WHERE o.iface=? AND o.state=?
AND NOT EXISTS(SELECT 1 FROM outbound p WHERE p.iface=o.iface AND p.chat=o.chat AND p.id<o.id AND p.state IN (?,?)
 AND (p.state=? OR p.next_at<=? OR json_extract(nullif(p.extra,''),'$.delivery_window') IS NULL OR json_extract(nullif(o.extra,''),'$.delivery_window') IS NOT NULL))`, iface, OutPending, OutPending, OutSending, OutSending, now()).Scan(&at)
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
	return s.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE outbound SET state=?,error=? WHERE id=?`, OutFailed, clip(why), id); err != nil {
			return err
		}
		var job, creator, extra string
		var k core.ConvKey
		var notice int64
		err := tx.QueryRow(`SELECT a.job_id,a.creator,a.failure_notice,j.created_iface,j.created_chat,j.created_thread,o.extra FROM job_alerts a JOIN jobs j ON j.id=a.job_id AND j.creator=a.creator JOIN outbound o ON o.id=a.outbound_id WHERE a.outbound_id=? AND j.created_private=1`, id).Scan(&job, &creator, &notice, &k.Interface, &k.Chat, &k.Thread, &extra)
		if errors.Is(err, sql.ErrNoRows) || err == nil && notice != 0 {
			return nil
		}
		if err != nil {
			return err
		}
		var x outExtra
		if extra != "" {
			if err := json.Unmarshal([]byte(extra), &x); err != nil {
				return err
			}
		}
		// Fixed, private, windowed, once per receipt. This row is not an alert,
		// so its own terminal failure cannot recursively create a notice.
		body := fmt.Sprintf("Workflow alert delivery failed. Job %s, outbound %d. Inspect checkpoint receipts; explicitly acknowledge this failed receipt after reconciliation. No automatic replay. — heimdall", job, id)
		n, err := enqueue(tx, core.Outbound{Conv: k, Text: body, Window: x.Window})
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE job_alerts SET failure_notice=? WHERE job_id=? AND creator=? AND outbound_id=?`, n, job, creator, id)
		return err
	})
}

// Undelivered counts replies not yet delivered or failed.
func (s *Store) Undelivered() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM outbound WHERE state IN (?,?)`, OutPending, OutSending).Scan(&n)
	return n, err
}

// Closed-window pending deliveries are durable deferred work, not in-flight.
// In-window backoff and all sending rows still block drain.
func (s *Store) drainReplies(at time.Time) (int, error) {
	rows, err := s.db.Query(`SELECT state,extra FROM outbound WHERE state IN (?,?)`, OutPending, OutSending)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var state, extra string
		if err := rows.Scan(&state, &extra); err != nil {
			return 0, err
		}
		var x outExtra
		if state == OutPending && json.Unmarshal([]byte(extra), &x) == nil && x.Window != nil && x.Window.Validate() == nil && x.Window.Next(at).After(at) {
			continue
		}
		n++
	}
	return n, rows.Err()
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
		// A scheduled run's notice is the scheduler's (no notice here).
		if out := turnNotice(t); out.Conv.Interface != "" {
			if _, err := enqueue(tx, out); err != nil {
				return rec, err
			}
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

// AmbientKeep is how many recent messages a conversation keeps as context
// (the hub's PI_TELEGRAM_CONTEXT_MAX_LINES).
const AmbientKeep = 1000

// Ambient is one recent message of a group conversation, kept as context.
type Ambient struct {
	ID    int64
	Conv  core.ConvKey
	MsgID string
	At    time.Time
	Who   string // verified principal key
	Name  string // display name, untrusted
	Text  string // already scrubbed and clipped
	File  string // attachment label, if any
}

// AddAmbient keeps one message as context, once, and forgets the
// conversation's messages beyond the newest AmbientKeep.
func (s *Store) AddAmbient(a Ambient) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO ambient(iface,chat,thread,msg_id,at,who,name,text,file) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(iface,chat,msg_id) DO NOTHING`,
		a.Conv.Interface, a.Conv.Chat, a.Conv.Thread, a.MsgID, a.At.UnixMilli(), a.Who, a.Name, a.Text, a.File)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil
	}
	if _, err := tx.Exec(`DELETE FROM ambient WHERE iface=? AND chat=? AND thread=? AND id <= (
	SELECT id FROM ambient WHERE iface=? AND chat=? AND thread=? ORDER BY id DESC LIMIT 1 OFFSET ?)`,
		a.Conv.Interface, a.Conv.Chat, a.Conv.Thread, a.Conv.Interface, a.Conv.Chat, a.Conv.Thread, AmbientKeep); err != nil {
		return err
	}
	return tx.Commit()
}

// RecentAmbient is the conversation's newest limit messages kept as
// context, oldest first, without the messages in skip.
func (s *Store) RecentAmbient(conv core.ConvKey, skip []string, limit int) ([]Ambient, error) {
	q := `SELECT id,msg_id,at,who,name,text,file FROM ambient WHERE iface=? AND chat=? AND thread=?`
	args := []any{conv.Interface, conv.Chat, conv.Thread}
	if len(skip) > 0 {
		q += ` AND msg_id NOT IN (?` + strings.Repeat(",?", len(skip)-1) + `)`
		for _, id := range skip {
			args = append(args, id)
		}
	}
	rows, err := s.db.Query(q+` ORDER BY id DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Ambient
	for rows.Next() {
		a := Ambient{Conv: conv}
		var at int64
		if err := rows.Scan(&a.ID, &a.MsgID, &at, &a.Who, &a.Name, &a.Text, &a.File); err != nil {
			return nil, err
		}
		a.At = time.UnixMilli(at)
		out = append(out, a)
	}
	slices.Reverse(out)
	return out, rows.Err()
}

// FirstMessage records that p has written and reports whether p never
// had before.
func (s *Store) FirstMessage(p core.Principal) (bool, error) {
	res, err := s.db.Exec(`INSERT INTO callers(principal, first_at) VALUES(?,?) ON CONFLICT(principal) DO NOTHING`, p.Key(), now())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// CallerSeen is read-only: failed owner resolution must not consume the notice.
func (s *Store) CallerSeen(p core.Principal) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM callers WHERE principal=?`, p.Key()).Scan(&n)
	return n > 0, err
}

// NotifyCaller commits the seen marker only together with durable deliveries.
func (s *Store) NotifyCaller(p core.Principal, notices []core.Outbound) error {
	if len(notices) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO callers(principal,first_at) VALUES(?,?) ON CONFLICT(principal) DO NOTHING`, p.Key(), now())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	for _, out := range notices {
		if _, err = enqueue(tx, out); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// OwnerNotified checks durable per-owner delivery, independent of other owners.
func (s *Store) OwnerNotified(p core.Principal, owner string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM caller_notices WHERE principal=? AND (owner=? OR owner='*')`, p.Key(), owner).Scan(&n)
	return n > 0, err
}
func (s *Store) NotifyCallerOwner(p core.Principal, owner string, out core.Outbound) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO caller_notices(principal,owner) VALUES(?,?) ON CONFLICT DO NOTHING`, p.Key(), owner)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if _, err = enqueue(tx, out); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO callers(principal,first_at) VALUES(?,?) ON CONFLICT DO NOTHING`, p.Key(), now()); err != nil {
		return err
	}
	return tx.Commit()
}
