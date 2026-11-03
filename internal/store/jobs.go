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
)

// Job states.
const (
	JobDraft   = "draft"
	JobActive  = "active"
	JobPaused  = "paused"
	JobDone    = "done"
	JobDeleted = "deleted" // tombstone, purged after 30 days
)

// Run states (docs/scheduler.md §3.9).
const (
	RunPending     = "pending" // waiting for its start (capacity, retry backoff)
	RunRunning     = "running" // handed to the router
	RunOK          = "ok"
	RunQuiet       = "quiet"
	RunFailed      = "failed"
	RunUnverified  = "unverified"
	RunSkipped     = "skipped"
	RunMissed      = "missed"
	RunInterrupted = "interrupted"
	RunCancelled   = "cancelled"
)

// Job is a job or a draft of one. Def is its normalized definition
// (scheduler.Def, JSON).
type Job struct {
	ID, Name, Kind   string
	Creator          core.Principal
	CreatedIn        core.ConvKey
	CreatedPrivate   bool
	Def              string
	State, Reason    string
	EditOf           string // a draft that changes this job
	DraftExpires     time.Time
	NextSlot         string
	NextDue          time.Time // zero: no further slot
	Version          int
	Fails, Skips     int // in a row: failed or unverified runs; skipped slots
	Created, Updated time.Time
}

const jobCols = `id,name,kind,principal,created_iface,created_chat,created_thread,created_private,def,state,reason,edit_of,draft_expires_at,next_slot,next_due,version,fails,skips,created_at,updated_at`

func scanJob(row interface{ Scan(...any) error }) (Job, error) {
	var j Job
	var principal string
	var expires, due, created, updated int64
	err := row.Scan(&j.ID, &j.Name, &j.Kind, &principal, &j.CreatedIn.Interface, &j.CreatedIn.Chat, &j.CreatedIn.Thread, &j.CreatedPrivate, &j.Def, &j.State, &j.Reason, &j.EditOf, &expires, &j.NextSlot, &due, &j.Version, &j.Fails, &j.Skips, &created, &updated)
	if err != nil {
		return j, err
	}
	if err := json.Unmarshal([]byte(principal), &j.Creator); err != nil {
		return j, fmt.Errorf("job %s: unreadable creator: %w", j.ID, err)
	}
	j.DraftExpires, j.NextDue, j.Created, j.Updated = ms(expires), ms(due), ms(created), ms(updated)
	return j, nil
}

func ms(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(v)
}

func unix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func insertJob(tx *sql.Tx, j Job) error {
	principal, err := json.Marshal(j.Creator)
	if err != nil {
		return err
	}
	created := unix(j.Created) // the anchor of fixed intervals
	if created == 0 {
		created = now()
	}
	_, err = tx.Exec(`INSERT INTO jobs(id,name,kind,creator,principal,created_iface,created_chat,created_thread,created_private,def,state,reason,edit_of,draft_expires_at,next_slot,next_due,version,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)`, j.ID, j.Name, j.Kind, j.Creator.Key(), string(principal), j.CreatedIn.Interface, j.CreatedIn.Chat, j.CreatedIn.Thread, j.CreatedPrivate, j.Def, j.State, j.Reason, j.EditOf, unix(j.DraftExpires), j.NextSlot, unix(j.NextDue), created, now())
	return err
}

// JobChange is what one job command changes. The router stores it with the
// command's answer in one transaction (AnswerCommand), so a crash between
// the two can neither lose the answer nor replay the change.
type JobChange struct {
	Add     *Job   // a new job or draft
	Update  string // a job Set changes
	Confirm *Job   // a draft Set confirms (a new job, or an edit of its job)
	Set     Set
	Drop    string // a draft to delete
	Run     *Run   // a manual run
}

func (ch *JobChange) apply(tx *sql.Tx) error {
	switch {
	case ch == nil:
	case ch.Add != nil:
		return insertJob(tx, *ch.Add)
	case ch.Update != "":
		return updateJob(tx, ch.Update, ch.Set)
	case ch.Confirm != nil:
		return confirmDraft(tx, *ch.Confirm, ch.Set)
	case ch.Drop != "":
		_, err := tx.Exec(`DELETE FROM jobs WHERE id=? AND state=?`, ch.Drop, JobDraft)
		return err
	case ch.Run != nil:
		_, err := createRun(tx, *ch.Run, "", time.Time{}, nil)
		return err
	}
	return nil
}

// Job is one job by ID.
func (s *Store) Job(id string) (Job, bool, error) {
	j, err := scanJob(s.db.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, false, nil
	}
	return j, err == nil, err
}

// JobFilter selects jobs; zero fields match everything.
type JobFilter struct {
	Creator string       // principal key
	States  []string     // any of
	Conv    core.ConvKey // created in
	DueBy   time.Time    // with a slot due by then
}

// Jobs lists jobs by creation, oldest first.
func (s *Store) Jobs(f JobFilter) ([]Job, error) {
	q, args := `SELECT `+jobCols+` FROM jobs WHERE 1=1`, []any{}
	if f.Creator != "" {
		q, args = q+` AND creator=?`, append(args, f.Creator)
	}
	if len(f.States) > 0 {
		q += ` AND state IN (?` + strings.Repeat(",?", len(f.States)-1) + `)`
		for _, st := range f.States {
			args = append(args, st)
		}
	}
	if f.Conv.Interface != "" {
		q, args = q+` AND created_iface=? AND created_chat=? AND created_thread=?`, append(args, f.Conv.Interface, f.Conv.Chat, f.Conv.Thread)
	}
	if !f.DueBy.IsZero() {
		q, args = q+` AND next_due>0 AND next_due<=?`, append(args, f.DueBy.UnixMilli())
	}
	rows, err := s.db.Query(q+` ORDER BY created_at, id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Set names job columns to change.
type Set map[string]any

var settable = []string{"name", "kind", "def", "state", "reason", "next_slot", "next_due", "version", "fails", "skips", "draft_expires_at"}

// UpdateJob changes a job's columns and stores notices, in one transaction.
// Times are stored as Unix milliseconds.
func (s *Store) UpdateJob(id string, set Set, outs ...core.Outbound) error {
	return s.tx(func(tx *sql.Tx) error {
		if err := updateJob(tx, id, set); err != nil {
			return err
		}
		return enqueueAll(tx, outs)
	})
}

func updateJob(tx *sql.Tx, id string, set Set) error {
	q, args := `UPDATE jobs SET updated_at=?`, []any{now()}
	for _, k := range settable {
		if v, ok := set[k]; ok {
			if t, ok := v.(time.Time); ok {
				v = unix(t)
			}
			q, args = q+`,`+k+`=?`, append(args, v)
		}
	}
	if len(args) != len(set)+1 {
		return errors.New("store: unknown job column")
	}
	_, err := tx.Exec(q+` WHERE id=?`, append(args, id)...)
	return err
}

// confirmDraft makes a new job's draft a job (active unless set says
// otherwise), or applies an edit draft to its job (version+1) and drops the
// draft, with set's columns.
func confirmDraft(tx *sql.Tx, draft Job, set Set) error {
	if draft.EditOf == "" {
		if _, ok := set["state"]; !ok {
			set["state"] = JobActive
		}
		return updateJob(tx, draft.ID, set)
	}
	if _, err := tx.Exec(`UPDATE jobs SET version=version+1 WHERE id=?`, draft.EditOf); err != nil {
		return err
	}
	if err := updateJob(tx, draft.EditOf, set); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM jobs WHERE id=? AND state=?`, draft.ID, JobDraft)
	return err
}

// Run is one job run.
type Run struct {
	ID        int64
	Job       string
	Version   int
	Slot      string
	Due       time.Time
	Trigger   string // schedule, manual
	State     string
	Attempt   int
	NotBefore time.Time
	Inbound   int64
	Cause     string
	Started   time.Time
	Ended     time.Time
	Replayed  bool // re-run once after a restart (idempotent jobs)
}

const runCols = `id,job_id,version,slot,due_at,trigger,state,attempt,not_before,coalesce(inbound_id,0),cause,started_at,ended_at,replayed`

func scanRun(row interface{ Scan(...any) error }) (Run, error) {
	var r Run
	var due, nb, started, ended int64
	err := row.Scan(&r.ID, &r.Job, &r.Version, &r.Slot, &due, &r.Trigger, &r.State, &r.Attempt, &nb, &r.Inbound, &r.Cause, &started, &ended, &r.Replayed)
	r.Due, r.NotBefore, r.Started, r.Ended = ms(due), ms(nb), ms(started), ms(ended)
	return r, err
}

// CreateRun records a slot's run and moves the job to its next slot, in
// one transaction. UNIQUE(job_id, slot) makes a second run of a slot
// impossible: then nothing is created, but the job still moves on. A
// skipped slot counts towards the job's skips in a row; a pending run
// resets them.
func (s *Store) CreateRun(r Run, nextSlot string, nextDue time.Time, outs ...core.Outbound) (created bool, err error) {
	err = s.tx(func(tx *sql.Tx) error {
		created, err = createRun(tx, r, nextSlot, nextDue, outs)
		return err
	})
	return created, err
}

func createRun(tx *sql.Tx, r Run, nextSlot string, nextDue time.Time, outs []core.Outbound) (created bool, err error) {
	ended := int64(0)
	if r.State != RunPending {
		ended = now()
	}
	res, err := tx.Exec(`INSERT INTO job_runs(job_id,version,slot,due_at,trigger,state,cause,not_before,ended_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id,slot) DO NOTHING`,
		r.Job, r.Version, r.Slot, unix(r.Due), r.Trigger, r.State, clip(r.Cause), unix(r.NotBefore), ended)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	created = n == 1
	skips := `skips`
	switch {
	case created && r.State == RunSkipped:
		skips = `skips+1`
	case created && r.State == RunPending:
		skips = `0`
	}
	if r.Trigger == "manual" {
		_, err = tx.Exec(`UPDATE jobs SET skips=`+skips+` WHERE id=?`, r.Job)
	} else {
		_, err = tx.Exec(`UPDATE jobs SET next_slot=?,next_due=?,skips=`+skips+` WHERE id=?`, nextSlot, unix(nextDue), r.Job)
	}
	if err != nil || !created {
		return created, err
	}
	return created, enqueueAll(tx, outs)
}

// RunFilter selects runs; zero fields match everything.
type RunFilter struct {
	Job    string
	States []string
	Limit  int // newest first when set; otherwise by due time
}

// Runs lists runs.
func (s *Store) Runs(f RunFilter) ([]Run, error) {
	q, args := `SELECT `+runCols+` FROM job_runs WHERE 1=1`, []any{}
	if f.Job != "" {
		q, args = q+` AND job_id=?`, append(args, f.Job)
	}
	if len(f.States) > 0 {
		q += ` AND state IN (?` + strings.Repeat(",?", len(f.States)-1) + `)`
		for _, st := range f.States {
			args = append(args, st)
		}
	}
	if f.Limit > 0 {
		q, args = q+` ORDER BY id DESC LIMIT ?`, append(args, f.Limit)
	} else {
		q += ` ORDER BY due_at, id`
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Run is one run by ID.
func (s *Store) Run(id int64) (Run, error) {
	return scanRun(s.db.QueryRow(`SELECT `+runCols+` FROM job_runs WHERE id=?`, id))
}

// StartRun hands a pending run to the router: its message is stored as
// pending inbound and the run becomes running, in one transaction.
func (s *Store) StartRun(run Run, in core.Inbound) (InboundRow, error) {
	extra, err := inExtraOf(in)
	if err != nil {
		return InboundRow{}, err
	}
	var row InboundRow
	err = s.tx(func(tx *sql.Tx) error {
		var dup bool
		if row, dup, err = addInbound(tx, in, in.At, extra); err != nil {
			return err
		}
		if dup {
			return fmt.Errorf("run %d: message %s already stored", run.ID, in.MsgID)
		}
		res, err := tx.Exec(`UPDATE job_runs SET state=?,inbound_id=?,started_at=? WHERE id=? AND state=?`, RunRunning, row.ID, now(), run.ID, RunPending)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("run %d is no longer pending", run.ID)
		}
		return nil
	})
	return row, err
}

// RunEnd settles a run.
type RunEnd struct {
	ID        int64
	Job       string
	State     string
	Cause     string
	Retry     time.Time // set: back to pending, due then, as the next attempt
	Replay    bool      // with Retry: the run's one re-run after a restart
	Inbound   int64     // set: its message, still pending, is cancelled
	JobState  string    // set: the job becomes paused or done
	JobReason string
}

func endRun(tx *sql.Tx, e RunEnd) error {
	if !e.Retry.IsZero() {
		_, err := tx.Exec(`UPDATE job_runs SET state=?,attempt=attempt+1,not_before=?,cause=?,inbound_id=NULL,replayed=replayed OR ? WHERE id=?`, RunPending, e.Retry.UnixMilli(), clip(e.Cause), e.Replay, e.ID)
		return err
	}
	if _, err := tx.Exec(`UPDATE job_runs SET state=?,cause=?,ended_at=? WHERE id=?`, e.State, clip(e.Cause), now(), e.ID); err != nil {
		return err
	}
	fails := map[string]string{RunFailed: `fails+1`, RunUnverified: `fails+1`, RunOK: `0`, RunQuiet: `0`}[e.State]
	if fails != "" {
		if _, err := tx.Exec(`UPDATE jobs SET fails=`+fails+` WHERE id=?`, e.Job); err != nil {
			return err
		}
	}
	if e.Inbound != 0 {
		if _, err := tx.Exec(`UPDATE inbound SET state=?,detail=? WHERE id=? AND state=?`, InCancelled, clip(e.Cause), e.Inbound, InPending); err != nil {
			return err
		}
	}
	if e.JobState != "" {
		_, err := tx.Exec(`UPDATE jobs SET state=?,reason=?,updated_at=? WHERE id=? AND state IN (?,?)`, e.JobState, e.JobReason, now(), e.Job, JobActive, JobPaused)
		return err
	}
	return nil
}

// EndRun settles a run outside a turn, with its notices.
func (s *Store) EndRun(e RunEnd, outs ...core.Outbound) error {
	return s.tx(func(tx *sql.Tx) error {
		if err := endRun(tx, e); err != nil {
			return err
		}
		return enqueueAll(tx, outs)
	})
}

// HeldRun is a running run at startup, with its message's state and whether
// its turn reached the harness's ready signal.
type HeldRun struct {
	Run
	InState string
	Ready   bool
}

// HeldRuns lists the runs handed to the router before a restart.
func (s *Store) HeldRuns() ([]HeldRun, error) {
	rows, err := s.db.Query(`SELECT r.id,r.job_id,r.version,r.slot,r.due_at,r.trigger,r.state,r.attempt,r.not_before,coalesce(r.inbound_id,0),r.cause,r.started_at,r.ended_at,r.replayed,coalesce(i.state,''),coalesce(t.ready,0)
FROM job_runs r LEFT JOIN inbound i ON i.id=r.inbound_id LEFT JOIN turns t ON t.id=i.turn_id WHERE r.state=? ORDER BY r.id`, RunRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HeldRun
	for rows.Next() {
		var h HeldRun
		var due, nb, started, ended int64
		if err := rows.Scan(&h.ID, &h.Job, &h.Version, &h.Slot, &due, &h.Trigger, &h.State, &h.Attempt, &nb, &h.Inbound, &h.Cause, &started, &ended, &h.Replayed, &h.InState, &h.Ready); err != nil {
			return nil, err
		}
		h.Due, h.NotBefore, h.Started, h.Ended = ms(due), ms(nb), ms(started), ms(ended)
		out = append(out, h)
	}
	return out, rows.Err()
}

// TurnReady records that a turn's harness signalled ready: from then on an
// interruption may have had side effects.
func (s *Store) TurnReady(turn int64) error {
	_, err := s.db.Exec(`UPDATE turns SET ready=1 WHERE id=?`, turn)
	return err
}

// Meta reads a scheduler value (0 when unset).
func (s *Store) Meta(key string) (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT value FROM sched_meta WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// SetMeta writes a scheduler value.
func (s *Store) SetMeta(key string, v int64) error {
	_, err := s.db.Exec(`INSERT INTO sched_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, v)
	return err
}

// PrivateChat is the newest private chat p wrote to the bot from, if any.
func (s *Store) PrivateChat(p core.Principal) (core.ConvKey, bool, error) {
	var k core.ConvKey
	err := s.db.QueryRow(`SELECT iface,chat,thread FROM inbound WHERE platform=? AND user_id=? AND private=1 AND origin=? ORDER BY id DESC LIMIT 1`, p.Platform, p.ID, core.OriginLive).Scan(&k.Interface, &k.Chat, &k.Thread)
	if errors.Is(err, sql.ErrNoRows) {
		return k, false, nil
	}
	return k, err == nil, err
}

// JobReplies are the newest n texts delivered for a job's runs, newest
// first.
func (s *Store) JobReplies(job string, n int) ([]string, error) {
	rows, err := s.db.Query(`SELECT text FROM outbound WHERE state=? AND text!='' AND json_valid(extra)
AND json_extract(extra,'$.turn_id') IN (SELECT id FROM turns WHERE chat=?) ORDER BY id DESC LIMIT ?`, OutSent, core.SchedChat(job), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Prune drops ended runs older than runAge (keeping each job's newest 100),
// deleted jobs older than jobAge, and expired drafts.
func (s *Store) Prune(at time.Time, runAge, jobAge time.Duration) error {
	return s.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM job_runs WHERE ended_at>0 AND ended_at<? AND id NOT IN (
	SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY id DESC) n FROM job_runs) WHERE n<=100)`, at.Add(-runAge).UnixMilli()); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM job_runs WHERE job_id IN (SELECT id FROM jobs WHERE state=? AND updated_at<?)`, JobDeleted, at.Add(-jobAge).UnixMilli()); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM jobs WHERE state=? AND updated_at<?`, JobDeleted, at.Add(-jobAge).UnixMilli()); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM jobs WHERE state=? AND draft_expires_at<?`, JobDraft, at.UnixMilli())
		return err
	})
}

// Busy counts running turns and undelivered replies: drain waits for both
// to be zero.
func (s *Store) Busy() (turns, replies int, err error) {
	if err = s.db.QueryRow(`SELECT count(*) FROM turns WHERE state=?`, TurnRunning).Scan(&turns); err != nil {
		return 0, 0, err
	}
	replies, err = s.Undelivered()
	return turns, replies, err
}

func (s *Store) tx(f func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func enqueueAll(tx *sql.Tx, outs []core.Outbound) error {
	for _, o := range outs {
		if _, err := enqueue(tx, o); err != nil {
			return err
		}
	}
	return nil
}

// RunStates are the states a run ends in, for display.
var RunStates = []string{RunOK, RunQuiet, RunFailed, RunUnverified, RunSkipped, RunMissed, RunInterrupted, RunCancelled}

// Ended reports a terminal run state.
func Ended(state string) bool { return slices.Contains(RunStates, state) }
