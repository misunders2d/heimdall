package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// Checkpoint contains workflow-owned reader state. Delivery receipts are derived
// from the sole outbox, never assertions made by a model.
type Checkpoint struct {
	Version     int             `json:"version"`
	ScanThrough string          `json:"scan_through,omitempty"`
	Partial     bool            `json:"partial"`
	Cause       string          `json:"cause,omitempty"`
	Through     string          `json:"through"`
	State       json.RawMessage `json:"state"`
	Receipts    []AlertReceipt  `json:"receipts"`
}

type AlertReceipt struct {
	Acknowledged bool   `json:"acknowledged"`
	ContentHash  string `json:"content_hash"`
	Key          string `json:"key"`
	Revision     string `json:"revision"`
	Outbound     int64  `json:"outbound_id"`
	Status       string `json:"status"` // pending/sending/sent/failed; sending is not confirmation
}

type Alert struct {
	Key      string `json:"key"`
	Revision string `json:"revision"`
	Text     string `json:"text"`
}

// Acknowledgement never changes the outbox state or replays an effect. The
// failed receipt and original body remain visible for reconciliation/audit.
type AlertAcknowledgement struct {
	Job, Creator, Key, Revision string
	Outbound                    int64
}

type CheckpointUpdate struct {
	Version     int                  `json:"version"`
	Complete    bool                 `json:"complete"`
	Cause       string               `json:"cause,omitempty"`
	ScanThrough string               `json:"-"`
	Progress    bool                 `json:"-"`
	Through     string               `json:"through"`
	State       json.RawMessage      `json:"state"`
	Alerts      []Alert              `json:"alerts"`
	Retire      []string             `json:"retire,omitempty"` // resolved keys, only after confirmed delivery
	Target      core.ConvKey         `json:"-"`
	Window      *core.DeliveryWindow `json:"-"`
}

func (s *Store) Checkpoint(job, creator string) (Checkpoint, error) {
	c := Checkpoint{State: json.RawMessage(`{}`), Receipts: []AlertReceipt{}}
	var state string
	err := s.db.QueryRow(`SELECT version,through_at,state,scan_through,partial,cause FROM job_checkpoints WHERE job_id=? AND creator=?`, job, creator).Scan(&c.Version, &c.Through, &state, &c.ScanThrough, &c.Partial, &c.Cause)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	if state != "" {
		c.State = json.RawMessage(state)
	}
	rows, err := s.db.Query(`SELECT a.key,a.revision,a.outbound_id,o.state,o.text,a.acknowledged_at>0 FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? AND a.retired=0 ORDER BY a.key,a.outbound_id`, job, creator)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var r AlertReceipt
		var body string
		if err := rows.Scan(&r.Key, &r.Revision, &r.Outbound, &r.Status, &body, &r.Acknowledged); err != nil {
			return c, err
		}
		r.ContentHash = fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
		c.Receipts = append(c.Receipts, r)
	}
	return c, rows.Err()
}

// Retired sent revisions still deduplicate and cannot fake new scan progress.
func (s *Store) AlertKnown(job, key, revision string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM job_alerts WHERE job_id=? AND key=? AND revision=? AND creator=(SELECT creator FROM jobs WHERE id=?)`, job, key, revision, job).Scan(&n)
	return n > 0, err
}

func saveCheckpoint(tx *sql.Tx, turn int64, who string, run *RunEnd, u CheckpointUpdate) error {
	if run == nil || (run.State != RunOK && run.State != RunQuiet && run.State != RunPartial) {
		return errors.New("checkpoint requires a clean validated run")
	}
	var creator, state string
	var inboundTurn int64
	var version int
	if err := tx.QueryRow(`SELECT j.creator,r.state,coalesce(i.turn_id,0),j.version=r.version FROM jobs j JOIN job_runs r ON r.job_id=j.id LEFT JOIN inbound i ON i.id=r.inbound_id WHERE j.id=? AND r.id=?`, run.Job, run.ID).Scan(&creator, &state, &inboundTurn, &version); err != nil {
		return err
	}
	if creator != who || state != RunRunning || inboundTurn != turn || version != 1 {
		return errors.New("checkpoint run, version or creator is no longer current")
	}
	version = 0
	var through string
	err := tx.QueryRow(`SELECT version,through_at FROM job_checkpoints WHERE job_id=? AND creator=?`, run.Job, who).Scan(&version, &through)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version != u.Version {
		return errors.New("checkpoint changed since scan started")
	}
	if !u.Complete && u.Through != through {
		return errors.New("partial scan cannot advance watermark")
	}
	if u.Complete {
		a, ea := time.Parse(time.RFC3339Nano, u.Through)
		b, _ := time.Parse(time.RFC3339Nano, through)
		if ea != nil || a.Before(b) || a.After(time.Now().Add(time.Minute)) {
			return errors.New("invalid checkpoint watermark")
		}
	}
	if len(u.State) > 24576 || !json.Valid(u.State) {
		return errors.New("invalid checkpoint state")
	}
	for _, key := range u.Retire {
		var pending int
		if err := tx.QueryRow(`SELECT count(*) FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? AND a.key=? AND a.retired=0 AND o.state!=?`, run.Job, who, key, OutSent).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return errors.New("only confirmed resolved receipts may retire")
		}
		if _, err := tx.Exec(`UPDATE job_alerts SET retired=1 WHERE job_id=? AND creator=? AND key=?`, run.Job, who, key); err != nil {
			return err
		}
	}
	// Every finding, including overflow, gets its original durable outbox row in
	// this transaction. Only twenty new rows are eligible for delivery per scan.
	// Held rows are released by later clean scans, never by re-generating text.
	releases := 20
	for _, alert := range u.Alerts {
		var exists int
		if err := tx.QueryRow(`SELECT count(*) FROM job_alerts WHERE job_id=? AND creator=? AND key=? AND revision=?`, run.Job, who, alert.Key, alert.Revision).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			continue
		}
		id, err := enqueue(tx, core.Outbound{TurnID: turn, Conv: u.Target, Text: alert.Text, Formatted: true, Window: u.Window})
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO job_alerts(job_id,creator,key,revision,outbound_id) VALUES(?,?,?,?,?)`, run.Job, who, alert.Key, alert.Revision, id); err != nil {
			return err
		}
		// A held row cannot be mistaken for a pending/ambiguous delivery.
		if _, err = tx.Exec(`UPDATE outbound SET state=? WHERE id=?`, OutHeld, id); err != nil {
			return err
		}
	}
	rows, err := tx.Query(`SELECT a.outbound_id FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? AND o.state=? ORDER BY a.outbound_id LIMIT ?`, run.Job, who, OutHeld, releases)
	if err != nil {
		return err
	}
	var release []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		release = append(release, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range release {
		if _, err := tx.Exec(`UPDATE outbound SET state=? WHERE id=? AND state=?`, OutPending, id, OutHeld); err != nil {
			return err
		}
	}
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM job_alerts WHERE job_id=? AND creator=? AND retired=0`, run.Job, who).Scan(&count); err != nil {
		return err
	}
	if count > 512 {
		return errors.New("checkpoint receipt limit reached; owner review needed")
	}
	_, err = tx.Exec(`INSERT INTO job_checkpoints(job_id,creator,version,through_at,state,run_id,scan_through,partial,cause) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version,through_at=excluded.through_at,state=excluded.state,run_id=excluded.run_id,scan_through=excluded.scan_through,partial=excluded.partial,cause=excluded.cause WHERE creator=excluded.creator`, run.Job, who, version+1, u.Through, string(u.State), run.ID, u.ScanThrough, !u.Complete, u.Cause)
	return err
}

// ControlBinding reads a real private platform ingress, without reading its text.
// A selector is deployment configuration, not caller-supplied authority. Admission
// and, for Slack, live platform identity still have to be rechecked by scheduler.
func (s *Store) ControlBinding(key string, source ...*core.ConvKey) (core.Principal, core.ConvKey, error) {
	p, err := core.ParsePrincipalKey(key)
	if err != nil {
		return p, core.ConvKey{}, err
	}
	var k core.ConvKey
	query := `SELECT iface,chat,thread,user_email,user_team,user_key_id FROM inbound WHERE platform=? AND user_id=? AND private=1 AND origin=? AND coalesce(json_extract(nullif(extra,''),'$.mcp_control'),0)=0`
	args := []any{p.Platform, p.ID, core.OriginLive}
	if len(source) > 0 && source[0] != nil {
		k := source[0]
		query += ` AND iface=? AND chat=? AND thread=?`
		args = append(args, k.Interface, k.Chat, k.Thread)
	}
	err = s.db.QueryRow(query+` ORDER BY id DESC LIMIT 1`, args...).Scan(&k.Interface, &k.Chat, &k.Thread, &p.Email, &p.Team, &p.KeyID)
	if err != nil {
		return p, k, fmt.Errorf("owner has no registered private platform conversation")
	}
	if k.Interface != p.Platform || (p.Platform != core.PlatformTelegram && p.Platform != core.PlatformSlack) {
		return p, k, errors.New("invalid private platform binding")
	}
	return p, k, nil
}

// ControlCommit applies a canonical JobChange, reply and content-free operator
// provenance atomically. Request IDs make lost control replies safely retryable.
func (s *Store) ControlCommit(creator, requestID, digest, action, job, response string, uid uint32, ch *JobChange, outs []core.Outbound) error {
	return s.tx(func(tx *sql.Tx) error {
		if err := ch.apply(tx); err != nil {
			return err
		}
		if err := enqueueAll(tx, outs); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO scheduler_controls(creator,request_id,digest,operator_uid,action,job_id,response,created_at) VALUES(?,?,?,?,?,?,?,?)`, creator, requestID, digest, uid, action, job, response, now())
		return err
	})
}

func (s *Store) ControlResponse(creator, requestID, digest string) (string, bool, error) {
	var d, response string
	err := s.db.QueryRow(`SELECT digest,response FROM scheduler_controls WHERE creator=? AND request_id=?`, creator, requestID).Scan(&d, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if d != digest {
		return "", false, errors.New("request_id already used for another operation")
	}
	return response, true, nil
}

// ControlOrigin verifies an exact trusted live ingress without choosing a source.
// The caller and conversation come from the active harness turn, never arguments.
func (s *Store) ControlOrigin(p core.Principal, k core.ConvKey, private bool) error {
	if k.Interface != p.Platform || (p.Platform != core.PlatformTelegram && p.Platform != core.PlatformSlack) {
		return errors.New("invalid platform request origin")
	}
	var found int
	err := s.db.QueryRow(`SELECT 1 FROM inbound WHERE platform=? AND user_id=? AND iface=? AND chat=? AND thread=? AND private=? AND user_team=? AND origin=? AND coalesce(json_extract(nullif(extra,''),'$.mcp_control'),0)=0 LIMIT 1`, p.Platform, p.ID, k.Interface, k.Chat, k.Thread, private, p.Team, core.OriginLive).Scan(&found)
	if err != nil {
		return errors.New("request origin has no matching live ingress")
	}
	return nil
}
