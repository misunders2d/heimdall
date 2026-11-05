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
	Version  int             `json:"version"`
	Through  string          `json:"through,omitempty"`
	State    json.RawMessage `json:"state"`
	Receipts []AlertReceipt  `json:"receipts"`
}

type AlertReceipt struct {
	ContentHash string `json:"content_hash"`
	Key         string `json:"key"`
	Revision    string `json:"revision"`
	Outbound    int64  `json:"outbound_id"`
	Status      string `json:"status"` // pending/sending/sent/failed; sending is not confirmation
}

type Alert struct {
	Key      string `json:"key"`
	Revision string `json:"revision"`
	Text     string `json:"text"`
}

type CheckpointUpdate struct {
	Version  int                  `json:"version"`
	Complete bool                 `json:"complete"`
	Through  string               `json:"through"`
	State    json.RawMessage      `json:"state"`
	Alerts   []Alert              `json:"alerts"`
	Retire   []string             `json:"retire,omitempty"` // resolved keys, only after confirmed delivery
	Target   core.ConvKey         `json:"-"`
	Window   *core.DeliveryWindow `json:"-"`
}

func (s *Store) Checkpoint(job, creator string) (Checkpoint, error) {
	c := Checkpoint{State: json.RawMessage(`{}`), Receipts: []AlertReceipt{}}
	var state string
	err := s.db.QueryRow(`SELECT version,through_at,state FROM job_checkpoints WHERE job_id=? AND creator=?`, job, creator).Scan(&c.Version, &c.Through, &state)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	if state != "" {
		c.State = json.RawMessage(state)
	}
	rows, err := s.db.Query(`SELECT a.key,a.revision,a.outbound_id,o.state,o.text FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? ORDER BY a.key`, job, creator)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var r AlertReceipt
		var body string
		if err := rows.Scan(&r.Key, &r.Revision, &r.Outbound, &r.Status, &body); err != nil {
			return c, err
		}
		r.ContentHash = fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
		c.Receipts = append(c.Receipts, r)
	}
	return c, rows.Err()
}

func saveCheckpoint(tx *sql.Tx, turn int64, who string, run *RunEnd, u CheckpointUpdate) error {
	if run == nil || (run.State != RunOK && run.State != RunQuiet) || !u.Complete {
		return errors.New("checkpoint requires a successful complete run")
	}
	var creator, state string
	var inboundTurn int64
	if err := tx.QueryRow(`SELECT j.creator,r.state,coalesce(i.turn_id,0) FROM jobs j JOIN job_runs r ON r.job_id=j.id LEFT JOIN inbound i ON i.id=r.inbound_id WHERE j.id=? AND r.id=?`, run.Job, run.ID).Scan(&creator, &state, &inboundTurn); err != nil {
		return err
	}
	if creator != who || state != RunRunning || inboundTurn != turn {
		return errors.New("checkpoint run or creator is no longer current")
	}
	var version int
	var through string
	err := tx.QueryRow(`SELECT version,through_at FROM job_checkpoints WHERE job_id=? AND creator=?`, run.Job, who).Scan(&version, &through)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version != u.Version {
		return errors.New("checkpoint changed since scan started")
	}
	a, ea := time.Parse(time.RFC3339Nano, u.Through)
	b, _ := time.Parse(time.RFC3339Nano, through)
	if ea != nil || a.Before(b) || a.After(time.Now().Add(time.Minute)) || len(u.State) > 24576 || !json.Valid(u.State) {
		return errors.New("invalid checkpoint watermark or state")
	}
	for _, key := range u.Retire {
		var status string
		err := tx.QueryRow(`SELECT o.state FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? AND a.key=?`, run.Job, who, key).Scan(&status)
		if err != nil || status != OutSent {
			return errors.New("only confirmed resolved receipts may retire")
		}
		if _, err := tx.Exec(`DELETE FROM job_alerts WHERE job_id=? AND creator=? AND key=?`, run.Job, who, key); err != nil {
			return err
		}
	}
	for _, alert := range u.Alerts {
		var rev, body string
		err := tx.QueryRow(`SELECT a.revision,o.text FROM job_alerts a JOIN outbound o ON o.id=a.outbound_id WHERE a.job_id=? AND a.creator=? AND a.key=?`, run.Job, who, alert.Key).Scan(&rev, &body)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if rev == alert.Revision {
			if body != alert.Text {
				return errors.New("alert text changed without a new revision")
			}
			continue // pending, ambiguous, failed or confirmed: never blindly replay
		}
		id, err := enqueue(tx, core.Outbound{TurnID: turn, Conv: u.Target, Text: alert.Text, Formatted: true, Window: u.Window})
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO job_alerts(job_id,creator,key,revision,outbound_id) VALUES(?,?,?,?,?) ON CONFLICT(job_id,creator,key) DO UPDATE SET revision=excluded.revision,outbound_id=excluded.outbound_id`, run.Job, who, alert.Key, alert.Revision, id); err != nil {
			return err
		}
	}
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM job_alerts WHERE job_id=? AND creator=?`, run.Job, who).Scan(&count); err != nil {
		return err
	}
	if count > 512 {
		return errors.New("checkpoint alert key limit reached; owner review needed")
	}
	_, err = tx.Exec(`INSERT INTO job_checkpoints(job_id,creator,version,through_at,state,run_id) VALUES(?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version,through_at=excluded.through_at,state=excluded.state,run_id=excluded.run_id WHERE creator=excluded.creator`, run.Job, who, version+1, u.Through, string(u.State), run.ID)
	return err
}

// ControlBinding reads a real private platform ingress, without reading its text.
// A selector is deployment configuration, not caller-supplied authority. Admission
// and, for Slack, live platform identity still have to be rechecked by scheduler.
func (s *Store) ControlBinding(key string) (core.Principal, core.ConvKey, error) {
	p, err := core.ParsePrincipalKey(key)
	if err != nil {
		return p, core.ConvKey{}, err
	}
	var k core.ConvKey
	err = s.db.QueryRow(`SELECT iface,chat,thread,user_email,user_team,user_key_id FROM inbound WHERE platform=? AND user_id=? AND private=1 AND origin=? AND coalesce(json_extract(nullif(extra,''),'$.mcp_control'),0)=0 ORDER BY id DESC LIMIT 1`, p.Platform, p.ID, core.OriginLive).Scan(&k.Interface, &k.Chat, &k.Thread, &p.Email, &p.Team, &p.KeyID)
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
