package store

import (
	"context"
	"time"
)

// Retention (owner, 2026-11-10): stored message text and context are kept
// for a limited time (config retention_days, default 30). The running
// service removes them; everything that identity, dedupe and history need
// stays: rows, message IDs, principals, states, timestamps, receipts, effect
// keys, job and run history. Only finished rows lose text:
//
//   - inbound in a terminal state: text, and the extra JSON's context and
//     review message (every other extra field, read by authority lookups,
//     stays);
//   - outbound that was sent or failed: text, except receipts of live
//     (unretired) workflow alerts, whose content hash is checkpoint state;
//   - turns that ended: detail, except durable review turns, whose detail
//     is the result the durable workflow reads back (pruned with durable.db);
//   - ambient group context: the whole row.
//
// Pending, running, steering, held and sending rows are never touched.

// pruneBatch rows per statement; each batch is its own short transaction on
// the single connection, so live traffic is never held for long.
const pruneBatch = 500

// Pruned counts what one PruneText removed text from.
type Pruned struct{ Inbound, Outbound, Turns, Ambient int64 }

func (p Pruned) Total() int64 { return p.Inbound + p.Outbound + p.Turns + p.Ambient }

var pruneSteps = []struct {
	field string
	query string
}{
	{"inbound", `UPDATE inbound SET text='',
 extra=CASE WHEN json_valid(extra) THEN json_remove(extra,'$.context','$.review.message') ELSE extra END
 WHERE id IN (SELECT id FROM inbound WHERE received_at<? AND state IN ('done','failed','needs_human','cancelled','interrupted','unconfirmed','command','rejected')
 AND (text!='' OR CASE WHEN json_valid(extra) THEN json_type(extra,'$.context') IS NOT NULL OR json_type(extra,'$.review.message') IS NOT NULL ELSE 0 END) LIMIT ?)`},
	{"outbound", `UPDATE outbound SET text=''
 WHERE id IN (SELECT o.id FROM outbound o WHERE o.created_at<? AND o.state IN ('sent','failed') AND o.text!=''
 AND NOT EXISTS(SELECT 1 FROM job_alerts a WHERE a.outbound_id=o.id AND a.retired=0) LIMIT ?)`},
	{"turns", `UPDATE turns SET detail=''
 WHERE id IN (SELECT t.id FROM turns t WHERE t.state!='running' AND t.ended_at IS NOT NULL AND t.ended_at<? AND t.detail!=''
 AND NOT EXISTS(SELECT 1 FROM inbound i WHERE i.turn_id=t.id AND CASE WHEN json_valid(i.extra) THEN json_type(i.extra,'$.review') IS NOT NULL ELSE 0 END) LIMIT ?)`},
	{"ambient", `DELETE FROM ambient WHERE id IN (SELECT id FROM ambient WHERE at<? LIMIT ?)`},
}

// PruneText removes message text and context stored before cutoff (see
// above). Freed space is overwritten (secure_delete) and the write-ahead log
// is checkpointed and truncated afterwards, so removed text does not linger
// in the database files. Backups and harness transcripts are not touched.
func (s *Store) PruneText(ctx context.Context, cutoff time.Time) (Pruned, error) {
	var p Pruned
	counts := map[string]*int64{"inbound": &p.Inbound, "outbound": &p.Outbound, "turns": &p.Turns, "ambient": &p.Ambient}
	for _, step := range pruneSteps {
		for {
			n, err := s.pruneBatch(ctx, step.query, cutoff.UnixMilli())
			if err != nil {
				return p, err
			}
			*counts[step.field] += n
			if n < pruneBatch {
				break
			}
			select { // let waiting writers in between batches
			case <-ctx.Done():
				return p, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	if p.Total() > 0 {
		if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return p, err
		}
	}
	return p, nil
}

func (s *Store) pruneBatch(ctx context.Context, query string, cutoff int64) (int64, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var mode int
	if err := conn.QueryRowContext(ctx, `PRAGMA secure_delete`).Scan(&mode); err != nil {
		return 0, err
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA secure_delete=ON`); err != nil {
		return 0, err
	}
	defer conn.ExecContext(context.Background(), `PRAGMA secure_delete=`+secureMode(mode))
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, query, cutoff, pruneBatch)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

func secureMode(m int) string {
	switch m {
	case 1:
		return "ON"
	case 2:
		return "FAST"
	}
	return "OFF"
}

// KeepText prunes text older than keep once shortly after start and then
// daily, until ctx ends. keep <= 0 keeps text forever. logf gets counts
// only, never content.
func (s *Store) KeepText(ctx context.Context, keep time.Duration, logf func(string, ...any)) {
	if keep <= 0 {
		return
	}
	wait := time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = 24 * time.Hour
		p, err := s.PruneText(ctx, time.Now().Add(-keep))
		switch {
		case err != nil && ctx.Err() == nil:
			logf("retention: prune failed: %v", err)
			wait = time.Hour
		case p.Total() > 0:
			logf("retention: removed text from %d inbound, %d outbound, %d turns; %d ambient rows deleted", p.Inbound, p.Outbound, p.Turns, p.Ambient)
		}
	}
}
