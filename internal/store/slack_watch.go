package store

import (
	"database/sql"
	"errors"

	"github.com/misunders2d/heimdall/internal/core"
)

// SlackWatch finds the thread override first, including an explicit off.
// A missing row leaves the existing startup config as the fallback.
func (s *Store) SlackWatch(chat, thread string) (core.SlackWatch, error) {
	var watch core.SlackWatch
	err := s.db.QueryRow(`
		SELECT thread,mode,rule,revision,changed FROM slack_watches
		WHERE chat=? AND thread IN ('',?)
		ORDER BY thread DESC LIMIT 1`, chat, thread).
		Scan(&watch.Thread, &watch.Mode, &watch.Rule, &watch.Revision, &watch.Changed)
	if errors.Is(err, sql.ErrNoRows) {
		return watch, nil
	}
	return watch, err
}

// SetSlackWatch retains off rows so a scope stays off across restarts and
// pending work cannot inherit a later grant with the same wording.
func (s *Store) SetSlackWatch(chat string, watch core.SlackWatch) error {
	_, err := s.db.Exec(`
		INSERT INTO slack_watches(chat,thread,mode,rule,revision,changed)
		VALUES(?,?,?,?,1,?)
		ON CONFLICT(chat,thread) DO UPDATE SET mode=excluded.mode,rule=excluded.rule,
		revision=slack_watches.revision+1,changed=excluded.changed
		WHERE mode<>excluded.mode OR rule<>excluded.rule`,
		chat, watch.Thread, watch.Mode, watch.Rule, now())
	return err
}
