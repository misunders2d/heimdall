package store

// TurnIsRunning reports whether turn is still recorded as running: an end
// whose commit was reported as failed may have been recorded after all.
func (s *Store) TurnIsRunning(turn int64) (bool, error) {
	var state string
	if err := s.db.QueryRow(`SELECT state FROM turns WHERE id=?`, turn).Scan(&state); err != nil {
		return false, err
	}
	return state == TurnRunning, nil
}

// CardIsLatest reports whether nothing happened in the conversation of
// draft id since the draft was stored with its card (both by this store's
// clock): no turn is running there or ended after it, and nothing was
// delivered there after the card (enqueued in the draft's own
// transaction) but replies to messages received after it (Details, !jobs,
// other commands). Scheduled output, task notices (which reply to the
// task's original, older message) and other cards all count. A message's
// received time is the platform's (Telegram: whole seconds), so it gets a
// minute of slack against the card's. Only then
// may a bare Yes or No answer a launcher proposal
// (docs/external-origin.md).
func (s *Store) CardIsLatest(id string) (bool, error) {
	var busy, later int
	err := s.db.QueryRow(`SELECT
	(SELECT count(*) FROM turns t WHERE t.iface=j.created_iface AND t.chat=j.created_chat AND t.thread=j.created_thread AND (t.state=? OR t.ended_at>=j.updated_at)),
	(SELECT count(*) FROM outbound o WHERE o.iface=j.created_iface AND o.chat=j.created_chat AND o.thread=j.created_thread AND o.created_at>=j.updated_at
		AND NOT EXISTS (SELECT 1 FROM inbound i WHERE i.iface=o.iface AND i.chat=o.chat AND i.thread=o.thread AND i.msg_id=o.reply_to AND i.received_at>=j.updated_at-60000))
	FROM jobs j WHERE j.id=? AND j.state=?`, TurnRunning, id, JobDraft).Scan(&busy, &later)
	if err != nil {
		return false, err
	}
	// The card itself is the one delivery allowed.
	return busy == 0 && later <= 1, nil
}
