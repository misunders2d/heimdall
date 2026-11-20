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
// clock): no turn is running there or ended after it, and no unprompted
// delivery (reply_to ”: scheduled output, task notices, other cards)
// followed the card, which was enqueued in the draft's own transaction.
// Replies to commands (Details, !jobs) do not count. Only then may a bare
// Yes or No answer a launcher proposal (docs/external-origin.md).
func (s *Store) CardIsLatest(id string) (bool, error) {
	var busy, unprompted int
	err := s.db.QueryRow(`SELECT
	(SELECT count(*) FROM turns t WHERE t.iface=j.created_iface AND t.chat=j.created_chat AND t.thread=j.created_thread AND (t.state=? OR t.ended_at>=j.updated_at)),
	(SELECT count(*) FROM outbound o WHERE o.iface=j.created_iface AND o.chat=j.created_chat AND o.thread=j.created_thread AND o.reply_to='' AND o.created_at>=j.updated_at)
	FROM jobs j WHERE j.id=? AND j.state=?`, TurnRunning, id, JobDraft).Scan(&busy, &unprompted)
	if err != nil {
		return false, err
	}
	return busy == 0 && unprompted <= 1, nil
}
