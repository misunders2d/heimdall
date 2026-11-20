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
