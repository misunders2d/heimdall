package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
)

// AdminMinSchema is the oldest schema the admin commands (heimdall sched,
// heimdall drain) can read and write without migrating. Raise it only when an
// admin command starts using a later step's tables or columns. Until then, a
// newer binary can still drain a service running an older one.
const AdminMinSchema = 17

// Schema is the number of schema steps this build applies.
func Schema() int { return len(steps) }

// OpenAdmin opens an existing database for an admin command. It never
// creates the file and never migrates. Only "heimdall run" upgrades the
// schema, so an operator command run from a new binary cannot change the
// database under the running service, and the old binary can still be put
// back after a rollback. A schema outside AdminMinSchema..Schema() is
// refused with a message naming both versions.
func OpenAdmin(path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no Heimdall database at %s: start heimdall run first", path)
		}
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		db.Close()
		return nil, err
	}
	switch {
	case v > len(steps):
		db.Close()
		return nil, fmt.Errorf("database schema %d is newer than this Heimdall (%d); use the binary that runs the service", v, len(steps))
	case v < AdminMinSchema:
		db.Close()
		return nil, fmt.Errorf("database schema %d is older than this command supports (%d..%d); use the binary that runs the service, admin commands never migrate", v, AdminMinSchema, len(steps))
	}
	return &Store{db: db}, nil
}

// CreateAtSchema creates a database with only the first n schema steps, as an
// older Heimdall left it. For tests and rollback drills; never for service
// data.
func CreateAtSchema(path string, n int) error {
	if n < 0 || n > len(steps) {
		return fmt.Errorf("schema %d out of range 0..%d", n, len(steps))
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	return s.migrateTo(n)
}
