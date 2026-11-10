package router

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// answerEnv is a router whose database the test can break with triggers.
func answerEnv(t *testing.T, attempts int) (*env, *sql.DB) {
	t.Helper()
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.tune = func(r *Router) { r.endRetry, r.endAttempts = 40*time.Millisecond, attempts }
	e.start()
	e.send(owner, "c0", "!help") // serving, past startup recovery
	e.reply()
	db, err := sql.Open("sqlite", "file:"+e.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return e, db
}

func exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

const noTurnEnd = `CREATE TRIGGER no_end BEFORE UPDATE OF state ON turns WHEN NEW.state != 'running' BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END`

// Item 4: a database hiccup when a turn ends never loses the answer. The
// end is written again (only while the turn is still running) and the
// answer is delivered exactly once.
func TestAnswerSurvivesFailedEndWrite(t *testing.T) {
	e, db := answerEnv(t, 8)
	exec(t, db, noTurnEnd)
	e.send(owner, "c1", "hello")
	time.Sleep(300 * time.Millisecond)
	exec(t, db, `DROP TRIGGER no_end`)
	want(t, e.reply(), "omp#1", `last="hello"`)
	e.noReply(500 * time.Millisecond)
	e.send(owner, "c1", "again")
	want(t, e.reply(), "omp#2 resumed=true")
}

// When the end cannot be recorded at all, the answer is still sent once,
// straight to the chat; the turn is left for restart recovery.
func TestAnswerSentDirectlyWhenEndCannotBeRecorded(t *testing.T) {
	e, db := answerEnv(t, 2)
	exec(t, db, noTurnEnd)
	e.send(owner, "c1", "hello")
	want(t, e.reply(), "omp#1", `last="hello"`)
	e.noReply(500 * time.Millisecond)
	var state string
	if err := db.QueryRow(`SELECT state FROM turns ORDER BY id DESC LIMIT 1`).Scan(&state); err != nil || state != "running" {
		t.Fatalf("turn state %q (%v), want running for restart recovery", state, err)
	}
}

// Item 4: when recording a successful send fails, the write is retried
// before the next reply: no resend, and the chat is not blocked until a
// restart.
func TestFailedSentWriteNeitherResendsNorBlocks(t *testing.T) {
	e, db := answerEnv(t, 5)
	exec(t, db, `CREATE TRIGGER no_sent BEFORE UPDATE OF state ON outbound WHEN NEW.state = 'sent' BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END`)
	e.send(owner, "c1", "one")
	want(t, e.reply(), `last="one"`)
	time.Sleep(300 * time.Millisecond)
	exec(t, db, `DROP TRIGGER no_sent`)
	e.send(owner, "c1", "two")
	out := e.reply()
	if strings.Contains(out.Text, `last="one"`) {
		t.Fatalf("first reply sent again: %q", out.Text)
	}
	want(t, out, `last="two"`)
	e.noReply(300 * time.Millisecond)
}
