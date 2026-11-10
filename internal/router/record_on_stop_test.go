package router

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// stopAfterSend stops the router right after a reply reached the chat,
// before its delivery is recorded: a service stop landing in that moment.
type stopAfterSend struct {
	*fakeIface
	stop context.CancelFunc
}

func (s *stopAfterSend) Send(ctx context.Context, out core.Outbound) error {
	err := s.fakeIface.Send(ctx, out)
	if err == nil {
		s.stop()
	}
	return err
}

// A reply sent just as the service stops is still recorded as sent, so the
// next start does not send it again.
func TestDeliveryRecordedWhenStoppedRightAfterSend(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.wrap = func(f *fakeIface) Interface { return &stopAfterSend{fakeIface: f, stop: e.cancel} }
	e.start()
	e.send(owner, "c1", "hi")
	want(t, e.reply(), "omp#1")
	e.stop()

	db, err := sql.Open("sqlite", "file:"+e.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	var state string
	err = db.QueryRow(`SELECT state FROM outbound WHERE chat='c1' ORDER BY id DESC LIMIT 1`).Scan(&state)
	db.Close()
	if err != nil || state != store.OutSent {
		t.Fatalf("reply state = %q, %v; want %q", state, err, store.OutSent)
	}

	e.wrap = nil
	e.start()
	e.noReply(time.Second)
}
