package slack

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// Item 3: an access file broken for a moment does not drop the event. It
// stays pending (Slack's ingress retry backs off) and is admitted once the
// files read again.
func TestSlackEventWaitsForUnreadableAccessFiles(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	dir := t.TempDir()
	s.opt.Access = &core.Access{Dir: dir}
	t.Setenv("ADMIN_USER_IDS", "1")
	write := func(white string) {
		for name, data := range map[string]string{"whitelist.json": white, "blacklist.json": `{}`, "admins.json": `{}`, "identities.json": `{}`, "capabilities.json": `{}`} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(`{"users":["sl_user_Uowner"`) // mid-rewrite
	m := f.event("D1", "im", "Uowner", "synthetic", "")
	raw, _ := json.Marshal(map[string]any{"type": "event_callback", "event_id": "E" + m.TS, "event": m})
	if err := s.opt.Store.StageEvent("slack", "E"+m.TS, raw); err != nil {
		t.Fatal(err)
	}
	e, _, err := s.opt.Store.Event("slack", "E"+m.TS)
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	accept := func(context.Context, core.Inbound) error { accepted++; return nil }
	if err := s.process(context.Background(), e, accept); err == nil {
		t.Fatal("event finished while the access files were unreadable")
	}
	if pending, err := s.opt.Store.EventPending(e.ID); err != nil || !pending || accepted != 0 {
		t.Fatalf("event pending %v (%v), accepted %d; want held", pending, err, accepted)
	}
	write(`{"users":["sl_user_Uowner"]}`)
	if err := s.process(context.Background(), e, accept); err != nil || accepted != 1 {
		t.Fatalf("after repair: process %v, accepted %d", err, accepted)
	}
}
