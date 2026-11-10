package telegram

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// Item 3: while an access file is unreadable (mid-rewrite) the update is
// neither ignored nor acknowledged: the poller keeps its offset, backs off,
// and admits the message once the files read again.
func TestUpdateWaitsForUnreadableAccessFiles(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	c := &collector{}
	i := newIface(t, api.srv.URL, botName, &logs)
	dir := t.TempDir()
	i.opt.Access = &core.Access{Dir: dir}
	t.Setenv("ADMIN_USER_IDS", "42")
	write := func(white string) {
		for name, data := range map[string]string{"whitelist.json": white, "blacklist.json": `{}`, "admins.json": `{}`, "identities.json": `{}`, "capabilities.json": `{}`} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(`{"users":[`)
	api.push(upd(1, msg(1, 42, "private", 42, "hello")))
	stop := run(t, i, c.accept)
	defer stop()
	time.Sleep(300 * time.Millisecond)
	if n := len(c.list()); n != 0 || api.lastOffset() > 1 {
		t.Fatalf("accepted %d, offset %d while access unreadable", n, api.lastOffset())
	}
	write(`{"users":[]}`)
	eventually(t, "message admitted after repair", func() bool { return len(c.list()) == 1 })
	if in := c.list()[0]; in.MsgID != "1" || in.Text != "hello" {
		t.Fatalf("admitted %+v", in)
	}
}
