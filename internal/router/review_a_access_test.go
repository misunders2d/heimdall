package router

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// retryIface is the fake interface with a real transport's contract: a
// message accept refuses is offered again, never dropped.
type retryIface struct{ namedFakeIface }

func (f retryIface) Run(ctx context.Context, accept func(context.Context, core.Inbound) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case in := <-f.in:
			for accept(ctx, in) != nil {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
	}
}

// accessEnv is a router whose callers are decided by Pi access files in
// dir, on a telegram-named interface that retries refused messages. The
// owner is telegram:42 (ADMIN_USER_IDS).
func accessEnv(t *testing.T, h string, parallel int) (*env, func(white string)) {
	t.Helper()
	e := setup(t, core.ModeCorporate, []string{h}, h)
	e.opt.MaxParallel = parallel
	e.stop()
	stopOnce.Delete(e.st)
	dir := t.TempDir()
	t.Setenv("ADMIN_USER_IDS", "42")
	write := func(white string) {
		for name, data := range map[string]string{"whitelist.json": white, "blacklist.json": `{}`, "admins.json": `{}`, "identities.json": `{}`, "capabilities.json": `{}`} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(`{"users":[]}`)
	e.opt.Access = core.Access{Mode: core.ModeCorporate, Dir: dir}
	st, err := store.Open(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	e.st, e.f = st, newFakeIface()
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel, e.done = cancel, make(chan error, 1)
	r := New(e.opt, st, retryIface{namedFakeIface{e.f, "telegram"}})
	r.accessRetry = 20 * time.Millisecond
	e.r = r
	go func() { e.done <- r.Run(ctx) }()
	t.Cleanup(e.stop)
	return e, write
}

var (
	accessBoss = core.Principal{Platform: core.PlatformTelegram, ID: "42", Name: "boss"}
	accessChat = core.ConvKey{Interface: "telegram", Chat: "42"}
)

// Item 3: an access file broken for a moment never throws a message away.
// At intake the message is not acknowledged (the interface offers it
// again); once stored it is held and checked again; at turn start the turn
// waits. Each runs once the files read again.
func TestMessageWaitsForUnreadableAccessFiles(t *testing.T) {
	t.Run("intake", func(t *testing.T) {
		e, write := accessEnv(t, "omp", 4)
		write(`{"users":[`) // mid-rewrite
		e.sendIn(accessBoss, accessChat, "hello")
		e.noReply(300 * time.Millisecond)
		if p, _ := e.st.Pending(); len(p) != 0 {
			t.Fatalf("stored while unreadable: %+v", p)
		}
		write(`{"users":[]}`)
		want(t, e.reply(), "omp#1", `last="hello"`)
	})
	t.Run("stored", func(t *testing.T) {
		e, write := accessEnv(t, "omp", 4)
		e.sendIn(accessBoss, accessChat, "!status") // the router is serving
		e.reply()
		write(`{"users":[`)
		row, _, err := e.st.AddInbound(core.Inbound{Conv: accessChat, MsgID: "stored", From: accessBoss, Origin: core.OriginLive, Text: "kept", Private: true})
		if err != nil {
			t.Fatal(err)
		}
		e.r.dispatch(row)
		e.noReply(300 * time.Millisecond)
		write(`{"users":[]}`)
		want(t, e.reply(), "omp#1", `last="kept"`)
		if strings.Contains(e.logs.String(), "rejected: caller no longer authorized") {
			t.Fatal("held message was rejected")
		}
	})
	t.Run("turn start", func(t *testing.T) {
		// One turn slot, held by a turn in another chat that ends by itself
		// (AGY answers when its process exits, a second later). The message
		// here is routed while the files read, then its turn starts while
		// they do not.
		e, write := accessEnv(t, "agy", 1)
		other := core.ConvKey{Interface: "telegram", Chat: "-100"}
		e.sendIn(accessBoss, other, "busy LINGER=1")
		time.Sleep(200 * time.Millisecond)
		e.sendIn(accessBoss, accessChat, "second")
		time.Sleep(300 * time.Millisecond)
		write(`{"users":[`)
		want(t, e.reply(), `last="busy LINGER=1"`)
		eventuallyTrue(t, "turn waits for access", func() bool { return strings.Contains(e.logs.String(), "waits: access files unavailable") })
		e.noReply(200 * time.Millisecond)
		write(`{"users":[]}`)
		out := e.reply()
		want(t, out, `last="second"`)
		if strings.Contains(out.Text, "turn refused") {
			t.Fatalf("turn refused for unreadable files: %q", out.Text)
		}
	})
}
