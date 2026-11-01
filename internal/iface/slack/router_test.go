package slack

import (
	"bytes"
	"context"
	"image"
	"image/png"
	"io"
	"log"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

type sameUser struct{ harness.Accounts }

func (a sameUser) Prepare(_ core.Role, memory bool, bin string) (harness.Launch, error) {
	return a.Accounts.Prepare(core.RoleOwner, memory, bin)
}
func routerFor(t *testing.T, s *Slack, st *store.Store, bin string) *router.Router {
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	return router.New(router.Options{Access: core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"slack:uowner@example.test": true}}, Order: []string{"omp", "codex"}, MaxParallel: 3, Accounts: sameUser{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}}, Log: log.New(io.Discard, "", 0), OutboxDir: s.opt.DataDir + "/outbox"}, st, s)
}
func startRouter(t *testing.T, r *router.Router) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return cancel, done
}
func TestSlackThroughNativeProtocolsAndRouter(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	bin, state := fakeharness.Install(t, "omp", "codex", "claude", "pi")
	r := routerFor(t, s, st, bin)
	cancel, done := startRouter(t, r)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-f.connected:
	case <-time.After(3 * time.Second):
		t.Fatal("socket did not start")
	}
	dm := f.event("D1", "im", "Uowner", "hello", "")
	f.push(dm)
	waitFor(t, "owner harness reply", func() bool { return f.hasText("omp#1") })
	if !f.hasText("uowner@example.test") {
		t.Fatal("verified email missing from harness environment")
	}
	follow := f.event("D1", "im", "Uowner", "again", dm.TS)
	f.push(follow)
	waitFor(t, "native resume", func() bool { return f.hasText("omp#2 resumed=true") })
	// Another caller in this thread gets a separate native session, and a
	// private owner notice containing identity/conversation but never text.
	colleague := f.event("D1", "im", "Uemployee", "private-message-body", dm.TS)
	f.push(colleague)
	waitFor(t, "new caller notice", func() bool { return f.hasText("New caller") })
	for _, c := range f.all("chat.postMessage") {
		if strings.Contains(str(c.Body["text"]), "New caller") && strings.Contains(str(c.Body["text"]), "private-message-body") {
			t.Fatal("notice leaked input")
		}
	}
	waitFor(t, "employee session", func() bool {
		for _, in := range fakeharness.Inputs(state) {
			if strings.Contains(in.Text, "private-message-body") {
				return true
			}
		}
		return false
	})
	// Channel ambient plus current live history is supplied, without running
	// ambient text. The addressed turn stays alive for a photo steer.
	ambient := f.event("C1", "channel", "Uowner", "ambient-only-evidence", "")
	f.push(ambient)
	work := f.event("C1", "channel", "Uowner", "<@Ubot> work WAIT_STEER PROGRESS", "")
	f.push(work)
	waitFor(t, "running work", func() bool {
		for _, in := range fakeharness.Inputs(state) {
			if in.Kind == "prompt" && strings.Contains(in.Text, "work WAIT_STEER") {
				return true
			}
		}
		return false
	})
	var encoded bytes.Buffer
	_ = png.Encode(&encoded, image.NewRGBA(image.Rect(0, 0, 2, 2)))
	f.mu.Lock()
	f.file = encoded.Bytes()
	f.mu.Unlock()
	photo := f.event("C1", "channel", "Uowner", "<@Ubot> photo FINISH", work.TS)
	photo.Files = []slackFile{{ID: "F1", Name: "photo.png", MIME: "image/png", Size: int64(encoded.Len()), URL: f.srv.URL + "/file"}}
	f.push(photo)
	waitFor(t, "native photo steer", func() bool {
		for _, in := range fakeharness.Inputs(state) {
			if in.Kind == "steer" && in.Images == 1 && strings.Contains(in.Text, "photo FINISH") {
				return true
			}
		}
		return false
	})
	waitFor(t, "steered answer", func() bool { return f.hasText("steers=1") })
	for _, in := range fakeharness.Inputs(state) {
		if in.Kind == "prompt" && strings.Contains(in.Text, "work WAIT_STEER") && !strings.Contains(in.Text, "ambient-only-evidence") {
			t.Fatal("ambient context missing")
		}
	}
	// !use is sticky only for this thread.
	f.push(f.event("D1", "im", "Uowner", "!use codex", dm.TS))
	waitFor(t, "sticky command", func() bool { return f.hasText("now uses codex") })
	f.push(f.event("D1", "im", "Uowner", "chosen harness", dm.TS))
	waitFor(t, "Codex native protocol", func() bool { return f.hasText("codex#1") })
	reaction := f.event("D2", "im", "Uowner", "REACT=thumbsup REACTONLY", "")
	f.push(reaction)
	waitFor(t, "Slack reaction", func() bool { return len(f.all("reactions.add")) > 0 })
	artifact := f.event("D3", "im", "Uowner", "WRITE=hello.txt:hello", "")
	f.push(artifact)
	waitFor(t, "checked outbox upload", func() bool { return len(f.all("files.completeUploadExternal")) > 0 })
	// Shared stop works without mention; it does not require Slack to supply
	// slash commands, and uses native interrupt before bounded process cleanup.
	sleeping := f.event("Cstop", "channel", "Uowner", "<@Ubot> SLEEP", "")
	f.push(sleeping)
	waitFor(t, "sleep turn ready", func() bool {
		for _, in := range fakeharness.Inputs(state) {
			if strings.Contains(in.Text, "SLEEP") {
				return true
			}
		}
		return false
	})
	f.push(f.event("Cstop", "channel", "Uowner", "stop", sleeping.TS))
	waitFor(t, "unmentioned stop", func() bool { return f.hasText("Stopping current run.") })
	waitFor(t, "stop settled", r.Idle)
	count := 0
	for _, call := range f.all("chat.postMessage") {
		if str(call.Body["channel"]) == "Cstop" {
			text := str(call.Body["text"])
			if strings.Contains(text, "Stopping current run.") {
				count++
			}
			if strings.Contains(text, "⏹ Stopped.") {
				t.Fatal("duplicate terminal reply")
			}
		}
	}
	if count != 1 {
		t.Fatalf("stop acknowledgements: %d", count)
	}
	f.push(f.event("D1", "im", "Uowner", "reset", dm.TS))
	waitFor(t, "caller reset", func() bool { return f.hasText("Session reset") })
	f.push(f.event("D1", "im", "Uowner", "fresh harness", dm.TS))
	waitFor(t, "fresh session after reset", func() bool { return f.hasText(`last="fresh harness"`) })
}
func TestSlackRestartDoesNotReplayStartedTurn(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	bin, state := fakeharness.Install(t, "omp")
	r := routerFor(t, s, st, bin)
	cancel, done := startRouter(t, r)
	work := f.event("D1", "im", "Uowner", "work WAIT_STEER", "")
	f.push(work)
	waitFor(t, "started turn", func() bool { return len(fakeharness.Inputs(state)) == 1 })
	key := core.ConvKey{Interface: "slack", Chat: "D1", Thread: work.TS}
	previous, _, err := st.Session(key, "slack:uowner@example.test", "omp")
	if err != nil || previous == "" {
		t.Fatal("initial binding missing")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// New transport and router, same durable queues/native-session bindings.
	next, err := New(s.opt)
	if err != nil {
		t.Fatal(err)
	}
	next.socket.allowTestSocket = true
	next.fileHost = s.fileHost
	next.statusEvery = 20 * time.Millisecond
	next.partEvery = 0
	next.bot.pause = s.bot.pause
	r = routerFor(t, next, st, bin)
	cancel, done = startRouter(t, r)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	waitFor(t, "interrupted report", func() bool { return f.hasText("not re-run") })
	if got := len(fakeharness.Inputs(state)); got != 1 {
		t.Fatalf("interrupted input replayed: %d", got)
	}
	f.push(f.event("D1", "im", "Uowner", "next request", work.TS))
	waitFor(t, "fresh process", func() bool { return len(fakeharness.Inputs(state)) == 2 })
	current, _, err := st.Session(key, "slack:uowner@example.test", "omp")
	if err != nil || current == previous {
		t.Fatal("unclean session resumed")
	}
	// The fake recognizes WAIT_STEER even inside live history. Finish its
	// second synthetic run explicitly; real harnesses receive it as untrusted.
	f.push(f.event("D1", "im", "Uowner", "FINISH", work.TS))
	waitFor(t, "fresh after restart", func() bool { return f.hasText("previous turn there did not finish cleanly") })
}
