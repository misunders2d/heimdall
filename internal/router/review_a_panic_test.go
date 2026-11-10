package router

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// boomIface is the fake interface with optional features that panic on
// demand, as a bug in interface code would.
type boomIface struct {
	*fakeIface
	note, typing, send, react, group, observe atomic.Bool
	sends                                     atomic.Int64
}

const secret = "SECRET-MESSAGE-TEXT"

func (b *boomIface) PromptNote() string {
	if b.note.Load() {
		panic("prompt " + secret)
	}
	return b.fakeIface.PromptNote()
}

func (b *boomIface) Typing(ctx context.Context, conv core.ConvKey) {
	if b.typing.Load() {
		panic("typing " + secret)
	}
	b.fakeIface.Typing(ctx, conv)
}

func (b *boomIface) Send(ctx context.Context, out core.Outbound) error {
	if b.send.CompareAndSwap(true, false) {
		panic("send " + secret)
	}
	b.sends.Add(1)
	return b.fakeIface.Send(ctx, out)
}

func (b *boomIface) ReactionNote(ref string) string {
	if b.react.Load() {
		panic("react " + secret)
	}
	return b.fakeIface.ReactionNote(ref)
}

func (b *boomIface) GroupContext() bool {
	if b.group.CompareAndSwap(true, false) {
		panic("group " + secret)
	}
	return b.fakeIface.GroupContext()
}

func (b *boomIface) ObserveTurn(context.Context, TurnEvent) {
	if b.observe.Load() {
		panic("observe " + secret)
	}
}

func boomEnv(t *testing.T, parallel int) (*env, *boomIface) {
	t.Helper()
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.MaxParallel = parallel
	var b *boomIface
	e.wrap = func(f *fakeIface) Interface { b = &boomIface{fakeIface: f}; return b }
	e.start()
	t.Cleanup(func() {
		if strings.Contains(e.logs.String(), secret) {
			t.Error("a panic's value reached the log")
		}
	})
	return e, b
}

func (e *env) conv(chat string) *conv {
	e.t.Helper()
	e.r.mu.Lock()
	defer e.r.mu.Unlock()
	c := e.r.convs[core.ConvKey{Interface: "fake", Chat: chat}]
	if c == nil {
		e.t.Fatalf("no conversation %s", chat)
	}
	return c
}

// Item 5: a bug while one conversation is handled fails only that
// conversation's work. Its running turn is stopped and failed, the chat is
// told once, its slot is free again, and it and every other chat go on.
func TestConversationPanicFailsOnlyThatChat(t *testing.T) {
	e, _ := boomEnv(t, 1) // one slot: the next turn needs the one given back
	e.send(owner, "a", "long WAIT_STEER")
	e.waitReadyOn("a", "omp")
	// An event the loop cannot handle panics on the conversation loop.
	e.conv("a").events <- doneEvent{}
	out := e.reply()
	if out.Conv.Chat != "a" || out.Text != panicNotice {
		t.Fatalf("notice = %+v", out)
	}
	if !strings.Contains(e.logs.String(), "recovered panic (runtime.") || !strings.Contains(e.logs.String(), "in conversation fake/a") {
		t.Fatalf("panic not logged by type and site:\n%s", e.logs.String())
	}
	e.noReply(500 * time.Millisecond) // the stopped turn sends nothing more
	// The slot was given back: another chat's turn can take it.
	e.send(employee, "b", "elsewhere")
	want(t, e.reply(), `last="elsewhere"`)
	e.send(owner, "a", "again")
	want(t, e.reply(), `last="again"`)
}

// A panic in the turn runner (here interface code it calls) fails that
// turn with one plain notice; the conversation goes on.
func TestTurnRunnerPanicFailsThatTurn(t *testing.T) {
	e, b := boomEnv(t, 4)
	b.note.Store(true)
	e.send(owner, "a", "hello")
	out := e.reply()
	want(t, out, "Something went wrong in Heimdall during this turn", "— heimdall")
	b.note.Store(false)
	e.send(owner, "a", "again")
	want(t, e.reply(), `last="again"`)
}

// Panics in the typing indicator, in turn progress and in one delivery
// never stop the turn or the reply: a failed send is retried.
func TestTypingProgressAndSendPanicsAreContained(t *testing.T) {
	e, b := boomEnv(t, 4)
	b.typing.Store(true)
	b.observe.Store(true)
	b.send.Store(true)
	e.send(owner, "a", "hello")
	want(t, e.reply(), `last="hello"`)
	if n := b.sends.Load(); n != 1 {
		t.Fatalf("sends = %d, want the retried reply once", n)
	}
}

// A panic while a steer is handed over leaves its outcome unknown: the
// sender is told, it is never replayed, and the turn still ends.
func TestSteerPanicIsUnknownNotReplayed(t *testing.T) {
	e, b := boomEnv(t, 4)
	e.send(owner, "a", "long WAIT_STEER")
	e.waitReadyOn("a", "omp")
	b.react.Store(true)
	e.send(owner, "a", "steer me")
	want(t, e.reply(), "may or may not have reached the running omp turn")
	b.react.Store(false)
	e.send(owner, "a", "!stop")
	want(t, e.reply(), "Stopping")
	e.noReply(300 * time.Millisecond)
}

// A panic accepting one message drops only that message; the interface
// keeps delivering the next ones.
func TestAcceptPanicDropsOnlyThatMessage(t *testing.T) {
	e, b := boomEnv(t, 4)
	b.group.Store(true)
	e.send(owner, "a", "dropped")
	e.noReply(300 * time.Millisecond)
	e.send(owner, "a", "next")
	want(t, e.reply(), `last="next"`)
}

// restartIface fails its Run on demand; Restartable says it may run again.
type restartIface struct {
	runs  atomic.Int64
	fail  chan error
	panic atomic.Bool
}

func (f *restartIface) Name() string      { return "restart" }
func (f *restartIface) Restartable() bool { return true }
func (f *restartIface) Send(context.Context, core.Outbound) error {
	return nil
}
func (f *restartIface) Run(ctx context.Context, _ func(context.Context, core.Inbound) error) error {
	f.runs.Add(1)
	select {
	case <-ctx.Done():
		return nil
	case err := <-f.fail:
		if f.panic.Load() {
			panic("run " + secret)
		}
		return err
	}
}

// After start-up, an interface that fails (error or panic) restarts alone
// with backoff; the router keeps running. Within the start-up grace it is
// fatal, as before.
func TestInterfaceRestartsAfterStartup(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	var logs syncBuffer
	f := &restartIface{fail: make(chan error)}
	r := New(Options{Access: core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{owner: true}}, Order: []string{"omp"}, Log: log.New(&logs, "", 0)}, st, f)
	r.ifaceGrace, r.ifaceBackoff = 100*time.Millisecond, 20*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	eventuallyTrue(t, "first run", func() bool { return f.runs.Load() == 1 })
	time.Sleep(150 * time.Millisecond) // past the grace
	f.fail <- errors.New("connection lost")
	eventuallyTrue(t, "restart after an error", func() bool { return f.runs.Load() == 2 })
	f.panic.Store(true)
	f.fail <- errors.New("unused")
	eventuallyTrue(t, "restart after a panic", func() bool { return f.runs.Load() == 3 })
	select {
	case err := <-done:
		t.Fatalf("router stopped: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), secret) || !strings.Contains(logs.String(), "recovered panic (string) in interface restart") {
		t.Fatalf("log:\n%s", logs.String())
	}
}
