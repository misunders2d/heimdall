package slack

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// The router restarts an interface that fails after start-up. A second Run
// of the same Slack must not panic (ready is closed once) and must serve
// events again with the identity found the first time.
func TestRunAgainAfterFailureServesEvents(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	if !s.Restartable() {
		t.Fatal("slack must report restartable")
	}
	var mu sync.Mutex
	var got []core.Inbound
	accept := func(_ context.Context, in core.Inbound) error {
		mu.Lock()
		got = append(got, in)
		mu.Unlock()
		return nil
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(got) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := make(chan error, 1)
	go func() { first <- s.Run(ctx, accept) }()
	select {
	case <-f.connected:
	case <-time.After(3 * time.Second):
		t.Fatal("no socket")
	}
	f.push(f.event("D1", "im", "Uowner", "before", ""))
	waitFor(t, "first message", func() bool { return count() == 1 })
	botID := s.botID
	// Slack disables the link: Run fails after start-up.
	f.events <- []byte(`{"type":"disconnect","reason":"link_disabled"}`)
	select {
	case err := <-first:
		if !errors.Is(err, errLinkDisabled) {
			t.Fatalf("first run: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("first run did not stop")
	}

	second := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				second <- errors.New("second Run panicked")
			}
		}()
		second <- s.Run(ctx, accept)
	}()
	f.push(f.event("D1", "im", "Uowner", "after", ""))
	waitFor(t, "message after restart", func() bool { return count() == 2 })
	if s.botID != botID {
		t.Fatal("bot identity changed across restart")
	}
	mu.Lock()
	text := got[1].Text
	mu.Unlock()
	if text != "after" {
		t.Fatalf("restart served %q", text)
	}
	cancel()
	select {
	case err := <-second:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("second run: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("second run did not stop")
	}
}
