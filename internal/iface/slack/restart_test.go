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
	waitFor(t, "second socket", func() bool { return len(f.all("apps.connections.open")) >= 2 })
	// The old fake socket may still take an event before it notices the
	// close; Slack redelivers unacknowledged envelopes, so do the same.
	after := f.event("D1", "im", "Uowner", "after", "")
	f.push(after)
	until := time.Now().Add(8 * time.Second)
	for next := time.Now().Add(time.Second); count() < 2; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(until) {
			t.Fatal("timed out: message after restart")
		}
		if time.Now().After(next) {
			f.push(after)
			next = time.Now().Add(time.Second)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if count() != 2 {
		t.Fatalf("redelivery repeated processing: %d", count())
	}
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
