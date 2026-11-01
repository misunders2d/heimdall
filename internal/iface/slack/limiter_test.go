package slack

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// Register one waiter at a time while slots are closed. This establishes actual
// queue arrival order rather than relying on goroutine launch order or sleeps.
func enqueuePacer(t *testing.T, p *apiPacer, ctx context.Context, priority bool, ordinary, controls int) <-chan error {
	t.Helper()
	if priority {
		ctx = context.WithValue(ctx, controlIdentityKey{}, true)
	}
	result := make(chan error, 1)
	go func() { result <- p.wait(ctx, "users.info") }()
	awaitPacerQueue(t, p, ordinary, controls)
	return result
}

func awaitPacerQueue(t *testing.T, p *apiPacer, ordinary, controls int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		q := p.queues["users.info"]
		ready := q != nil && len(q.ordinary) == ordinary && len(q.priority) == controls
		p.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiters did not register: ordinary=%d priority=%d", ordinary, controls)
}

func openPacer(p *apiPacer) {
	p.mu.Lock()
	p.next["users.info"] = time.Now()
	p.queues["users.info"].notify()
	p.mu.Unlock()
}

func expectPacerGrant(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter starved")
	}
}

// Freeze each next slot after a grant, so assertions observe grant order even
// when the test goroutine is descheduled. No wall-clock speed expectations.
func stepPacer(t *testing.T, p *apiPacer, result <-chan error, ordinary, controls int) {
	t.Helper()
	openPacer(p)
	expectPacerGrant(t, result)
	awaitPacerQueue(t, p, ordinary, controls)
}

func closedPacer(t *testing.T) (*apiPacer, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &apiPacer{interval: time.Hour, next: map[string]time.Time{"users.info": time.Now().Add(time.Hour)}}
	t.Cleanup(func() {
		cancel()
		awaitPacerQueue(t, p, 0, 0)
	})
	return p, ctx
}

func TestPacerPriorityFloodBoundedShare(t *testing.T) {
	p, ctx := closedPacer(t)
	var ordinary, priority []<-chan error
	for i := range 6 {
		ordinary = append(ordinary, enqueuePacer(t, p, ctx, false, i+1, 0))
	}
	// More priority work than available grants, replenished on every pair: the
	// priority queue remains nonempty throughout the measurement.
	for i := range 12 {
		priority = append(priority, enqueuePacer(t, p, ctx, true, 6, i+1))
	}
	for i := range ordinary {
		stepPacer(t, p, priority[i], 6-i, 11)
		stepPacer(t, p, ordinary[i], 5-i, 11)
		priority = append(priority, enqueuePacer(t, p, ctx, true, 5-i, 12))
	}
}

func TestPacerOrdinaryFIFO(t *testing.T) {
	p, ctx := closedPacer(t)
	var results []<-chan error
	for i := range 8 {
		results = append(results, enqueuePacer(t, p, ctx, false, i+1, 0))
	}
	for i, result := range results {
		stepPacer(t, p, result, len(results)-i-1, 0)
	}
}

func TestPacerPriorityOvertakesOrdinary(t *testing.T) {
	p, ctx := closedPacer(t)
	first := enqueuePacer(t, p, ctx, false, 1, 0)
	second := enqueuePacer(t, p, ctx, false, 2, 0)
	control := enqueuePacer(t, p, ctx, true, 2, 1)
	stepPacer(t, p, control, 2, 0)
	stepPacer(t, p, first, 1, 0)
	stepPacer(t, p, second, 0, 0)
}

func TestPacerCanceledHeadsReleaseQueues(t *testing.T) {
	p, ctx := closedPacer(t)
	ordinaryCtx, cancelOrdinary := context.WithCancel(ctx)
	priorityCtx, cancelPriority := context.WithCancel(ctx)
	first := enqueuePacer(t, p, ordinaryCtx, false, 1, 0)
	control := enqueuePacer(t, p, priorityCtx, true, 1, 1)
	last := enqueuePacer(t, p, ctx, false, 2, 1)
	cancelOrdinary()
	cancelPriority()
	for _, result := range []<-chan error{first, control} {
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation returned %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancellation stuck")
		}
	}
	awaitPacerQueue(t, p, 1, 0)
	stepPacer(t, p, last, 0, 0)
}

func TestControlIdentityPriorityRequiresAddressing(t *testing.T) {
	for _, tc := range []struct {
		name, surface, text string
		priority            bool
	}{
		{"unaddressed", "channel", "stop", false},
		{"mentioned", "channel", "<@Ubot> stop", true},
		{"private", "im", "stop", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSlackFake(t)
			s, _ := testSlack(t, f)
			s.botID = "Ubot"
			s.opt.UnmentionedCommands = []string{}
			p, ctx := closedPacer(t)
			s.bot.pacer = p
			e := stageReview(t, s, f.event("C1", tc.surface, "Uowner", tc.text, "root"))
			result := make(chan error, 1)
			ctx, cancel := context.WithCancel(ctx)
			defer func() { cancel(); <-result }()
			go func() { result <- s.process(ctx, e, func(context.Context, core.Inbound) error { return nil }) }()
			ordinary, controls := 1, 0
			if tc.priority {
				ordinary, controls = 0, 1
			}
			awaitPacerQueue(t, p, ordinary, controls)
		})
	}
}
