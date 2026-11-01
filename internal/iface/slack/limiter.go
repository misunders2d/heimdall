package slack

import (
	"context"
	"sync"
	"time"
)

// Process-wide pacing: Tier 4 reads 100/minute, lookupByEmail Tier 3 50/minute.
// Injected fake endpoints opt out; limiter behavior has its own fake-API test.
var tier4 = &apiPacer{interval: 600 * time.Millisecond, next: map[string]time.Time{}}

type apiPacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     map[string]time.Time
	queues   map[string]*pacerQueue
}

type pacerWaiter struct{ priority bool }

type pacerQueue struct {
	ordinary, priority []*pacerWaiter
	lastPriority       bool
	changed            chan struct{}
}

func (q *pacerQueue) notify() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *pacerQueue) head() *pacerWaiter {
	// A priority grant always yields the next slot to the oldest ordinary
	// waiter, if one exists. Each class retains arrival order.
	if len(q.priority) > 0 && (!q.lastPriority || len(q.ordinary) == 0) {
		return q.priority[0]
	}
	if len(q.ordinary) > 0 {
		return q.ordinary[0]
	}
	return nil
}

func (p *apiPacer) wait(ctx context.Context, method string) error {
	if method != "users.info" && method != "users.lookupByEmail" && method != "users.profile.get" {
		return nil
	}
	priority, _ := ctx.Value(controlIdentityKey{}).(bool)
	w := &pacerWaiter{priority: priority}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queues == nil {
		p.queues = map[string]*pacerQueue{}
	}
	q := p.queues[method]
	if q == nil {
		q = &pacerQueue{changed: make(chan struct{})}
		p.queues[method] = q
	}
	queue := &q.ordinary
	if priority {
		queue = &q.priority
	}
	*queue = append(*queue, w)
	q.notify()
	defer func() {
		for i, item := range *queue {
			if item == w {
				*queue = append((*queue)[:i], (*queue)[i+1:]...)
				break
			}
		}
		q.notify() // Cancellation and grants both release the next waiter.
	}()
	interval := p.interval
	if method == "users.lookupByEmail" {
		interval *= 2
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := time.Until(p.next[method])
		head := q.head() == w
		if head && delay <= 0 {
			if p.next == nil {
				p.next = map[string]time.Time{}
			}
			p.next[method] = time.Now().Add(interval)
			q.lastPriority = priority
			return nil
		}
		changed := q.changed
		var timer *time.Timer
		var tick <-chan time.Time
		if head {
			timer = time.NewTimer(delay)
			tick = timer.C
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		case <-tick:
		}
		if timer != nil {
			timer.Stop()
		}
		p.mu.Lock()
	}
}

type controlIdentityKey struct{}
