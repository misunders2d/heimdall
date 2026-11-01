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
	priority map[string]int
}

func (p *apiPacer) wait(ctx context.Context, method string) error {
	if method != "users.info" && method != "users.lookupByEmail" && method != "users.profile.get" {
		return nil
	}
	priority, _ := ctx.Value(controlIdentityKey{}).(bool)
	p.mu.Lock()
	if p.priority == nil {
		p.priority = map[string]int{}
	}
	if priority {
		p.priority[method]++
	}
	p.mu.Unlock()
	if priority {
		defer func() { p.mu.Lock(); p.priority[method]--; p.mu.Unlock() }()
	}
	interval := p.interval
	if method == "users.lookupByEmail" {
		interval *= 2
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.Lock()
		delay := time.Until(p.next[method])
		if delay <= 0 && (priority || p.priority[method] == 0) {
			p.next[method] = time.Now().Add(interval)
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		if delay <= 0 {
			delay = p.interval
		}
		if err := pause(ctx, delay); err != nil {
			return err
		}
	}
}

type controlIdentityKey struct{}
