package slack

import (
	"context"
	"sync"
	"time"
)

// Process-wide pacing across all clients. No bursts; 100 calls/minute/method.
// Injected fake endpoints opt out; limiter behavior has its own fake-API test.
var tier4 = &apiPacer{interval: 600 * time.Millisecond, next: map[string]time.Time{}}

type apiPacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     map[string]time.Time
}

func (p *apiPacer) wait(ctx context.Context, method string) error {
	if method != "users.info" && method != "users.lookupByEmail" && method != "users.profile.get" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := pause(ctx, time.Until(p.next[method])); err != nil {
		return err
	}
	p.next[method] = time.Now().Add(p.interval)
	return nil
}
