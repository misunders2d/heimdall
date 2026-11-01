package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestFinalExhaustedPermanentAndCaps(t *testing.T) {
	for _, kind := range []string{"thread_not_found", "channel_not_found", "not_in_channel", "window", "attempts", "rateLimits"} {
		t.Run(kind, func(t *testing.T) {
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			s.botID = "Ubot"
			e := stageReview(t, s, f.event("C1", "channel", "Uowner", "hello", "root"))
			r := eventRetry{exhausted: true, started: time.Now()}
			code := kind
			switch kind {
			case "window":
				r.started = time.Now().Add(-time.Hour)
				code = "internal_error"
			case "attempts":
				r.attempts = 2
				code = "missing_scope"
			case "rateLimits":
				r.rateLimits = 99
				code = "ratelimited"
			}
			s.retries = map[int64]eventRetry{e.ID: r}
			f.setHook(func(w http.ResponseWriter, req *http.Request, _ map[string]any) bool {
				if req.URL.Path == "/conversations.replies" {
					fmt.Fprintf(w, `{"ok":false,"error":%q}`, code)
					return true
				}
				return false
			})
			if err := s.retryEvent(context.Background(), e, nil); err != nil {
				t.Fatal(err)
			}
			if p, _ := st.EventPending(e.ID); p {
				t.Fatal("exhausted head still pending")
			}
		})
	}
}

func TestFinalRateLimit100AndTokenThree(t *testing.T) {
	for _, code := range []string{"ratelimited", "missing_scope", "account_inactive"} {
		t.Run(code, func(t *testing.T) {
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			s.botID = "Ubot"
			e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", ""))
			f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
				if r.URL.Path == "/users.info" {
					fmt.Fprintf(w, `{"ok":false,"error":%q}`, code)
					return true
				}
				return false
			})
			n := 3
			if code == "ratelimited" {
				n = 100
			}
			for i := 0; i < n; i++ {
				if err := s.retryEvent(context.Background(), e, nil); err != nil {
					t.Fatal(err)
				}
				p, _ := st.EventPending(e.ID)
				if p != (i < n-1) {
					t.Fatalf("attempt %d pending=%v", i+1, p)
				}
				s.retryMu.Lock()
				r := s.retries[e.ID]
				r.next = time.Time{}
				s.retries[e.ID] = r
				s.retryMu.Unlock()
			}
			if len(f.all("users.info")) != n {
				t.Fatal("wrong token/rate-limit budget")
			}
		})
	}
}

func TestFinalReceivedWindowAndInlineRetryAfter(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "hello", "root"))
	if e.ReceivedAt.IsZero() {
		t.Fatal("missing durable receipt")
	}
	// Represents row received before restart; no in-memory retry state.
	e.ReceivedAt = time.Now().Add(-time.Hour)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" {
			w.WriteHeader(503)
			return true
		}
		if r.URL.Path == "/conversations.replies" {
			w.Header().Set("Retry-After", "90")
			w.WriteHeader(429)
			return true
		}
		return false
	})
	_ = s.retryEvent(context.Background(), e, nil)
	r := s.retries[e.ID]
	if !r.exhausted || time.Until(r.next) < 89*time.Second {
		t.Fatal("receipt window or inline Retry-After ignored")
	}
	if p, _ := st.EventPending(e.ID); !p {
		t.Fatal("retry lost")
	}
}

func TestFinalOAuthGaps(t *testing.T) {
	for _, raw := range []string{
		"Sign in to MCP server private:\n\n \t https://example.test/authorize?secret=hidden",
		"http://127.1/callback?state=s&code=c",
		"http://localhost.../callback?state=s&error=e",
	} {
		if strings.Contains(hideOAuth(raw), "http") {
			t.Fatalf("leaked %q", raw)
		}
	}
	for _, raw := range []string{"https://example.test/?state=s", "https://example.test/?code=c", "http://example.test/?state=s&code=c"} {
		if hideOAuth(raw) != raw {
			t.Fatal("ordinary URL hidden")
		}
	}
}

func TestFinalBacklogConcurrencyAndScan(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.opt.DispatchConcurrency = 3
	var events []store.InterfaceEvent
	for i := 0; i < 2500; i++ {
		m := f.event("D1", "im", "Uowner", strings.Repeat("x", 2000), "")
		m.TS = fmt.Sprintf("%d.000001", i+1)
		events = append(events, stageReview(t, s, m))
	}
	cache := map[int64]eventMessage{}
	start := time.Now()
	s.cacheEvents(events, cache)
	cold := time.Since(start)
	if cold > 5*time.Second {
		t.Fatalf("cold decode exceeds budget: %s", cold)
	}
	start = time.Now()
	for range 5 {
		s.cacheEvents(events, cache)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("cached scans exceed budget: %s", time.Since(start))
	}
	t.Logf("cold decode %s; five cached scans %s", cold, time.Since(start))
	if len(cache) != 2500 {
		t.Fatal("cache incomplete")
	}
	entered := make(chan struct{}, 3000)
	release := make(chan struct{})
	var live, peak atomic.Int32
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path != "/users.info" {
			return false
		}
		n := live.Add(1)
		defer live.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		fmt.Fprint(w, `{"ok":false,"error":"user_not_found"}`)
		return true
	})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- s.processEvents(ctx, make(chan store.InterfaceEvent), func(context.Context, core.Inbound) error { return nil })
	}()
	defer func() {
		cancel()
		close(release)
		select {
		case <-finished:
		case <-time.After(15 * time.Second):
			t.Error("dispatcher cancellation stuck")
		}
	}()
	for range 3 {
		select {
		case <-entered:
		case <-time.After(15 * time.Second):
			t.Fatal("workers not dispatched")
		}
	}
	time.Sleep(250 * time.Millisecond)
	if peak.Load() > 3 {
		t.Fatalf("unbounded concurrency %d", peak.Load())
	}
}

func TestFinalPacerAndJitter(t *testing.T) {
	p := &apiPacer{interval: 25 * time.Millisecond, next: map[string]time.Time{}}
	var wg sync.WaitGroup
	start := time.Now()
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.wait(context.Background(), "users.info"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if time.Since(start) < 95*time.Millisecond {
		t.Fatal("pacer allowed burst")
	}
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool { w.WriteHeader(503); return true })
	var delays []time.Duration
	for i := 0; i < 6; i++ {
		m := f.event("D1", "im", "Uowner", "hello", "")
		m.TS = fmt.Sprint(i)
		e := stageReview(t, s, m)
		before := time.Now()
		_ = s.retryEvent(context.Background(), e, nil)
		d := s.retries[e.ID].next.Sub(before)
		delays = append(delays, d)
	}
	distinct := false
	for _, d := range delays {
		if d > 1050*time.Millisecond {
			distinct = true
		}
	}
	if !distinct {
		t.Fatal("no retry jitter")
	}
}

func TestFinalOwnerFenceVerifiedCountAndLockPrune(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	s.opt.Owners = []string{"slack:uowner@example.test"}
	s.opt.UnmentionedCommands = []string{"stop"}
	s.liveUser(context.Background(), "Uguest")
	s.liveUser(context.Background(), "Umissing")
	stageReview(t, s, f.event("C1", "channel", "Uguest", "<@Ubot> foreign", "root"))
	m := f.event("C1", "channel", "Umissing", "<@Ubot> missing", "root")
	stageReview(t, s, m)
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "stop", "root"))
	if err := s.process(context.Background(), e, func(_ context.Context, in core.Inbound) error {
		if in.ControlIngress != 0 {
			t.Fatalf("counted unverifiable callers: %d", in.ControlIngress)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(s.ingressLocks) != 0 {
		t.Fatal("idle lock leaked")
	}
}

func TestFinalCacheStable(t *testing.T) {
	s := &Slack{}
	cache := map[int64]eventMessage{}
	raw, _ := json.Marshal(map[string]any{"event": eventMessage{TS: "2", Text: "stop"}})
	events := []store.InterfaceEvent{{ID: 1, Payload: raw}}
	s.cacheEvents(events, cache)
	events[0].Payload = []byte("invalid") // Durable timestamp cached, no repeated decode.
	s.cacheEvents(events, cache)
	if !cache[1].ingressControl {
		t.Fatal("control classification not cached")
	}
	if cache[1].TS != "2" {
		t.Fatal("event decoded again")
	}
	s.cacheEvents(nil, cache)
	if len(cache) != 0 {
		t.Fatal("cache not pruned")
	}
}

func TestFinalFileImageRegexAllocations(t *testing.T) {
	f := slackFile{FileType: "jpeg", Name: "x.jpeg"}
	if n := testing.AllocsPerRun(100, func() {
		if !f.image() {
			t.Fatal("image type lost")
		}
	}); n > 2 {
		t.Fatalf("regex rebuilt: %v allocations", n)
	}
}

func TestFinalDrainAllCompletions(t *testing.T) {
	done := make(chan int64, 8)
	active := map[int64]bool{}
	for i := int64(1); i <= 8; i++ {
		active[i] = true
		done <- i
	}
	drainIngressDone(done, active)
	if len(active) != 0 || len(done) != 0 {
		t.Fatal("completion left behind scan")
	}
}

func TestFinalInlineExhaustionRetryAfter(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "hello", "root"))
	s.retries = map[int64]eventRetry{e.ID: {attempts: 2, started: time.Now()}}
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" {
			fmt.Fprint(w, `{"ok":false,"error":"missing_scope"}`)
			return true
		}
		if r.URL.Path == "/conversations.replies" {
			w.Header().Set("Retry-After", "90")
			w.WriteHeader(429)
			return true
		}
		return false
	})
	_ = s.retryEvent(context.Background(), e, nil)
	if r := s.retries[e.ID]; !r.exhausted || time.Until(r.next) < 89*time.Second {
		t.Fatal("inline exhaustion ignored Retry-After")
	}
}

func TestFinalPacerWiredToAPI(t *testing.T) {
	if newAPI("", "fake", nil).pacer != tier4 {
		t.Fatal("production client missing process limiter")
	}
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.bot.pacer = &apiPacer{interval: 25 * time.Millisecond, next: map[string]time.Time{}}
	start := time.Now()
	for range 5 {
		if _, err := s.liveUser(context.Background(), "Uowner"); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) < 95*time.Millisecond {
		t.Fatal("users.info bypassed limiter")
	}
}

func TestFinalImageMIMERegexAllocations(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	file := slackFile{Name: "x.jpeg", FileType: "jpeg", Size: -1}
	if n := testing.AllocsPerRun(100, func() {
		a := s.download(context.Background(), file)
		if a.MIME != "image/jpeg" {
			t.Fatal("MIME lost")
		}
	}); n > 25 {
		t.Fatalf("MIME regex rebuilt: %v", n)
	}
}

func TestFinalFileNameRegexAllocations(t *testing.T) {
	f := slackFile{Name: "x.jpeg"}
	if n := testing.AllocsPerRun(100, func() {
		if !f.image() {
			t.Fatal("image name lost")
		}
	}); n > 2 {
		t.Fatalf("filename regex rebuilt: %v", n)
	}
}
