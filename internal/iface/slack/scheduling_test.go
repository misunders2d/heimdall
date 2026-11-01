package slack

import (
	"context"
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

func startDispatcher(t *testing.T, s *Slack, accept func(context.Context, core.Inbound) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.processEvents(ctx, make(chan store.InterfaceEvent), accept) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("dispatcher stuck")
		}
	})
}
func receiveInput(t *testing.T, ch <-chan core.Inbound) core.Inbound {
	t.Helper()
	select {
	case in := <-ch:
		return in
	case <-time.After(3 * time.Second):
		t.Fatal("no admission")
		return core.Inbound{}
	}
}

func TestIngressRetryHeadPreservesTimestampOrder(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	second := f.event("D1", "im", "Uowner", "second", "root")
	second.TS = "2.000002"
	first := f.event("D1", "im", "Uowner", "first", "root")
	first.TS = "2.000001"
	stageReview(t, s, second)
	head := stageReview(t, s, first)
	var calls atomic.Int32
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" && calls.Add(1) == 1 {
			w.WriteHeader(503)
			return true
		}
		return false
	})
	inputs := make(chan core.Inbound, 8)
	startDispatcher(t, s, func(_ context.Context, in core.Inbound) error { inputs <- in; return nil })
	time.Sleep(350 * time.Millisecond)
	select {
	case <-inputs:
		t.Fatal("later input bypassed retrying head")
	default:
	}
	s.retryMu.Lock()
	r := s.retries[head.ID]
	if r.attempts != 1 {
		t.Error("head not attempted")
	}
	r.next = time.Time{}
	s.retries[head.ID] = r
	s.retryMu.Unlock()
	if receiveInput(t, inputs).Text != "first" || receiveInput(t, inputs).Text != "second" {
		t.Fatal("wrong order")
	}
}

func TestIngressSlowConversationDoesNotBlockOthers(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	s.bot.timeout = time.Minute
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	defer close(release)
	var history atomic.Int32
	f.setHook(func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/conversations.replies" && args["ts"] != "fast" {
			history.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			fmt.Fprint(w, `{"ok":true,"messages":[]}`)
			return true
		}
		return false
	})
	for i := 0; i < 4; i++ {
		stageReview(t, s, f.event("C1", "channel", "Uowner", "<@Ubot> slow", fmt.Sprint(i)))
	}
	inputs := make(chan core.Inbound, 8)
	startDispatcher(t, s, func(_ context.Context, in core.Inbound) error { inputs <- in; return nil })
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("conversations serialized")
		}
	}
	// Exceeds the old 20-second ingress stall, while periodic scans run.
	time.Sleep(21 * time.Second)
	stageReview(t, s, f.event("C1", "channel", "Uowner", "<@Ubot> fast", "fast"))
	if receiveInput(t, inputs).Text != "fast" {
		t.Fatal("other conversation delayed")
	}
	if history.Load() != 4 {
		t.Fatal("in-flight event re-enqueued")
	}
}
func TestIngress429DoesNotConsumeAttempt(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(429)
			return true
		}
		return false
	})
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", ""))
	for i := 0; i < 5; i++ {
		if err := s.retryEvent(context.Background(), e, nil); err != nil {
			t.Fatal(err)
		}
		s.retryMu.Lock()
		r := s.retries[e.ID]
		if r.attempts != 0 || time.Until(r.next) < 59*time.Second {
			t.Error("429 consumed attempt or ignored Retry-After")
		}
		r.next = time.Time{}
		s.retries[e.ID] = r
		s.retryMu.Unlock()
	}
}
func TestIngressExhaustionNoticeAddressedOnly(t *testing.T) {
	for _, kind := range []string{"dm", "mention", "control", "ambient", "permanent"} {
		t.Run(kind, func(t *testing.T) {
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			s.botID = "Ubot"
			s.opt.UnmentionedCommands = []string{"stop", "cancel"}
			s.opt.IngressRetryWindow = time.Nanosecond
			f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
				if r.URL.Path == "/users.info" {
					if kind == "permanent" {
						fmt.Fprint(w, `{"ok":false,"error":"user_not_found"}`)
					} else {
						w.WriteHeader(503)
					}
					return true
				}
				return false
			})
			m := f.event("C1", "channel", "Uowner", "secret-text", "root")
			if kind == "dm" || kind == "permanent" {
				m.ChannelType = "im"
			}
			if kind == "mention" {
				m.Text = "<@Ubot> secret-text"
			}
			if kind == "control" {
				m.Text = "stop"
			}
			e := stageReview(t, s, m)
			for i := 0; i < 2; i++ {
				if err := s.retryEvent(context.Background(), e, nil); err != nil {
					t.Fatal(err)
				}
			}
			n, err := st.Undelivered()
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if kind == "ambient" || kind == "permanent" {
				want = 0
			}
			if n != want {
				t.Fatalf("notices %d want %d", n, want)
			}
			if want == 1 {
				out, _, _ := st.NextOutbound("slack")
				if out.Conv.Thread != "root" || strings.Contains(out.Text, "secret-text") {
					t.Fatal("notice leaked text/wrong thread")
				}
			}
		})
	}
}
func TestIngressFenceAmbientCountAndRetryCleanup(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	s.opt.UnmentionedCommands = []string{"stop", "cancel"}
	s.opt.Owners = []string{"slack:uowner@example.test"}
	ambient := stageReview(t, s, f.event("C1", "channel", "Uowner", "ambient", "root"))
	bot := stageReview(t, s, f.event("C1", "channel", "Ubot", "<@Ubot> bot", "root"))
	work := stageReview(t, s, f.event("C1", "channel", "Uowner", "<@Ubot> work", "root"))
	command := stageReview(t, s, f.event("C1", "channel", "Uowner", "<@Ubot> !status", "root"))
	s.retries = map[int64]eventRetry{work.ID: {attempts: 1, next: time.Now().Add(time.Minute)}}
	stop := stageReview(t, s, f.event("C1", "channel", "Uowner", "stop", "root"))
	if err := s.process(context.Background(), stop, func(_ context.Context, in core.Inbound) error {
		if in.ControlIngress != 1 {
			t.Errorf("count %d want 1", in.ControlIngress)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.retries[work.ID]; ok {
		t.Fatal("fenced retry retained")
	}
	for _, e := range []store.InterfaceEvent{ambient, bot, command} {
		if p, _ := st.EventPending(e.ID); !p {
			t.Fatal("fence dropped non-turn input")
		}
	}
	var gotAmbient bool
	s.process(context.Background(), ambient, func(_ context.Context, in core.Inbound) error { gotAmbient = in.Ambient; return nil })
	if !gotAmbient {
		t.Fatal("ambient did not flow")
	}
}
func TestIngressOwnerLiveLookupRetries(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	var n atomic.Int32
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" && n.Add(1) == 1 {
			w.WriteHeader(503)
			return true
		}
		return false
	})
	if _, ok := s.OwnerChat("uowner@example.test"); !ok {
		t.Fatal("one transient lookup lost owner notice")
	}
}
func TestOAuthLoopbackForms(t *testing.T) {
	for _, url := range []string{"http://127.2.3.4/?state=SECRET&code=SECRET", "http://[0:0:0:0:0:0:0:1]/?state=SECRET&code=SECRET", "http://localhost./?state=SECRET;code=SECRET", "http://localhost/#state=SECRET&code=SECRET", "http://localhost/?state=SECRET#code=SECRET", "Sign in to MCP server test in your browser:\nhttps://authorize.example/?token=SECRET", "Sign in to MCP server test:\nhttps://authorize.example/?token=SECRET"} {
		if got := hideOAuth(url); strings.Contains(got, "SECRET") {
			t.Errorf("leaked %s", got)
		}
	}
}
func TestFakeIgnoresStrayProbes(t *testing.T) {
	f := newSlackFake(t)
	for _, path := range []string{"/", "/favicon.ico"} {
		res, err := http.Get(f.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 404 {
			t.Fatal(res.StatusCode)
		}
	}
}

func TestIngressSlowAdmissionDoesNotLockOtherConversations(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	slow := stageReview(t, s, f.event("D1", "im", "Uowner", "slow", "root"))
	fast := stageReview(t, s, f.event("D2", "im", "Uowner", "fast", "root"))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		done <- s.process(context.Background(), slow, func(context.Context, core.Inbound) error { close(entered); <-release; return nil })
	}()
	<-entered
	fastDone := make(chan error, 1)
	go func() {
		fastDone <- s.process(context.Background(), fast, func(context.Context, core.Inbound) error { return nil })
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Error("other admission blocked")
	}
	// Avoid closing the store while slow admission unwinds.
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestIngressExhaustionWakesDeliveryAndBotThread(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	f.mu.Lock()
	f.messages = append(f.messages, map[string]any{"channel": "C1", "ts": "root", "user": "Ubot", "text": "earlier answer"})
	f.mu.Unlock()
	var wakes atomic.Int32
	s.SetOutboundWake(func() { wakes.Add(1) })
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "secret", "root"))
	if err := s.exhaustEvent(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if err := s.exhaustEvent(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	n, _ := st.Undelivered()
	if n != 1 || wakes.Load() == 0 {
		t.Fatal("missing/duplicated notice or no delivery wake")
	}
}
func TestIngressRouterAdmissionErrorRetries(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", "root"))
	if err := s.retryEvent(context.Background(), e, func(context.Context, core.Inbound) error { return fmt.Errorf("temporary admission error") }); err != nil {
		t.Fatal(err)
	}
	if pending, _ := st.EventPending(e.ID); !pending {
		t.Fatal("failed admission discarded")
	}
	s.retryMu.Lock()
	r := s.retries[e.ID]
	r.next = time.Time{}
	s.retries[e.ID] = r
	s.retryMu.Unlock()
	var n int
	if err := s.retryEvent(context.Background(), e, func(context.Context, core.Inbound) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("admission not retried")
	}
}

func TestIngressUnknownThreadExhaustionWaitsForClassification(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "secret", "root"))
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.replies" {
			w.WriteHeader(503)
			return true
		}
		return false
	})
	if err := s.exhaustEvent(context.Background(), e); err == nil {
		t.Fatal("unknown thread silently completed")
	}
	if p, _ := st.EventPending(e.ID); !p {
		t.Fatal("unknown thread lost")
	}
	f.setHook(nil)
	f.mu.Lock()
	f.messages = append(f.messages, map[string]any{"channel": "C1", "ts": "root", "user": "Ubot"})
	f.mu.Unlock()
	if err := s.exhaustEvent(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	n, _ := st.Undelivered()
	if n != 1 {
		t.Fatal("bot thread notice lost")
	}
}

func TestIngressJSONRateLimitRetryAfter(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" {
			w.Header().Set("Retry-After", "45")
			fmt.Fprint(w, `{"ok":false,"error":"ratelimited"}`)
			return true
		}
		return false
	})
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", ""))
	if err := s.retryEvent(context.Background(), e, nil); err != nil {
		t.Fatal(err)
	}
	r := s.retries[e.ID]
	if r.attempts != 0 || time.Until(r.next) < 44*time.Second {
		t.Fatal("JSON rate limit ignored")
	}
}

func TestIngressNoticeClassificationHonorsRetryAfter(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	e := stageReview(t, s, f.event("C1", "channel", "Uowner", "secret", "root"))
	s.retries = map[int64]eventRetry{e.ID: {exhausted: true, started: time.Now()}}
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.replies" {
			w.Header().Set("Retry-After", "45")
			w.WriteHeader(429)
			return true
		}
		return false
	})
	if err := s.retryEvent(context.Background(), e, func(context.Context, core.Inbound) error { t.Error("exhausted event admitted"); return nil }); err != nil {
		t.Fatal(err)
	}
	r := s.retries[e.ID]
	if r.attempts != 0 || time.Until(r.next) < 44*time.Second {
		t.Fatal("notice classification ignored rate limit")
	}
	if p, _ := st.EventPending(e.ID); !p {
		t.Fatal("unclassified thread lost")
	}
}

func TestIngressIdentityAlwaysLive(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	var admitted []core.Inbound
	accept := func(_ context.Context, in core.Inbound) error { admitted = append(admitted, in); return nil }
	directEvent(t, s, f.event("C1", "channel", "Uowner", "ambient", "root"), accept)
	f.mu.Lock()
	u := f.users["Uowner"]
	u.Deleted = true
	f.users["Uowner"] = u
	f.mu.Unlock()
	directEvent(t, s, f.event("C1", "channel", "Uowner", "<@Ubot> work", "root"), accept)
	if len(admitted) != 1 {
		t.Fatal("cached identity authorized addressed input")
	}
	f.mu.Lock()
	u.Deleted = false
	u.Profile.Email = "changed@example.test"
	f.users["Uowner"] = u
	f.mu.Unlock()
	directEvent(t, s, f.event("C1", "channel", "Uowner", "stop", "root"), accept)
	if len(admitted) != 2 || admitted[1].From.Email != "changed@example.test" {
		t.Fatal("control did not use live identity")
	}
	if n := len(f.all("users.info")); n != 3 {
		t.Fatalf("users.info calls %d want 3", n)
	}
}

func TestEmbeddedMCPCommandFieldStillRedacted(t *testing.T) {
	m := eventMessage{Text: "summarize", Attachments: []any{map[string]any{"text": "/mcp login private-server", "fallback": "ordinary context"}}}
	got := embeddedContent(m)
	if strings.Contains(got, "/mcp") || strings.Contains(got, "private-server") || !strings.Contains(got, "ordinary context") {
		t.Fatal("embedded command leaked or ordinary content lost")
	}
}
