package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReviewIdentityFailureRetried(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	var fail atomic.Bool
	fail.Store(true)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" && fail.Load() {
			w.WriteHeader(503)
			return true
		}
		return false
	})
	m := f.event("D1", "im", "Uowner", "hello", "")
	raw, _ := json.Marshal(map[string]any{"event": m})
	st.StageEvent("slack", "retry", raw)
	e, _, _ := st.Event("slack", "retry")
	var accepted atomic.Int32
	accept := func(context.Context, core.Inbound) error { accepted.Add(1); return nil }
	if err := s.process(context.Background(), e, accept); err == nil {
		t.Fatal("verification failure swallowed")
	}
	if pending, _ := st.EventPending(e.ID); !pending || accepted.Load() != 0 {
		t.Fatal("failed verification admitted/completed")
	}
	fail.Store(false)
	if err := s.retryEvent(context.Background(), e, accept); err != nil {
		t.Fatal(err)
	}
	if accepted.Load() != 1 {
		t.Fatal("message lost")
	}
}

func TestReviewFailureStatusSurvivesRecovery(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	k := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1"}
	st.SaveProgress(store.Progress{Turn: 71, Conv: k, TS: "status", Phase: "done"})
	s.progress[71] = &statusUpdate{done: make(chan struct{})}
	close(s.progress[71].done)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/chat.postMessage" || r.URL.Path == "/chat.update" {
			fmt.Fprint(w, `{"ok":false,"error":"not_in_channel"}`)
			return true
		}
		return false
	})
	if s.Send(context.Background(), core.Outbound{ID: 91, TurnID: 71, Conv: k, Text: "answer"}) == nil {
		t.Fatal("expected rejected answer")
	}
	if len(s.progress) != 0 {
		t.Fatal("progress memory leaked")
	}
	p, ok, _ := st.Progress(71)
	if !ok || p.Phase != "delivery_failed" {
		t.Fatal("failure not persisted")
	}
	before := len(f.all("chat.update"))
	if err := s.recoverProgress(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.all("chat.delete")) != 0 || len(f.all("chat.update")) != before {
		t.Fatal("failed status overwritten")
	}
}

func TestReviewDownloadIdleAndScaledCap(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	privateDir(s.UploadDir())
	s.fileIdle = 80 * time.Millisecond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if r.URL.Path == "/idle" {
			<-r.Context().Done()
			return
		}
		for i := 0; i < 5; i++ {
			w.Write([]byte("x"))
			w.(http.Flusher).Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer server.Close()
	s.fileHost = func(raw string) bool { return strings.HasPrefix(raw, server.URL) }
	if a := s.download(context.Background(), slackFile{URL: server.URL + "/slow", Size: 5}); a.Stored == "" {
		t.Fatal(a.Note)
	}
	if a := s.download(context.Background(), slackFile{URL: server.URL + "/idle", Size: 5}); a.Stored != "" {
		t.Fatal("idle stream retained")
	}
	if downloadCap(200_000_000) <= downloadCap(1) || downloadCap(1) < 10*time.Minute {
		t.Fatal("cap not scaled")
	}
}

func TestReviewControlsBypassBlockedIngress(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	blocked := make(chan struct{})
	defer close(blocked)
	var entered atomic.Int32
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.history" {
			entered.Add(1)
			select {
			case <-blocked:
			case <-r.Context().Done():
			}
			fmt.Fprint(w, `{"ok":true,"messages":[]}`)
			return true
		}
		return false
	})
	got := make(chan core.Inbound, 8)
	runSlack(t, s, func(_ context.Context, in core.Inbound) error { got <- in; return nil })
	<-f.connected
	for i := 0; i < 4; i++ {
		f.push(f.event(fmt.Sprintf("D%d", i), "im", "Uowner", "slow", ""))
	}
	waitFor(t, "four blocked workers", func() bool { return entered.Load() == 4 })
	f.push(f.event("D0", "im", "Uowner", "/abort", ""))
	select {
	case in := <-got:
		if in.Text != "!cancel" {
			t.Fatalf("wrong fast input %q", in.Text)
		}
	case <-time.After(time.Second):
		t.Fatal("control queued behind history")
	}
}

func TestReviewChunkAtomicLinksAndInlineFence(t *testing.T) {
	token := "<https://example.test/" + strings.Repeat("x", 1900) + "|link label>"
	for _, lead := range []string{strings.Repeat("a", 1400), ""} {
		parts := splitReply(lead + token + strings.Repeat(" prose", 1000))
		found := false
		for _, p := range parts {
			if strings.Contains(p, token) {
				found = true
			}
			if strings.Count(p, "<") != strings.Count(p, ">") {
				t.Fatal("split link")
			}
		}
		if !found {
			t.Fatal("link changed")
		}
	}
	parts := splitReply("```x``` tail\n" + strings.Repeat("prose ", 1500))
	for _, p := range parts[1:] {
		if strings.Contains(p, "```") {
			t.Fatal("inline fence leaked into prose")
		}
	}
	huge := "<https://example.test/" + strings.Repeat("x", 5000) + ">"
	for _, p := range splitReply(huge) {
		if strings.Contains(p, "<") {
			t.Fatal("oversized token split unsafely")
		}
	}
}

func TestReviewDiskRemainingAndTurnCopy(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	privateDir(s.UploadDir())
	remaining := int64(80)
	s.reserved = 80
	file, err := os.CreateTemp(s.UploadDir(), "part")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	w := downloadWriter{file: file, s: s, remaining: &remaining, idle: timer}
	w.Write(make([]byte, 60))
	if remaining != 20 || s.reserved != 20 {
		t.Fatal("bytes counted twice")
	}
	s.freeBytes = func(string) (int64, error) { return diskReserve + 40, nil }
	copied := false
	if err := s.WithUploadCopy(s.UploadDir(), 21, func() error { copied = true; return nil }); err == nil || copied {
		t.Fatal("turn copy not reserved")
	}
	if err := s.WithUploadCopy(s.UploadDir(), 20, func() error { copied = true; return nil }); err != nil || !copied {
		t.Fatal("remaining download counted twice", err)
	}
}

func TestReviewUploadCompletionUncertainAndSafeReason(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	path := s.opt.DataDir + "/out.txt"
	os.WriteFile(path, []byte("ok"), 0600)
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/files.completeUploadExternal" {
			w.WriteHeader(503)
			return true
		}
		return false
	})
	err := s.Send(context.Background(), core.Outbound{ID: 34, Conv: core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1"}, File: &core.File{Path: path, Name: "out.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.all("files.completeUploadExternal")) != 1 || !f.hasText("File upload completion uncertain: out.txt") {
		t.Fatal("ambiguous completion retried or false failure")
	}
	reason := uploadReason(&apiError{Code: "not_in_channel", Status: 200})
	if !strings.Contains(reason, "not_in_channel") {
		t.Fatal("safe reason lost")
	}
}

func TestReviewThinkingDeferredAndNoReadyFlicker(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.statusEvery = time.Hour
	k := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1"}
	s.ObserveTurn(ctx, router.TurnEvent{ID: 88, Conv: k, Phase: "start"})
	waitFor(t, "status", func() bool { return len(f.all("chat.postMessage")) == 1 })
	s.ObserveTurn(ctx, router.TurnEvent{ID: 88, Conv: k, Phase: "progress", Progress: harness.Progress{Phase: "thinking", Text: "Checking public sources"}})
	u := s.progress[88]
	u.mu.Lock()
	if u.activity != "" || u.thinking == "" {
		t.Error("filter ran on reader")
	}
	u.mu.Unlock()
	s.flushStatus(ctx, u, false)
	s.ObserveTurn(ctx, router.TurnEvent{ID: 88, Conv: k, Phase: "done"})
	<-u.done
	for _, c := range f.all("chat.update") {
		if strings.Contains(str(c.Body["text"]), "Answer prepared") {
			t.Fatal("ready flicker")
		}
	}
}

func TestReviewMCPInputNeverForwardedAndMentionRequired(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	for _, surface := range []string{"im", "channel", "mpim"} {
		for _, text := range []string{"/mcp login server", "http://localhost:123/callback?code=private-code&state=x"} {
			for _, mentioned := range []bool{false, true} {
				input := text
				if mentioned {
					input = "<@Ubot> " + input
				}
				m := f.event("C1", surface, "Uowner", input, "")
				directEvent(t, s, m, func(_ context.Context, in core.Inbound) error {
					if surface != "im" && !mentioned {
						if !in.Ambient {
							t.Error("unmentioned warning")
						}
					} else if in.InterfaceReply != mcpNotice || in.Ambient {
						t.Error("notice missing")
					}
					if strings.Contains(in.Text, "private-code") || strings.Contains(in.Context, "private-code") || strings.Contains(in.Text, "/mcp") {
						t.Error("OAuth input leaked")
					}
					return nil
				})
			}
		}
	}
	// Header uses the verified email, matching hub userId selection.
	directEvent(t, s, f.event("D1", "im", "Uowner", "hello", ""), func(_ context.Context, in core.Inbound) error {
		if !strings.Contains(in.Context, "| from uowner@example.test |") {
			t.Error(in.Context)
		}
		return nil
	})
}

func TestReviewRetryAfterContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Retry-After", "180"); w.WriteHeader(429) }))
	defer server.Close()
	a := newAPI(server.URL, "synthetic", nil)
	err, retry, after := a.once(context.Background(), "users.info", []byte(`{}`), nil)
	if err == nil || !retry || after != 180*time.Second {
		t.Fatal("long Retry-After truncated", after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := a.call(ctx, "users.info", nil, nil); err != context.DeadlineExceeded {
		t.Fatal("deadline not honored", err)
	}
}

func TestReviewHarnessOverridesKeepNormalContext(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	for _, text := range []string{"!next hello", "!omp hello"} {
		directEvent(t, s, f.event("D1", "im", "Uowner", text, ""), func(_ context.Context, in core.Inbound) error {
			if in.Text != text || !strings.Contains(in.Context, "Slack DM") {
				t.Fatal("message override took control fast path")
			}
			return nil
		})
	}
}
