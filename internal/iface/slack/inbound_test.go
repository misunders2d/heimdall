package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

func directEvent(t *testing.T, s *Slack, m eventMessage, accept func(context.Context, core.Inbound) error) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"type": "event_callback", "event_id": "E" + m.TS, "event": m})
	if err := s.opt.Store.StageEvent("slack", "E"+m.TS, raw); err != nil {
		t.Fatal(err)
	}
	e, ok, err := s.opt.Store.Event("slack", "E"+m.TS)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return
	}
	if err = s.process(context.Background(), e, accept); err != nil {
		t.Fatal(err)
	}
}
func TestSlackSharedCommandPolicies(t *testing.T) {
	for _, policy := range []struct {
		name  string
		words []string
	}{{"default", nil}, {"all", []string{"all"}}, {"none", []string{}}} {
		t.Run(policy.name, func(t *testing.T) {
			f := newSlackFake(t)
			s, _ := testSlack(t, f)
			s.botID = "Ubot"
			s.opt.UnmentionedCommands = policy.words
			for _, text := range []string{"stop", "cancel", "/stop", "/stop@Ubot", "abort", "nevermind", "/abort", "/nevermind", "reset", "chatid", "/start", "!help", "!status", "!use omp", "!next hello", "!omp hello", "stop now", "hello", "<@Ubot> reset", "<@Ubot|bot> !status"} {
				m := f.event("C1", "channel", "Uowner", text, "")
				var got core.Inbound
				directEvent(t, s, m, func(_ context.Context, in core.Inbound) error { got = in; return nil })
				control := text == "stop" || text == "cancel" || text == "/stop" || text == "/stop@Ubot" || text == "abort" || text == "nevermind" || text == "/abort" || text == "/nevermind"
				command := text != "hello" && text != "stop now"
				want := strings.Contains(text, "<@Ubot") || policy.name == "all" && command || policy.name == "default" && control
				if got.MsgID == "" || got.Ambient == want {
					t.Errorf("%s: addressed %v want %v", text, !got.Ambient, want)
				}
				dm := f.event("D1", "im", "Uowner", text, "")
				directEvent(t, s, dm, func(_ context.Context, in core.Inbound) error {
					if in.Ambient {
						t.Error("DM gated")
					}
					return nil
				})
			}
		})
	}
}
func TestSlackAdmissionAndEmbeddedBoundary(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	var admitted []core.Inbound
	accept := func(_ context.Context, in core.Inbound) error { admitted = append(admitted, in); return nil }
	for _, user := range []string{"Uguest", "Unoemail", "Ubot"} {
		directEvent(t, s, f.event("C1", "channel", user, "<@Ubot> !help", ""), accept)
	}
	for _, sub := range []string{"message_changed", "message_deleted", "channel_join", "bot_message"} {
		m := f.event("C1", "channel", "Uowner", "<@Ubot> !help", "")
		m.Subtype = sub
		directEvent(t, s, m, accept)
	}
	if len(admitted) != 0 {
		t.Fatal("unverified or unsupported event admitted")
	}
	m := f.event("C1", "channel", "Uemployee", "plain text", "")
	m.Attachments = []any{map[string]any{"text": "<@Ubot> !reset", "author_name": "spoofed owner", "message_blocks": []any{map[string]any{"message": map[string]any{"text": "forwarded content"}}}}}
	directEvent(t, s, m, accept)
	if len(admitted) != 1 || !admitted[0].Ambient || admitted[0].From.Email != "uemployee@example.test" {
		t.Fatalf("embedded changed routing: %+v", admitted)
	}
	m = f.event("C1", "channel", "Uemployee", "<@Ubot> summarize &lt;data&gt;", "")
	m.Attachments = []any{map[string]any{"text": "untrusted !stop", "author_name": "do not include"}}
	directEvent(t, s, m, accept)
	in := admitted[1]
	if in.Text != "summarize <data>" || !strings.Contains(in.Context, "untrusted !stop") || strings.Contains(in.Context, "do not include") {
		t.Fatalf("wrong content boundary %+v", in)
	}
	m = f.event("C1", "channel", "Uowner", "http://localhost/?code=private-code&state=state", "")
	directEvent(t, s, m, accept)
	in = admitted[2]
	if !in.Ambient || strings.Contains(in.Text, "private-code") {
		t.Fatal("OAuth safety warning missing")
	}
	rows, err := s.opt.Store.RecentAmbient(core.ConvKey{Interface: "slack", Chat: "C1"}, nil, 40)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if strings.Contains(row.Text, "private-code") {
			t.Fatal("callback persisted in ambient")
		}
	}
}
func TestEmbeddedBoundsAndTypes(t *testing.T) {
	m := eventMessage{Text: "control", Blocks: []any{map[string]any{"type": "rich_text", "elements": []any{map[string]any{"type": "rich_text_section", "elements": []any{map[string]any{"type": "text", "text": "control"}}}}}}}
	if got := embeddedContent(m); got != "" {
		t.Fatalf("duplicated root control %q", got)
	}
	m.Attachments = []any{map[string]any{"text": strings.Repeat("x", 13000)}, map[string]any{"fallback": "fallback", "author_name": "secret name"}}
	got := embeddedContent(m)
	if !strings.Contains(got, "truncated") || len([]rune(got)) > 25000 || strings.Contains(got, "secret name") {
		t.Fatal("embedded bounds or field allowlist")
	}
	x := extractor{}
	list := map[string]any{"type": "rich_text_list", "style": "ordered", "elements": []any{map[string]any{"type": "rich_text_section", "elements": []any{map[string]any{"type": "link", "url": "https://example.test"}, map[string]any{"type": "emoji", "name": "wave"}}}}}
	if got := x.rich(list, 0); !strings.Contains(got, "1. https://example.test:wave:") {
		t.Fatalf("rich list %q", got)
	}
	recursive := map[string]any{"type": "rich_text_section"}
	recursive["elements"] = []any{recursive}
	x = extractor{}
	_ = x.rich(recursive, 0)
	if !x.truncated {
		t.Fatal("recursive input unbounded")
	}
}
func TestSlackContextCombinesSourcesAndImageOrder(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	k := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	for i := 1; i <= 7; i++ {
		m := map[string]any{"ts": fmt.Sprintf("1000.%06d", i), "channel": "C1", "thread_ts": k.Thread, "user": "Uother", "text": "live history", "files": []map[string]any{{"id": fmt.Sprint(i), "name": "image.png", "mimetype": "image/png", "size": 10, "url_private": f.srv.URL + "/file"}}}
		f.messages = append(f.messages, m)
	}
	if err := st.AddAmbient(store.Ambient{Conv: core.ConvKey{Interface: "slack", Chat: "C1"}, MsgID: "ambient", Who: "slack:a@example.test", Text: "stored channel ambient", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	id, err := st.Enqueue(core.Outbound{Conv: k, Text: "prior delivered answer"})
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Sent(id); err != nil {
		t.Fatal(err)
	}
	text, images := s.context(context.Background(), eventMessage{Channel: "C1", TS: "1000.000020", ThreadTS: k.Thread}, k)
	for _, want := range []string{"live history", "prior delivered answer"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(text, "stored channel ambient") {
		t.Fatal("thread leaked channel ambient")
	}
	if len(images) != 5 || images[0].ID != "3" || images[4].ID != "7" {
		t.Fatalf("image selection/order %+v", images)
	}
}
func TestSlackDownloadReservationsAndSecurity(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	if err := privateDir(s.UploadDir()); err != nil {
		t.Fatal(err)
	}
	f.file = []byte("hello")
	a := s.download(context.Background(), slackFile{Name: "../hello.txt", Size: 5, URL: f.srv.URL + "/file", MIME: "text/plain"})
	if a.Stored == "" || a.Name != "hello.txt" {
		t.Fatalf("download %+v", a)
	}
	if st, _ := os.Stat(a.Stored); st.Mode().Perm() != 0600 {
		t.Fatal("file permissions")
	}
	if a = s.download(context.Background(), slackFile{URL: f.srv.URL + "/file", External: true}); a.Stored != "" {
		t.Fatal("external downloaded")
	}
	if a = s.download(context.Background(), slackFile{URL: "https://attacker.invalid/file", Size: 5}); a.Stored != "" {
		t.Fatal("foreign host downloaded")
	}
	s.opt.MaxFileBytes = 4
	if a = s.download(context.Background(), slackFile{URL: f.srv.URL + "/file", Size: 0}); a.Stored != "" {
		t.Fatal("stream exceeded cap")
	}
	s.opt.MaxFileBytes = 100
	s.freeBytes = func(string) (int64, error) { return diskReserve + 100, nil }
	s.mu.Lock()
	s.reserved = 90
	s.mu.Unlock()
	if a = s.download(context.Background(), slackFile{URL: f.srv.URL + "/file", Size: 20}); a.Stored != "" {
		t.Fatal("parallel reservation ignored")
	}
	s.mu.Lock()
	s.reserved = 0
	s.mu.Unlock()
	f.mu.Lock()
	f.file = []byte("x")
	f.mu.Unlock()
	a = s.download(context.Background(), slackFile{URL: f.srv.URL + "/file", Name: "img.png", FileType: "png", Size: 1})
	if a.MIME != "image/png" || a.Kind != "photo" {
		t.Fatal("filetype image normalization")
	}
	if s.reserved != 0 {
		t.Fatal("reservation leak")
	}
	old := filepath.Join(s.UploadDir(), "old.part")
	os.WriteFile(old, []byte("x"), 0600)
	past := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(old, past, past)
	s.pruneUploads()
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale upload retained")
	}
}
func TestReceiverDurabilityAndProcessingAfterAck(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	var mu sync.Mutex
	var got []core.Inbound
	runSlack(t, s, func(_ context.Context, in core.Inbound) error {
		mu.Lock()
		got = append(got, in)
		mu.Unlock()
		return nil
	})
	select {
	case <-f.connected:
	case <-time.After(3 * time.Second):
		t.Fatal("no socket")
	}
	m := f.event("D1", "im", "Uowner", "hello", "")
	id := f.push(m)
	select {
	case ack := <-f.acks:
		if ack != id {
			t.Fatal("wrong envelope ack")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no ack")
	}
	waitFor(t, "verified message", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 1 })
	// Redelivery cannot repeat completed processing.
	f.push(m)
	waitFor(t, "second ack", func() bool { return len(f.acks) > 0 })
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	if len(got) != 1 {
		t.Error("duplicate input")
	}
	mu.Unlock()
	if rows, err := st.PendingEvents("slack"); err != nil || len(rows) != 0 {
		t.Fatalf("pending events %d %v", len(rows), err)
	}
}

func TestConcurrentDownloadsReserveDiskAndNeverLeakRedirectToken(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	if err := privateDir(s.UploadDir()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.Write([]byte("data")) }))
	defer server.Close()
	s.fileHost = func(raw string) bool { return strings.HasPrefix(raw, server.URL+"/") }
	s.freeBytes = func(string) (int64, error) { return diskReserve + 100, nil }
	first := make(chan core.Attachment, 1)
	go func() { first <- s.download(context.Background(), slackFile{URL: server.URL + "/file", Size: 80}) }()
	<-entered
	if second := s.download(context.Background(), slackFile{URL: server.URL + "/file", Size: 80}); second.Stored != "" || !strings.Contains(second.Note, "disk space") {
		t.Error("concurrent reservation ignored")
	}
	close(release)
	if a := <-first; a.Stored == "" {
		t.Fatalf("reserved download failed %+v", a)
	}
	var leaked atomic.Bool
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer foreign.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, foreign.URL+"/file", 302) }))
	defer redirect.Close()
	s.fileHost = func(raw string) bool { return strings.HasPrefix(raw, redirect.URL+"/") }
	a := s.download(context.Background(), slackFile{URL: redirect.URL + "/file", Size: 1})
	if a.Stored != "" || leaked.Load() {
		t.Fatal("credential redirect followed")
	}
}
func TestIngressKeepsRawEventsUntilRouterAcceptance(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	m := f.event("D1", "im", "Uowner", "hello", "")
	raw, _ := json.Marshal(map[string]any{"event": m})
	if err := st.StageEvent("slack", "test", raw); err != nil {
		t.Fatal(err)
	}
	e, _, _ := st.Event("slack", "test")
	if err := s.process(context.Background(), e, func(context.Context, core.Inbound) error { return errors.New("disk full") }); err == nil {
		t.Fatal("acceptance error ignored")
	}
	if _, pending, err := st.Event("slack", "test"); err != nil || !pending {
		t.Fatal("failed router acceptance removed durable input")
	}
}

func TestMentionStrippingOnlyChangesBoundaryToken(t *testing.T) {
	r := regexp.MustCompile(`<@Ubot(?:\|[^>]+)?>`)
	for in, want := range map[string]string{"<@Ubot> reset": "reset", "reset <@Ubot>": "reset", "explain <@Ubot> token": "explain <@Ubot> token", "<@Ubot> <@Ubot> reset": "<@Ubot> reset"} {
		if got := stripBoundaryMention(in, r); got != want {
			t.Errorf("%q -> %q want %q", in, got, want)
		}
	}
}

// A reconnect can enqueue a duplicate before the first worker commits
// completion. Once that worker finishes, the queued copy must do nothing.
func TestAlreadyQueuedRedeliveryDoesNotProcessCompletedEvent(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	m := f.event("D1", "im", "Uowner", "hello", "")
	raw, _ := json.Marshal(map[string]any{"event": m})
	if err := st.StageEvent("slack", "queued-duplicate", raw); err != nil {
		t.Fatal(err)
	}
	e, _, err := st.Event("slack", "queued-duplicate")
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	accept := func(context.Context, core.Inbound) error { accepted++; return nil }
	if err = s.process(context.Background(), e, accept); err != nil {
		t.Fatal(err)
	}
	if err = s.process(context.Background(), e, accept); err != nil {
		t.Fatal(err)
	}
	if accepted != 1 {
		t.Fatalf("completed event processed %d times", accepted)
	}
}
