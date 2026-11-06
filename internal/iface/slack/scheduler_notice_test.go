package slack

import (
	"context"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

type noticeTransport func(*http.Request) (*http.Response, error)

func (f noticeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSchedulerNoticeFakePayload(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := scheduler.Caller{Principal: core.SlackPrincipal("owner@example.test", "T1", ""), Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "D1", Thread: "1000.1"}, Private: true}
	jobs := scheduler.New(scheduler.Config{DefaultTZ: "UTC", Interfaces: []string{"slack"}}, st, core.Access{}, log.New(io.Discard, "", 0))
	_, draft, card := jobs.Draft(c, []string{"```heimdall-job\n" + `{"name":"Literal *star* _u_ ` + "`code`" + `","summary":"[click](https://example.test) <https://example.test|link> <!channel> &","schedule":{"every":"1h"},"prompt":"PRIVATE PROMPT","paused":true}` + "\n```"})
	if draft == nil {
		t.Fatal(card)
	}
	var payload map[string]any
	transport := noticeTransport(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/chat.postMessage") {
			t.Fatalf("unexpected call %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		payload = map[string]any{}
		for k, v := range r.PostForm {
			payload[k] = v[0]
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"ts":"1000.2"}`))}, nil
	})
	s, err := New(Options{BotToken: "synthetic", AppToken: "synthetic", TeamID: "T1", Store: st, DataDir: t.TempDir(), APIBase: "https://fake.test", HTTPClient: &http.Client{Transport: transport}, Log: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	close(s.ready)
	if err = s.Send(context.Background(), core.Outbound{ID: 1, Conv: c.Conv, Text: card, Formatted: scheduler.Formatted(card)}); err != nil {
		t.Fatal(err)
	}
	text, _ := payload["text"].(string)
	for _, want := range []string{"*Set up scheduled task?*", "Literal ∗star∗ ＿u＿ ˋcodeˋ", "[click](https://example.test)", "&lt;!channel&gt;", "&amp;", "*Yes*"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q: %s", want, text)
		}
	}
	for _, bad := range []string{"<!channel>", "<https://", "PRIVATE PROMPT", draft.ID, "\\*", "\\[", "**Set up"} {
		if strings.Contains(text, bad) {
			t.Fatal("unsafe formatting", text)
		}
	}
	if payload["thread_ts"] != "1000.1" {
		t.Fatal("thread lost", payload)
	}
}
