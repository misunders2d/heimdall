package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestStrictFakeRejectsJSONReadBodies(t *testing.T) {
	f := newSlackFake(t)
	for _, method := range []string{"users.info", "users.lookupByEmail", "conversations.info", "conversations.history", "conversations.replies", "files.info", "bots.info", "auth.test"} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, f.srv.URL+"/"+method, strings.NewReader(`{"user":"Uowner","email":"uowner@example.test","channel":"D1","file":"F1","bot":"B1"}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer test-bot")
			req.Header.Set("Content-Type", "application/json")
			res, err := f.srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			var result struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
				t.Fatal(err)
			}
			if result.OK || result.Error == "" {
				t.Fatalf("JSON read unexpectedly accepted: %+v", result)
			}
			if method == "users.info" && result.Error != "user_not_found" {
				t.Fatal(result.Error)
			}
		})
	}
}

func TestFormClientAdmitsVerifiedOwner(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.opt.Owners = []string{"slack:uowner@example.test"}
	s.botID = "Ubot"
	var got core.Inbound
	directEvent(t, s, f.event("D1", "im", "Uowner", "hello", ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
	if got.From.Key() != "slack:uowner@example.test" {
		t.Fatalf("owner not admitted: %+v", got.From)
	}
	if role, ok := (core.Access{Owners: map[string]bool{"slack:uowner@example.test": true}}).Role(got.From); !ok || role != core.RoleOwner {
		t.Fatal("verified caller did not resolve to owner")
	}
	if len(f.all("users.info")) != 1 {
		t.Fatal("expected live users.info verification")
	}
}

func TestAPIFormEncoding(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Error("expected form content type")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		for key, want := range map[string]string{"text": "a & b=+é", "unfurl_links": "false", "limit": "100", "blocks": `[{"type":"divider"}]`, "files": `[{"id":"F1"}]`, "metadata": `{"event_type":"test"}`} {
			if got := r.PostForm.Get(key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	args := map[string]any{"text": "a & b=+é", "unfurl_links": false, "limit": 100, "blocks": []map[string]string{{"type": "divider"}}, "files": []map[string]string{{"id": "F1"}}, "metadata": map[string]string{"event_type": "test"}}
	if err := newAPI(srv.URL, "test-bot", nil).call(context.Background(), "chat.postMessage", args, nil); err != nil {
		t.Fatal(err)
	}
}
