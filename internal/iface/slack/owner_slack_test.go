package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestOwnerSlackLiveDMVerification(t *testing.T) {
	for _, kind := range []string{"im", "mpim", "wrong-user", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			f := newSlackFake(t)
			s, _ := testSlack(t, f)
			s.botID = "Ubot"
			s.opt.OwnerSlackEnabled = true
			s.opt.Owners = []string{"slack:uowner@example.test"}
			f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
				if r.URL.Path != "/conversations.info" {
					return false
				}
				if kind == "unavailable" {
					fmt.Fprint(w, `{"ok":false,"error":"channel_not_found"}`)
				} else {
					user := "Uowner"
					if kind == "wrong-user" {
						user = "Uother"
					}
					fmt.Fprintf(w, `{"ok":true,"channel":{"is_im":true,"is_mpim":%t,"user":%q}}`, kind == "mpim", user)
				}
				return true
			})
			var got core.Inbound
			directEvent(t, s, f.event("D1", "im", "Uowner", "hello", ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
			if !got.Private || got.SlackReadVerified != (kind == "im") || got.OAuthActor != "sl_user_Uowner" || got.OAuthInstallation != "sl_user_Ubot" {
				t.Fatal(got)
			}
		})
	}
}
func TestOwnerSlackCallbackNeverStaged(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	s.botID = "Ubot"
	s.opt.OwnerSlackEnabled = true
	s.opt.Owners = []string{"slack:uowner@example.test"}
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path != "/conversations.info" {
			return false
		}
		fmt.Fprint(w, `{"ok":true,"channel":{"is_im":true,"user":"Uowner"}}`)
		return true
	})
	m := f.event("D1", "im", "Uowner", "<http://localhost:18765/callback?state=synthetic&code=SYNTHETIC_CALLBACK>", "")
	raw, _ := json.Marshal(map[string]any{"type": "event_callback", "event_id": "Ecallback", "event": m})
	called := false
	handled, err := s.mcpCallback(context.Background(), raw, func(_ context.Context, in core.Inbound) error {
		called = true
		if !in.Private || !core.MCPCallback(in.Text) {
			t.Fatal("callback not verified")
		}
		return nil
	})
	if err != nil || !handled || !called {
		t.Fatal(handled, called, err)
	}
	if _, ok, err := st.Event("slack", "Ecallback"); err != nil || ok {
		t.Fatal("callback persisted", err)
	}
	if strings.Contains(string(redactMCPEvent(raw)), "SYNTHETIC_CALLBACK") {
		t.Fatal("callback retained in raw event")
	}
}
