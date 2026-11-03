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

func TestGoogleEmployeeLoginRequiresVerifiedDMPeer(t *testing.T) {
	for _, verified := range []bool{true, false} {
		f := newSlackFake(t)
		s, _ := testSlack(t, f)
		s.opt.Owners = []string{"slack:someone-else@example.test"}
		f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
			if r.URL.Path == "/conversations.info" {
				peer := "Uemployee"
				if !verified {
					peer = "Uother"
				}
				fmt.Fprintf(w, `{"ok":true,"channel":{"is_im":true,"is_mpim":false,"user":%q}}`, peer)
				return true
			}
			return false
		})
		var got core.Inbound
		directEvent(t, s, f.event("D1", "im", "Uemployee", "google login", ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
		if got.Text != "google login" || got.Private != verified {
			t.Fatal("Google login trusted channel_type alone")
		}
	}
}
func TestGoogleCallbackEphemeralIngressAndHistoryRedaction(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "B1"
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.info" {
			fmt.Fprint(w, `{"ok":true,"channel":{"is_im":true,"is_mpim":false,"user":"Uemployee"}}`)
			return true
		}
		return false
	})
	callback := "https://example.test/oauth/google/callback?state=hmg_fixture&code=SYNTHETIC_CALLBACK"
	raw, _ := json.Marshal(map[string]any{"event": f.event("D1", "im", "Uemployee", "<"+callback+">", "")})
	if callbackValue(raw) != callback {
		t.Fatal("Google callback would be staged as raw event")
	}
	accepted := false
	handled, err := s.mcpCallback(context.Background(), raw, func(_ context.Context, in core.Inbound) error {
		accepted = true
		if !in.Private || in.OAuthActor != "sl_user_Uemployee" || in.OAuthInstallation != "sl_user_B1" {
			t.Fatal("callback identity or privacy missing")
		}
		return nil
	})
	if err != nil || !handled || !accepted {
		t.Fatal("Google callback not relayed", err)
	}
	embedded, _ := json.Marshal(map[string]any{"event": map[string]any{"text": "before " + callback + " after", "attachments": []any{map[string]any{"text": callback}}}})
	if strings.Contains(string(redactMCPEvent(embedded)), "SYNTHETIC_CALLBACK") {
		t.Fatal("Google callback leaked in raw event")
	}
	if strings.Contains(hideOAuth(callback), "SYNTHETIC_CALLBACK") {
		t.Fatal("Google callback leaked in history")
	}
}
