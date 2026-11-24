package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestOwnerAuthorizedBotDMStaysLive(t *testing.T) {
	f := newSlackFake(t)
	f.allowWorkUser = true
	u := User{ID: "UOWNER", TeamID: "Ttest", Name: "Owner"}
	u.Profile.Email = "owner@example.test"
	f.users[u.ID] = u
	f.setHook(func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.Header.Get("Authorization") == "Bearer test-work-user" {
			switch r.URL.Path {
			case "/auth.test":
				fmt.Fprint(w, `{"ok":true,"team_id":"Ttest","user_id":"UOWNER"}`)
				return true
			case "/users.conversations":
				if args["types"] != "im" {
					t.Errorf("expected direct conversations, got %v", args)
				}
				if args["cursor"] == "next" {
					fmt.Fprint(w, `{"ok":true,"channels":[{"id":"DOWNERBOT","user":"Ubot","is_im":true}]}`)
				} else {
					fmt.Fprint(w, `{"ok":true,"channels":[{"id":"DOTHER","user":"Uemployee","is_im":true}],"response_metadata":{"next_cursor":"next"}}`)
				}
				return true
			}
		}
		if r.URL.Path == "/conversations.info" && args["channel"] == "DOWNERBOT" {
			fmt.Fprint(w, `{"ok":true,"channel":{"is_im":true,"user":"UOWNER"}}`)
			return true
		}
		return false
	})
	s, st := testSlack(t, f)
	s.opt.WorkOwnerID, s.opt.WorkUserToken = "UOWNER", "test-work-user"
	s.opt.Owners = []string{"slack:owner@example.test"}
	got := make(chan core.Inbound, 3)
	runSlack(t, s, func(_ context.Context, in core.Inbound) error { got <- in; return nil })
	waitFor(t, "Slack ready", s.started)
	for _, mode := range []string{core.SlackWatchOff, core.SlackWatchAnswer} {
		if err := st.SetSlackWatch("DOWNERBOT", core.SlackWatch{Mode: mode}); err != nil {
			t.Fatal(err)
		}
		m := f.event("DOWNERBOT", "im", "UOWNER", "Find this in my memory", "")
		waitAck(t, f, f.pushAuthorized(m, "UOWNER"))
		select {
		case in := <-got:
			if in.MsgID != m.TS || in.From.Key() != "slack:owner@example.test" || in.Origin != core.OriginLive ||
				in.Ambient || in.ConversationWork != nil || !in.SlackPrivateOwnerVerified {
				t.Fatalf("%s: wrong route for owner-authorized bot DM: %+v", mode, in)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s: owner-authorized bot DM was dropped", mode)
		}
	}
	for _, m := range []eventMessage{
		f.event("DOTHER", "im", "UOWNER", "A private message to someone else", ""),
		f.event("DOWNERBOT", "im", "UOWNER", workAttribution("UOWNER")+"\n\nAn attributed reply", ""),
	} {
		id := f.pushAuthorized(m, "UOWNER")
		waitAck(t, f, id)
		if _, found, err := st.Event("slack", id); err != nil || found {
			t.Fatalf("unrelated or assistant-authored event was staged: found=%t, err=%v", found, err)
		}
	}
}

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

func TestMCPSignInPrivateOwnerIndependentOfSlackRead(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, kind := range []string{"im", "employee", "mpim", "channel", "wrong-user", "not-im", "unavailable"} {
			t.Run(fmt.Sprintf("read=%t/%s", enabled, kind), func(t *testing.T) {
				f := newSlackFake(t)
				s, st := testSlack(t, f)
				s.botID = "Ubot"
				s.opt.OwnerSlackEnabled = enabled
				// Environment admins own; file admins remain employees, even if config lists them.
				access := t.TempDir()
				for _, name := range []string{"whitelist.json", "blacklist.json", "identities.json", "capabilities.json"} {
					if err := os.WriteFile(filepath.Join(access, name), []byte(`{}`), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(access, "admins.json"), []byte(`{"users":["sl_user_Uemployee"]}`), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("ADMIN_USER_IDS", "uowner@example.test")
				s.opt.Access = &core.Access{Dir: access}
				s.opt.Owners = []string{"slack:uemployee@example.test"}
				f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
					if r.URL.Path != "/conversations.info" {
						return false
					}
					if kind == "unavailable" {
						fmt.Fprint(w, `{"ok":false,"error":"channel_not_found"}`)
						return true
					}
					user := "Uowner"
					if kind == "wrong-user" {
						user = "Uother"
					}
					fmt.Fprintf(w, `{"ok":true,"channel":{"is_im":%t,"is_mpim":%t,"user":%q}}`, kind != "not-im", kind == "mpim", user)
					return true
				})
				surface, sender := "im", "Uowner"
				if kind == "mpim" || kind == "channel" {
					surface = kind
				}
				if kind == "employee" {
					sender = "Uemployee"
				}
				text := "mcp login t3"
				if surface != "im" {
					text = "<@Ubot> " + text
				}
				var got core.Inbound
				directEvent(t, s, f.event("D1", surface, sender, text, ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
				verified := kind == "im"
				if got.SlackPrivateOwnerVerified != verified || got.SlackReadVerified != (verified && enabled) {
					t.Fatal("verification gates", got)
				}
				if verified {
					if got.Text != "mcp login t3" || got.InterfaceReply != "" {
						t.Fatal("owner command refused", got)
					}
				} else if got.InterfaceReply != mcpNotice || got.Text != omittedMCP {
					t.Fatal("unsafe command accepted", got)
				}
				m := f.event("D1", surface, sender, "http://localhost:18765/callback?state=x&code=SYNTHETIC_CALLBACK", "")
				raw, _ := json.Marshal(map[string]any{"event_id": "Ecallback", "event": m})
				called := false
				handled, err := s.mcpCallback(context.Background(), raw, func(_ context.Context, in core.Inbound) error {
					called = true
					if in.SlackPrivateOwnerVerified != verified || in.SlackReadVerified != (verified && enabled) || !core.MCPCallback(in.Text) {
						t.Fatal("callback gates", in)
					}
					return nil
				})
				if err != nil || !handled || called != (surface == "im") {
					t.Fatal("callback relay", handled, called, err)
				}
				if _, exists, err := st.Event("slack", "Ecallback"); exists || err != nil {
					t.Fatal("callback staged", err)
				}
				if strings.Contains(string(redactMCPEvent(raw)), "SYNTHETIC_CALLBACK") {
					t.Fatal("callback leaked")
				}
			})
		}
	}
}

func TestEmployeeBusinessMCPPrivateDMIngress(t *testing.T) {
	for _, validDM := range []bool{false, true} {
		f := newSlackFake(t)
		s, _ := testSlack(t, f)
		s.botID = "Ubot"
		s.opt.Owners = []string{"slack:uowner@example.test"}
		f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
			if r.URL.Path != "/conversations.info" {
				return false
			}
			fmt.Fprintf(w, `{"ok":true,"channel":{"is_im":%t,"is_mpim":false,"user":"Uemployee"}}`, validDM)
			return true
		})
		var got core.Inbound
		directEvent(t, s, f.event("D1", "im", "Uemployee", "mcp login mellanni", ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
		if validDM {
			if got.Text != "mcp login mellanni" || !got.Private || got.SlackPrivateOwnerVerified || got.OAuthActor != "sl_user_Uemployee" {
				t.Fatal(got)
			}
		} else if got.InterfaceReply != mcpNotice || got.Text != omittedMCP {
			t.Fatal("unverified DM command forwarded", got)
		}
	}
}
