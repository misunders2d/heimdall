package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/misunders2d/heimdall/internal/core"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMCPCallbackFailuresKeepRouterRunning(t *testing.T) {
	for _, status := range []int{429, 500, 0} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Setenv("MEMENTO_PROCESSING", "1")
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			f.setHook(func(w http.ResponseWriter, r *http.Request, body map[string]any) bool {
				if body["user"] == "Ubot" || r.URL.Path != "/users.info" {
					return false
				}
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-r.Context().Done():
					return true
				}
				if status == 0 {
					c, _, _ := w.(http.Hijacker).Hijack()
					_ = c.Close()
				} else {
					w.WriteHeader(status)
					fmt.Fprint(w, `{"ok":false}`)
				}
				return true
			})
			// Router owns Slack here: any fatal ingress error would stop every interface.
			r := routerFor(t, s, st, "")
			cancel, done := startRouter(t, r)
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			id := f.push(f.event("D1", "im", "Uowner", "http://localhost:18765/callback?code=SYNTHETIC_PRIVATE&state=x", ""))
			select {
			case <-f.acks:
			case <-time.After(3 * time.Second):
				t.Fatal("callback not acknowledged before network")
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("post-ack callback not relayed")
			}
			if _, ok, err := st.Event("slack", id); err != nil || ok {
				t.Fatal("callback staged", err)
			}
			close(release)
			select {
			case err := <-done:
				t.Fatalf("router stopped: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}
func TestMCPCallbackFiltersBeforeIdentity(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	for _, change := range []func(*eventMessage){func(m *eventMessage) { m.Type = "other" }, func(m *eventMessage) { m.BotID = "B" }, func(m *eventMessage) { m.Subtype = "bot_message" }, func(m *eventMessage) { m.ChannelType = "channel" }} {
		m := f.event("D1", "im", "Uowner", "http://localhost:18765/callback?code=x&state=x", "")
		change(&m)
		raw, _ := json.Marshal(map[string]any{"event": m})
		_, err := s.mcpCallback(context.Background(), raw, func(context.Context, core.Inbound) error { t.Error("accepted"); return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(f.all("users.info")) != 0 {
		t.Fatal("identity called before filter")
	}
}
func TestMCPRawRedactionWiring(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	runSlack(t, s, func(context.Context, core.Inbound) error { return nil })
	// Embedded callback cannot take ephemeral control path, must be redacted before staging.
	m := f.event("C1", "channel", "Uowner", "ordinary", "")
	m.Attachments = []any{map[string]any{"text": "http://localhost:18765/callback?code=SYNTHETIC_PRIVATE&state=x"}}
	id := f.push(m)
	select {
	case <-f.acks:
	case <-time.After(3 * time.Second):
		t.Fatal("no ack")
	}
	row, ok, err := st.Event("slack", id)
	if err != nil || !ok {
		t.Fatal("missing staged event", err)
	}
	if strings.Contains(string(row.Payload), "SYNTHETIC_PRIVATE") {
		t.Fatal("raw callback persisted")
	}
}
func TestMCPDisabledPreservesDMPrivacy(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.opt.Owners = []string{"slack:uowner@example.test"}
	f.setHook(func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.info" {
			t.Error("disabled gate queried DM")
			w.WriteHeader(500)
			return true
		}
		return false
	})
	var got core.Inbound
	directEvent(t, s, f.event("D1", "im", "Uowner", "stop", ""), func(_ context.Context, in core.Inbound) error { got = in; return nil })
	if !got.Private || got.SlackReadVerified {
		t.Fatal("private surface lost", got)
	}
}

func TestMCPCommandArgumentRedaction(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"event": map[string]any{"text": "mcp login http://localhost:18765/callback?code=SYNTHETIC_PRIVATE&state=x"}})
	if strings.Contains(string(redactMCPEvent(raw)), "SYNTHETIC_PRIVATE") {
		t.Fatal("callback argument persisted as native command")
	}
}
