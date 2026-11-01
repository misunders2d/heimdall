package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

func stageReview(t *testing.T, s *Slack, m eventMessage) store.InterfaceEvent {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"event": m})
	if err := s.opt.Store.StageEvent("slack", m.TS, raw); err != nil {
		t.Fatal(err)
	}
	e, _, err := s.opt.Store.Event("slack", m.TS)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestRereviewIdentityPermanentAndBoundedRetries(t *testing.T) {
	for _, code := range []string{"user_not_found", "user_not_visible", "account_inactive", "invalid_auth", "not_authed", "token_revoked", "missing_scope", "internal_error"} {
		t.Run(code, func(t *testing.T) {
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			f.hook = func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
				if r.URL.Path == "/users.info" {
					fmt.Fprintf(w, `{"ok":false,"error":%q}`, code)
					return true
				}
				return false
			}
			e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", ""))
			for i := 0; i < 3; i++ {
				start := time.Now()
				if err := s.retryEvent(context.Background(), e, func(context.Context, core.Inbound) error { t.Error("unverified admitted"); return nil }); err != nil {
					t.Fatal(err)
				}
				if time.Since(start) > 500*time.Millisecond {
					t.Fatal("retry held lane")
				}
				if i == 0 && (code == "user_not_found" || code == "user_not_visible" || code == "account_inactive") {
					if p, _ := st.EventPending(e.ID); p {
						t.Fatal("permanent user error retried")
					}
				}
				s.retryMu.Lock()
				r := s.retries[e.ID]
				r.next = time.Time{}
				s.retries[e.ID] = r
				s.retryMu.Unlock()
			}
			if pending, _ := st.EventPending(e.ID); pending {
				t.Fatal("poison remains pending")
			}
		})
	}
}
func TestRereviewInvalidRecordFailsClosed(t *testing.T) {
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	f.hook = func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/users.info" {
			fmt.Fprint(w, `{"ok":true,"user":{"id":"different"}}`)
			return true
		}
		return false
	}
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "hello", ""))
	if err := s.process(context.Background(), e, func(context.Context, core.Inbound) error { t.Error("admitted"); return nil }); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.EventPending(e.ID); p {
		t.Fatal("invalid record retained")
	}
}
func TestRereviewControlsFenceEarlierIngress(t *testing.T) {
	for _, command := range []string{"Stop", "STOP", "Cancel", "abort"} {
		for _, owner := range []bool{false, true} {
			t.Run(fmt.Sprint(command, owner), func(t *testing.T) {
				f := newSlackFake(t)
				s, st := testSlack(t, f)
				s.botID = "Ubot"
				if owner {
					s.opt.Owners = []string{"slack:uowner@example.test"}
				}
				own := stageReview(t, s, f.event("D1", "im", "Uowner", "earlier", "root"))
				other := stageReview(t, s, f.event("D1", "im", "Uemployee", "other", "root"))
				control := stageReview(t, s, f.event("D1", "im", "Uowner", command, "root"))
				later := stageReview(t, s, f.event("D1", "im", "Uowner", "later", "root"))
				if !s.controlEvent(control.Payload) {
					t.Fatal("case-sensitive fast lane")
				}
				if err := s.process(context.Background(), control, func(_ context.Context, in core.Inbound) error {
					want := 1
					if owner {
						want = 2
					}
					if in.ControlIngress != want {
						t.Errorf("count %d want %d", in.ControlIngress, want)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if p, _ := st.EventPending(own.ID); p {
					t.Fatal("earlier own input retained")
				}
				if p, _ := st.EventPending(other.ID); p == owner {
					t.Fatal("owner/caller boundary")
				}
				if p, _ := st.EventPending(later.ID); !p {
					t.Fatal("later input lost")
				}
				s.process(context.Background(), own, func(context.Context, core.Inbound) error { t.Error("fenced input admitted"); return nil })
			})
		}
	}
}
func TestRereviewMCPRedactsOnlyNativeCallbacks(t *testing.T) {
	for _, ordinary := range []string{"https://github.com/pulls?state=closed", "https://shop.test/?code=SAVE10", "http://localhost/?code=alone", "https://localhost/?state=x&code=y", "http://user@localhost/?state=x&code=y"} {
		if got := hideOAuth(ordinary); got != ordinary {
			t.Errorf("ordinary URL changed: %s", got)
		}
	}
	for _, host := range []string{"localhost:1234", "127.0.0.1", "[::1]:42"} {
		callback := "http://" + host + "/callback?state=private-state&code=private-code"
		got := hideOAuth("before " + callback + " after")
		if strings.Contains(got, "private-") || !strings.Contains(got, "before") || !strings.Contains(got, "after") {
			t.Fatal(got)
		}
		f := newSlackFake(t)
		s, _ := testSlack(t, f)
		directEvent(t, s, f.event("D1", "im", "Uowner", "please review "+callback+" thanks", ""), func(_ context.Context, in core.Inbound) error {
			if in.InterfaceReply != "" || !strings.Contains(in.Text, "please review") || strings.Contains(in.Text+in.Context, "private-code") {
				t.Errorf("bad redaction %+v", in)
			}
			return nil
		})
	}
}
func TestRereviewCopyDoesNotBlockDownloadWriter(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	privateDir(s.UploadDir())
	s.freeBytes = func(string) (int64, error) { return diskReserve + 100, nil }
	if err := s.WithUploadCopy(s.UploadDir(), 20, func() error {
		locked := make(chan struct{})
		go func() { s.diskMu.Lock(); s.diskMu.Unlock(); close(locked) }()
		select {
		case <-locked:
		case <-time.After(time.Second):
			t.Error("copy holds disk lock")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s.reserved != 0 {
		t.Fatal("copy reservation leaked")
	}
}
func TestRereviewRecoveredDoneStatusDelivery(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f := newSlackFake(t)
			s, st := readySlack(t, f)
			ctx := context.Background()
			k := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1"}
			turn, err := st.StartTurn(k, core.RoleOwner, "slack:uowner@example.test", "1", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = st.EndTurn(turn, k, "slack:uowner@example.test", store.TurnEnd{State: "done", Reply: &core.Outbound{Conv: k, TurnID: turn, Text: "answer"}})
			if err != nil {
				t.Fatal(err)
			}
			st.SaveProgress(store.Progress{Turn: turn, Conv: k, TS: "status", Phase: "running"})
			if err := s.recoverProgress(ctx); err != nil {
				t.Fatal(err)
			}
			p, _, _ := st.Progress(turn)
			if p.Phase != "done" {
				t.Fatal("stale phase", p.Phase)
			}
			if fail {
				f.hook = func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
					if r.URL.Path == "/chat.postMessage" || r.URL.Path == "/chat.update" {
						fmt.Fprint(w, `{"ok":false,"error":"not_in_channel"}`)
						return true
					}
					return false
				}
			}
			err = s.Send(ctx, core.Outbound{ID: 99, TurnID: turn, Conv: k, Text: "answer"})
			if fail {
				p, _, _ := st.Progress(turn)
				if err == nil || p.Phase != "delivery_failed" {
					t.Fatal("failure lost")
				}
			} else if err != nil || len(f.all("chat.delete")) != 1 {
				t.Fatal("status not deleted", err)
			}
		})
	}
}

func TestRereviewControlDuringHistoryAndReceiptRetry(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	f.hook = func(w http.ResponseWriter, r *http.Request, _ map[string]any) bool {
		if r.URL.Path == "/conversations.replies" {
			close(entered)
			<-release
			fmt.Fprint(w, `{"ok":true,"messages":[]}`)
			return true
		}
		return false
	}
	earlier := stageReview(t, s, f.event("D1", "im", "Uowner", "do the thing", "root"))
	go func() {
		done <- s.process(context.Background(), earlier, func(context.Context, core.Inbound) error { t.Error("earlier turn started after stop"); return nil })
	}()
	<-entered
	control := stageReview(t, s, f.event("D1", "im", "Uowner", "stop", "root"))
	err := s.process(context.Background(), control, func(_ context.Context, in core.Inbound) error {
		if in.ControlIngress != 1 {
			t.Error("dropped count lost")
		}
		return fmt.Errorf("synthetic admission failure")
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	if err := s.process(context.Background(), control, func(_ context.Context, in core.Inbound) error {
		if in.ControlIngress != 1 {
			t.Error("retry receipt lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestRereviewEmbeddedCallbackRedaction(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	m := f.event("D1", "im", "Uowner", "summarize forwarded content", "")
	m.Attachments = []any{map[string]any{"text": "keep me http://localhost/?state=secret-state&code=secret-code and https://shop.test/?code=SAVE10"}}
	directEvent(t, s, m, func(_ context.Context, in core.Inbound) error {
		if in.InterfaceReply != "" || strings.Contains(in.Context, "secret-") || !strings.Contains(in.Context, "keep me") || !strings.Contains(in.Context, "SAVE10") {
			t.Fatalf("embedded content lost/leaked: %s", in.Context)
		}
		return nil
	})
}

func TestRereviewFullControlBufferStillAcks(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	blocked := make(chan struct{})
	defer close(blocked)
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/users.info" && args["user"] == "Uowner" {
			select {
			case <-blocked:
			case <-r.Context().Done():
			}
			fmt.Fprint(w, `{"ok":false,"error":"user_not_visible"}`)
			return true
		}
		return false
	}
	runSlack(t, s, func(context.Context, core.Inbound) error { t.Error("unverified admitted"); return nil })
	<-f.connected
	for i := 0; i < 70; i++ {
		id := f.push(f.event("D1", "im", "Uowner", "stop", "root"))
		select {
		case ack := <-f.acks:
			if ack != id {
				t.Fatal("wrong ack")
			}
		case <-time.After(time.Second):
			t.Fatal("socket ack blocked by control buffer")
		}
	}
}
