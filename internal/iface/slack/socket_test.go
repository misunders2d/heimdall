package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSocketAcknowledgesOnlyAfterPersistence(t *testing.T) {
	persisted := make(chan struct{})
	release := make(chan struct{})
	acked := make(chan string, 1)
	serverDone := make(chan struct{})
	var base string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apps.connections.open" {
			if r.Header.Get("Authorization") != "Bearer test-app" {
				t.Error("wrong app identity")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": strings.Replace(base, "http:", "ws:", 1) + "/socket"})
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		defer close(serverDone)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`))
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"events_api","envelope_id":"E1","payload":{"event_id":"event-1"}}`))
		_, raw, err := c.Read(ctx)
		if err == nil {
			acked <- string(raw)
			_, _, _ = c.Read(ctx)
		}
	}))
	defer server.Close()
	base = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	s := socketTransport{app: newAPI(base, "test-app", nil), allowTestSocket: true}
	go func() {
		done <- s.run(ctx, func(ctx context.Context, raw json.RawMessage) error {
			close(persisted)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		})
	}()
	select {
	case <-persisted:
	case <-time.After(3 * time.Second):
		t.Fatal("no envelope")
	}
	select {
	case <-acked:
		t.Fatal("ack before persistence")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case raw := <-acked:
		var ack map[string]string
		if json.Unmarshal([]byte(raw), &ack) != nil || ack["envelope_id"] != "E1" {
			t.Fatal("bad ack")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no durable ack")
	}
	cancel()
	<-done
	<-serverDone
}
func TestSocketRejectsUntrustedURLWithoutExposingTicket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"ok":true,"url":"wss://attacker.invalid/?ticket=private-value"}`)
	}))
	defer server.Close()
	s := socketTransport{app: newAPI(server.URL, "test", nil)}
	_, err := s.connect(context.Background())
	if err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("error %v", err)
	}
}
func TestSocketFailureDoesNotAcknowledge(t *testing.T) {
	var base string
	result := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apps.connections.open" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": strings.Replace(base, "http:", "ws:", 1) + "/socket"})
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"events_api","envelope_id":"E1","payload":{"event_id":"event-1"}}`))
		_, _, err = c.Read(ctx)
		result <- err == nil
	}))
	defer server.Close()
	base = server.URL
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	s := socketTransport{app: newAPI(base, "test", nil), allowTestSocket: true}
	err := s.run(ctx, func(context.Context, json.RawMessage) error { return errors.New("disk full") })
	if err == nil {
		t.Fatal("no persistence error")
	}
	if <-result {
		t.Fatal("failed persistence acknowledged")
	}
}

func TestSocketRefreshOverlapsAndLinkDisabledStops(t *testing.T) {
	for _, reason := range []string{"warning", "refresh_requested", "dropped"} {
		t.Run(reason, func(t *testing.T) {
			var base string
			var connections atomic.Int32
			var firstClosed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/apps.connections.open" {
					if reason != "dropped" && connections.Load() == 1 && firstClosed.Load() {
						t.Error("old socket closed before replacement opened")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": strings.Replace(base, "http:", "ws:", 1) + "/socket"})
					return
				}
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				n := connections.Add(1)
				if n == 1 {
					if reason == "dropped" {
						firstClosed.Store(true)
						return
					}
					raw, _ := json.Marshal(map[string]string{"type": "disconnect", "reason": reason})
					_ = c.Write(ctx, websocket.MessageText, raw)
					_, _, _ = c.Read(ctx)
					firstClosed.Store(true)
					return
				}
				_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"disconnect","reason":"link_disabled"}`))
				_, _, _ = c.Read(ctx)
			}))
			defer server.Close()
			base = server.URL
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := socketTransport{app: newAPI(base, "test", nil), allowTestSocket: true}
			s.app.pause = func(context.Context, time.Duration) error { return nil }
			err := s.run(ctx, func(context.Context, json.RawMessage) error { t.Error("unexpected event"); return nil })
			if !errors.Is(err, errLinkDisabled) || connections.Load() != 2 {
				t.Fatalf("result %v connections %d", err, connections.Load())
			}
		})
	}
}
