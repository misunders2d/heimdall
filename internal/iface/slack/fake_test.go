package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestMain(m *testing.M) { fakeharness.Main(); os.Exit(m.Run()) }

type apiCall struct {
	Method string
	Body   map[string]any
}
type slackFake struct {
	t         *testing.T
	srv       *httptest.Server
	mu        sync.Mutex
	seq       int
	messages  []map[string]any
	calls     []apiCall
	events    chan []byte
	acks      chan string
	connected chan struct{}
	once      sync.Once
	hook      func(http.ResponseWriter, *http.Request, map[string]any) bool
	users     map[string]User
	file      []byte
}

func newSlackFake(t *testing.T) *slackFake {
	f := &slackFake{t: t, events: make(chan []byte, 64), acks: make(chan string, 64), connected: make(chan struct{}), users: map[string]User{}}
	for _, id := range []string{"Ubot", "Uowner", "Uemployee", "Uguest", "Unoemail"} {
		u := User{ID: id, TeamID: "Ttest", Name: id}
		u.Profile.Email = strings.ToLower(id) + "@example.test"
		u.Profile.DisplayName = id
		if id == "Uguest" {
			u.TeamID = "Tother"
		}
		if id == "Unoemail" {
			u.Profile.Email = ""
		}
		if id == "Ubot" {
			u.IsBot = true
		}
		f.users[id] = u
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}
func (f *slackFake) serve(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/")
	if r.Method != http.MethodPost && method != "socket" && method != "file" && method != "upload" {
		http.NotFound(w, r)
		return
	}
	if !strings.Contains(method, ".") && method != "socket" && method != "file" && method != "upload" {
		http.NotFound(w, r)
		return
	}
	if method == "socket" {
		f.socket(w, r)
		return
	}
	if method == "file" {
		if r.Header.Get("Authorization") != "Bearer test-bot" {
			f.t.Error("file token missing")
		}
		f.mu.Lock()
		body := append([]byte(nil), f.file...)
		f.mu.Unlock()
		_, _ = w.Write(body)
		return
	}
	if method == "upload" {
		if r.Header.Get("Authorization") != "" {
			f.t.Error("bearer sent to upload endpoint")
		}
		_, _ = io.Copy(io.Discard, r.Body)
		return
	}
	var args map[string]any
	_ = json.NewDecoder(r.Body).Decode(&args)
	if args == nil {
		args = map[string]any{}
	}
	expected := "Bearer test-bot"
	if method == "apps.connections.open" {
		expected = "Bearer test-app"
	}
	if r.Header.Get("Authorization") != expected {
		f.t.Error("wrong API token")
	}
	f.mu.Lock()
	f.calls = append(f.calls, apiCall{method, args})
	hook := f.hook
	f.mu.Unlock()
	if hook != nil && hook(w, r, args) {
		return
	}
	response := map[string]any{"ok": true}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch method {
	case "auth.test":
		response["team_id"] = "Ttest"
		response["user_id"] = "Ubot"
		response["user"] = "Heimdall"
	case "users.info":
		response["user"] = f.users[str(args["user"])]
	case "users.lookupByEmail":
		for _, u := range f.users {
			if u.Profile.Email == args["email"] {
				response["user"] = u
			}
		}
	case "conversations.open":
		response["channel"] = map[string]string{"id": "Downer"}
	case "apps.connections.open":
		response["url"] = strings.Replace(f.srv.URL, "http:", "ws:", 1) + "/socket"
	case "conversations.history", "conversations.replies":
		var found []map[string]any
		for _, m := range f.messages {
			if m["channel"] != args["channel"] {
				continue
			}
			if method == "conversations.replies" && m["thread_ts"] != args["ts"] && m["ts"] != args["ts"] {
				continue
			}
			if method == "conversations.history" && m["thread_ts"] != "" && m["thread_ts"] != nil {
				continue
			}
			found = append(found, m)
		}
		response["messages"] = found
	case "chat.postMessage":
		f.seq++
		ts := fmt.Sprintf("1000.%06d", f.seq)
		message := map[string]any{"channel": args["channel"], "thread_ts": args["thread_ts"], "ts": ts, "user": "Ubot", "bot_id": "Bbot", "text": args["text"], "metadata": args["metadata"]}
		f.messages = append(f.messages, message)
		response["ts"] = ts
	case "chat.update":
		for _, m := range f.messages {
			if m["ts"] == args["ts"] {
				m["text"] = args["text"]
				if args["metadata"] != nil {
					m["metadata"] = args["metadata"]
				}
			}
		}
	case "chat.delete":
		for i, m := range f.messages {
			if m["ts"] == args["ts"] {
				f.messages = append(f.messages[:i], f.messages[i+1:]...)
				break
			}
		}
	case "files.getUploadURLExternal":
		response["upload_url"] = f.srv.URL + "/upload"
		response["file_id"] = "Fout"
	}
	_ = json.NewEncoder(w).Encode(response)
}
func str(v any) string { s, _ := v.(string); return s }
func (f *slackFake) socket(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`))
	f.once.Do(func() { close(f.connected) })
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			var ack struct {
				ID string `json:"envelope_id"`
			}
			if json.Unmarshal(b, &ack) == nil {
				select {
				case f.acks <- ack.ID:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			<-readDone
			return
		case raw := <-f.events:
			if c.Write(ctx, websocket.MessageText, raw) != nil {
				cancel()
				<-readDone
				return
			}
		}
	}
}
func (f *slackFake) event(channel, surface, user, text, thread string) eventMessage {
	f.mu.Lock()
	f.seq++
	ts := fmt.Sprintf("1000.%06d", f.seq)
	f.mu.Unlock()
	return eventMessage{Type: "message", Channel: channel, ChannelType: surface, User: user, Text: text, ThreadTS: thread, TS: ts}
}
func (f *slackFake) push(m eventMessage) string {
	id := "E" + m.TS
	raw, _ := json.Marshal(map[string]any{"type": "events_api", "envelope_id": id, "payload": map[string]any{"type": "event_callback", "event_id": id, "event": m}})
	body, _ := json.Marshal(m)
	var object map[string]any
	_ = json.Unmarshal(body, &object)
	object["channel"] = m.Channel
	object["ts"] = m.TS
	object["thread_ts"] = m.ThreadTS
	object["user"] = m.User
	object["text"] = m.Text
	f.mu.Lock()
	f.messages = append(f.messages, object)
	f.mu.Unlock()
	f.events <- raw
	return id
}
func (f *slackFake) all(method string) []apiCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var calls []apiCall
	for _, c := range f.calls {
		if c.Method == method {
			calls = append(calls, c)
		}
	}
	return calls
}
func (f *slackFake) hasText(part string) bool {
	for _, c := range f.all("chat.postMessage") {
		if strings.Contains(str(c.Body["text"]), part) {
			return true
		}
	}
	return false
}
func waitFor(t *testing.T, label string, fn func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out: " + label)
}
func testSlack(t *testing.T, f *slackFake) (*Slack, *store.Store) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	s, err := New(Options{BotToken: "test-bot", AppToken: "test-app", TeamID: "Ttest", DataDir: dir, Store: st, APIBase: f.srv.URL, Log: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	s.socket.allowTestSocket = true
	s.fileHost = func(raw string) bool { return strings.HasPrefix(raw, f.srv.URL+"/") }
	s.freeBytes = func(string) (int64, error) { return 10 << 30, nil }
	s.statusEvery = 20 * time.Millisecond
	s.partEvery = 0
	s.bot.pause = func(ctx context.Context, d time.Duration) error { return pause(ctx, min(d, 5*time.Millisecond)) }
	s.app.pause = s.bot.pause
	return s, st
}
func runSlack(t *testing.T, s *Slack, accept func(context.Context, core.Inbound) error) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, accept) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			t.Error("Slack shutdown did not finish")
		}
	})
	return cancel
}

func (f *slackFake) setHook(h func(http.ResponseWriter, *http.Request, map[string]any) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = h
}
