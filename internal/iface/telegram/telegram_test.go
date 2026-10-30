package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

const (
	token   = "123456:TEST-token-never-shown"
	botID   = 999
	botName = "Heimdall_Test_Bot"
)

// fakeAPI is a Bot API server: getMe, getUpdates (long poll over queued
// updates) and sendMessage. Handlers can be replaced per method.
type fakeAPI struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	updates []map[string]any
	offsets []int64 // offset of every getUpdates call
	calls   []string
	sent    []map[string]any
	actions []map[string]any // sendChatAction bodies
	newUpd  chan struct{}
	handle  map[string]func(w http.ResponseWriter, body map[string]any) bool // true: handled
}

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{t: t, newUpd: make(chan struct{}, 1), handle: map[string]func(http.ResponseWriter, map[string]any) bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func ok(w http.ResponseWriter, result any) {
	reply(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func fail(w http.ResponseWriter, status int, desc string, retryAfter int) {
	body := map[string]any{"ok": false, "error_code": status, "description": desc}
	if retryAfter > 0 {
		body["parameters"] = map[string]any{"retry_after": retryAfter}
	}
	reply(w, status, body)
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	method, found := strings.CutPrefix(r.URL.Path, "/bot"+token+"/")
	if !found {
		fail(w, http.StatusNotFound, "Not Found", 0)
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.calls = append(f.calls, method)
	h := f.handle[method]
	f.mu.Unlock()
	if h != nil && h(w, body) {
		return
	}
	switch method {
	case "getMe":
		ok(w, map[string]any{"id": botID, "is_bot": true, "first_name": "Heimdall", "username": botName})
	case "getUpdates":
		offset := int64(body["offset"].(float64))
		f.mu.Lock()
		f.offsets = append(f.offsets, offset)
		f.mu.Unlock()
		deadline := time.After(time.Duration(body["timeout"].(float64)) * time.Second)
		for {
			f.mu.Lock()
			var out []map[string]any
			for _, u := range f.updates {
				if int64(u["update_id"].(int)) >= offset {
					out = append(out, u)
				}
			}
			f.mu.Unlock()
			if len(out) > 0 {
				ok(w, out)
				return
			}
			select {
			case <-f.newUpd:
			case <-deadline:
				ok(w, []any{})
				return
			case <-r.Context().Done():
				return
			}
		}
	case "sendMessage":
		f.mu.Lock()
		f.sent = append(f.sent, body)
		f.mu.Unlock()
		ok(w, map[string]any{"message_id": 1})
	case "sendChatAction":
		f.mu.Lock()
		f.actions = append(f.actions, body)
		f.mu.Unlock()
		ok(w, true)
	default:
		fail(w, http.StatusBadRequest, "unexpected method "+method, 0)
	}
}

func (f *fakeAPI) push(updates ...map[string]any) {
	f.mu.Lock()
	f.updates = append(f.updates, updates...)
	f.mu.Unlock()
	select {
	case f.newUpd <- struct{}{}:
	default:
	}
}

func (f *fakeAPI) set(method string, h func(http.ResponseWriter, map[string]any) bool) {
	f.mu.Lock()
	f.handle[method] = h
	f.mu.Unlock()
}

func (f *fakeAPI) lastOffset() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.offsets) == 0 {
		return -1
	}
	return f.offsets[len(f.offsets)-1]
}

func (f *fakeAPI) called(method string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == method {
			return true
		}
	}
	return false
}

// syncBuffer is a log destination safe for concurrent use.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func newIface(t *testing.T, api, username string, logs *syncBuffer) *Interface {
	t.Helper()
	i, err := New(Options{Token: token, BotUsername: username, API: api, Log: log.New(logs, "", 0), PollTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	i.backoffMin, i.backoffMax, i.maxRetryWait = 20*time.Millisecond, 50*time.Millisecond, 2*time.Second
	return i
}

// run starts Run and returns a stop function that cancels it and returns
// its error.
func run(t *testing.T, i *Interface, accept func(context.Context, core.Inbound) error) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, accept) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Error("Run did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { stop() })
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// collector records accepted messages.
type collector struct {
	mu   sync.Mutex
	got  []core.Inbound
	fail func(core.Inbound) error
}

func (c *collector) accept(_ context.Context, in core.Inbound) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		if err := c.fail(in); err != nil {
			return err
		}
	}
	c.got = append(c.got, in)
	return nil
}

func (c *collector) list() []core.Inbound {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]core.Inbound(nil), c.got...)
}

func msg(id int, chatID int64, chatType string, from int64, text string) map[string]any {
	m := map[string]any{
		"message_id": id,
		"date":       1700000000,
		"chat":       map[string]any{"id": chatID, "type": chatType},
		"text":       text,
	}
	if from != 0 {
		m["from"] = map[string]any{"id": from, "is_bot": false, "first_name": "Ann", "last_name": "Lee"}
	}
	return m
}

func upd(id int, m map[string]any) map[string]any {
	return map[string]any{"update_id": id, "message": m}
}

func TestRefusesATokenOfAnotherBot(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	err := newIface(t, api.srv.URL, "production_bot", &logs).Run(context.Background(), (&collector{}).accept)
	if err == nil || !strings.Contains(err.Error(), "@"+botName) || !strings.Contains(err.Error(), "@production_bot") || !strings.Contains(err.Error(), "refusing to poll") {
		t.Fatalf("err = %v", err)
	}
	if api.called("getUpdates") || api.called("deleteWebhook") || api.called("setWebhook") {
		t.Fatalf("calls after a failed token check: %v", api.calls)
	}
}

func TestUsernameCheckIgnoresCaseAndAt(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	stop := run(t, newIface(t, api.srv.URL, "@heimdall_test_bot", &logs), (&collector{}).accept)
	eventually(t, "polling", func() bool { return api.called("getUpdates") })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "polling as @"+botName) {
		t.Fatalf("logs: %q", logs.String())
	}
}

func TestMessagesBecomeVerifiedInbound(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	c := &collector{}
	topic := msg(9, -1002, "supergroup", 42, "@heimdall_test_bot in a topic")
	topic["is_topic_message"], topic["message_thread_id"] = true, 77
	notTopic := msg(10, -1002, "supergroup", 42, "plain reply to the bot")
	notTopic["message_thread_id"] = 5 // a reply thread, not a forum topic
	notTopic["reply_to_message"] = map[string]any{"from": map[string]any{"id": botID, "is_bot": true}}
	caption := msg(11, 42, "private", 42, "")
	caption["caption"] = "look at this"
	api.push(
		upd(1, msg(1, 42, "private", 42, "hello there")),
		upd(2, topic),
		upd(3, notTopic),
		upd(4, caption),
		upd(5, msg(12, 42, "private", 42, "/start")),
		upd(6, msg(13, -1002, "group", 43, "/start@Heimdall_Test_Bot")),
		upd(7, msg(14, -1002, "group", 43, "@heimdall_test_bot !status")),
		upd(8, msg(15, -1002, "group", 43, "!use codex @Heimdall_Test_Bot")),
	)
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "all accepted", func() bool { return len(c.list()) == 8 })
	stop()
	got := c.list()
	want := []struct {
		chat, thread, msgID, text string
		private                   bool
	}{
		{"42", "", "1", "hello there", true},
		{"-1002", "77", "9", "in a topic", false},
		{"-1002", "", "10", "plain reply to the bot", false},
		{"42", "", "11", "look at this", true},
		{"42", "", "12", "!help", true},
		{"-1002", "", "13", "!help", false},
		{"-1002", "", "14", "!status", false},
		{"-1002", "", "15", "!use codex", false},
	}
	for i, w := range want {
		in := got[i]
		if in.Conv != (core.ConvKey{Interface: "telegram", Chat: w.chat, Thread: w.thread}) || in.MsgID != w.msgID || in.Text != w.text || in.Private != w.private {
			t.Errorf("message %d: %+v, want %+v", i, in, w)
		}
		if in.From.Platform != "telegram" || in.Origin != core.OriginLive || in.At.Unix() != 1700000000 {
			t.Errorf("message %d: from %+v origin %q at %v", i, in.From, in.Origin, in.At)
		}
	}
	if got[0].From.ID != "42" || got[0].From.Name != "Ann Lee" || got[5].From.ID != "43" {
		t.Errorf("principals: %+v, %+v", got[0].From, got[5].From)
	}
	if api.lastOffset() != 9 {
		t.Errorf("offset after all updates = %d, want 9", api.lastOffset())
	}
}

func TestIgnoredUpdatesAreAcknowledgedButNeverAccepted(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	c := &collector{}
	bot := msg(2, 42, "private", 42, "from a bot")
	bot["from"] = map[string]any{"id": 7, "is_bot": true, "first_name": "B"}
	senderChat := msg(3, -1002, "supergroup", 42, "@heimdall_test_bot as a channel")
	senderChat["sender_chat"] = map[string]any{"id": -1003, "type": "channel"}
	forward := msg(4, -1002, "supergroup", 42, "@heimdall_test_bot forwarded")
	forward["is_automatic_forward"] = true
	sticker := msg(7, 42, "private", 42, "")
	replyToOther := msg(8, -1002, "group", 42, "reply to a person")
	replyToOther["reply_to_message"] = map[string]any{"from": map[string]any{"id": 5, "is_bot": false}}
	api.push(
		upd(1, msg(1, 42, "private", 0, "no sender")),
		upd(2, bot),
		upd(3, senderChat),
		upd(4, forward),
		upd(5, msg(5, -1004, "channel", 42, "@heimdall_test_bot channel post")),
		upd(6, msg(6, -1002, "group", 42, "ambient SECRET-GROUP-TEXT")),
		upd(7, sticker),
		upd(8, replyToOther),
		upd(9, msg(9, -1002, "group", 42, "@heimdall_test_bot_fan is someone else")),
		upd(10, map[string]any{"update_id": 10}),
		upd(11, msg(11, 42, "private", 42, "the only one")),
	)
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "offset past every update", func() bool { return api.lastOffset() == 12 })
	stop()
	if got := c.list(); len(got) != 1 || got[0].Text != "the only one" {
		t.Fatalf("accepted %+v", got)
	}
	if !strings.Contains(logs.String(), "update 7 in chat 42 ignored: no text or caption") {
		t.Errorf("logs: %q", logs.String())
	}
	if strings.Contains(logs.String(), "SECRET-GROUP-TEXT") || strings.Contains(logs.String(), "the only one") {
		t.Errorf("logs carry message text: %q", logs.String())
	}
}

func TestOffsetWaitsForAccept(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	failures := 2
	c := &collector{fail: func(in core.Inbound) error {
		if in.MsgID == "2" && failures > 0 {
			failures--
			return errors.New("database is locked")
		}
		return nil
	}}
	api.push(upd(1, msg(1, 42, "private", 42, "one")), upd(2, msg(2, 42, "private", 42, "two")), upd(3, msg(3, 42, "private", 42, "three")))
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "all accepted", func() bool { return len(c.list()) == 3 })
	eventually(t, "final offset", func() bool { return api.lastOffset() == 4 })
	stop()
	api.mu.Lock()
	offsets := append([]int64(nil), api.offsets...)
	api.mu.Unlock()
	// Update 2 was never acknowledged until it was accepted: the polls after
	// its failures start at 2, and update 3 is accepted only after it.
	if len(offsets) < 4 || offsets[0] != 0 || offsets[1] != 2 || offsets[2] != 2 {
		t.Fatalf("offsets %v", offsets)
	}
	var texts []string
	for _, in := range c.list() {
		texts = append(texts, in.Text)
	}
	if strings.Join(texts, ",") != "one,two,three" {
		t.Fatalf("accepted %v", texts)
	}
	if !strings.Contains(logs.String(), "update 2 in chat 42 not accepted") {
		t.Fatalf("logs: %q", logs.String())
	}
}

func TestConflictIsLoggedAndRetriedWithoutTouchingWebhooks(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	conflicts := 2
	api.set("getUpdates", func(w http.ResponseWriter, _ map[string]any) bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		if conflicts == 0 {
			return false
		}
		conflicts--
		fail(w, http.StatusConflict, "Conflict: terminated by other getUpdates request", 0)
		return true
	})
	c := &collector{}
	api.push(upd(1, msg(1, 42, "private", 42, "after the conflict")))
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "accepted after conflicts", func() bool { return len(c.list()) == 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "another poller or a webhook") {
		t.Fatalf("logs: %q", logs.String())
	}
	if api.called("deleteWebhook") || api.called("setWebhook") {
		t.Fatal("webhook methods called")
	}
}

func TestRejectedTokenStopsPolling(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	api.set("getUpdates", func(w http.ResponseWriter, _ map[string]any) bool {
		fail(w, http.StatusUnauthorized, "Unauthorized", 0)
		return true
	})
	err := newIface(t, api.srv.URL, botName, &logs).Run(context.Background(), (&collector{}).accept)
	if err == nil || !strings.Contains(err.Error(), "rejected") || strings.Contains(err.Error(), token) {
		t.Fatalf("err = %v", err)
	}
}

func TestStopEndsTheLongPollPromptly(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	i.opt.PollTimeout = 50 * time.Second
	stop := run(t, i, (&collector{}).accept)
	eventually(t, "polling", func() bool { return api.called("getUpdates") })
	start := time.Now()
	if err := stop(); err != nil || time.Since(start) > 2*time.Second {
		t.Fatalf("stop: %v after %s", err, time.Since(start))
	}
}

func (f *fakeAPI) sentBodies() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.sent...)
}

func TestSendIsPlainTextInTheRightPlace(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	if err := i.Send(context.Background(), core.Outbound{Conv: core.ConvKey{Interface: "telegram", Chat: "-1002", Thread: "77"}, ReplyTo: "9", Text: "*not markdown* <b>"}); err != nil {
		t.Fatal(err)
	}
	sent := api.sentBodies()
	if len(sent) != 1 {
		t.Fatalf("sent %v", sent)
	}
	b := sent[0]
	rp, _ := b["reply_parameters"].(map[string]any)
	if b["chat_id"] != float64(-1002) || b["text"] != "*not markdown* <b>" || b["message_thread_id"] != float64(77) || b["parse_mode"] != nil ||
		rp["message_id"] != float64(9) || rp["allow_sending_without_reply"] != true {
		t.Fatalf("body %v", b)
	}
	if err := i.Send(context.Background(), core.Outbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, Text: "no thread"}); err != nil {
		t.Fatal(err)
	}
	b = api.sentBodies()[1]
	if _, has := b["message_thread_id"]; has {
		t.Fatalf("body %v", b)
	}
	if _, has := b["reply_parameters"]; has {
		t.Fatalf("body %v", b)
	}
}

func TestLongRepliesSplitAtUTF16Limit(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	// 3000 emoji are 6000 UTF-16 units but only 3000 runes.
	text := strings.Repeat("😀", 3000) + "\n— omp"
	if err := i.Send(context.Background(), core.Outbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, ReplyTo: "5", Text: text}); err != nil {
		t.Fatal(err)
	}
	sent := api.sentBodies()
	if len(sent) != 2 {
		t.Fatalf("%d parts", len(sent))
	}
	var joined string
	for n, b := range sent {
		s := b["text"].(string)
		if u := len(utf16.Encode([]rune(s))); u > maxText {
			t.Fatalf("part %d has %d UTF-16 units", n, u)
		}
		joined += s
		_, reply := b["reply_parameters"]
		if reply != (n == 0) {
			t.Fatalf("part %d reply parameters: %v", n, b)
		}
	}
	if joined != text {
		t.Fatal("parts do not add up to the reply")
	}
	// A line break in the second half of a part is preferred.
	parts := split(strings.Repeat("a", 3000)+"\n"+strings.Repeat("b", 3000), maxText)
	if len(parts) != 2 || parts[0] != strings.Repeat("a", 3000)+"\n" {
		t.Fatalf("split at %d", len(parts[0]))
	}
}

func TestSendErrorsAreClassified(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		retry     int
		permanent bool
		minWait   time.Duration
	}{
		{"bad request", http.StatusBadRequest, 0, true, 0},
		{"blocked by the user", http.StatusForbidden, 0, true, 0},
		{"rate limited", http.StatusTooManyRequests, 1, false, time.Second},
		{"server error", http.StatusBadGateway, 0, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI(t)
			var logs syncBuffer
			api.set("sendMessage", func(w http.ResponseWriter, _ map[string]any) bool {
				fail(w, tc.status, "nope", tc.retry)
				return true
			})
			start := time.Now()
			err := newIface(t, api.srv.URL, botName, &logs).Send(context.Background(), core.Outbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, Text: "hi"})
			var perm router.PermanentError
			if err == nil || errors.As(err, &perm) != tc.permanent || time.Since(start) < tc.minWait {
				t.Fatalf("err = %v (permanent %v) after %s", err, errors.As(err, &perm), time.Since(start))
			}
		})
	}
}

func TestTokenNeverInErrorsOrLogs(t *testing.T) {
	api := newFakeAPI(t)
	url := api.srv.URL
	api.srv.Close() // every request now fails with a *url.Error carrying the URL
	var logs syncBuffer
	i := newIface(t, url, botName, &logs)
	err := i.Run(context.Background(), (&collector{}).accept)
	if err == nil || !strings.Contains(err.Error(), "token check failed") {
		t.Fatalf("err = %v", err)
	}
	sendErr := i.Send(context.Background(), core.Outbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, Text: "hi"})
	var perm router.PermanentError
	if sendErr == nil || errors.As(sendErr, &perm) {
		t.Fatalf("network failure must be transient: %v", sendErr)
	}
	// Polling errors are logged.
	api2 := newFakeAPI(t)
	broken := false
	api2.set("getUpdates", func(w http.ResponseWriter, _ map[string]any) bool {
		api2.mu.Lock()
		defer api2.mu.Unlock()
		if !broken {
			broken = true
			// Drop the connection: the client sees a transport error.
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			conn.Close()
			return true
		}
		return false
	})
	stop := run(t, newIface(t, api2.srv.URL, botName, &logs), (&collector{}).accept)
	eventually(t, "poll error logged", func() bool { return strings.Contains(logs.String(), "getUpdates failed") })
	stop()
	for _, s := range []string{err.Error(), sendErr.Error(), logs.String()} {
		if strings.Contains(s, token) || strings.Contains(s, "TEST-token") {
			t.Fatalf("token leaked: %q", s)
		}
	}
	// The request URL was in the transport error, with the token scrubbed.
	if !strings.Contains(err.Error(), "/bot<token>/getMe") || !strings.Contains(sendErr.Error(), "/bot<token>/sendMessage") {
		t.Fatalf("errors: %v / %v", err, sendErr)
	}
}

func TestNewNeedsTokenAndUsername(t *testing.T) {
	if _, err := New(Options{BotUsername: "x_bot"}); err == nil || !strings.Contains(err.Error(), TokenEnv) {
		t.Fatalf("err = %v", err)
	}
	if _, err := New(Options{Token: token}); err == nil {
		t.Fatal("no username accepted")
	}
}

// Typing is sendChatAction "typing" in the conversation's chat and topic;
// a failure is ignored.
func TestTypingIsAChatAction(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	i.Typing(context.Background(), core.ConvKey{Interface: "telegram", Chat: "-1005", Thread: "7"})
	i.Typing(context.Background(), core.ConvKey{Interface: "telegram", Chat: "42"})
	api.mu.Lock()
	got := append([]map[string]any(nil), api.actions...)
	api.mu.Unlock()
	if len(got) != 2 || got[0]["action"] != "typing" || got[0]["chat_id"] != float64(-1005) || got[0]["message_thread_id"] != float64(7) ||
		got[1]["chat_id"] != float64(42) || got[1]["message_thread_id"] != nil {
		t.Fatalf("chat actions: %v", got)
	}
	api.set("sendChatAction", func(w http.ResponseWriter, _ map[string]any) bool {
		fail(w, 400, "Bad Request: chat not found", 0)
		return true
	})
	i.Typing(context.Background(), core.ConvKey{Interface: "telegram", Chat: "1"}) // no panic, no error
}
