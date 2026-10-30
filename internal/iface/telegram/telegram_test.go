package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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
	actions []map[string]any  // sendChatAction bodies
	files   map[string][]byte // downloadable files by file_path; getFile ids are "id:" + path
	newUpd  chan struct{}
	handle  map[string]func(w http.ResponseWriter, body map[string]any) bool // true: handled
}

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{t: t, newUpd: make(chan struct{}, 1), files: map[string][]byte{}, handle: map[string]func(http.ResponseWriter, map[string]any) bool{}}
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
	if p, ok := strings.CutPrefix(r.URL.Path, "/file/bot"+token+"/"); ok {
		f.mu.Lock()
		b, found := f.files[p]
		f.mu.Unlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
		return
	}
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
	case "getFile":
		id, _ := body["file_id"].(string)
		p, valid := strings.CutPrefix(id, "id:")
		f.mu.Lock()
		b, found := f.files[p]
		f.mu.Unlock()
		if !valid {
			fail(w, http.StatusBadRequest, "Bad Request: invalid file_id", 0)
			return
		}
		res := map[string]any{"file_id": id, "file_path": p}
		if found {
			res["file_size"] = len(b)
		}
		ok(w, res)
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
	empty := msg(7, 42, "private", 42, "")
	replyToOther := msg(8, -1002, "group", 42, "reply to a person")
	replyToOther["reply_to_message"] = map[string]any{"from": map[string]any{"id": 5, "is_bot": false}}
	api.push(
		upd(1, msg(1, 42, "private", 0, "no sender")),
		upd(2, bot),
		upd(3, senderChat),
		upd(4, forward),
		upd(5, msg(5, -1004, "channel", 42, "@heimdall_test_bot channel post")),
		upd(6, msg(6, -1002, "group", 42, "ambient SECRET-GROUP-TEXT")),
		upd(7, empty),
		upd(8, replyToOther),
		upd(9, msg(9, -1002, "group", 42, "@heimdall_test_bot_fan is someone else")),
		upd(10, map[string]any{"update_id": 10}),
		upd(11, msg(11, 42, "private", 42, "the only one")),
	)
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "offset past every update", func() bool { return api.lastOffset() == 12 })
	stop()
	// Group messages from people that are not for the bot are context
	// only (Ambient); everything else is dropped.
	var run, ambient []string
	for _, in := range c.list() {
		if in.Ambient {
			ambient = append(ambient, in.MsgID)
		} else {
			run = append(run, in.Text)
		}
	}
	if !slices.Equal(run, []string{"the only one"}) || !slices.Equal(ambient, []string{"6", "8", "9"}) {
		t.Fatalf("accepted %v, context %v", run, ambient)
	}
	if !strings.Contains(logs.String(), "update 7 in chat 42 ignored: no text, caption or attachment") {
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

// Long replies are split as the hub splits them: at most 3500 UTF-16 units
// per part, at a paragraph, line or space boundary, numbered (i/n), and
// only the first part replies to the message.
func TestLongRepliesSplitLikeTheHub(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	// 3000 emoji are 6000 UTF-16 units but only 3000 runes: no boundary, so
	// the cut is at the limit and never inside a surrogate pair.
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
		suffix := fmt.Sprintf("\n\n(%d/2)", n+1)
		if !strings.HasSuffix(s, suffix) {
			t.Fatalf("part %d lacks %q: %q", n, suffix, s[max(0, len(s)-20):])
		}
		s = strings.TrimSuffix(s, suffix)
		if u := len(utf16.Encode([]rune(s))); u > 3500 {
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

	a, b, c := strings.Repeat("a", 2000), strings.Repeat("b", 1000), strings.Repeat("c", 1000)
	for _, tc := range []struct {
		name, text string
		want       []string
	}{
		{"short", "hello", []string{"hello"}},
		{"empty", "", nil},
		{"paragraph first", a + "\n\n" + b + "\n" + c, []string{a, b + "\n" + c}},
		{"line before a later space", a + "\n" + b + " " + c, []string{a, b + " " + c}},
		{"space", a + " " + b + " " + c, []string{a + " " + b, c}},
		{"paragraph too early", strings.Repeat("x", 1000) + "\n\n" + strings.Repeat("y", 3000), []string{strings.Repeat("x", 1000) + "\n\n" + strings.Repeat("y", 2498), strings.Repeat("y", 502)}},
	} {
		if got := splitText(tc.text, 3500); !slices.Equal(got, tc.want) {
			t.Errorf("%s: %d parts %v", tc.name, len(got), lens(got))
		}
	}
}

func lens(parts []string) []int {
	var out []int
	for _, p := range parts {
		out = append(out, len(p))
	}
	return out
}

// A harness's answer goes out as Telegram Markdown; a part Telegram cannot
// parse is sent again as plain text, and Heimdall's own text is never
// Markdown.
func TestFormattedAnswersUseMarkdownWithPlainFallback(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	api.set("sendMessage", func(w http.ResponseWriter, body map[string]any) bool {
		if body["parse_mode"] != nil && strings.Contains(body["text"].(string), "broken") {
			fail(w, http.StatusBadRequest, "Bad Request: can't parse entities: can't find end of the entity", 0)
			return true
		}
		return false
	})
	conv := core.ConvKey{Interface: "telegram", Chat: "42"}
	for _, out := range []core.Outbound{
		{Conv: conv, ReplyTo: "3", Text: "*bold* answer", Formatted: true},
		{Conv: conv, ReplyTo: "4", Text: "broken *markdown", Formatted: true},
		{Conv: conv, Text: "!status a_b", Formatted: false},
	} {
		if err := i.Send(context.Background(), out); err != nil {
			t.Fatal(err)
		}
	}
	sent := api.sentBodies()
	if len(sent) != 3 {
		t.Fatalf("sent %v", sent)
	}
	if sent[0]["parse_mode"] != "Markdown" || sent[0]["text"] != "*bold* answer" {
		t.Fatalf("formatted answer: %v", sent[0])
	}
	rp, _ := sent[1]["reply_parameters"].(map[string]any)
	if sent[1]["parse_mode"] != nil || sent[1]["text"] != "broken *markdown" || rp["message_id"] != float64(4) {
		t.Fatalf("plain retry: %v", sent[1])
	}
	if sent[2]["parse_mode"] != nil {
		t.Fatalf("Heimdall's text as Markdown: %v", sent[2])
	}
	// A plain retry that fails too is classified as usual.
	api.set("sendMessage", func(w http.ResponseWriter, _ map[string]any) bool {
		fail(w, http.StatusBadRequest, "Bad Request: chat not found", 0)
		return true
	})
	var perm router.PermanentError
	if err := i.Send(context.Background(), core.Outbound{Conv: conv, Text: "x", Formatted: true}); !errors.As(err, &perm) {
		t.Fatalf("err %v", err)
	}
	if !strings.Contains(i.PromptNote(), "Telegram Markdown") {
		t.Fatalf("note %q", i.PromptNote())
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

// Telegram /commands, alone or addressed to this bot, are Heimdall's
// commands; anything else stays text for the harness.
func TestSlashCommands(t *testing.T) {
	i := &Interface{botName: botName}
	for in, want := range map[string]string{
		"/start":                       "!help",
		"/start deep-link":             "!help",
		"/start@Heimdall_Test_Bot":     "!help",
		"/help":                        "!help",
		"/status":                      "!status",
		"/stop":                        "!stop",
		"/STOP@heimdall_test_bot":      "!stop",
		"@Heimdall_Test_Bot /stop":     "!stop",
		"/cancel":                      "!cancel",
		"/reset@Heimdall_Test_Bot":     "!reset",
		"/chatid":                      "!chatid",
		"/stop now":                    "/stop now",
		"/stop@other_bot":              "/stop@other_bot",
		"/frobnicate":                  "/frobnicate",
		"stop":                         "stop",
		"@Heimdall_Test_Bot stop":      "stop",
		"please /stop":                 "please /stop",
		"!status @Heimdall_Test_Bot":   "!status",
		"/use codex@Heimdall_Test_Bot": "/use codex",
	} {
		if got := i.command(in); got != want {
			t.Errorf("command(%q) = %q, want %q", in, got, want)
		}
	}
}

// A reply carries the replied message's sender, date and preview, a
// forward its origin, as the hub's telegramContextPayload; the text stays
// the sender's own words.
func TestReplyAndForwardContext(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	c := &collector{}
	photo := msg(5, 42, "private", 77, "")
	photo["from"] = map[string]any{"id": 77, "first_name": "Bob", "last_name": "Ray"}
	photo["caption"] = "look\n  at this"
	photo["photo"] = []any{map[string]any{"file_id": "small"}, map[string]any{"file_id": "big", "file_unique_id": "u1"}}
	reply := msg(6, 42, "private", 42, "what is it?")
	reply["reply_to_message"] = photo
	fwdUser := msg(7, 42, "private", 42, "fyi")
	fwdUser["forward_origin"] = map[string]any{"type": "user", "date": 1700000100, "sender_user": map[string]any{"id": 8, "first_name": "Cy"}}
	fwdChannel := msg(8, 42, "private", 42, "news")
	fwdChannel["forward_origin"] = map[string]any{"type": "channel", "date": 1700000200, "chat": map[string]any{"id": -1009, "type": "channel", "title": "Daily"}}
	fwdHidden := msg(9, 42, "private", 42, "rumour")
	fwdHidden["forward_origin"] = map[string]any{"type": "hidden_user", "date": 0, "sender_user_name": "Anon"}
	signIn := msg(10, 42, "private", 999, "Sign in to MCP server linear in your browser: https://x/?state=s")
	signIn["from"] = map[string]any{"id": botID, "is_bot": true, "first_name": "Heimdall"}
	toBot := msg(11, -1002, "supergroup", 43, "done")
	toBot["reply_to_message"] = signIn
	topic := msg(12, -1002, "supergroup", 43, "@heimdall_test_bot in a topic")
	topic["is_topic_message"], topic["message_thread_id"] = true, 3
	topic["reply_to_message"] = map[string]any{"message_id": 3, "date": 1, "chat": map[string]any{"id": -1002, "type": "supergroup"}, "from": map[string]any{"id": botID, "is_bot": true}}
	api.push(upd(1, reply), upd(2, fwdUser), upd(3, fwdChannel), upd(4, fwdHidden), upd(5, toBot), upd(6, topic))
	stop := run(t, newIface(t, api.srv.URL, botName, &logs), c.accept)
	eventually(t, "all accepted", func() bool { return len(c.list()) == 6 })
	stop()
	got := c.list()
	for i, w := range []struct{ text, context string }{
		{"what is it?", "[Telegram reply context | replied_msg 5 | from Bob Ray (tg_user_77) | date 2023-11-14T22:13:20.000Z]\nlook at this [Photo]"},
		{"fyi", "[Telegram forward context | source Cy (tg_user_8) | date 2023-11-14T22:15:00.000Z]"},
		{"news", "[Telegram forward context | source Daily | date 2023-11-14T22:16:40.000Z]"},
		{"rumour", "[Telegram forward context | source Anon (hidden user) | date unknown]"},
		{"done", "[Telegram reply context | replied_msg 10 | from Heimdall (tg_user_999) | date 2023-11-14T22:13:20.000Z]\n[Private native MCP sign-in UI omitted]"},
		{"in a topic", ""},
	} {
		if got[i].Text != w.text || got[i].Context != w.context {
			t.Errorf("message %d: text %q context %q\nwant %q %q", i, got[i].Text, got[i].Context, w.text, w.context)
		}
	}
}

func TestAttachmentLabels(t *testing.T) {
	for _, tc := range []struct {
		m    map[string]any
		want string
	}{
		{map[string]any{"document": map[string]any{"file_id": "d", "file_name": "a.pdf"}}, "[Document: a.pdf]"},
		{map[string]any{"voice": map[string]any{"file_id": "v"}}, "[Voice Message]"},
		{map[string]any{"audio": map[string]any{"file_id": "a", "title": "Song"}}, "[Audio: Song]"},
		{map[string]any{"video": map[string]any{"file_id": "v"}}, "[Video: unknown]"},
		{map[string]any{"video_note": map[string]any{"file_id": "n"}}, "[Video Message]"},
		{map[string]any{"contact": map[string]any{"first_name": "Di", "phone_number": "+1", "user_id": 5}}, "[Contact: Di, +1, tg_user_id=5]"},
		{map[string]any{"location": map[string]any{"latitude": 1.5, "longitude": -2.25}}, "[Location: lat=1.5, lon=-2.25]"},
		{map[string]any{"venue": map[string]any{"title": "Cafe", "address": "Main St"}}, "[Venue: Cafe — Main St]"},
	} {
		raw, _ := json.Marshal(tc.m)
		var m message
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if a := detect(&m); a == nil || a.Label != tc.want {
			t.Errorf("%v: %+v, want %q", tc.m, a, tc.want)
		}
	}
}

// Files on a message and on the message it replies to are downloaded into
// the private upload directory; a file Telegram will not hand over is
// described instead; contacts and locations are labels.
func TestAttachmentsAreDownloaded(t *testing.T) {
	api := newFakeAPI(t)
	api.files["photos/f1.jpg"] = []byte("jpeg bytes")
	api.files["docs/report.txt"] = []byte("report body")
	var logs syncBuffer
	c := &collector{}
	i := newIface(t, api.srv.URL, botName, &logs)
	i.opt.UploadDir = filepath.Join(t.TempDir(), "uploads")
	photo := msg(1, 42, "private", 42, "")
	photo["caption"] = "what is this?"
	photo["photo"] = []any{map[string]any{"file_id": "id:photos/f1.jpg", "file_unique_id": "p1", "width": 90, "height": 90}}
	doc := msg(2, 42, "private", 77, "")
	doc["document"] = map[string]any{"file_id": "id:docs/report.txt", "file_unique_id": "d1", "file_name": "../report 1.txt", "mime_type": "text/plain"}
	replyToDoc := msg(3, 42, "private", 42, "summarise")
	replyToDoc["reply_to_message"] = doc
	big := msg(4, 42, "private", 42, "")
	big["video"] = map[string]any{"file_id": "id:videos/big.mp4", "file_unique_id": "v1", "file_size": 30 << 20, "duration": 61}
	missing := msg(5, 42, "private", 42, "")
	missing["voice"] = map[string]any{"file_id": "id:voice/gone.ogg", "file_unique_id": "g1"}
	where := msg(6, 42, "private", 42, "")
	where["location"] = map[string]any{"latitude": 1.5, "longitude": 2.5}
	sticker := msg(7, 42, "private", 42, "")
	sticker["sticker"] = map[string]any{"file_id": "s"}
	api.push(upd(1, photo), upd(2, replyToDoc), upd(3, big), upd(4, missing), upd(5, where), upd(6, sticker))
	stop := run(t, i, c.accept)
	eventually(t, "accepted", func() bool { return len(c.list()) == 5 })
	eventually(t, "offset past all", func() bool { return api.lastOffset() == 7 })
	stop()
	got := c.list()
	read := func(a core.Attachment) string { b, _ := os.ReadFile(a.Stored); return string(b) }

	if a := got[0].Attachments; len(a) != 1 || a[0].Kind != "photo" || a[0].Source != "message" || a[0].Name != "photo.jpg" || a[0].Size != 10 || read(a[0]) != "jpeg bytes" || got[0].Text != "what is this?" || got[0].Context != "[Photo]" {
		t.Fatalf("photo: %+v", got[0])
	}
	st, err := os.Stat(got[0].Attachments[0].Stored)
	if dir, _ := os.Stat(i.opt.UploadDir); err != nil || st.Mode().Perm() != 0o600 || dir.Mode().Perm() != 0o700 || filepath.Dir(got[0].Attachments[0].Stored) != i.opt.UploadDir {
		t.Fatalf("stored file %v %v", st, err)
	}
	if a := got[1].Attachments; len(a) != 1 || a[0].Source != "reply_to_message" || a[0].Name != "report_1.txt" || read(a[0]) != "report body" {
		t.Fatalf("replied document: %+v", got[1].Attachments)
	}
	if a := got[2].Attachments; len(a) != 1 || a[0].Stored != "" || !strings.Contains(a[0].Note, "telegram reported file too large: 31457280 bytes") || !strings.Contains(a[0].Note, "duration: 61s") {
		t.Fatalf("big video: %+v", got[2].Attachments)
	}
	if a := got[3].Attachments; len(a) != 1 || a[0].Stored != "" || !strings.Contains(a[0].Note, "file download HTTP 404") {
		t.Fatalf("missing voice: %+v", got[3].Attachments)
	}
	if got[4].Attachments != nil || got[4].Context != "[Location: lat=1.5, lon=2.5]" {
		t.Fatalf("location: %+v", got[4])
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("token in logs")
	}
}

// An album's parts carry its media_group_id as the message group. In a
// group chat only the first part has the caption that mentions the bot;
// the same sender's later parts of that album are addressed too, nobody
// else's.
func TestAlbumParts(t *testing.T) {
	api := newFakeAPI(t)
	c := &collector{}
	var logs syncBuffer
	part := func(id int, from int64, caption, group string) map[string]any {
		m := msg(id, -1002, "group", from, "")
		m["caption"] = caption
		m["media_group_id"] = group
		m["photo"] = []any{map[string]any{"file_id": fmt.Sprintf("id:p/%d.jpg", id), "file_unique_id": fmt.Sprintf("u%d", id)}}
		api.files[fmt.Sprintf("p/%d.jpg", id)] = []byte("jpeg")
		return m
	}
	i := newIface(t, api.srv.URL, botName, &logs)
	i.opt.UploadDir = filepath.Join(t.TempDir(), "uploads")
	api.push(
		upd(1, part(1, 42, "@heimdall_test_bot what are these?", "A1")),
		upd(2, part(2, 42, "", "A1")),
		upd(3, part(3, 43, "", "A1")),
		upd(4, part(4, 42, "", "B2")),
		upd(5, part(5, 42, "", "A1")),
	)
	stop := run(t, i, c.accept)
	eventually(t, "offset past all", func() bool { return api.lastOffset() == 6 })
	stop()
	var ids, ambient []string
	for _, in := range c.list() {
		if in.Ambient {
			ambient = append(ambient, in.MsgID)
			continue
		}
		if in.Group != "A1" || len(in.Attachments) != 1 || in.Attachments[0].Stored == "" {
			t.Errorf("part %+v", in)
		}
		ids = append(ids, in.MsgID)
	}
	if !slices.Equal(ids, []string{"1", "2", "5"}) || !slices.Equal(ambient, []string{"3", "4"}) {
		t.Fatalf("accepted %v, context %v", ids, ambient)
	}
}

// Reactions are set after the text with setMessageReaction; a failure is
// only logged, unless the reaction was the whole answer, which then becomes
// the hub's notice.
func TestReactionsAreSetAfterTheText(t *testing.T) {
	api := newFakeAPI(t)
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	var mu sync.Mutex
	var reactions []map[string]any
	var order []string
	api.set("sendMessage", func(w http.ResponseWriter, b map[string]any) bool {
		mu.Lock()
		order = append(order, "text:"+b["text"].(string))
		mu.Unlock()
		ok(w, map[string]any{"message_id": 9})
		return true
	})
	failOn := float64(0)
	api.set("setMessageReaction", func(w http.ResponseWriter, b map[string]any) bool {
		mu.Lock()
		reactions = append(reactions, b)
		order = append(order, "react")
		mu.Unlock()
		if b["message_id"] == failOn {
			fail(w, 400, "Bad Request: REACTION_INVALID", 0)
			return true
		}
		ok(w, true)
		return true
	})
	conv := core.ConvKey{Interface: "telegram", Chat: "-1002", Thread: "5"}
	if err := i.Send(context.Background(), core.Outbound{Conv: conv, ReplyTo: "3", Text: "answer", Reactions: []core.Reaction{{MsgID: "3", Emoji: "👍"}}}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(order, []string{"text:answer", "react"}) || len(reactions) != 1 {
		t.Fatalf("order %v", order)
	}
	r := reactions[0]
	if r["chat_id"] != float64(-1002) || r["message_id"] != float64(3) || r["message_thread_id"] != nil || fmt.Sprint(r["reaction"]) != "[map[emoji:👍 type:emoji]]" {
		t.Fatalf("reaction body %v", r)
	}
	order, failOn = nil, 4
	if err := i.Send(context.Background(), core.Outbound{Conv: conv, ReplyTo: "3", Text: "answer", Reactions: []core.Reaction{{MsgID: "4", Emoji: "👍"}}}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(order, []string{"text:answer", "react"}) {
		t.Fatalf("a failed reaction after text: %v", order)
	}
	order = nil
	if err := i.Send(context.Background(), core.Outbound{Conv: conv, ReplyTo: "4", Reactions: []core.Reaction{{MsgID: "4", Emoji: "👍"}}}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(order, []string{"react", "text:Could not confirm Telegram reaction delivery."}) {
		t.Fatalf("a failed reaction-only answer: %v", order)
	}
	order = nil
	if err := i.Send(context.Background(), core.Outbound{Conv: conv, Reactions: []core.Reaction{{MsgID: "5", Emoji: "👍"}}}); err != nil || !slices.Equal(order, []string{"react"}) {
		t.Fatalf("reaction-only answer: %v %v", order, err)
	}
	if strings.Contains(logs.String(), "👍") {
		t.Fatalf("logs carry the reaction: %q", logs.String())
	}
}

func TestReactionEmoji(t *testing.T) {
	i := &Interface{}
	for in, want := range map[string]string{"👍": "👍", "❤️": "❤", "❤": "❤", "❤️‍🔥": "❤‍🔥", "🤷‍♂️": "🤷‍♂"} {
		if got, ok := i.Reaction(in); !ok || got != want {
			t.Errorf("%q: %q %v", in, got, ok)
		}
	}
	for _, in := range []string{"🦀", "ok", "", "👍👍"} {
		if _, ok := i.Reaction(in); ok {
			t.Errorf("%q accepted", in)
		}
	}
	if len(reactionEmoji) != 73 {
		t.Fatalf("%d emoji, the hub has 73", len(reactionEmoji))
	}
	if n := i.ReactionNote("REF"); !strings.Contains(n, "Optional final line: [react: REF <emoji>].") {
		t.Fatalf("note %q", n)
	}
}

// A group message not for the bot is context: its text and its (or its
// replied-to message's) file label, never downloaded; a topic is its own
// conversation.
func TestAmbientMessages(t *testing.T) {
	api := newFakeAPI(t)
	c := &collector{}
	var logs syncBuffer
	i := newIface(t, api.srv.URL, botName, &logs)
	i.opt.UploadDir = filepath.Join(t.TempDir(), "uploads")
	photo := msg(1, -1002, "supergroup", 43, "")
	photo["caption"] = "holiday"
	photo["photo"] = []any{map[string]any{"file_id": "id:p/1.jpg", "file_unique_id": "u1"}}
	api.files["p/1.jpg"] = []byte("jpeg")
	topic := msg(2, -1002, "supergroup", 43, "in a topic")
	topic["is_topic_message"], topic["message_thread_id"] = true, 7
	empty := msg(3, -1002, "supergroup", 43, "")
	api.push(upd(1, photo), upd(2, topic), upd(3, empty))
	stop := run(t, i, c.accept)
	eventually(t, "offset past all", func() bool { return api.lastOffset() == 4 })
	stop()
	got := c.list()
	if len(got) != 2 || !got[0].Ambient || got[0].Text != "holiday" || len(got[0].Attachments) != 1 || got[0].Attachments[0].Stored != "" || got[0].Attachments[0].Kind != "photo" ||
		!got[1].Ambient || got[1].Conv.Thread != "7" || got[1].Text != "in a topic" {
		t.Fatalf("accepted %+v", got)
	}
	if api.called("getFile") {
		t.Fatal("a context message's file was downloaded")
	}
}
