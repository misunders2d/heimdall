package telegram

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	osuser "os/user"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

// Caption shapes, core.telegram.org/bots/api read 2026-11-10 (Bot API
// 10.4): sendAnimation takes the file as "animation"; its caption is
// "0-1024 characters after entities parsing"; caption_entities "can be
// specified instead of parse_mode"; MessageEntity type "spoiler"; entity
// offsets are UTF-16 code units.

func write64(name, body string) string {
	return "WRITE64=" + name + ":" + base64.StdEncoding.EncodeToString([]byte(body))
}

// captionRig is a router with a cold Claude fake and a Telegram fake API
// that records every upload.
type captionRig struct {
	api     *fakeAPI
	st      *store.Store
	outbox  string
	logs    *syncBuffer
	mu      sync.Mutex
	uploads []map[string]any
	reject  atomic.Bool // uploads answer 500 while set
	tries   atomic.Int32
	stop    func()
	start   func()
}

func newCaptionRig(t *testing.T) *captionRig {
	t.Helper()
	bin, _ := fakeharness.Install(t, "claude")
	u, err := osuser.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	g := &captionRig{api: newFakeAPI(t), st: st, outbox: filepath.Join(t.TempDir(), "outbox"), logs: &syncBuffer{}}
	for _, m := range []string{"sendPhoto", "sendDocument", "sendAnimation"} {
		g.api.set(m, func(w http.ResponseWriter, b map[string]any) bool {
			g.tries.Add(1)
			if g.reject.Load() {
				fail(w, http.StatusInternalServerError, "Internal Server Error", 0)
				return true
			}
			g.mu.Lock()
			b["method"] = m
			g.uploads = append(g.uploads, b)
			n := len(g.uploads)
			g.mu.Unlock()
			ok(w, map[string]any{"message_id": 900 + n})
			return true
		})
	}
	access := core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"telegram:42": true}}
	var accounts router.Accounts = plainAccounts{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}}
	g.start = func() {
		tg := newIface(t, g.api.srv.URL, botName, g.logs)
		tg.opt.Access = &access
		r := router.New(router.Options{Access: access, Order: []string{"claude"}, MaxParallel: 2, Accounts: accounts, OutboxDir: g.outbox, Log: log.New(g.logs, "", 0)}, st, tg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		g.stop = func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}
	g.start()
	t.Cleanup(func() { g.stop() })
	return g
}

func (g *captionRig) got() []map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]map[string]any(nil), g.uploads...)
}

func entitiesOf(t *testing.T, b map[string]any) []map[string]any {
	t.Helper()
	raw, _ := b["caption_entities"].(string)
	if raw == "" {
		return nil
	}
	var e []map[string]any
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("caption_entities %q: %v", raw, err)
	}
	return e
}

// A GIF and its .caption sidecar go out as one animation message whose
// caption carries the reply markup as entities, the hidden part as a
// spoiler; the sidecar is never a file of its own, never counts toward the
// five-file limit, and a caption without its file is ignored and logged.
func TestTelegramOutboxCaptionIsOneMessage(t *testing.T) {
	g := newCaptionRig(t)
	caption := "*Draft* post — ||the hidden part|| end"
	ask := "WRITE=post.gif:GIF89a-bytes " + write64("post.gif.caption", caption) +
		" WRITE=b.txt:b WRITE=c.txt:c WRITE=d.txt:d WRITE=e.txt:e " + write64("ghost.png.caption", "nobody")
	g.api.push(upd(1, msg(31, 42, "private", 42, ask)))
	eventually(t, "answer and five files", func() bool { return len(g.got()) == 5 && len(g.api.sentBodies()) >= 1 })
	time.Sleep(200 * time.Millisecond)
	ups := g.got()
	if len(ups) != 5 {
		t.Fatalf("uploads: %d", len(ups))
	}
	var gif map[string]any
	for _, b := range ups {
		for k, v := range b {
			if strings.Contains(fmt.Sprint(v), ".caption") || k == "document" && strings.Contains(fmt.Sprint(v), "nobody") {
				t.Fatalf("sidecar sent as a file: %s=%v", k, v)
			}
		}
		if b["method"] == "sendAnimation" {
			gif = b
		}
	}
	if gif == nil || fmt.Sprint(gif["animation"]) != "map[data:GIF89a-bytes name:post.gif type:image/gif]" {
		t.Fatalf("the GIF is not an animation: %v", ups)
	}
	if gif["caption"] != "Draft post — the hidden part end" || gif["parse_mode"] != nil {
		t.Fatalf("caption: %q parse_mode=%v", gif["caption"], gif["parse_mode"])
	}
	want := `[map[length:5 offset:0 type:bold] map[length:15 offset:13 type:spoiler]]`
	if got := fmt.Sprint(entitiesOf(t, gif)); got != want {
		t.Fatalf("caption entities: %s", got)
	}
	answer := fmt.Sprint(g.api.sentBodies()[0]["text"])
	if strings.Contains(answer, "file not sent") || strings.Contains(answer, "caption") && strings.Contains(answer, "more than") {
		t.Fatalf("answer reports a skipped file: %q", answer)
	}
	if !strings.Contains(g.logs.String(), "1 outbox caption(s) without a matching file ignored") || strings.Contains(g.logs.String(), "nobody") {
		t.Fatalf("orphan caption not logged as metadata:\n%s", g.logs.String())
	}
	for _, b := range g.api.sentBodies() {
		if strings.Contains(fmt.Sprint(b["text"]), "nobody") {
			t.Fatalf("orphan caption sent: %v", b)
		}
	}
	if entries, _ := os.ReadDir(g.outbox); len(entries) != 0 {
		t.Fatalf("sent copies kept: %v", entries)
	}
}

// A caption over Telegram's 1024 characters after formatting is never cut:
// the file goes bare and the whole caption follows as a normal reply.
func TestTelegramOutboxCaptionOverLimit(t *testing.T) {
	g := newCaptionRig(t)
	long := "*Long* " + strings.Repeat("word ", 210) + "||end||" // 1058 characters once formatted
	g.api.push(upd(1, msg(31, 42, "private", 42, "WRITE=clip.gif:GIF89a "+write64("clip.gif.caption", long))))
	eventually(t, "file and caption message", func() bool { return len(g.got()) == 1 && len(g.api.sentBodies()) >= 2 })
	time.Sleep(200 * time.Millisecond)
	ups := g.got()
	if len(ups) != 1 || ups[0]["method"] != "sendAnimation" || ups[0]["caption"] != nil || ups[0]["caption_entities"] != nil {
		t.Fatalf("file not sent bare: %v", ups)
	}
	sent := g.api.sentBodies()
	if len(sent) != 2 {
		t.Fatalf("messages: %d", len(sent))
	}
	text, _ := sent[1]["text"].(string)
	if !strings.HasPrefix(text, "Long word word") || !strings.HasSuffix(text, "word end") || len(text) != len("Long ")+len(strings.Repeat("word ", 210))+len("end") {
		t.Fatalf("caption message: %q", text)
	}
	if got := fmt.Sprint(sent[1]["entities"]); !strings.Contains(got, "spoiler") || !strings.Contains(got, "bold") {
		t.Fatalf("caption message entities: %s", got)
	}
}

// A file and caption enqueued but not yet sent when Heimdall stops are sent
// after the restart, once, still as one captioned message.
func TestTelegramOutboxCaptionSurvivesRestart(t *testing.T) {
	g := newCaptionRig(t)
	g.reject.Store(true)
	g.api.push(upd(1, msg(31, 42, "private", 42, "WRITE=post.gif:GIF89a "+write64("post.gif.caption", "Hi ||there||"))))
	eventually(t, "answer and a failed upload", func() bool { return len(g.api.sentBodies()) >= 1 && g.tries.Load() > 0 })
	g.stop()
	if len(g.got()) != 0 {
		t.Fatal("upload succeeded before restart")
	}
	g.reject.Store(false)
	g.start()
	eventually(t, "upload after restart", func() bool { return len(g.got()) == 1 })
	time.Sleep(1500 * time.Millisecond)
	ups := g.got()
	if len(ups) != 1 || ups[0]["method"] != "sendAnimation" || ups[0]["caption"] != "Hi there" {
		t.Fatalf("after restart: %v", ups)
	}
	if got := fmt.Sprint(entitiesOf(t, ups[0])); got != "[map[length:5 offset:3 type:spoiler]]" {
		t.Fatalf("entities: %s", got)
	}
	if n := len(g.api.sentBodies()); n != 1 {
		t.Fatalf("messages after restart: %d", n)
	}
}

// Replies keep Telegram's legacy Markdown, except that a reply with a
// spoiler pair is sent with entities so the spoiler shows.
func TestTelegramReplySpoiler(t *testing.T) {
	api := newFakeAPI(t)
	tg := newIface(t, api.srv.URL, botName, &syncBuffer{})
	conv := core.ConvKey{Interface: core.PlatformTelegram, Chat: "42"}
	if err := tg.Send(context.Background(), core.Outbound{Conv: conv, Text: "*plain* reply", Formatted: true}); err != nil {
		t.Fatal(err)
	}
	if err := tg.Send(context.Background(), core.Outbound{Conv: conv, Text: "_see_ ||secret|| and `a || b`", Formatted: true}); err != nil {
		t.Fatal(err)
	}
	sent := api.sentBodies()
	if len(sent) != 2 || sent[0]["parse_mode"] != "Markdown" || sent[0]["text"] != "*plain* reply" {
		t.Fatalf("plain reply: %v", sent)
	}
	if sent[1]["parse_mode"] != nil || sent[1]["text"] != "see secret and a || b" {
		t.Fatalf("spoiler reply: %v", sent[1])
	}
	if got := fmt.Sprint(sent[1]["entities"]); got != "[map[length:3 offset:0 type:italic] map[length:6 offset:4 type:spoiler] map[length:6 offset:15 type:code]]" {
		t.Fatalf("spoiler reply entities: %s", got)
	}
}
