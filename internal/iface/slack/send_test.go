package slack

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

func readySlack(t *testing.T, f *slackFake) (*Slack, *store.Store) {
	s, st := testSlack(t, f)
	if err := s.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(s.ready)
	return s, st
}
func TestStartupScopeWorkspaceTokensAndLock(t *testing.T) {
	for _, kind := range []string{"workspace", "scope", "invalid_auth"} {
		t.Run(kind, func(t *testing.T) {
			f := newSlackFake(t)
			s, _ := testSlack(t, f)
			f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
				if r.URL.Path != "/auth.test" {
					return false
				}
				switch kind {
				case "workspace":
					fmt.Fprint(w, `{"ok":true,"team_id":"Tother","user_id":"Ubot"}`)
				case "scope":
					w.Header().Set("x-oauth-scopes", "users:read")
					fmt.Fprint(w, `{"ok":true,"team_id":"Ttest","user_id":"Ubot"}`)
				default:
					fmt.Fprint(w, `{"ok":false,"error":"invalid_auth"}`)
				}
				return true
			}
			err := s.Run(context.Background(), func(context.Context, core.Inbound) error { t.Error("input during failed startup"); return nil })
			if err == nil || strings.Contains(err.Error(), "test-bot") || len(f.all("apps.connections.open")) != 0 {
				t.Fatalf("startup %v", err)
			}
		})
	}
	if _, err := New(Options{AppToken: "test-app"}); err == nil {
		t.Fatal("missing bot token accepted")
	}
	dir := t.TempDir()
	lock, err := lockDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := lockDirectory(dir); err == nil {
		other.Close()
		t.Fatal("duplicate lock accepted")
	}
	lock.Close()
	lock, err = lockDirectory(dir)
	if err != nil {
		t.Fatal("released lock unavailable")
	}
	lock.Close()
	bad := filepath.Join(t.TempDir(), "link")
	if err = os.Symlink(dir, bad); err != nil {
		t.Fatal(err)
	}
	if lock, err = lockDirectory(bad); err == nil {
		lock.Close()
		t.Fatal("symlink directory accepted")
	}
}
func TestFinalPostReconcilesLandedResponseAndRetry(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	var intercepted atomic.Bool
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path != "/chat.postMessage" || intercepted.Swap(true) {
			return false
		}
		f.mu.Lock()
		f.messages = append(f.messages, map[string]any{"channel": args["channel"], "thread_ts": args["thread_ts"], "ts": "1000.000002", "metadata": args["metadata"], "text": args["text"]})
		f.mu.Unlock()
		w.WriteHeader(503)
		return true
	}
	f.mu.Unlock()
	out := core.Outbound{ID: 42, Conv: core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}, Text: "<!channel> **answer**", Formatted: true}
	if err := s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	out.Attempt = 1
	if err := s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	posts := f.all("chat.postMessage")
	if len(posts) != 1 || strings.Contains(str(posts[0].Body["text"]), "<!channel>") {
		t.Fatalf("replayed/pinging final %+v", posts)
	}
	for _, call := range f.all("conversations.replies") {
		if call.Body["oldest"] != "1000.000001" {
			t.Fatal("reconciliation timestamp units")
		}
	}
}
func TestChunkedRetrySkipsEarlierParts(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	var count atomic.Int32
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/chat.postMessage" && count.Add(1) == 2 {
			w.WriteHeader(503)
			return true
		}
		return false
	}
	f.mu.Unlock()
	out := core.Outbound{ID: 43, Conv: core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}, Text: "```go\n" + strings.Repeat("example line\n", 900) + "```", Formatted: true}
	if err := s.Send(context.Background(), out); err == nil {
		t.Fatal("lost chunk not transient")
	}
	out.Attempt = 1
	if err := s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	firstID := 0
	for _, c := range f.all("chat.postMessage") {
		text := str(c.Body["text"])
		if len([]rune(text)) > 3500 || strings.Count(text, "```")%2 != 0 {
			t.Fatal("unbounded or broken fenced chunk")
		}
		meta, _ := c.Body["metadata"].(map[string]any)
		payload, _ := meta["event_payload"].(map[string]any)
		if payload["delivery_id"] == "43#1" {
			firstID++
		}
	}
	if firstID != 1 {
		t.Fatal("earlier chunk reposted")
	}
}
func TestProgressThrottleDisableTerminalAndReactionCleanup(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	s.statusEvery = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	s.ObserveTurn(ctx, router.TurnEvent{ID: 9, Conv: conv, Phase: "start"})
	waitFor(t, "status post", func() bool { return len(f.all("chat.postMessage")) == 1 })
	for i := 0; i < 20; i++ {
		s.ObserveTurn(ctx, router.TurnEvent{ID: 9, Conv: conv, Phase: "progress", Progress: harness.Progress{Phase: "thinking", Text: "Checking public sources", Reset: true}})
	}
	time.Sleep(25 * time.Millisecond)
	if len(f.all("chat.update")) != 0 {
		t.Fatal("status not throttled")
	}
	waitFor(t, "thinking summary", func() bool {
		for _, c := range f.all("chat.update") {
			if strings.Contains(str(c.Body["text"]), "Checking public sources") {
				return true
			}
		}
		return false
	})
	n := len(f.all("chat.update"))
	time.Sleep(100 * time.Millisecond)
	if len(f.all("chat.update")) != n {
		t.Fatal("unchanged status updated")
	}
	s.ObserveTurn(ctx, router.TurnEvent{ID: 9, Conv: conv, Phase: "done"})
	s.mu.Lock()
	u := s.progress[9]
	s.mu.Unlock()
	<-u.done
	n = len(f.all("chat.update"))
	s.ObserveTurn(ctx, router.TurnEvent{ID: 9, Conv: conv, Phase: "progress", Progress: harness.Progress{Phase: "tools", Tool: "bash"}})
	time.Sleep(90 * time.Millisecond)
	if len(f.all("chat.update")) != n {
		t.Fatal("late update after close")
	}
	if err := s.Send(ctx, core.Outbound{ID: 90, TurnID: 9, Conv: conv, Reactions: []core.Reaction{{MsgID: "1000.000001", Emoji: "thumbsup"}}}); err != nil {
		t.Fatal(err)
	}
	if len(f.all("chat.delete")) != 1 {
		t.Fatal("reaction-only status not deleted")
	}
	if _, ok, err := st.Progress(9); err != nil || ok {
		t.Fatal("progress retained after cleanup")
	}
	// A failed status update disables further updates for this turn.
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/chat.update" {
			fmt.Fprint(w, `{"ok":false,"error":"not_in_channel"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	s.ObserveTurn(ctx, router.TurnEvent{ID: 10, Conv: conv, Phase: "start"})
	waitFor(t, "second status", func() bool { return len(f.all("chat.postMessage")) == 2 })
	s.ObserveTurn(ctx, router.TurnEvent{ID: 10, Conv: conv, Phase: "steer"})
	waitFor(t, "disabled status", func() bool {
		s.mu.Lock()
		v := s.progress[10]
		s.mu.Unlock()
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.disabled
	})
	n = len(f.all("chat.update"))
	s.ObserveTurn(ctx, router.TurnEvent{ID: 10, Conv: conv, Phase: "cancelled"})
	s.mu.Lock()
	u = s.progress[10]
	s.mu.Unlock()
	<-u.done
	if len(f.all("chat.update")) != n {
		t.Fatal("disabled status updated")
	}
}
func TestProgressDeleteFallbackAndPermanentFinalFallback(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	ctx := context.Background()
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/chat.delete" {
			fmt.Fprint(w, `{"ok":false,"error":"cant_delete_message"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if err := st.SaveProgress(store.Progress{Turn: 2, Conv: conv, TS: "status", Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(ctx, core.Outbound{ID: 2, TurnID: 2, Conv: conv, Text: "answer"}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range f.all("chat.update") {
		found = found || strings.Contains(str(c.Body["text"]), "answered below")
	}
	if !found {
		t.Fatal("delete failure not compacted")
	}
	if err := st.SaveProgress(store.Progress{Turn: 3, Conv: conv, TS: "status3", Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/chat.postMessage" {
			fmt.Fprint(w, `{"ok":false,"error":"msg_too_long"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if err := s.Send(ctx, core.Outbound{ID: 3, TurnID: 3, Conv: conv, Text: "fallback answer"}); err != nil {
		t.Fatal(err)
	}
	found = false
	for _, c := range f.all("chat.update") {
		found = found || c.Body["text"] == "fallback answer"
	}
	if !found {
		t.Fatal("permanent fallback lost answer")
	}
}
func TestReactionErrorAndExternalUpload(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	ctx := context.Background()
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/reactions.add" {
			fmt.Fprint(w, `{"ok":false,"error":"invalid_name"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if err := s.Send(ctx, core.Outbound{ID: 1, Conv: conv, Text: "answer", Reactions: []core.Reaction{{MsgID: "m1", Emoji: "invalid"}}}); err != nil {
		t.Fatal(err)
	}
	if f.hasText("Could not confirm") {
		t.Fatal("reaction failure with text posted warning")
	}
	if err := s.Send(ctx, core.Outbound{ID: 2, Conv: conv, Reactions: []core.Reaction{{MsgID: "m1", Emoji: "invalid"}}}); err != nil {
		t.Fatal(err)
	}
	if !f.hasText("Could not confirm Slack reaction delivery") {
		t.Fatal("reaction-only failure silent")
	}
	path := filepath.Join(t.TempDir(), "artifact.txt")
	os.WriteFile(path, []byte("artifact"), 0600)
	if err := s.Send(ctx, core.Outbound{ID: 3, Conv: conv, File: &core.File{Path: path, Name: "artifact.txt"}}); err != nil {
		t.Fatal(err)
	}
	calls := f.all("files.completeUploadExternal")
	if len(calls) != 1 || calls[0].Body["thread_ts"] != conv.Thread || calls[0].Body["channel_id"] != "C1" {
		t.Fatal("wrong external upload destination")
	}
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/reactions.add" {
			fmt.Fprint(w, `{"ok":false,"error":"already_reacted"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if err := s.Send(ctx, core.Outbound{ID: 4, Conv: conv, Reactions: []core.Reaction{{MsgID: "m1", Emoji: "thumbsup"}}}); err != nil {
		t.Fatal(err)
	}
}
func TestProgressRestartTidyAndOwnerLiveVerification(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	if err := st.SaveProgress(store.Progress{Turn: 99, Conv: conv, TS: "old-status", Phase: "working"}); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverProgress(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := f.all("chat.update"); len(calls) != 1 || !strings.Contains(str(calls[0].Body["text"]), "interrupted") {
		t.Fatal("orphaned progress not tidied")
	}
	if k, ok := s.OwnerChat("uowner@example.test"); !ok || k.Chat != "Downer" {
		t.Fatal("owner DM not resolved")
	}
	if _, ok := s.OwnerChat("uguest@example.test"); ok {
		t.Fatal("foreign-workspace owner accepted")
	}
	// A remembered successful lookup cannot verify a later failure.
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/users.info" {
			fmt.Fprint(w, `{"ok":false,"error":"user_not_found"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if _, ok := s.OwnerChat("uowner@example.test"); ok {
		t.Fatal("cached owner identity accepted")
	}
}
func TestSafeThinkingDisplay(t *testing.T) {
	for _, text := range []string{"secret token abc", "ｔｏｋｅｎ private", "https://example.test", "user@example.test", "salary discussion", "/private/path", "<!channel>", "U0123456789", "key=value"} {
		if got := safeActivity(text); got != "" {
			t.Errorf("unsafe activity %q -> %q", text, got)
		}
	}
	if got := safeActivity("Ｃｈｅｃｋｉｎｇ public sources"); got != "Checking public sources" {
		t.Fatalf("NFKC summary %q", got)
	}
	if got := safeActivity("Considering sources"); got == "" {
		t.Fatal("ordinary words mistaken for Slack IDs")
	}
	if got := safeActivity("**Checking public sources.**"); got != "Checking public sources" {
		t.Fatalf("summary %q", got)
	}
	if got := safeActivity(strings.Repeat("😀", 121)); utf16Length(got) > 120 {
		t.Fatal("UTF-16 thinking display budget")
	}
	if got := safeActivity(strings.Repeat("word ", 60)); len([]rune(got)) > 120 {
		t.Fatal("thinking display budget")
	}
}

func TestProgressPostLostResponseIsReconciledAndRemoved(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	var lost atomic.Bool
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path != "/chat.postMessage" || lost.Swap(true) {
			return false
		}
		f.mu.Lock()
		f.messages = append(f.messages, map[string]any{"channel": args["channel"], "thread_ts": args["thread_ts"], "ts": "1000.000002", "metadata": args["metadata"], "text": args["text"]})
		f.mu.Unlock()
		w.WriteHeader(503)
		return true
	}
	f.mu.Unlock()
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	s.ObserveTurn(context.Background(), router.TurnEvent{ID: 50, Conv: conv, Phase: "start"})
	waitFor(t, "reconciled progress", func() bool { p, ok, _ := st.Progress(50); return ok && p.TS == "1000.000002" })
	s.ObserveTurn(context.Background(), router.TurnEvent{ID: 50, Conv: conv, Phase: "done"})
	if err := s.Send(context.Background(), core.Outbound{ID: 51, TurnID: 50, Conv: conv, Text: "done"}); err != nil {
		t.Fatal(err)
	}
	n := len(f.all("chat.postMessage"))
	s.ObserveTurn(context.Background(), router.TurnEvent{ID: 50, Conv: conv, Phase: "progress", Progress: harness.Progress{Phase: "thinking", Text: "Late update"}})
	time.Sleep(30 * time.Millisecond)
	if len(f.all("chat.delete")) != 1 || len(f.all("chat.postMessage")) != n {
		t.Fatal("orphan or resurrected status")
	}
}
func TestDeliveryOldestUsesStableEpochSeconds(t *testing.T) {
	out := core.Outbound{CreatedAt: time.Unix(1000, 123456000)}
	if got := reconciliationOldest(out); got != "970.123456" {
		t.Fatalf("oldest %q", got)
	}
}
func TestExactTurnPathCleanupAndLargeNativeLink(t *testing.T) {
	s := &Slack{}
	got := s.CleanAnswer("```\n/home/test/turn/out/file.txt\n```\nRead /home/test/turn/in/input.txt and /other/private/report.txt.", "/home/test/turn/in", "/home/test/turn/out")
	if strings.Contains(got, "/home/test/turn/") || !strings.Contains(got, "/other/private/report.txt") || strings.Contains(got, "```") {
		t.Fatalf("cleanup %q", got)
	}
	if got := mrkdwn("<https://example.test|label> <!channel>"); got != "<https://example.test|label> &lt;!channel&gt;" {
		t.Fatal("native links or escaping")
	}
	for _, part := range splitReply(mrkdwn("[long](https://example.test/" + strings.Repeat("x", 7000) + ")")) {
		if len([]rune(part)) > 3500 {
			t.Fatal("long link bypassed chunk limit")
		}
	}
}

func TestReactionNameGrammarAndParallelDelivery(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	for _, name := range []string{"thumbsup", ":thumbsup:", "custom_name"} {
		if _, ok := s.Reaction(name); !ok {
			t.Error("valid reaction rejected")
		}
	}
	for _, name := range []string{"::thumbsup::", "👍", "", strings.Repeat("x", 65)} {
		if _, ok := s.Reaction(name); ok {
			t.Error("invalid reaction accepted")
		}
	}
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path != "/reactions.add" {
			return false
		}
		arrived <- struct{}{}
		<-release
		fmt.Fprint(w, `{"ok":true}`)
		return true
	}
	f.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		done <- s.Send(context.Background(), core.Outbound{ID: 70, Conv: core.ConvKey{Interface: "slack", Chat: "C1"}, Reactions: []core.Reaction{{MsgID: "m1", Emoji: "thumbsup"}, {MsgID: "m2", Emoji: "wave"}}})
	}()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("reactions not delivered in parallel")
		}
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMultipartPermanentFallbackKeepsWholeAnswerAndReceipt(t *testing.T) {
	f := newSlackFake(t)
	s, st := readySlack(t, f)
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	if err := st.SaveProgress(store.Progress{Turn: 75, Conv: conv, TS: "status75", Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.messages = append(f.messages, map[string]any{"channel": "C1", "thread_ts": conv.Thread, "ts": "status75"})
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/chat.postMessage" {
			fmt.Fprint(w, `{"ok":false,"error":"msg_too_long"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	out := core.Outbound{ID: 76, TurnID: 75, Conv: conv, Text: strings.Repeat("answer ", 1000)}
	if err := s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	updates := f.all("chat.update")
	if len(updates) != 1 || updates[0].Body["text"] != out.Text {
		t.Fatal("multi-part fallback dropped text")
	}
	out.Attempt = 1
	if err := s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if len(f.all("chat.postMessage")) != 1 {
		t.Fatal("whole-answer fallback replayed after retry")
	}
}

func TestFileUploadFailureKeepsDeliveredReaction(t *testing.T) {
	f := newSlackFake(t)
	s, _ := readySlack(t, f)
	ctx := context.Background()
	conv := core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.000001"}
	if err := s.Send(ctx, core.Outbound{ID: 80, Conv: conv, Text: "answer", Reactions: []core.Reaction{{MsgID: "m1", Emoji: "thumbsup"}}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "file.txt")
	os.WriteFile(path, []byte("file"), 0600)
	f.mu.Lock()
	f.hook = func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/files.getUploadURLExternal" {
			fmt.Fprint(w, `{"ok":false,"error":"file_uploads_disabled"}`)
			return true
		}
		return false
	}
	f.mu.Unlock()
	if err := s.Send(ctx, core.Outbound{ID: 81, Conv: conv, File: &core.File{Path: path, Name: "file.txt"}}); err != nil {
		t.Fatal(err)
	}
	if len(f.all("reactions.add")) != 1 || !f.hasText("File upload failed: file.txt") {
		t.Fatal("file failure suppressed reaction or warning")
	}
}
