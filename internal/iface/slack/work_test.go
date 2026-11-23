package slack

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
)

func TestConversationWorkFromSlackPost(t *testing.T) {
	t.Run("bot event", func(t *testing.T) { testConversationWorkFromSlackPost(t, false) })
	t.Run("owner event without bot access", func(t *testing.T) { testConversationWorkFromSlackPost(t, true) })
}

func testConversationWorkFromSlackPost(t *testing.T, userEvents bool) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	f := newSlackFake(t)
	f.allowWorkUser = userEvents
	for _, id := range []string{"UOWNER", "UMASAO", "UCOLLEAGUE", "UOUTSIDE"} {
		u := User{ID: id, TeamID: "Ttest", Name: id}
		u.Profile.Email = strings.ToLower(id) + "@example.test"
		f.users[id] = u
	}
	f.setHook(func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		userToken := r.Header.Get("Authorization") == "Bearer test-work-user"
		if r.URL.Path == "/auth.test" && userToken {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "team_id": "Ttest", "user_id": "UOWNER"})
			return true
		}
		switch r.URL.Path {
		case "/conversations.members", "/conversations.replies", "/chat.postMessage":
			if args["channel"] == "G1" && userToken != userEvents {
				t.Errorf("wrong identity for %s in work group", r.URL.Path)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "channel_not_found"})
				return true
			}
		}
		if r.URL.Path != "/conversations.members" {
			return false
		}
		// The qualifying member sits on page two. A same-workspace
		// author can still be absent from the membership response.
		members, cursor := []string{"UOWNER"}, "next"
		if args["cursor"] == "next" {
			members, cursor = []string{"UMASAO", "UCOLLEAGUE"}, ""
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "members": members, "response_metadata": map[string]string{"next_cursor": cursor}})
		return true
	})
	s, st := testSlack(t, f)
	s.opt.WorkOwnerID = "UOWNER"
	authorization := ""
	if userEvents {
		authorization = "UOWNER"
		s.opt.WorkUserToken = "test-work-user"
	}
	push := func(m eventMessage) string { return f.pushAuthorized(m, authorization) }
	s.opt.ConversationWork = map[string]string{"G1": "Create artwork or previews. Publishing requires owner approval."}
	worker, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	bin, state := fakeharness.Install(t, "omp")
	logger := log.New(io.Discard, "", 0)
	rt, err := durable.Open(durable.Options{Path: filepath.Join(s.opt.DataDir, "durable.db"), Outbox: st, Log: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	r := router.New(router.Options{
		Work: rt, Order: []string{"omp"}, Log: logger, MaxParallel: 2,
		Access:   core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"slack:uowner@example.test": true}},
		Accounts: harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: worker.Username, Path: bin}}},
	}, st, s)
	rt.SetAssistant(r)
	rt.SetWake(r.Wake)
	if err := rt.Launch(); err != nil {
		t.Fatal(err)
	}
	cancel, done := startRouter(t, r)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-f.connected:
	case <-time.After(3 * time.Second):
		t.Fatal("socket did not start")
	}

	say := func(answer string) string { return " SAY=" + base64.StdEncoding.EncodeToString([]byte(answer)) }
	work := f.event("G1", "mpim", "UMASAO", "Can we get new artwork?"+say("REPORT\nArtwork preview: https://example.test/draft\nSecond detail."), "900.000001")
	push(work)
	waitFor(t, "attributed work delivered", func() bool { return f.hasText("Artwork preview:") })
	silent := f.event("G1", "mpim", "UCOLLEAGUE", "Please publish it now."+say("SILENT"), "900.000002")
	push(silent)
	silentID := sha256.Sum256([]byte("Ttest\x00G1\x00" + silent.TS))
	waitFor(t, "silent request finished", func() bool {
		state, err := rt.State(fmt.Sprintf("SLACK-%X", silentID[:16]))
		return err == nil && state.Phase == durable.PhaseClosed
	})
	answer := f.event("G1", "mpim", "UCOLLEAGUE", "Did the redesign land?"+say("REPORT\nYes. The merge is recorded at https://example.test/commit."), "900.000003")
	push(answer)
	waitFor(t, "supported reply delivered", func() bool { return f.hasText("merge is recorded") })

	posts := f.all("chat.postMessage")
	if len(posts) != 2 {
		t.Fatalf("expected work and factual reply only, got %v", posts)
	}
	for i, want := range []eventMessage{work, answer} {
		body := posts[i].Body
		if body["channel"] != want.Channel || body["thread_ts"] != want.ThreadTS || !strings.HasPrefix(str(body["text"]), "AI assistant covering for <@UOWNER> while they’re away.") {
			t.Fatalf("wrong thread or attribution: %v", body)
		}
	}
	if !strings.Contains(str(posts[0].Body["text"]), "Second detail.") {
		t.Fatal("result lost its second line")
	}
	inputs := fakeharness.Inputs(state)
	for _, want := range []string{"umasao@example.test", "ucolleague@example.test", "Publishing requires owner approval."} {
		seen := false
		for _, in := range inputs {
			seen = seen || strings.Contains(in.Text, want)
		}
		if !seen {
			t.Errorf("harness did not receive %q", want)
		}
	}

	// A verified workspace user outside the group cannot start work.
	memberCalls := len(f.all("conversations.members"))
	f.push(f.event("G1", "mpim", "UOUTSIDE", "Generate artwork."+say("REPORT\nOutside grant."), "900.000004"))
	waitFor(t, "non-member checked on both pages", func() bool { return len(f.all("conversations.members")) >= memberCalls+2 })
	ungranted := push(f.event("G2", "mpim", "UMASAO", "Generate artwork."+say("REPORT\nWrong group."), "900.000005"))
	group := core.ConvKey{Interface: "slack", Chat: "G2", Thread: "900.000005"}
	if userEvents {
		waitAck(t, f, ungranted)
		if rows, err := st.RecentAmbient(group, nil, 5); err != nil || len(rows) > 0 {
			t.Fatalf("owner event stored outside its grant: %d rows, %v", len(rows), err)
		}
		echo := f.event("G1", "mpim", "UOWNER", workAttribution("UOWNER")+say("REPORT\nReply loop."), "900.000006")
		waitAck(t, f, push(echo))
		if rows, err := st.RecentAmbient(core.ConvKey{Interface: "slack", Chat: "G1", Thread: echo.ThreadTS}, nil, 5); err != nil || len(rows) > 0 {
			t.Fatalf("outbound AI reply was read back as a request: %d rows, %v", len(rows), err)
		}
	} else {
		waitFor(t, "message in ungranted group stored as ambient", func() bool {
			rows, _ := st.RecentAmbient(group, nil, 5)
			return len(rows) > 0
		})
	}
	if got := f.all("chat.postMessage"); len(got) != 2 {
		t.Fatalf("out-of-scope Slack response: %v", got)
	}
}

func waitAck(t *testing.T, f *slackFake, want string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case id := <-f.acks:
			if id == want {
				return
			}
		case <-timer.C:
			t.Fatalf("Slack event %s was not acknowledged", want)
		}
	}
}
