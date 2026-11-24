package slack

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestLiveSlackAnswerWatch(t *testing.T) {
	for _, engine := range []string{"omp", "codex"} {
		t.Run(engine, func(t *testing.T) { testLiveSlackAnswerWatch(t, engine) })
	}
}

func testLiveSlackAnswerWatch(t *testing.T, engine string) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "0")
	f := newSlackFake(t)
	f.allowWorkUser = true
	for _, id := range []string{"UOWNER", "UCOLLEAGUE"} {
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
			if args["channel"] == "D1" && !userToken || args["channel"] == "C1" && userToken {
				t.Errorf("wrong token for %s on %s", r.URL.Path, args["channel"])
			}
		}
		if r.URL.Path == "/conversations.members" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "members": []string{"UOWNER", "UCOLLEAGUE"}})
			return true
		}
		return false
	})
	s, st := testSlack(t, f)
	s.opt.WorkOwnerID, s.opt.WorkUserToken = "UOWNER", "test-work-user"
	operator, err := store.Open(filepath.Join(s.opt.DataDir, "heimdall.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer operator.Close()
	owner, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	bin, state := fakeharness.Install(t, engine)
	logger := log.New(io.Discard, "", 0)
	rt, err := durable.Open(durable.Options{Path: filepath.Join(s.opt.DataDir, "durable.db"), Outbox: st, Log: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	r := router.New(router.Options{
		Work: rt, Order: []string{engine}, Log: logger,
		Access:   core.Access{Owners: map[string]bool{"slack:uowner@example.test": true}},
		Accounts: harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: owner.Username, Path: bin}}},
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
	waitFor(t, "Slack user authorization ready with no watches", s.started)
	set := func(chat string, watch core.SlackWatch) {
		t.Helper()
		if err := operator.SetSlackWatch(chat, watch); err != nil {
			t.Fatal(err)
		}
	}
	post := func(chat, surface, who, thread, answer, authorization string) {
		t.Helper()
		text := "What is the project?"
		if answer != "" {
			text += " SAY=" + base64.StdEncoding.EncodeToString([]byte(answer))
		}
		m := f.event(chat, surface, who, text, thread)
		waitAck(t, f, f.pushAuthorized(m, authorization))
	}
	const thread = "980.000002"
	post("D1", "im", "UCOLLEAGUE", thread, "", "UOWNER")
	set("D1", core.SlackWatch{Mode: core.SlackWatchAnswer, Thread: thread})
	post("D1", "im", "UCOLLEAGUE", "980.000003", "", "UOWNER")
	post("D1", "im", "UOWNER", thread, "", "UOWNER")
	post("D1", "im", "UCOLLEAGUE", thread, "REPORT\nThe project is Maple.", "UOWNER")
	waitFor(t, "attributed answer from owner-visible DM", func() bool { return f.hasText("The project is Maple.") })

	w, err := operator.SlackWatch("D1", thread)
	if err != nil || w.Revision != 1 || w.Thread != thread {
		t.Fatalf("initial thread watch: %+v, %v", w, err)
	}
	set("D1", core.SlackWatch{Mode: core.SlackWatchOff, Thread: thread})
	post("D1", "im", "UCOLLEAGUE", thread, "", "UOWNER")
	set("D1", core.SlackWatch{Mode: core.SlackWatchAnswer, Thread: thread})
	old := core.Outbound{Conv: core.ConvKey{Interface: "slack", Chat: "D1", Thread: thread}, Text: "stale", SlackOwnerMention: "UOWNER", SlackWatch: w.Ref()}
	var permanent router.PermanentError
	if err := s.sendText(context.Background(), old); !errors.As(err, &permanent) {
		t.Fatalf("old delivery was not rejected after off/on: %v", err)
	}

	set("C1", core.SlackWatch{Mode: core.SlackWatchAnswer})
	post("C1", "channel", "UCOLLEAGUE", "981.000002", "REPORT\nA general-knowledge answer.", "")
	waitFor(t, "answer from bot-visible channel", func() bool { return f.hasText("A general-knowledge answer.") })
	posts := f.all("chat.postMessage")
	if len(posts) != 2 || posts[0].Body["thread_ts"] != thread ||
		!strings.HasPrefix(str(posts[0].Body["text"]), workAttribution("UOWNER")) {
		t.Fatalf("wrong replies or delivery route: %v", posts)
	}
	inputs := fakeharness.Inputs(state)
	if len(inputs) != 2 {
		t.Fatalf("expected two harness launches, got %d", len(inputs))
	}
	for _, in := range inputs {
		prompt := in.Text + "\n" + in.Context
		if in.Harness != engine || !strings.Contains(prompt, "native Memento recall") ||
			!strings.Contains(prompt, "read-only Mellanni MCP") || !strings.Contains(prompt, "another configured harness") {
			t.Fatalf("selected harness did not receive answer and delegation instructions: %+v", in)
		}
	}
}
