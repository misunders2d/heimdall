package telegram

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	osuser "os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestMain(m *testing.M) {
	fakeharness.Main()
	os.Exit(m.Run())
}

// sameUser runs every role as the test user; harness tests cover the
// account rules.
type sameUser struct{ harness.Accounts }

func (a sameUser) Prepare(_ core.Role, memory bool, bin string) (harness.Launch, error) {
	return a.Accounts.Prepare(core.RoleOwner, memory, bin)
}

// A Telegram message goes through the router to a harness that sees the
// caller's hub identity, and the reply comes back to the same chat as a
// reply to that message.
func TestTelegramThroughRouter(t *testing.T) {
	bin, _ := fakeharness.Install(t, "omp")
	u, err := osuser.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "")
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	api := newFakeAPI(t)
	var logs syncBuffer
	tg := newIface(t, api.srv.URL, botName, &logs)
	r := router.New(router.Options{
		Access:      core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"telegram:42": true}},
		Order:       []string{"omp"},
		MaxParallel: 2,
		Accounts:    sameUser{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}},
		Log:         log.New(io.Discard, "", 0),
	}, st, tg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	api.push(
		upd(1, msg(101, 42, "private", 42, "hello from the owner")),
		upd(2, msg(102, -1002, "group", 43, "@heimdall_test_bot hello from a colleague")),
		upd(3, msg(103, -1002, "group", 43, "not for the bot")),
	)
	eventually(t, "two replies and the owner's new-caller notice", func() bool { return len(api.sentBodies()) == 3 })
	byChat := map[float64]map[string]any{}
	var notice map[string]any
	for _, b := range api.sentBodies() {
		if text, _ := b["text"].(string); strings.HasPrefix(text, "ℹ New caller") {
			notice = b
			continue
		}
		byChat[b["chat_id"].(float64)] = b
	}
	if text, _ := notice["text"].(string); notice["chat_id"] != float64(42) || !strings.Contains(text, `"Ann Lee" (telegram:43)`) || strings.Contains(text, "colleague") {
		t.Fatalf("new-caller notice %v", notice)
	}
	dm, group := byChat[42], byChat[-1002]
	for name, tc := range map[string]struct {
		b     map[string]any
		reply float64
		parts []string
	}{
		"private": {dm, 101, []string{"omp#1", "role owner", "verified telegram:42", `last="hello from the owner"`, "PI_EXTERNAL_CHAT_ID=tg_chat_42 PI_EXTERNAL_MCP_PRIVATE=1 PI_EXTERNAL_PLATFORM=telegram PI_EXTERNAL_USER_ID=tg_user_42 PI_RESTRICTED_EXTERNAL_USER=0 PI_TRANSPORT_CHILD=1", "mem= ", "— omp"}},
		"group":   {group, 102, []string{"omp#1", "role employee", "verified telegram:43", `last="hello from a colleague"`, "PI_EXTERNAL_CHAT_ID=tg_chat_-1002 PI_EXTERNAL_MCP_PRIVATE=0 PI_EXTERNAL_PLATFORM=telegram PI_EXTERNAL_USER_ID=tg_user_43 PI_RESTRICTED_EXTERNAL_USER=1 PI_TRANSPORT_CHILD=1", "mem=1 ", "— omp"}},
	} {
		if tc.b == nil {
			t.Fatalf("%s: no reply in %v", name, api.sentBodies())
		}
		text, _ := tc.b["text"].(string)
		for _, p := range tc.parts {
			if !strings.Contains(text, p) {
				t.Errorf("%s reply %q lacks %q", name, text, p)
			}
		}
		if rp, _ := tc.b["reply_parameters"].(map[string]any); rp["message_id"] != tc.reply {
			t.Errorf("%s reply parameters %v", name, tc.b["reply_parameters"])
		}
	}
	// The unaddressed group message was acknowledged and kept as context
	// only: never run, never logged.
	eventually(t, "offset past every update", func() bool { return api.lastOffset() == 4 })
	time.Sleep(200 * time.Millisecond)
	if n := len(api.sentBodies()); n != 3 {
		t.Fatalf("%d replies", n)
	}
	if strings.Contains(logs.String(), "hello") {
		t.Fatalf("logs carry message text: %q", logs.String())
	}
}

// An album whose later part downloads for longer than the album wait still
// runs as one turn: the part is announced before its download.
func TestSlowAlbumPartStaysInTheAlbum(t *testing.T) {
	bin, state := fakeharness.Install(t, "omp")
	u, err := osuser.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	api := newFakeAPI(t)
	var logs syncBuffer
	tg := newIface(t, api.srv.URL, botName, &logs)
	tg.opt.UploadDir = filepath.Join(t.TempDir(), "uploads")
	r := router.New(router.Options{
		Access:      core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"telegram:42": true}},
		Order:       []string{"omp"},
		MaxParallel: 2,
		Accounts:    sameUser{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}},
		Log:         log.New(io.Discard, "", 0),
		AlbumWait:   500 * time.Millisecond,
	}, st, tg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	part := func(id int, caption string) map[string]any {
		m := msg(id, 42, "private", 42, "")
		m["caption"], m["media_group_id"] = caption, "A1"
		m["photo"] = []any{map[string]any{"file_id": fmt.Sprintf("id:p/%d.jpg", id), "file_unique_id": fmt.Sprintf("u%d", id)}}
		api.files[fmt.Sprintf("p/%d.jpg", id)] = []byte("jpeg")
		return m
	}
	api.slow["p/2.jpg"] = 1500 * time.Millisecond
	api.push(upd(1, part(1, "what are these?")), upd(2, part(2, "")))
	eventually(t, "a reply", func() bool { return len(api.sentBodies()) >= 1 })
	time.Sleep(time.Second)
	if n := len(api.sentBodies()); n != 1 {
		t.Fatalf("%d replies: the album split", n)
	}
	if in := fakeharness.Inputs(state); len(in) != 1 || strings.Count(in[0].Text, "[File saved]") != 2 {
		t.Fatalf("inputs %+v", in)
	}
}
