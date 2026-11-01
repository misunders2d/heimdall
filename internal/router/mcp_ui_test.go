package router

import (
	"context"
	"github.com/misunders2d/heimdall/internal/store"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestMCPOwnerCommandAndCallbackPrivacy(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r := New(Options{Access: core.Access{Owners: map[string]bool{"telegram:42": true}}}, st)
	r.convs[core.ConvKey{Interface: "telegram", Chat: "42"}] = &conv{signal: make(chan struct{}, 1)}
	for _, role := range []core.Role{core.RoleOwner, core.RoleEmployee} {
		for _, private := range []bool{true, false} {
			in := core.Inbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, From: core.Principal{Platform: "telegram", ID: "42"}, Origin: core.OriginLive, Private: private, Text: "mcp login linear", MsgID: "callback-test"}
			if r.mcpControl(context.Background(), &in, role) {
				t.Fatal("command swallowed")
			}
			if role == core.RoleOwner && private {
				if in.Text != "!pi /mcp login linear" {
					t.Fatal(in.Text)
				}
			} else if in.InterfaceReply == "" || strings.Contains(in.Text, "mcp login") {
				t.Fatal("nonowner command not redacted")
			}
			in.Text = "http://127.0.0.1:18765/callback?state=synthetic&code=SYNTHETIC_CALLBACK"
			in.MsgID = "callback-test"
			if err := r.accept(context.Background(), in); err != nil {
				t.Fatal(err)
			}
		}
	}
	rows, err := st.Pending()
	if err != nil || len(rows) != 0 {
		t.Fatal("callback queued", rows, err)
	}
}
func TestMCPNativeCommandPromptUnframed(t *testing.T) {
	e := setup(t, core.ModePersonal, []string{"pi"}, "pi")
	defer e.stop()
	c := &conv{r: e.r, key: core.ConvKey{Interface: "fake", Chat: "dm"}}
	got, images := c.prompt(&turn{}, []item{{text: "/mcp login linear"}})
	if got != "/mcp login linear" || len(images) != 0 {
		t.Fatal("native command wrapped as model prompt", got)
	}
}
