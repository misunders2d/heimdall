package router

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	got, images := c.prompt(&turn{}, []item{{row: store.InboundRow{Inbound: core.Inbound{MCPControl: true}}, text: "/mcp login linear"}})
	if got != "/mcp login linear" || len(images) != 0 {
		t.Fatal("native command wrapped as model prompt", got)
	}
}

func TestMCPExplicitPiEligibility(t *testing.T) {
	for _, tc := range []struct {
		role    core.Role
		enabled bool
		notice  string
	}{{core.RoleEmployee, true, mcpNotice}, {core.RoleOwner, false, "Owner Slack is disabled."}} {
		r := New(Options{OwnerSlack: &harness.OwnerSlack{Enabled: tc.enabled}}, nil)
		in := core.Inbound{Text: "!pi /mcp login slack", Private: true, Origin: core.OriginLive, Conv: core.ConvKey{Interface: "telegram"}}
		r.mcpControl(context.Background(), &in, tc.role)
		if in.MCPControl || in.InterfaceReply != tc.notice {
			t.Fatal(in)
		}
	}
	r := New(Options{}, nil)
	in := core.Inbound{Text: "!pi /mcp logout linear", Private: true, Origin: core.OriginLive, Conv: core.ConvKey{Interface: "telegram"}}
	r.mcpControl(context.Background(), &in, core.RoleEmployee)
	if in.MCPControl || strings.Contains(in.Text, "logout") {
		t.Fatal(in)
	}
}
func TestMCPUnmarkedPromptFramed(t *testing.T) {
	e := setup(t, core.ModePersonal, []string{"pi"}, "pi")
	defer e.stop()
	c := &conv{r: e.r, key: core.ConvKey{Interface: "fake", Chat: "dm"}}
	got, _ := c.prompt(&turn{}, []item{{text: "/mcp logout linear"}})
	if got == "/mcp logout linear" {
		t.Fatal("unmarked command bypassed framing")
	}
}
func TestMCPAmbientCallbackSilent(t *testing.T) {
	f := newFakeIface()
	r := New(Options{}, nil, f)
	in := core.Inbound{Ambient: true, Text: "http://localhost:18765/callback?code=x&state=x", Conv: core.ConvKey{Interface: "fake"}}
	if !r.mcpControl(context.Background(), &in, core.RoleEmployee) {
		t.Fatal("not consumed")
	}
	if len(f.out) != 0 {
		t.Fatal("ambient callback answered")
	}
}
func TestMCPCustomSecretScrub(t *testing.T) {
	t.Setenv("CUSTOM_OAUTH_SECRET", "synthetic-custom-private")
	r := New(Options{OwnerSlack: &harness.OwnerSlack{ClientSecretEnv: "CUSTOM_OAUTH_SECRET"}}, nil)
	if strings.Contains(r.scrub("reply synthetic-custom-private"), "synthetic-custom-private") {
		t.Fatal("custom secret leaked")
	}
}

type mcpNamedIface struct {
	*fakeIface
	name string
}

func (f mcpNamedIface) Name() string { return f.name }
func TestMCPTurnRoleAndSurfaceGate(t *testing.T) {
	e := setup(t, core.ModePersonal, []string{"claude"}, "claude")
	e.stop()
	capture := filepath.Join(t.TempDir(), "launch.json")
	t.Setenv("FAKE_CAPTURE_LAUNCH", capture)
	t.Setenv("SLACK_CLIENT_ID", "synthetic-id")
	st, err := store.Open(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := &harness.OwnerSlack{Enabled: true}
	_ = cfg.Validate()
	e.opt.OwnerSlack = cfg
	e.opt.SlackTeam = "T1"
	f := mcpNamedIface{newFakeIface(), "slack"}
	r := New(e.opt, st, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for n, tc := range []struct {
		email   string
		private bool
	}{{"employee@example.com", true}, {"boss@example.com", false}, {"boss@example.com", true}} {
		in := core.Inbound{From: core.SlackPrincipal(tc.email, "T1", ""), Private: tc.private, SlackReadVerified: true, OAuthActor: "sl_user_U1", OAuthInstallation: "sl_user_B1", Conv: core.ConvKey{Interface: "slack", Chat: fmt.Sprint(n)}, MsgID: "1", Text: "hello"}
		f.in <- in
		select {
		case out := <-f.out:
			if !strings.Contains(out.Text, "claude#") {
				t.Fatal(out.Text)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no reply")
		}
		raw, err := os.ReadFile(capture)
		if err != nil {
			t.Fatal(err)
		}
		var got struct{ Args []string }
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		enabled := tc.email == "boss@example.com" && tc.private
		if strings.Contains(strings.Join(got.Args, " "), "--mcp-config") != enabled {
			t.Fatal("role/surface gate bypassed", got.Args)
		}
	}
}
