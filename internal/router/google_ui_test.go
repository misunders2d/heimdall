package router

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/googleoauth"
	"github.com/misunders2d/heimdall/internal/store"
)

type googleUIInterface struct {
	mu       sync.Mutex
	messages []core.Outbound
}

func (*googleUIInterface) Name() string { return "slack" }
func (*googleUIInterface) Run(context.Context, func(context.Context, core.Inbound) error) error {
	return nil
}
func (f *googleUIInterface) Send(_ context.Context, o core.Outbound) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, o)
	return nil
}

func TestGooglePrivateEmployeeControlsAndEphemeralCallback(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root broker test")
	}
	t.Setenv(googleoauth.ClientIDEnv, "synthetic-client")
	t.Setenv(googleoauth.ClientSecretEnv, "SYNTHETIC_CLIENT_SECRET")
	broker, err := googleoauth.New(&googleoauth.Config{Enabled: true, RedirectURI: "https://example.test/oauth/google/callback"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	face := &googleUIInterface{}
	r := New(Options{Google: broker, EmployeeHarnesses: []string{"pi"}, Access: core.Access{Owners: map[string]bool{}}}, st, face)
	// Directly exercise role gate; no external transports/harnesses are launched.
	base := core.Inbound{From: core.SlackPrincipal("employee@example.test", "T1", ""), Conv: core.ConvKey{Interface: "slack", Chat: "D1", Thread: "1"}, Origin: core.OriginLive, Private: true, OAuthActor: "sl_user_U1", OAuthInstallation: "sl_user_B1", Text: "google login", MsgID: "1", At: time.Now()}
	for _, kind := range []string{"eligible", "shared", "scheduled", "ambient", "owner", "disabled-pi", "unverified", "local"} {
		t.Run(kind, func(t *testing.T) {
			in := base
			role := core.RoleEmployee
			r.opt.EmployeeHarnesses = []string{"pi"}
			switch kind {
			case "shared":
				in.Private = false
			case "scheduled":
				in.Origin = core.OriginScheduled
			case "ambient":
				in.Ambient = true
			case "owner":
				role = core.RoleOwner
			case "disabled-pi":
				r.opt.EmployeeHarnesses = nil
			case "unverified":
				in.OAuthActor = ""
			case "local":
				in.Conv.Interface = "local"
			}
			n := len(face.messages)
			if !r.googleControl(context.Background(), in, role) {
				t.Fatal("control reached model")
			}
			if kind == "ambient" || kind == "local" {
				return
			}
			if len(face.messages) != n+1 {
				t.Fatal("missing reply")
			}
			text := face.messages[n].Text
			if strings.Contains(text, "SYNTHETIC_CLIENT_SECRET") {
				t.Fatal("secret in reply")
			}
			if strings.Contains(text, "https://accounts.google.com/") != (kind == "eligible") {
				t.Fatal("private eligibility failed", kind)
			}
		})
	}
	r.opt.EmployeeHarnesses = []string{"pi"}
	for _, command := range []string{"google status", "google logout", "!pi /google login"} {
		in := base
		in.Text = command
		if !r.googleControl(context.Background(), in, core.RoleEmployee) {
			t.Fatal("command escaped control")
		}
	}
	in := base
	in.Text = "https://example.test/oauth/google/callback?state=hmg_unknown&code=SYNTHETIC_CALLBACK"
	if !r.googleControl(context.Background(), in, core.RoleEmployee) {
		t.Fatal("callback escaped control")
	}
	if strings.Contains(face.messages[len(face.messages)-1].Text, "SYNTHETIC_CALLBACK") {
		t.Fatal("callback echoed")
	}
	if rows, err := st.Pending(); err != nil || len(rows) != 0 {
		t.Fatal("Google control persisted", err)
	}
}

func TestGoogleCallbackAcceptNeverQueuesEvenWhenDisabled(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	face := &googleUIInterface{}
	r := New(Options{Access: core.Access{Owners: map[string]bool{"slack:owner@example.test": true}}}, st, face)
	in := core.Inbound{From: core.SlackPrincipal("owner@example.test", "T1", ""), Conv: core.ConvKey{Interface: "slack", Chat: "D1"}, Origin: core.OriginLive, Private: true, Text: "https://example.test/oauth/google/callback?state=hmg_unknown&code=SYNTHETIC_CALLBACK", MsgID: "1", At: time.Now()}
	if err := r.accept(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	rows, err := st.Pending()
	if err != nil || len(rows) != 0 {
		t.Fatal("disabled callback persisted", err)
	}
}
