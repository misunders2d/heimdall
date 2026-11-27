package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/mcpoauth"
)

type sendIface struct {
	mu  sync.Mutex
	out []core.Outbound
}

func (f *sendIface) Name() string { return "telegram" }
func (f *sendIface) Run(ctx context.Context, _ func(context.Context, core.Inbound) error) error {
	<-ctx.Done()
	return nil
}
func (f *sendIface) Send(_ context.Context, o core.Outbound) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.out = append(f.out, o)
	return nil
}
func (f *sendIface) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.out) == 0 {
		return ""
	}
	return f.out[len(f.out)-1].Text
}

func fakeMCPAS(t *testing.T) *httptest.Server {
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base := srv.URL
		switch r.URL.Path {
		case "/.well-known/oauth-protected-resource/mcp":
			json.NewEncoder(w).Encode(map[string]any{"resource": base + "/mcp", "authorization_servers": []string{base}})
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]any{"issuer": base, "authorization_endpoint": base + "/authorize", "token_endpoint": base + "/token", "registration_endpoint": base + "/register"})
		case "/register":
			json.NewEncoder(w).Encode(map[string]any{"client_id": "c1"})
		case "/token":
			json.NewEncoder(w).Encode(map[string]any{"access_token": "CALLER_ACCESS", "refresh_token": "CALLER_REFRESH", "expires_in": 3600, "token_type": "Bearer"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// MEL-846: with a non-Pi employee engine enabled, `mcp login` signs the
// caller in through Heimdall's broker, Pi absent; the callback never reaches
// a queue or a harness, and Linear still needs mcp.linear.
func TestEmployeeBrokerSignIn(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{"whitelist.json": `{"users":["tg_user_77"]}`, "blacklist.json": `{}`, "admins.json": `{}`, "identities.json": `{}`, "capabilities.json": `{}`} {
		os.WriteFile(filepath.Join(dir, name), []byte(value), 0600)
	}
	srv := fakeMCPAS(t)
	store := t.TempDir()
	os.Chmod(store, 0700)
	b, err := mcpoauth.NewLocal(store, srv.Client(), map[string]string{"mellanni": srv.URL + "/mcp", "linear": srv.URL + "/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	f := &sendIface{}
	r := New(Options{EmployeeHarnesses: []string{"claude"}, MCP: b, Access: core.Access{Dir: dir}}, nil, f)
	from := core.Principal{Platform: "telegram", ID: "77"}
	dm := core.ConvKey{Interface: "telegram", Chat: "77"}
	in := core.Inbound{From: from, Conv: dm, Origin: core.OriginLive, Private: true, Text: "mcp login mellanni"}
	if !r.mcpControl(context.Background(), &in, core.RoleEmployee) {
		t.Fatal("broker login not handled")
	}
	link := f.last()
	i := strings.Index(link, srv.URL+"/authorize?")
	if i < 0 {
		t.Fatal(link)
	}
	u, _ := url.Parse(strings.Fields(link[i:])[0])
	cb := mcpoauth.DefaultRedirect + "?code=SYNTHETIC&state=" + url.QueryEscape(u.Query().Get("state"))
	in = core.Inbound{From: from, Conv: dm, Origin: core.OriginLive, Private: true, Text: cb}
	if !r.mcpControl(context.Background(), &in, core.RoleEmployee) || !strings.Contains(f.last(), "mellanni connected") {
		t.Fatal("callback not completed:", f.last())
	}
	in = core.Inbound{From: from, Conv: dm, Origin: core.OriginLive, Private: true, Text: "mcp status"}
	if !r.mcpControl(context.Background(), &in, core.RoleEmployee) || !strings.Contains(f.last(), "mellanni") {
		t.Fatal(f.last())
	}
	// Linear without mcp.linear is refused; a shared chat never signs in.
	in = core.Inbound{From: from, Conv: dm, Origin: core.OriginLive, Private: true, Text: "mcp login linear"}
	r.mcpControl(context.Background(), &in, core.RoleEmployee)
	if strings.Contains(f.last(), "/authorize") {
		t.Fatal("Linear offered without capability")
	}
	in = core.Inbound{From: from, Conv: core.ConvKey{Interface: "telegram", Chat: "-100"}, Origin: core.OriginLive, Text: "mcp login mellanni"}
	if r.mcpControl(context.Background(), &in, core.RoleEmployee) || in.InterfaceReply != mcpNotice || strings.Contains(in.Text, "mcp login") {
		t.Fatal("shared-chat sign-in", in)
	}
	// A shared-chat callback dies at the router: handled, never queued.
	in = core.Inbound{From: from, Conv: core.ConvKey{Interface: "telegram", Chat: "-100"}, Origin: core.OriginLive, Text: cb}
	if !r.mcpControl(context.Background(), &in, core.RoleEmployee) {
		t.Fatal("shared callback passed on")
	}
	// Pi-only deployments keep Pi's native sign-in.
	r = New(Options{EmployeeHarnesses: []string{"pi"}, MCP: b, Access: core.Access{Dir: dir}}, nil, f)
	in = core.Inbound{From: from, Conv: dm, Origin: core.OriginLive, Private: true, Text: "mcp login mellanni"}
	if r.mcpControl(context.Background(), &in, core.RoleEmployee) || in.Text != "!pi /mcp login mellanni" {
		t.Fatal("Pi native route changed", in.Text)
	}
}
