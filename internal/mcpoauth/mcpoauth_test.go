package mcpoauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

type fakeAS struct {
	srv       *httptest.Server
	refreshes atomic.Int32
	expires   int
}

func newFakeAS(t *testing.T) *fakeAS {
	f := &fakeAS{expires: 3600}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		base := f.srv.URL
		switch r.URL.Path {
		case "/.well-known/oauth-protected-resource/mcp":
			json.NewEncoder(w).Encode(map[string]any{"resource": base + "/mcp", "authorization_servers": []string{base}, "scopes_supported": []string{"read"}})
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]any{"issuer": base, "authorization_endpoint": base + "/authorize", "token_endpoint": base + "/token", "registration_endpoint": base + "/register", "code_challenge_methods_supported": []string{"S256"}})
		case "/register":
			var reg map[string]any
			json.NewDecoder(r.Body).Decode(&reg)
			if uris, _ := reg["redirect_uris"].([]any); len(uris) != 1 || uris[0] != DefaultRedirect {
				t.Error("registration redirect", reg)
			}
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]any{"client_id": "synthetic-client"})
		case "/token":
			r.ParseForm()
			if r.Form.Get("client_id") != "synthetic-client" || r.Form.Get("resource") != base+"/mcp" {
				t.Error("client/resource missing", r.Form)
			}
			switch r.Form.Get("grant_type") {
			case "authorization_code":
				if r.Form.Get("code") != "SYNTHETIC_CODE" || r.Form.Get("code_verifier") == "" || r.Form.Get("redirect_uri") != DefaultRedirect {
					t.Error("bad code exchange", r.Form)
				}
			case "refresh_token":
				f.refreshes.Add(1)
				if r.Form.Get("refresh_token") != "SYNTHETIC_REFRESH" {
					t.Error("wrong refresh")
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "SYNTHETIC_ACCESS", "refresh_token": "SYNTHETIC_REFRESH", "expires_in": f.expires, "token_type": "bearer"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func fixture(t *testing.T) (*Broker, *fakeAS, Caller, core.ConvKey) {
	t.Helper()
	f := newFakeAS(t)
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	b, err := newBroker(&Config{Enabled: true}, dir, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	b.client = f.srv.Client()
	b.servers = map[string]string{"mellanni": f.srv.URL + "/mcp", "linear": f.srv.URL + "/mcp"}
	c := Caller{Principal: core.SlackPrincipal("employee@example.test", "T1", ""), Actor: "sl_user_U1", Installation: "sl_user_B1"}
	return b, f, c, core.ConvKey{Interface: "slack", Chat: "D1", Thread: "1"}
}

func signIn(t *testing.T, b *Broker, c Caller, conv core.ConvKey) {
	t.Helper()
	link, err := b.Login(context.Background(), c, conv, "mellanni")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(link)
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("resource") != b.servers["mellanni"] || q.Get("scope") != "read" || !strings.HasPrefix(q.Get("state"), statePrefix) {
		t.Fatal("authorize link", q)
	}
	cb := DefaultRedirect + "?code=SYNTHETIC_CODE&state=" + url.QueryEscape(q.Get("state"))
	if !b.Owns(cb) || b.Owns(DefaultRedirect+"?code=x&state=pi-native") {
		t.Fatal("callback ownership")
	}
	if server, err := b.Complete(context.Background(), c, conv, cb); err != nil || server != "mellanni" {
		t.Fatal(server, err)
	}
	// One use only.
	if _, err := b.Complete(context.Background(), c, conv, cb); err == nil {
		t.Fatal("callback replayed")
	}
}

func TestSignInSnapshotAccessOnly(t *testing.T) {
	b, f, c, conv := fixture(t)
	signIn(t, b, c, conv)
	got, failed := b.Snapshot(context.Background(), c, []string{"mellanni", "linear"})
	if len(failed) != 0 || got["mellanni"].Token != "SYNTHETIC_ACCESS" {
		t.Fatal(got, failed)
	}
	if _, ok := got["linear"]; ok {
		t.Fatal("unsigned server handed out")
	}
	if f.refreshes.Load() != 0 {
		t.Fatal("fresh token refreshed")
	}
	// Root-only store: 0700 directories, 0600 files; refresh never in Access.
	key, _ := c.Key()
	dir := filepath.Join(b.root, key)
	st, _ := os.Stat(dir)
	fst, _ := os.Stat(credPath(dir, "mellanni"))
	if st.Mode().Perm() != 0700 || fst.Mode().Perm() != 0600 {
		t.Fatal(st.Mode(), fst.Mode())
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "REFRESH") || strings.Contains(string(raw), "synthetic-client") {
		t.Fatal("access snapshot carries renewal material", string(raw))
	}
	if s, _ := b.Status(c); len(s) != 1 || s[0] != "mellanni" {
		t.Fatal(s)
	}
	if err := b.Logout(c, "mellanni"); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Snapshot(context.Background(), c, []string{"mellanni"}); len(got) != 0 {
		t.Fatal("logout kept access")
	}
}

func TestSnapshotRenewsShortTokens(t *testing.T) {
	b, f, c, conv := fixture(t)
	f.expires = 600 // shorter than one employee turn
	signIn(t, b, c, conv)
	f.expires = 3600
	got, failed := b.Snapshot(context.Background(), c, []string{"mellanni"})
	if len(failed) != 0 || got["mellanni"].Token == "" || f.refreshes.Load() != 1 {
		t.Fatal(got, failed, f.refreshes.Load())
	}
	f.expires = 60 // renewal cannot cover a turn
	now := b.now()
	b.now = func() time.Time { return now.Add(2 * time.Hour) }
	if got, failed := b.Snapshot(context.Background(), c, []string{"mellanni"}); len(got) != 0 || failed["mellanni"] == nil {
		t.Fatal("short renewal handed out", got)
	}
}

func TestCallbackIsolation(t *testing.T) {
	b, _, c, conv := fixture(t)
	link, err := b.Login(context.Background(), c, conv, "mellanni")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(link)
	cb := DefaultRedirect + "?code=SYNTHETIC_CODE&state=" + url.QueryEscape(u.Query().Get("state"))
	other := Caller{Principal: core.SlackPrincipal("other@example.test", "T1", ""), Actor: "sl_user_U2", Installation: "sl_user_B1"}
	if _, err := b.Complete(context.Background(), other, conv, cb); err == nil {
		t.Fatal("another caller completed the sign-in")
	}
	if _, err := b.Complete(context.Background(), c, core.ConvKey{Interface: "slack", Chat: "C9"}, cb); err == nil {
		t.Fatal("another chat completed the sign-in")
	}
	if _, err := b.Complete(context.Background(), c, conv, "http://127.0.0.1:9/elsewhere?code=SYNTHETIC_CODE&state=x"); err == nil {
		t.Fatal("foreign redirect accepted")
	}
	now := b.now()
	b.now = func() time.Time { return now.Add(11 * time.Minute) }
	if _, err := b.Complete(context.Background(), c, conv, cb); err == nil {
		t.Fatal("expired sign-in accepted")
	}
	if _, err := b.Login(context.Background(), Caller{Principal: core.Principal{Platform: "slack", ID: "U1"}}, conv, "mellanni"); err == nil {
		t.Fatal("unverified caller")
	}
	if _, err := b.Login(context.Background(), c, conv, "t3"); err == nil {
		t.Fatal("owner server offered")
	}
}

func TestConfigValidate(t *testing.T) {
	for _, bad := range []string{"https://127.0.0.1/cb", "http://example.com/cb", "http://127.0.0.1", "http://127.0.0.1/cb?x=1"} {
		if (&Config{Enabled: true, RedirectURI: bad}).Validate() == nil {
			t.Error("accepted", bad)
		}
	}
	if (&Config{Enabled: true}).Validate() != nil || (&Config{Enabled: true, RedirectURI: "http://localhost:4000/cb"}).Validate() != nil {
		t.Fatal("rejected loopback")
	}
}
