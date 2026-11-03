package googleoauth

import (
	"context"
	"encoding/json"
	"io"
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

func fixture(t *testing.T) (*Broker, Caller, core.ConvKey, *atomic.Int32) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("root broker test")
	}
	t.Setenv(ClientIDEnv, "synthetic-client")
	t.Setenv(ClientSecretEnv, "SYNTHETIC_CLIENT_SECRET")
	b, err := New(&Config{Enabled: true, RedirectURI: "https://example.test/oauth/google/callback"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	c := Caller{Principal: core.SlackPrincipal("employee@example.test", "T1", ""), Actor: "sl_user_U1", Installation: "sl_user_B1"}
	conv := core.ConvKey{Interface: "slack", Chat: "D1", Thread: "1"}
	calls := new(atomic.Int32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/token":
			calls.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("client_secret") != "SYNTHETIC_CLIENT_SECRET" || r.Form.Get("client_id") != "synthetic-client" {
				t.Error("client authentication missing")
			}
			if r.Form.Get("grant_type") == "authorization_code" {
				if r.Form.Get("code_verifier") == "" || r.Form.Get("redirect_uri") != b.redirect {
					t.Error("missing PKCE/redirect")
				}
			} else if r.Form.Get("refresh_token") != "SYNTHETIC_REFRESH" {
				t.Error("wrong refresh")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "SYNTHETIC_ACCESS", "refresh_token": "SYNTHETIC_REFRESH", "expires_in": 3600, "token_type": "Bearer", "scope": strings.Join(Scopes, " ")})
		case "/user":
			if r.Header.Get("Authorization") != "Bearer SYNTHETIC_ACCESS" {
				t.Error("userinfo bearer missing")
			}
			io.WriteString(w, `{"id":"google-subject-1","email":"employee@example.test","verified_email":true}`)
		default:
			t.Error("unexpected request")
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	b.client = srv.Client()
	b.tokenURL = srv.URL + "/token"
	b.userURL = srv.URL + "/user"
	return b, c, conv, calls
}
func begin(t *testing.T, b *Broker, c Caller, conv core.ConvKey) string {
	t.Helper()
	link, err := b.Login(c, conv)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("access_type") != "offline" || q.Get("prompt") != "consent" {
		t.Fatal("incomplete authorization request")
	}
	if strings.Contains(link, "SYNTHETIC_CLIENT_SECRET") {
		t.Fatal("secret in consent URL")
	}
	return b.redirect + "?" + url.Values{"state": {q.Get("state")}, "code": {"SYNTHETIC_CODE"}}.Encode()
}
func connect(t *testing.T, b *Broker, c Caller, conv core.ConvKey) {
	t.Helper()
	callback := begin(t, b, c, conv)
	if email, err := b.Complete(context.Background(), c, conv, callback); err != nil || email != "employee@example.test" {
		t.Fatal(email, err)
	}
}
func TestPerCallerStoreAndAccessOnlyRenewal(t *testing.T) {
	b, c, conv, calls := fixture(t)
	connect(t, b, c, conv)
	other := c
	other.Actor = "sl_user_U2"
	if email, err := b.Status(other); err != nil || email != "" {
		t.Fatal("other caller inherited identity", err)
	}
	if a, err := b.Snapshot(context.Background(), other); err != nil || a.Token != "" {
		t.Fatal("other caller inherited access", err)
	}
	a, err := b.Snapshot(context.Background(), c)
	if err != nil || a.Token != "SYNTHETIC_ACCESS" {
		t.Fatal("snapshot unavailable", err)
	}
	raw, _ := json.Marshal(a)
	for _, secret := range []string{"SYNTHETIC_REFRESH", "SYNTHETIC_CLIENT_SECRET", "refresh_token", "client_secret", "client_id"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("renewal material leaked into access schema")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("unnecessary refresh")
	}
	old := b.now()
	b.now = func() time.Time { return old.Add(40 * time.Minute) }
	if _, err = b.Snapshot(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("root did not refresh")
	}
	key, _ := c.key()
	dir := filepath.Join(b.root, key)
	for _, path := range []string{b.root, dir} {
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm() != 0700 {
			t.Fatal("unsafe store mode", err)
		}
	}
	st, err := os.Stat(filepath.Join(dir, "credentials.json"))
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("unsafe token mode", err)
	}
	if err = b.Logout(other); err != nil {
		t.Fatal(err)
	}
	if email, _ := b.Status(c); email == "" {
		t.Fatal("other logout affected caller")
	}
	if err = b.Logout(c); err != nil {
		t.Fatal(err)
	}
	if a, err = b.Snapshot(context.Background(), c); err != nil || a.Token != "" {
		t.Fatal("logout left token", err)
	}
}
func TestCallbackIsolationExpiryAndReplay(t *testing.T) {
	for _, kind := range []string{"caller", "installation", "team", "email", "chat", "state", "host", "expired", "denied", "replay"} {
		t.Run(kind, func(t *testing.T) {
			b, c, conv, calls := fixture(t)
			callback := begin(t, b, c, conv)
			input := c
			chat := conv
			switch kind {
			case "caller":
				input.Actor = "sl_user_U2"
			case "installation":
				input.Installation = "sl_user_B2"
			case "team":
				input.Principal.Team = "T2"
			case "email":
				input.Principal = core.SlackPrincipal("other@example.test", "T1", "")
			case "chat":
				chat.Chat = "D2"
			case "state":
				callback = strings.Replace(callback, "state=hmg_", "state=foreign_", 1)
			case "host":
				callback = strings.Replace(callback, "example.test", "evil.test", 1)
			case "expired":
				old := b.now()
				b.now = func() time.Time { return old.Add(11 * time.Minute) }
			case "denied":
				callback = strings.Replace(callback, "code=SYNTHETIC_CODE", "error=access_denied", 1)
			case "replay":
				if _, err := b.Complete(context.Background(), c, conv, callback); err != nil {
					t.Fatal(err)
				}
			}
			before := calls.Load()
			if _, err := b.Complete(context.Background(), input, chat, callback); err == nil {
				t.Fatal("callback accepted")
			}
			if calls.Load() != before {
				t.Fatal("invalid callback reached token endpoint")
			}
		})
	}
}
func TestPendingReplacedAndSameDMNewThread(t *testing.T) {
	b, c, conv, _ := fixture(t)
	first := begin(t, b, c, conv)
	second := begin(t, b, c, conv)
	if _, err := b.Complete(context.Background(), c, conv, first); err == nil {
		t.Fatal("replaced login accepted")
	}
	conv.Thread = "new-message"
	if _, err := b.Complete(context.Background(), c, conv, second); err != nil {
		t.Fatal("same DM paste rejected", err)
	}
}
func TestUserinfoAccountMismatchAndUnverified(t *testing.T) {
	for _, body := range []string{`{"id":"other","email":"owner@example.test","verified_email":true}`, `{"id":"sub","email":"employee@example.test","verified_email":false}`} {
		t.Run(body, func(t *testing.T) {
			b, c, conv, _ := fixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
			defer server.Close()
			b.userURL = server.URL
			if _, err := b.Complete(context.Background(), c, conv, begin(t, b, c, conv)); err == nil {
				t.Fatal("wrong Google account accepted")
			}
			if email, _ := b.Status(c); email != "" {
				t.Fatal("wrong Google account saved")
			}
		})
	}
}
func TestNoSecretInProviderErrors(t *testing.T) {
	b, c, conv, _ := fixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"error":"invalid_grant","error_description":"SYNTHETIC_CLIENT_SECRET SYNTHETIC_CODE SYNTHETIC_REFRESH"}`)
	}))
	defer server.Close()
	b.tokenURL = server.URL
	_, err := b.Complete(context.Background(), c, conv, begin(t, b, c, conv))
	if err == nil || strings.Contains(err.Error(), "SYNTHETIC_") {
		t.Fatal("provider error leaked")
	}
}
func TestUnsafeStoreAndMissingClientFailClosed(t *testing.T) {
	b, c, _, _ := fixture(t)
	if err := os.Chmod(b.root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Status(c); err == nil {
		t.Fatal("world-readable root accepted")
	}
	if err := os.Chmod(b.root, 0700); err != nil {
		t.Fatal(err)
	}
	key, _ := c.key()
	if err := os.Symlink(t.TempDir(), filepath.Join(b.root, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Status(c); err == nil {
		t.Fatal("caller symlink accepted")
	}
	t.Setenv(ClientSecretEnv, "")
	if _, err := New(&Config{Enabled: true, RedirectURI: b.redirect}, t.TempDir()); err == nil {
		t.Fatal("missing client accepted")
	}
}
func TestRefreshIdentityAndScopeChangesFailClosed(t *testing.T) {
	for _, kind := range []string{"identity", "scopes"} {
		t.Run(kind, func(t *testing.T) {
			b, c, conv, _ := fixture(t)
			connect(t, b, c, conv)
			old := b.now()
			b.now = func() time.Time { return old.Add(40 * time.Minute) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if kind == "identity" {
					io.WriteString(w, `{"id":"changed","email":"employee@example.test","verified_email":true}`)
				} else {
					io.WriteString(w, `{"access_token":"SYNTHETIC_ACCESS","expires_in":3600,"token_type":"Bearer","scope":"one"}`)
				}
			}))
			defer server.Close()
			if kind == "identity" {
				b.userURL = server.URL
			} else {
				b.tokenURL = server.URL
			}
			if a, err := b.Snapshot(context.Background(), c); err == nil || a.Token != "" {
				t.Fatal("changed account/scopes handed off")
			}
		})
	}
}

func TestConfigValidationAndDisabledBroker(t *testing.T) {
	for _, redirect := range []string{"http://example.test/oauth/google/callback", "https://example.test/other", "https://user@example.test/oauth/google/callback", "https://example.test/oauth/google/callback?extra=1"} {
		if err := (&Config{Enabled: true, RedirectURI: redirect}).Validate(); err == nil {
			t.Fatal("unsafe redirect accepted")
		}
	}
	if b, err := New(nil, "/must/not/be/created"); err != nil || b != nil {
		t.Fatal("disabled Google changed existing runtime")
	}
}

// Review fix: logout revokes the dropped refresh token at Google.
func TestLogoutRevokesRefreshToken(t *testing.T) {
	b, c, conv, _ := fixture(t)
	connect(t, b, c, conv)
	revoked := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		revoked <- r.Form.Get("token")
	}))
	t.Cleanup(srv.Close)
	b.revokeURL = srv.URL
	if err := b.Logout(c); err != nil {
		t.Fatal(err)
	}
	select {
	case tok := <-revoked:
		if tok != "SYNTHETIC_REFRESH" {
			t.Fatal("wrong token revoked")
		}
	default:
		t.Fatal("refresh token not revoked")
	}
	if email, err := b.Status(c); err != nil || email != "" {
		t.Fatal("still connected", email, err)
	}
}

// Review fix: one caller's slow Google call must not block another caller.
func TestSlowCallerDoesNotBlockOthers(t *testing.T) {
	b, c, conv, _ := fixture(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(500)
	}))
	t.Cleanup(slow.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	callback := begin(t, b, c, conv)
	b.tokenURL = slow.URL // only the first caller's exchange is in flight
	done := make(chan struct{})
	go func() { _, _ = b.Complete(context.Background(), c, conv, callback); close(done) }()
	<-entered
	other := c
	other.Actor = "sl_user_U2"
	finished := make(chan error, 1)
	go func() { _, err := b.Login(other, conv); finished <- err }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("other caller blocked behind a slow Google call")
	}
	close(release)
	<-done
}
