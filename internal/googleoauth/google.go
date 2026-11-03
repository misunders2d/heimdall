// Package googleoauth keeps employee Google OAuth renewal entirely in the root
// router. Employee tools receive access-only snapshots, never a refresh/client secret.
package googleoauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

const LoginNeeded = "Send `google login` in a DM."
const ClientIDEnv = "HEIMDALL_GOOGLE_CLIENT_ID"
const ClientSecretEnv = "HEIMDALL_GOOGLE_CLIENT_SECRET"

var Scopes = []string{
	"https://www.googleapis.com/auth/drive.file",
	"https://www.googleapis.com/auth/drive.readonly",
	"https://www.googleapis.com/auth/spreadsheets",
	"https://www.googleapis.com/auth/calendar.readonly",
	"https://www.googleapis.com/auth/calendar.events",
	"https://www.googleapis.com/auth/gmail.readonly",
	"https://www.googleapis.com/auth/gmail.compose",
	"https://www.googleapis.com/auth/userinfo.email",
}

type Config struct {
	Enabled     bool   `json:"enabled"`
	RedirectURI string `json:"redirect_uri"`
}

// Validate is side-effect free, including during check-config.
func (c *Config) Validate() error {
	if c == nil || !c.Enabled {
		return nil
	}
	u, err := url.Parse(c.RedirectURI)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "/oauth/google/callback" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("employee Google redirect must be the existing HTTPS /oauth/google/callback")
	}
	return nil
}

// Caller fields come only from a verified, admitted interface, outside model control.
type Caller struct {
	Principal    core.Principal
	Actor        string
	Installation string
}

var actorRE = regexp.MustCompile(`^sl_user_[A-Za-z0-9]+$`)
var numericRE = regexp.MustCompile(`^[0-9]+$`)

func (c Caller) key() (string, error) {
	p := c.Principal
	switch p.Platform {
	case "telegram":
		if !numericRE.MatchString(p.ID) {
			return "", errors.New("Google caller is unverified")
		}
	case "slack":
		if p.Team == "" || !strings.Contains(p.Email, "@") || p.ID != core.NormalizeEmail(p.Email) || !actorRE.MatchString(c.Actor) || !actorRE.MatchString(c.Installation) {
			return "", errors.New("Google caller is unverified")
		}
	default:
		return "", errors.New("Google caller is unverified")
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{p.Key(), p.Team, c.Actor, c.Installation}, "\x00")))
	return hex.EncodeToString(sum[:]), nil
}

type pending struct {
	State    string       `json:"state"`
	Verifier string       `json:"verifier"`
	Conv     core.ConvKey `json:"conv"`
	Created  time.Time    `json:"created"`
}
type credential struct {
	Email   string    `json:"email"`
	Subject string    `json:"subject"`
	Refresh string    `json:"refresh_token"`
	Access  string    `json:"access_token"`
	Expires time.Time `json:"expires"`
	Scopes  []string  `json:"scopes"`
}

// Access deliberately has no refresh token, OAuth client ID or client secret.
type Access struct {
	Email   string    `json:"email"`
	Token   string    `json:"access_token"`
	Expires time.Time `json:"expires"`
	Scopes  []string  `json:"scopes"`
}

type Broker struct {
	locks                                  sync.Map // caller key -> *sync.Mutex
	root, clientID, clientSecret, redirect string
	client                                 *http.Client
	tokenURL, userURL, revokeURL           string
	now                                    func() time.Time
}

func New(cfg *Config, dataDir string) (*Broker, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}
	if os.Geteuid() != 0 {
		return nil, errors.New("employee Google requires the root broker")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	id, secret := os.Getenv(ClientIDEnv), os.Getenv(ClientSecretEnv)
	if id == "" || secret == "" {
		return nil, errors.New("employee Google client environment is missing")
	}
	b := &Broker{root: filepath.Join(dataDir, "google-employees"), clientID: id, clientSecret: secret, redirect: cfg.RedirectURI, tokenURL: "https://oauth2.googleapis.com/token", revokeURL: "https://oauth2.googleapis.com/revoke", userURL: "https://www.googleapis.com/oauth2/v2/userinfo", now: time.Now, client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if err := b.secureDir(b.root); err != nil {
		return nil, err
	}
	return b, nil
}

// Reject symlink components and non-root/writable credential directories. The
// caller's shared employee uid must never own any part of this store's path.
func (b *Broker) secureDir(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("Google store must be absolute")
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/") {
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if os.IsNotExist(err) {
			if err = os.Mkdir(current, 0700); err != nil && !os.IsExist(err) {
				return errors.New("Google store unavailable")
			}
			st, err = os.Lstat(current)
		}
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("Google store path is unsafe")
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		// /tmp is only permitted as a root-owned sticky test ancestor.
		if !ok || stat.Uid != 0 || st.Mode().Perm()&0022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return errors.New("Google store ownership is unsafe")
		}
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0700 {
		return errors.New("Google store must have mode 0700")
	}
	return nil
}

// lock serialises one caller's store. Google network calls run under it, so
// a global lock would stall every other caller (and Telegram's ordered
// updates) behind one slow sign-in or refresh.
func (b *Broker) lock(c Caller) func() {
	key, err := c.key()
	if err != nil {
		key = "" // unverified callers fail in dir(); share one lock
	}
	m, _ := b.locks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (b *Broker) dir(c Caller) (string, error) {
	key, err := c.key()
	if err != nil {
		return "", err
	}
	if err = b.secureDir(b.root); err != nil {
		return "", err
	}
	dir := filepath.Join(b.root, key)
	return dir, b.secureDir(dir)
}
func read(path string, v any) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return errors.New("Google credential file is unsafe")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
func write(path string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".oauth-")
	if err != nil {
		return errors.New("Google store write failed")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("Google store write failed")
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return errors.New("Google store write failed")
	}
	return nil
}
func nonce() string {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("secure random unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func (b *Broker) Login(c Caller, conv core.ConvKey) (string, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return "", err
	}
	p := pending{State: "hmg_" + nonce(), Verifier: nonce(), Conv: conv, Created: b.now()}
	if err = write(filepath.Join(dir, "pending.json"), p); err != nil {
		return "", err
	}
	challenge := sha256.Sum256([]byte(p.Verifier))
	q := url.Values{"client_id": {b.clientID}, "response_type": {"code"}, "redirect_uri": {b.redirect}, "scope": {strings.Join(Scopes, " ")}, "state": {p.State}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}, "access_type": {"offline"}, "prompt": {"consent"}}
	return "https://accounts.google.com/o/oauth2/v2/auth?" + q.Encode(), nil
}

type tokenReply struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
	Expires int    `json:"expires_in"`
	Scope   string `json:"scope"`
	Type    string `json:"token_type"`
	Error   string `json:"error"`
}

func (b *Broker) exchange(ctx context.Context, values url.Values) (tokenReply, error) {
	var t tokenReply
	values.Set("client_id", b.clientID)
	values.Set("client_secret", b.clientSecret)
	req, err := http.NewRequestWithContext(ctx, "POST", b.tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return t, errors.New("Google token request failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := b.client.Do(req)
	if err != nil {
		return t, errors.New("Google token request failed")
	}
	defer resp.Body.Close()
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&t) != nil {
		return tokenReply{}, errors.New("Google token response invalid")
	}
	if resp.StatusCode != 200 || t.Error != "" || t.Access == "" || t.Expires <= 0 || !strings.EqualFold(t.Type, "Bearer") {
		return tokenReply{}, errors.New("Google authorization failed; send `google login` in a DM")
	}
	return t, nil
}
func hasScopes(scopes []string) bool {
	for _, s := range Scopes {
		if !slices.Contains(scopes, s) {
			return false
		}
	}
	return true
}
func (b *Broker) user(ctx context.Context, token string) (string, string, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", b.userURL, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := b.client.Do(req)
	if err != nil {
		return "", "", errors.New("Google userinfo unavailable")
	}
	defer resp.Body.Close()
	var u struct {
		ID       string `json:"id"`
		Email    string `json:"email"`
		Verified bool   `json:"verified_email"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&u) != nil || !u.Verified || u.ID == "" || !strings.Contains(u.Email, "@") {
		return "", "", errors.New("Google verified email unavailable")
	}
	return core.NormalizeEmail(u.Email), u.ID, nil
}

func (b *Broker) Complete(ctx context.Context, c Caller, conv core.ConvKey, callback string) (string, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(callback)
	if err != nil {
		return "", errors.New("Google callback invalid")
	}
	base := *u
	base.RawQuery = ""
	base.Fragment = ""
	if base.String() != b.redirect || u.Fragment != "" {
		return "", errors.New("Google callback redirect does not match")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q["state"]) != 1 {
		return "", errors.New("Google callback invalid")
	}
	var p pending
	path := filepath.Join(dir, "pending.json")
	if read(path, &p) != nil || p.State != q.Get("state") || (p.Conv.Interface != conv.Interface || p.Conv.Chat != conv.Chat) || b.now().Sub(p.Created) > 10*time.Minute || b.now().Before(p.Created) {
		return "", errors.New("Google sign-in expired or belongs to another caller/chat; send `google login` again")
	}
	// Consume once before any request; errors/denial require a fresh login.
	if err = os.Remove(path); err != nil {
		return "", errors.New("Google sign-in state unavailable")
	}
	if q.Has("error") || len(q["code"]) != 1 || q.Get("code") == "" {
		return "", errors.New("Google sign-in denied; send `google login` again")
	}
	t, err := b.exchange(ctx, url.Values{"grant_type": {"authorization_code"}, "code": {q.Get("code")}, "code_verifier": {p.Verifier}, "redirect_uri": {b.redirect}})
	if err != nil {
		return "", err
	}
	scopes := strings.Fields(t.Scope)
	if t.Refresh == "" || !hasScopes(scopes) {
		return "", errors.New("Google offline access or required scopes missing; send `google login` and approve all requested access")
	}
	email, subject, err := b.user(ctx, t.Access)
	if err != nil {
		return "", err
	}
	// Slack has an independently verified email. Refuse accidentally selected
	// owner/other accounts; Telegram binds its Google identity on first consent.
	if c.Principal.Platform == "slack" && email != core.NormalizeEmail(c.Principal.Email) {
		b.revoke(t.Refresh)
		return "", errors.New("Google account must match your verified Slack email")
	}
	cred := credential{Email: email, Subject: subject, Refresh: t.Refresh, Access: t.Access, Expires: b.now().Add(time.Duration(t.Expires) * time.Second), Scopes: scopes}
	if err = write(filepath.Join(dir, "credentials.json"), cred); err != nil {
		return "", err
	}
	return email, nil
}

// revoke asks Google to invalidate a dropped refresh token. Best effort: a
// failure leaves only the local deletion, as before.
func (b *Broker) revoke(token string) {
	if token == "" || b.revokeURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", b.revokeURL, strings.NewReader(url.Values{"token": {token}}.Encode()))
	if err != nil {
		return
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	if resp, err := b.client.Do(req); err == nil {
		resp.Body.Close()
	}
}

func (b *Broker) Status(c Caller) (string, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return "", err
	}
	var cred credential
	if err = read(filepath.Join(dir, "credentials.json"), &cred); os.IsNotExist(err) {
		return "", nil
	}
	return cred.Email, err
}
func (b *Broker) Logout(c Caller) error {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return err
	}
	var cred credential
	if read(filepath.Join(dir, "credentials.json"), &cred) == nil {
		b.revoke(cred.Refresh)
	}
	for _, name := range []string{"credentials.json", "pending.json"} {
		if err = os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return errors.New("Google logout failed")
		}
	}
	return nil
}
func (b *Broker) Snapshot(ctx context.Context, c Caller) (Access, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return Access{}, err
	}
	var cred credential
	if err = read(filepath.Join(dir, "credentials.json"), &cred); os.IsNotExist(err) {
		return Access{}, nil
	}
	if err != nil {
		return Access{}, errors.New("Google credentials unavailable")
	}
	if cred.Subject == "" || cred.Email == "" || cred.Refresh == "" || !hasScopes(cred.Scopes) || c.Principal.Platform == "slack" && cred.Email != core.NormalizeEmail(c.Principal.Email) {
		return Access{}, errors.New("Google connection invalid; send `google login` in a DM")
	}
	// Every process is limited to 25 minutes; never hand it a token that could
	// expire mid-turn. Renewal and userinfo run only as root.
	if cred.Expires.Before(b.now().Add(26 * time.Minute)) {
		t, err := b.exchange(ctx, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {cred.Refresh}})
		if err != nil {
			return Access{}, err
		}
		if t.Scope != "" && !hasScopes(strings.Fields(t.Scope)) {
			return Access{}, errors.New("Google scopes changed; send `google login` in a DM")
		}
		email, sub, err := b.user(ctx, t.Access)
		if err != nil {
			return Access{}, err
		}
		if email != cred.Email || sub != cred.Subject {
			return Access{}, errors.New("Google identity changed; sign in again")
		}
		cred.Access = t.Access
		cred.Expires = b.now().Add(time.Duration(t.Expires) * time.Second)
		if t.Refresh != "" {
			cred.Refresh = t.Refresh
		}
		if err = write(filepath.Join(dir, "credentials.json"), cred); err != nil {
			return Access{}, err
		}
	}
	if cred.Access == "" || cred.Expires.Before(b.now().Add(26*time.Minute)) {
		return Access{}, errors.New("Google access lifetime too short")
	}
	return Access{Email: cred.Email, Token: cred.Access, Expires: cred.Expires, Scopes: cred.Scopes}, nil
}
