// Package mcpoauth keeps employee business-MCP OAuth (Mellanni, Linear) in the
// root router, independent of any harness. It mirrors internal/googleoauth:
// one sign-in per verified caller, refresh tokens and client registrations
// root-only, and every employee turn receives access-only bearer snapshots
// that each engine's native MCP configuration consumes (MEL-846).
package mcpoauth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/googleoauth"
)

// Servers are the only business MCP servers an employee may reach. The router
// approves a subset per caller (Linear needs mcp.linear).
var Servers = map[string]string{
	"mellanni": "https://mcp.bezosapp.uk/mcp",
	"linear":   "https://mcp.linear.app/mcp",
}

// DefaultRedirect is a loopback address nothing listens on: the caller copies
// the final address from the browser back into the same private DM.
const DefaultRedirect = "http://127.0.0.1:33418/callback"

const statePrefix = "hmc_"

// MinLifetime covers the 25-minute employee process limit.
const MinLifetime = 26 * time.Minute

type Config struct {
	Enabled     bool   `json:"enabled"`
	RedirectURI string `json:"redirect_uri,omitempty"`
}

func (c *Config) redirect() string {
	if c.RedirectURI == "" {
		return DefaultRedirect
	}
	return c.RedirectURI
}

// Validate is side-effect free, including during check-config.
func (c *Config) Validate() error {
	if c == nil || !c.Enabled {
		return nil
	}
	u, err := url.Parse(c.redirect())
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" || u.Path == "/" || !loopback(u.Hostname()) {
		return errors.New("employee_mcp.redirect_uri must be a loopback http address with a path")
	}
	return nil
}

func loopback(host string) bool {
	host = strings.TrimRight(strings.ToLower(host), ".")
	ip := net.ParseIP(host)
	return host == "localhost" || ip != nil && ip.IsLoopback()
}

// Caller is the verified caller; the digest rules are googleoauth's.
type Caller = googleoauth.Caller

type client struct {
	ID       string `json:"client_id"`
	Secret   string `json:"client_secret,omitempty"`
	TokenURL string `json:"token_endpoint"`
	Resource string `json:"resource"`
}
type pending struct {
	State    string       `json:"state"`
	Verifier string       `json:"verifier"`
	Server   string       `json:"server"`
	Conv     core.ConvKey `json:"conv"`
	Created  time.Time    `json:"created"`
	Client   client       `json:"client"`
}
type credential struct {
	Client  client    `json:"client"`
	Refresh string    `json:"refresh_token,omitempty"`
	Access  string    `json:"access_token"`
	Expires time.Time `json:"expires"` // zero: the server gave no lifetime
}

// Access deliberately has no refresh token or client registration.
type Access struct {
	Token   string
	Expires time.Time
}

type Broker struct {
	locks    sync.Map // caller key -> *sync.Mutex
	root     string
	redirect string
	uid      int // store owner; 0 in production
	servers  map[string]string
	client   *http.Client
	now      func() time.Time
}

func New(cfg *Config, dataDir string) (*Broker, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}
	if os.Geteuid() != 0 {
		return nil, errors.New("employee MCP requires the root broker")
	}
	return newBroker(cfg, dataDir, 0)
}

func newBroker(cfg *Config, dataDir string, uid int) (*Broker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b := &Broker{root: filepath.Join(dataDir, "mcp-employees"), redirect: cfg.redirect(), uid: uid, servers: Servers, now: time.Now,
		client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if err := b.secureDir(b.root); err != nil {
		return nil, err
	}
	return b, nil
}

// Reject symlink components and directories the shared employee uid could
// modify. Ancestors may belong only to root (or the test owner).
func (b *Broker) secureDir(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("MCP store must be absolute")
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/") {
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if os.IsNotExist(err) {
			if err = os.Mkdir(current, 0700); err != nil && !os.IsExist(err) {
				return errors.New("MCP store unavailable")
			}
			st, err = os.Lstat(current)
		}
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("MCP store path is unsafe")
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 && int(stat.Uid) != b.uid || st.Mode().Perm()&0022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return errors.New("MCP store ownership is unsafe")
		}
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0700 {
		return errors.New("MCP store must have mode 0700")
	}
	return nil
}

func (b *Broker) lock(c Caller) func() {
	key, err := c.Key()
	if err != nil {
		key = ""
	}
	m, _ := b.locks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (b *Broker) dir(c Caller) (string, error) {
	key, err := c.Key()
	if err != nil {
		return "", errors.New("MCP caller is unverified")
	}
	if err = b.secureDir(b.root); err != nil {
		return "", err
	}
	dir := filepath.Join(b.root, key)
	return dir, b.secureDir(dir)
}

func (b *Broker) read(path string, v any) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != b.uid || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return errors.New("MCP credential file is unsafe")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// renewalWrite stores a renewed credential (replaceable in tests).
var renewalWrite = write

func write(path string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".oauth-")
	if err != nil {
		return errors.New("MCP store write failed")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil || os.Rename(f.Name(), path) != nil {
		return errors.New("MCP store write failed")
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

func credPath(dir, server string) string { return filepath.Join(dir, "server-"+server+".json") }

// Every endpoint Heimdall sends a code, verifier or token to must be HTTPS.
func secureURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == ""
}

func (b *Broker) getJSON(ctx context.Context, raw string, v any) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Accept", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200 && json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(v) == nil
}

type asMeta struct {
	Issuer       string   `json:"issuer"`
	Authorize    string   `json:"authorization_endpoint"`
	Token        string   `json:"token_endpoint"`
	Registration string   `json:"registration_endpoint"`
	Methods      []string `json:"code_challenge_methods_supported"`
}

// discover follows the MCP authorization spec: protected-resource metadata
// (RFC 9728), then authorization-server metadata (RFC 8414 / OIDC).
func (b *Broker) discover(ctx context.Context, resource string) (asMeta, []string, error) {
	u, _ := url.Parse(resource)
	origin := u.Scheme + "://" + u.Host
	path := strings.TrimRight(u.Path, "/")
	type prmDoc struct {
		Resource string   `json:"resource"`
		Servers  []string `json:"authorization_servers"`
		Scopes   []string `json:"scopes_supported"`
	}
	// The metadata must name exactly this resource (RFC 9728 §3.3), so a
	// document for another resource on the same host is never used.
	var prm prmDoc
	found := false
	for _, candidate := range []string{origin + "/.well-known/oauth-protected-resource" + path, origin + "/.well-known/oauth-protected-resource"} {
		var doc prmDoc
		if b.getJSON(ctx, candidate, &doc) && doc.Resource == resource && len(doc.Servers) > 0 {
			prm, found = doc, true
			break
		}
	}
	if !found {
		return asMeta{}, nil, errors.New("MCP protected-resource metadata for this server unavailable")
	}
	issuer := prm.Servers[0]
	if !secureURL(issuer) {
		return asMeta{}, nil, errors.New("MCP authorization server is not HTTPS")
	}
	iu, _ := url.Parse(issuer)
	iorigin := iu.Scheme + "://" + iu.Host
	ip := strings.TrimRight(iu.Path, "/")
	candidates := []string{iorigin + "/.well-known/oauth-authorization-server" + ip, iorigin + "/.well-known/openid-configuration" + ip}
	if ip != "" {
		candidates = append(candidates, iorigin+ip+"/.well-known/openid-configuration")
	}
	var meta asMeta
	for _, candidate := range candidates {
		if b.getJSON(ctx, candidate, &meta) {
			break
		}
	}
	// The issuer must be the one the resource named (RFC 8414 §3.3).
	if meta.Issuer != issuer {
		return asMeta{}, nil, errors.New("MCP authorization server issuer does not match")
	}
	if !secureURL(meta.Authorize) || !secureURL(meta.Token) {
		return asMeta{}, nil, errors.New("MCP authorization server metadata unavailable")
	}
	if len(meta.Methods) > 0 && !slices.Contains(meta.Methods, "S256") {
		return asMeta{}, nil, errors.New("MCP authorization server lacks PKCE S256")
	}
	if !secureURL(meta.Registration) {
		return asMeta{}, nil, errors.New("MCP authorization server lacks dynamic client registration")
	}
	return meta, prm.Scopes, nil
}

func (b *Broker) register(ctx context.Context, endpoint string) (client, error) {
	body, _ := json.Marshal(map[string]any{"client_name": "Heimdall employee MCP", "redirect_uris": []string{b.redirect}, "grant_types": []string{"authorization_code", "refresh_token"}, "response_types": []string{"code"}, "token_endpoint_auth_method": "none"})
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return client{}, errors.New("MCP client registration failed")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return client{}, errors.New("MCP client registration failed")
	}
	defer resp.Body.Close()
	var reg struct {
		ID     string `json:"client_id"`
		Secret string `json:"client_secret"`
	}
	if resp.StatusCode/100 != 2 || json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&reg) != nil || reg.ID == "" {
		return client{}, errors.New("MCP client registration failed")
	}
	return client{ID: reg.ID, Secret: reg.Secret}, nil
}

// Login starts one caller's sign-in for an approved server and returns the
// consent link. A new login replaces any pending one of that caller.
func (b *Broker) Login(ctx context.Context, c Caller, conv core.ConvKey, server string) (string, error) {
	resource, ok := b.servers[server]
	if !ok {
		return "", errors.New("unknown MCP server")
	}
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return "", err
	}
	meta, scopes, err := b.discover(ctx, resource)
	if err != nil {
		return "", err
	}
	cl, err := b.register(ctx, meta.Registration)
	if err != nil {
		return "", err
	}
	cl.TokenURL, cl.Resource = meta.Token, resource
	p := pending{State: statePrefix + nonce(), Verifier: nonce(), Server: server, Conv: conv, Created: b.now(), Client: cl}
	if err = write(filepath.Join(dir, "pending.json"), p); err != nil {
		return "", err
	}
	challenge := sha256.Sum256([]byte(p.Verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {cl.ID}, "redirect_uri": {b.redirect}, "state": {p.State}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}, "resource": {resource}}
	if len(scopes) > 0 {
		q.Set("scope", strings.Join(scopes, " "))
	}
	sep := "?"
	if strings.Contains(meta.Authorize, "?") {
		sep = "&"
	}
	return meta.Authorize + sep + q.Encode(), nil
}

// Owns reports whether a pasted callback belongs to this broker (not Pi's).
func (b *Broker) Owns(callback string) bool {
	if b == nil {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(callback))
	if err != nil {
		return false
	}
	q, err := url.ParseQuery(u.RawQuery)
	return err == nil && len(q["state"]) == 1 && strings.HasPrefix(q.Get("state"), statePrefix)
}

type tokenReply struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
	Expires int    `json:"expires_in"`
	Type    string `json:"token_type"`
	Error   string `json:"error"`
}

func (b *Broker) exchange(ctx context.Context, cl client, values url.Values) (tokenReply, error) {
	var t tokenReply
	values.Set("client_id", cl.ID)
	values.Set("resource", cl.Resource)
	if cl.Secret != "" {
		values.Set("client_secret", cl.Secret)
	}
	if !secureURL(cl.TokenURL) {
		return t, errors.New("MCP token endpoint is not HTTPS")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", cl.TokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return t, errors.New("MCP token request failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return t, errors.New("MCP token request failed")
	}
	defer resp.Body.Close()
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&t) != nil || resp.StatusCode != 200 || t.Error != "" || t.Access == "" || !strings.EqualFold(t.Type, "Bearer") {
		return tokenReply{}, errors.New("MCP authorization failed")
	}
	return t, nil
}

// expiry: a token without expires_in counts as short-lived (renewed before
// every turn), never as valid forever.
func (b *Broker) expiry(t tokenReply) (time.Time, bool) {
	if t.Expires <= 0 {
		return b.now(), false
	}
	return b.now().Add(time.Duration(t.Expires) * time.Second), true
}

// Complete consumes the caller's pending sign-in exactly once and stores the
// tokens. It returns the server that was connected.
func (b *Broker) Complete(ctx context.Context, c Caller, conv core.ConvKey, callback string) (string, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(strings.TrimSpace(callback))
	if err != nil {
		return "", errors.New("MCP callback invalid")
	}
	base := *u
	base.RawQuery, base.Fragment = "", ""
	if base.String() != b.redirect || u.Fragment != "" {
		return "", errors.New("MCP callback address does not match")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q["state"]) != 1 || len(q["code"]) > 1 {
		return "", errors.New("MCP callback invalid")
	}
	var p pending
	path := filepath.Join(dir, "pending.json")
	if b.read(path, &p) != nil || p.State != q.Get("state") || p.Conv.Interface != conv.Interface || p.Conv.Chat != conv.Chat || b.now().Sub(p.Created) > 10*time.Minute || b.now().Before(p.Created) {
		return "", errors.New("MCP sign-in expired or belongs to another caller/chat; send `mcp login <server>` again")
	}
	if err = os.Remove(path); err != nil {
		return "", errors.New("MCP sign-in state unavailable")
	}
	if q.Has("error") || q.Get("code") == "" {
		return "", errors.New("MCP sign-in denied; send `mcp login " + p.Server + "` again")
	}
	t, err := b.exchange(ctx, p.Client, url.Values{"grant_type": {"authorization_code"}, "code": {q.Get("code")}, "code_verifier": {p.Verifier}, "redirect_uri": {b.redirect}})
	if err != nil {
		return "", err
	}
	expires, _ := b.expiry(t)
	cred := credential{Client: p.Client, Refresh: t.Refresh, Access: t.Access, Expires: expires}
	if err = write(credPath(dir, p.Server), cred); err != nil {
		return "", err
	}
	return p.Server, nil
}

// Status lists the caller's connected servers.
func (b *Broker) Status(c Caller) ([]string, error) {
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return nil, err
	}
	var out []string
	for name := range b.servers {
		var cred credential
		if b.read(credPath(dir, name), &cred) == nil && cred.Access != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out, nil
}

// Logout deletes the caller's local connection to one server. Already issued
// access tokens stay valid until the provider's expiry.
func (b *Broker) Logout(c Caller, server string) error {
	if _, ok := b.servers[server]; !ok {
		return errors.New("unknown MCP server")
	}
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		return err
	}
	for _, path := range []string{credPath(dir, server), filepath.Join(dir, "pending.json")} {
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return errors.New("MCP logout failed")
		}
	}
	return nil
}

// Snapshot returns access-only tokens for the approved servers the caller
// has connected, renewing as root when a token could expire mid-turn.
// Servers that are not connected, or cannot be renewed, are left out with
// an error each; they never fail the whole turn.
func (b *Broker) Snapshot(ctx context.Context, c Caller, servers []string) (map[string]Access, map[string]error) {
	out, failed := map[string]Access{}, map[string]error{}
	if b == nil {
		return out, failed
	}
	defer b.lock(c)()
	dir, err := b.dir(c)
	if err != nil {
		for _, s := range servers {
			failed[s] = err
		}
		return out, failed
	}
	for _, server := range servers {
		if _, ok := b.servers[server]; !ok {
			failed[server] = errors.New("unknown MCP server")
			continue
		}
		var cred credential
		path := credPath(dir, server)
		if err = b.read(path, &cred); os.IsNotExist(err) {
			continue
		} else if err != nil || cred.Access == "" || cred.Client.Resource != b.servers[server] {
			failed[server] = errors.New("MCP credentials unavailable")
			continue
		}
		// A zero expiry (older store) is treated as unknown: renew.
		if cred.Expires.Before(b.now().Add(MinLifetime)) {
			if cred.Refresh == "" {
				failed[server] = errors.New("MCP access expired; send `mcp login " + server + "` in a DM")
				continue
			}
			t, err := b.exchange(ctx, cred.Client, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {cred.Refresh}})
			if err != nil {
				failed[server] = errors.New("MCP renewal failed; send `mcp login " + server + "` in a DM")
				continue
			}
			expires, known := b.expiry(t)
			cred.Access, cred.Expires = t.Access, expires
			rotated := t.Refresh != "" && t.Refresh != cred.Refresh
			if t.Refresh != "" {
				cred.Refresh = t.Refresh
			}
			if err = renewalWrite(path, cred); err != nil {
				// A rotated refresh token that cannot be stored is lost: the
				// old one may already be revoked. Fail closed and say so.
				log.Printf("employee MCP: renewed %s credentials could not be stored (rotated=%t); caller must sign in again", server, rotated)
				failed[server] = errors.New("MCP renewal could not be saved; send `mcp login " + server + "` in a DM")
				continue
			}
			// Unknown lifetime: this fresh token serves this turn only.
			if known && cred.Expires.Before(b.now().Add(MinLifetime)) {
				failed[server] = errors.New("MCP access lifetime too short")
				continue
			}
		}
		out[server] = Access{Token: cred.Access, Expires: cred.Expires}
	}
	return out, failed
}

// NewLocal is for tests in other packages: a store owned by the current uid,
// a given HTTP client and server URLs. Production uses New.
func NewLocal(dataDir string, client *http.Client, servers map[string]string) (*Broker, error) {
	b, err := newBroker(&Config{Enabled: true}, dataDir, os.Getuid())
	if err != nil {
		return nil, err
	}
	b.client, b.servers = client, servers
	return b, nil
}
