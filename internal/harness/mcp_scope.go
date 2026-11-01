package harness

// Port of misunderstood-pi-pack af9ffff, extensions/.shared/mcp-oauth-scope.ts
// and access-control.ts. Legacy digest labels are credential-store contracts.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
)

const mcpDenyPath = "/dev/null/pi-mcp-oauth-denied"

var slackActor = regexp.MustCompile(`^sl_user_[A-Za-z0-9]+$`)
var telegramActor = regexp.MustCompile(`^tg_(?:user_)?-?[0-9]+$`)

func mcpOAuthScopePath(root, platform, actor, installation string, servers []string) string {
	actor, installation = strings.TrimSpace(actor), strings.TrimSpace(installation)
	seen := map[string]bool{}
	valid := len(servers) > 0
	for _, server := range servers {
		server = strings.TrimSpace(server)
		if seen[server] || !slices.Contains([]string{"linear", "mellanni", "slack"}, server) {
			valid = false
		}
		seen[server] = true
	}
	scope := ""
	switch {
	case platform == "slack" && slackActor.MatchString(actor) && slackActor.MatchString(installation) && valid:
		scope = "slack:" + installation + ":" + actor + ":servers=helium10"
	case platform == "telegram" && telegramActor.MatchString(actor) && valid:
		scope = "telegram:" + actor
	case platform == "" && (actor == "local" || actor == "system"):
		scope = "local:" + actor
	}
	if scope == "" {
		return mcpDenyPath
	}
	digest := sha256.Sum256([]byte(scope))
	abs, _ := filepath.Abs(root)
	return filepath.Join(abs, "sha256-"+hex.EncodeToString(digest[:]))
}

// Same alias expansion as hub access-control.ts; Slack email remains role identity.
func identityVariants(platform, id string) []string {
	id = strings.TrimSpace(id)
	if strings.Contains(id, "@") {
		id = strings.ToLower(id)
	}
	if id == "" {
		return nil
	}
	raw := id
	prefixes := []string{platform + "_user_", platform + "_chat_", platform + "_"}
	if platform == "sl" {
		prefixes = []string{"sl_user_", "sl_channel_", "sl_chat_", "sl_"}
	}
	for _, p := range prefixes {
		raw = strings.TrimPrefix(raw, p)
	}
	out := []string{id, raw, platform + "_" + raw, platform + "_user_" + raw, platform + "_chat_" + raw}
	if platform == "sl" {
		out = append(out, "sl_channel_"+raw)
	}
	return out
}
func intersects(a, b []string) bool {
	for _, v := range a {
		if slices.Contains(b, v) {
			return true
		}
	}
	return false
}
func hubLinear(env []string, home, platform, actor string) bool {
	access := envValue(env, "PI_ACCESS_DIR")
	if access == "" {
		access = filepath.Join(home, ".pi", "agent", "access")
	}
	ids := identityVariants(platform, actor)
	var identities struct {
		Principals map[string]struct {
			Aliases []string `json:"aliases"`
		} `json:"principals"`
	}
	readJSON := func(name string, v any) {
		if b, e := os.ReadFile(filepath.Join(access, name)); e == nil {
			_ = json.Unmarshal(b, v)
		}
	}
	readJSON("identities.json", &identities)
	for principal, entry := range identities.Principals {
		var expanded []string
		for _, alias := range append([]string{principal}, entry.Aliases...) {
			expanded = append(expanded, identityVariants("tg", alias)...)
			expanded = append(expanded, identityVariants("sl", alias)...)
		}
		if intersects(ids, expanded) {
			ids = append(ids, expanded...)
		}
	}
	admins := strings.Split(envValue(env, "ADMIN_USER_IDS"), ",")
	var file struct {
		Users []string `json:"users"`
	}
	readJSON("admins.json", &file)
	admins = append(admins, file.Users...)
	for _, admin := range admins {
		if intersects(ids, identityVariants(platform, admin)) {
			return true
		}
	}
	var caps map[string][]string
	readJSON("capabilities.json", &caps)
	for _, id := range ids {
		if slices.Contains(caps[id], "mcp.linear") {
			return true
		}
	}
	return false
}

func piMCPEnv(l Launch, c Caller, env []string) ([]string, error) {
	actor, installation := c.OAuthActor, c.OAuthInstallation
	platform := c.Principal.Platform
	if platform == "telegram" {
		actor = "tg_user_" + c.Principal.ID
		installation = ""
	}
	servers := []string{}
	if platform == "telegram" && telegramActor.MatchString(actor) || platform == "slack" && slackActor.MatchString(actor) && slackActor.MatchString(installation) {
		accessPlatform := "tg"
		if platform == "slack" {
			accessPlatform = "sl"
		}
		if hubLinear(env, l.Home, accessPlatform, actor) {
			servers = append(servers, "linear")
		}
		servers = append(servers, "mellanni")
		if c.SlackRead {
			servers = append(servers, "slack")
		}
	}
	root := filepath.Join(l.Home, ".pi", "agent", "mcp-oauth-users")
	// Hub's fixed root is deliberately preserved for root; other OS users must
	// never receive root's home, policy, or credentials.
	path := mcpOAuthScopePath(root, platform, actor, installation, servers)
	if l.Home != "/root" {
		// The wrapper also hardcodes its config directory unless explicitly set.
		// Never let an employee select the owner's native configuration.
		filtered := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, "PI_CODING_AGENT_DIR=") {
				filtered = append(filtered, kv)
			}
		}
		env = append(filtered, "PI_CODING_AGENT_DIR="+filepath.Join(l.Home, ".pi", "agent"))
		path = mcpDenyPath // Hub seam defaults scopeRoot to /root, independently of agentDir.
	}
	path = prepareMCPScope(l, root, path)
	if path == mcpDenyPath {
		servers = []string{}
	}
	data, _ := json.Marshal(servers)
	return append(env, "MCP_OAUTH_DIR="+path, "PI_MCP_OAUTH_ACTOR="+actor, "PI_MCP_OAUTH_INSTALLATION="+installation, "PI_MCP_OAUTH_SERVERS="+string(data)), nil
}
func prepareMCPScope(l Launch, root, path string) string {
	if path != mcpDenyPath {
		for _, dir := range []string{root, path} {
			if privateMCPDir(dir, l) != nil {
				return mcpDenyPath
			}
		}
	}
	return path
}
func privateMCPDir(path string, l Launch) error {
	uid, gid := os.Getuid(), os.Getgid()
	if l.Cred != nil {
		uid, gid = int(l.Cred.Uid), int(l.Cred.Gid)
	}
	root, err := os.OpenRoot(l.Home)
	if err != nil {
		return err
	}
	defer root.Close()
	rel, err := filepath.Rel(l.Home, path)
	if err != nil {
		return err
	}
	current := ""
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if part == ".." {
			return fmt.Errorf("MCP path escapes home")
		}
		current = filepath.Join(current, part)
		err = root.Mkdir(current, 0700)
		if err == nil {
			if err = root.Lchown(current, uid, gid); err != nil {
				return err
			}
		} else if !os.IsExist(err) {
			return err
		}
		st, err := root.Lstat(current)
		if err != nil {
			return err
		}
		sys, ok := st.Sys().(*syscall.Stat_t)
		if !st.IsDir() || !ok || int(sys.Uid) != uid || st.Mode().Perm()&0022 != 0 {
			return fmt.Errorf("MCP OAuth directory must be a real directory owned by turn uid")
		}
		if current == rel && st.Mode().Perm() != 0700 {
			return fmt.Errorf("MCP OAuth directory permissions must be 0700")
		}
	}
	return nil
}
