package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
)

// Caller is who a turn runs for: the interface-verified principal, the role
// it was given and the conversation the turn belongs to.
type Caller struct {
	Principal core.Principal
	Role      core.Role
	Conv      core.ConvKey
	Private   bool // the conversation is the caller's one-to-one chat with the bot
}

// identityPrefixes name variables that claim who the caller is, or what
// the process is to hub tools. Inherited ones are dropped from every harness
// environment before the turn's own are set, so neither Heimdall's
// environment nor anything a sender wrote can supply them.
// BIGQUERY_CALLER_PRINCIPAL is an older caller claim that a hub tool still
// reads.
var identityPrefixes = []string{"PI_EXTERNAL_", "A2A_CALLER_", "PI_RESTRICTED_", "BIGQUERY_CALLER_PRINCIPAL=", "PI_TRANSPORT_CHILD="}

// identityVars maps a caller to the Pi hub's identity variables, in the
// hub's names and formats (misunderstood-pi-pack ec72c7d,
// extensions/.shared/external-pi-runner.ts and external-omp-runner.ts;
// access-control.ts canonUser/canonChat), so tools that gate on the hub's
// identity read the same values. This is the only place the mapping lives;
// docs/hub-parity.md lists every hub variable and its readers.
//
// The variables tell tools who is calling. Heimdall adds no tool flags:
// PI_RESTRICTED_EXTERNAL_USER is 1 for employees and every AgentNet caller,
// as the hub sets it for callers who are not admins, and 0 for the owner.
// Every turn also gets, as the hub sets them on every child it spawns:
// PI_EXTERNAL_MCP_PRIVATE (1 only in the caller's one-to-one chat with the
// bot; MCP sign-in is refused elsewhere) and PI_TRANSPORT_CHILD=1 (the
// process is a chat turn, not the transport owner: hub extensions then run
// no scheduler runtime, chat bridge or poller of their own, and an unset
// caller is never the hub's trusted local admin).
func identityVars(c Caller) ([]string, error) {
	vars, err := callerVars(c)
	if err != nil {
		return nil, err
	}
	private := "0"
	if c.Private {
		private = "1"
	}
	vars = append(vars, "PI_EXTERNAL_MCP_PRIVATE="+private, "PI_TRANSPORT_CHILD=1")
	var delivery any
	switch c.Principal.Platform {
	case core.PlatformSlack:
		delivery = map[string]any{"platform": "slack", "channel": c.Conv.Chat, "thread_ts": c.Conv.Thread, "sessionKey": "sl_channel_" + c.Conv.Chat + "_thread_" + regexp.MustCompile(`[^A-Za-z0-9_.-]`).ReplaceAllString(firstThread(c.Conv.Thread), "_")}
	case core.PlatformTelegram:
		chat, err := strconv.ParseInt(c.Conv.Chat, 10, 64)
		if err != nil {
			return nil, errors.New("invalid Telegram chat ID")
		}
		delivery = map[string]any{"platform": "telegram", "chatId": chat, "sessionKey": "tg_chat_" + c.Conv.Chat}
	}
	if delivery != nil {
		data, _ := json.Marshal(delivery)
		vars = append(vars, "PI_EXTERNAL_DEFAULT_DELIVERY="+string(data))
	}
	for _, kv := range vars {
		if strings.ContainsFunc(kv, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
			return nil, fmt.Errorf("caller identity %s contains a control character", strings.SplitN(kv, "=", 2)[0])
		}
	}
	return vars, nil
}

// callerVars is the caller's own identity in the hub's variables.
func callerVars(c Caller) ([]string, error) {
	p := c.Principal
	if p.Platform == "" || p.ID == "" || c.Conv.Chat == "" {
		return nil, errors.New("turn without a verified caller and chat")
	}
	restricted := "1"
	if c.Role == core.RoleOwner {
		restricted = "0"
	}
	var platform, user, chat, email string
	switch p.Platform {
	case core.PlatformTelegram:
		platform, user, chat = "telegram", "tg_user_"+p.ID, "tg_chat_"+c.Conv.Chat
	case core.PlatformSlack:
		// The verified email is the Slack identity; the hub uses it as the
		// user ID when Slack supplies one.
		e := core.NormalizeEmail(p.Email)
		if e == "" {
			return nil, errors.New("slack caller without a verified email")
		}
		platform, user, chat, email = "slack", e, "sl_channel_"+c.Conv.Chat, e
	case "agentnet":
		// Inert compatibility only: AgentNet is a separate module, never
		// a Heimdall interface. Hub A2A tools fail closed without identity.
		return []string{"PI_EXTERNAL_PLATFORM=a2a", "PI_RESTRICTED_EXTERNAL_USER=1"}, nil
	default:
		// No hub equivalent (local test interface). Never a bare "local" or
		// "system": hub guards trust those as the hub's own admin.
		platform, user, chat = p.Platform, p.Platform+"_user_"+p.ID, p.Platform+"_chat_"+c.Conv.Chat
	}
	vars := []string{
		"PI_EXTERNAL_PLATFORM=" + platform,
		"PI_EXTERNAL_USER_ID=" + user,
		"PI_EXTERNAL_CHAT_ID=" + chat,
		"PI_RESTRICTED_EXTERNAL_USER=" + restricted,
	}
	if email != "" {
		vars = append(vars, "PI_EXTERNAL_USER_EMAIL="+email)
	}
	return vars, nil
}

// HubIDs are the caller's and the chat's IDs as hub tools read them
// (PI_EXTERNAL_USER_ID, PI_EXTERNAL_CHAT_ID); "" for what is not set.
func HubIDs(c Caller) (user, chat string) {
	vars, _ := callerVars(c)
	for _, kv := range vars {
		switch k, v, _ := strings.Cut(kv, "="); k {
		case "PI_EXTERNAL_USER_ID":
			user = v
		case "PI_EXTERNAL_CHAT_ID":
			chat = v
		}
	}
	return user, chat
}

// WithIdentity returns env without inherited identity variables and with
// the caller's own.
func WithIdentity(env []string, c Caller) ([]string, error) {
	vars, err := identityVars(c)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(env)+len(vars))
	for _, kv := range env {
		if !hasIdentityPrefix(kv) {
			out = append(out, kv)
		}
	}
	return append(out, vars...), nil
}

func hasIdentityPrefix(kv string) bool {
	for _, p := range identityPrefixes {
		if strings.HasPrefix(kv, p) {
			return true
		}
	}
	return false
}

func firstThread(s string) string {
	if s == "" {
		return "root"
	}
	return s
}
