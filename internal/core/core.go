// Package core holds Heimdall's caller identity, role, mode and message
// types. Nothing here comes from message text: interfaces construct
// principals from platform-verified data only.
package core

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Role is what a verified caller may run as.
type Role string

const (
	RoleOwner    Role = "owner"    // root: listed in the config's owners
	RoleEmployee Role = "employee" // every other verified caller: non-root
)

// Mode decides who gets memory.
type Mode string

const (
	ModeCorporate Mode = "corporate" // owner and Slack callers get memory
	ModePersonal  Mode = "personal"  // only the owner gets memory
)

// Memory reports whether a turn for p in role gets Memento in mode: always
// for the owner; in corporate mode also for Slack callers, verified in the
// owner's own workspace. Other non-root callers, Telegram users included,
// never get memory, in either mode.
func Memory(mode Mode, role Role, p Principal) bool {
	return role == RoleOwner || mode == ModeCorporate && p.Platform == PlatformSlack && p.Email != ""
}

// Platforms with identity rules of their own.
const (
	PlatformTelegram = "telegram"
	PlatformSlack    = "slack"
)

// Principal is a caller as verified by its interface: the platform's
// authenticated identity, never a display name or anything a sender wrote.
// Telegram: the numeric user ID. Slack: the email Slack verified live for a
// member of the bot's own workspace, which is the Slack identity (see
// SlackPrincipal); any other Slack caller cannot be identified. Email and
// KeyID also travel with the caller to the harness.
type Principal struct {
	Platform string `json:"platform"`         // telegram, slack, agentnet, local
	ID       string `json:"id"`               // verified identity; Slack: the email
	Name     string `json:"name,omitempty"`   // display only, never authority
	Email    string `json:"email,omitempty"`  // interface-verified email (Slack), or ""
	Team     string `json:"team,omitempty"`   // Slack workspace (team ID) that verified the caller, or ""
	KeyID    string `json:"key_id,omitempty"` // verified signing key ID (AgentNet), or ""
}

// Key is the principal's identity, platform:id: what the owner list holds
// and what sessions are kept per.
func (p Principal) Key() string { return p.Platform + ":" + p.ID }

// SlackPrincipal is a Slack caller identified by the email Slack verified
// in workspace team. Only the Slack interface's verification builds one
// for a live caller.
func SlackPrincipal(email, team, name string) Principal {
	e := NormalizeEmail(email)
	return Principal{Platform: PlatformSlack, ID: e, Name: name, Email: e, Team: team}
}

// NormalizeEmail is the form emails are compared in.
func NormalizeEmail(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// ParsePrincipalKey validates an owner-list entry of the form platform:id.
// Telegram IDs are numeric; Slack IDs are emails, returned normalized.
func ParsePrincipalKey(key string) (Principal, error) {
	platform, id, ok := strings.Cut(key, ":")
	if !ok || !validToken(platform) || id == "" || strings.ContainsAny(id, " \t\r\n") {
		return Principal{}, fmt.Errorf("principal %q must be platform:id", key)
	}
	switch platform {
	case PlatformTelegram:
		if !numeric(id) {
			return Principal{}, fmt.Errorf("principal %q: a Telegram ID is the numeric user ID", key)
		}
	case PlatformSlack:
		p := SlackPrincipal(id, "", "")
		if !strings.Contains(p.ID, "@") {
			return Principal{}, fmt.Errorf("principal %q: a Slack principal is the caller's verified email", key)
		}
		return p, nil
	}
	return Principal{Platform: platform, ID: id}, nil
}

func numeric(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// Access maps verified principals to roles: listed owners are root, every
// other caller its interface identified is non-root.
type Access struct {
	Mode   Mode
	Owners map[string]bool // principal keys
}

// Role returns p's role, or false when p cannot be identified (fail
// closed): no verified identity, a Telegram ID that is not numeric, or a
// Slack caller without a verified email that is its ID and the workspace
// that verified it.
func (a Access) Role(p Principal) (Role, bool) {
	if p.Platform == "" || p.ID == "" {
		return "", false
	}
	switch p.Platform {
	case PlatformTelegram:
		if !numeric(p.ID) {
			return "", false
		}
	case PlatformSlack:
		if p.Email == "" || p.ID != NormalizeEmail(p.Email) || p.Team == "" {
			return "", false
		}
	}
	if a.Owners[p.Key()] {
		return RoleOwner, true
	}
	return RoleEmployee, true
}

// ConvKey identifies a conversation: interface + chat + thread. One turn
// runs at a time per key.
type ConvKey struct {
	Interface string `json:"interface"`
	Chat      string `json:"chat"`
	Thread    string `json:"thread,omitempty"`
}

func (k ConvKey) String() string {
	if k.Thread == "" {
		return k.Interface + "/" + k.Chat
	}
	return k.Interface + "/" + k.Chat + "/" + k.Thread
}

// Message origins. A scheduled job carries its creator as From and is
// authorized against the access list when it fires.
const (
	OriginLive      = "live"
	OriginScheduled = "scheduled"
)

// Inbound is one message from an interface.
type Inbound struct {
	Conv   ConvKey
	MsgID  string // interface message ID, unique within Conv.Interface+Conv.Chat
	From   Principal
	Origin string
	Text   string
	At     time.Time
	// Private is set by the interface for a one-to-one chat between the
	// caller and the bot (Telegram private chat, Slack DM). Tools allow
	// private actions such as MCP sign-in only there.
	Private bool
}

func (in Inbound) Validate() error {
	switch {
	case in.Conv.Interface == "" || in.Conv.Chat == "":
		return errors.New("inbound message without interface or chat")
	case in.MsgID == "":
		return errors.New("inbound message without message ID")
	case in.From.Platform == "" || in.From.ID == "":
		return errors.New("inbound message without verified sender")
	case in.Origin != OriginLive && in.Origin != OriginScheduled:
		return fmt.Errorf("inbound message with unknown origin %q", in.Origin)
	}
	return nil
}

// Outbound is one reply to deliver.
type Outbound struct {
	Conv    ConvKey
	ReplyTo string // interface message ID being answered, if any
	Text    string
	// Formatted marks a harness's answer, written for the interface's
	// prompt note (Telegram Markdown). Heimdall's own text is plain.
	Formatted bool
}

// RunAs is the OS account a role's turns run under.
type RunAs struct {
	User string `json:"user"`           // OS user name; owner is root in production
	Dir  string `json:"dir,omitempty"`  // harness working directory; default: the user's home
	Path string `json:"path,omitempty"` // PATH for finding and running harnesses; default: derived
}
