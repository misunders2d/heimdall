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
	RoleOwner    Role = "owner"
	RoleEmployee Role = "employee"
)

// Mode decides who gets memory.
type Mode string

const (
	ModeCorporate Mode = "corporate" // owner and employees get memory
	ModePersonal  Mode = "personal"  // only the owner gets memory
)

// Memory reports whether a turn of role gets Memento in mode.
func Memory(mode Mode, role Role) bool { return role == RoleOwner || mode == ModeCorporate }

// Principal is a caller as verified by its interface: the platform's
// authenticated user ID, never a display name or anything a sender wrote.
type Principal struct {
	Platform string `json:"platform"`       // telegram, slack, agentnet, local
	ID       string `json:"id"`             // verified user ID; Slack: TEAM/USER
	Name     string `json:"name,omitempty"` // display only, never authority
}

// Key is the principal's access-list form, platform:id.
func (p Principal) Key() string { return p.Platform + ":" + p.ID }

// ParsePrincipalKey validates an access-list entry of the form platform:id.
func ParsePrincipalKey(key string) (Principal, error) {
	platform, id, ok := strings.Cut(key, ":")
	if !ok || !validToken(platform) || id == "" || strings.ContainsAny(id, " \t\r\n") {
		return Principal{}, fmt.Errorf("principal %q must be platform:id", key)
	}
	return Principal{Platform: platform, ID: id}, nil
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

// Access maps verified principals to roles. Anyone not listed is rejected.
type Access struct {
	Mode      Mode
	Owners    map[string]bool // principal keys
	Employees map[string]bool
}

// Role returns p's role, or false when p is unknown (fail closed).
func (a Access) Role(p Principal) (Role, bool) {
	if p.Platform == "" || p.ID == "" {
		return "", false
	}
	switch k := p.Key(); {
	case a.Owners[k]:
		return RoleOwner, true
	case a.Employees[k]:
		return RoleEmployee, true
	}
	return "", false
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
}

// RunAs is the OS account a role's turns run under.
type RunAs struct {
	User string `json:"user"`           // OS user name; owner is root in production
	Dir  string `json:"dir,omitempty"`  // harness working directory; default: the user's home
	Path string `json:"path,omitempty"` // PATH for finding and running harnesses; default: derived
}
