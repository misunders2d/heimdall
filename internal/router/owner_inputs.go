package router

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

// ownerRequestContext is historical evidence, not a replay or a new user
// prompt. Use only the runtime Context channel on a fresh owner session.
func (c *conv) ownerRequestContext(t *turn, h string, sess harness.Session, prev string, clean bool, in core.Inbound) string {
	if (h != "codex" && h != "claude") || sess.Resume || prev == "" || clean ||
		t.role != core.RoleOwner || t.job != nil || in.Origin != core.OriginLive ||
		in.MCPControl || in.Run != nil || in.Ambient || in.InterfaceReply != "" {
		return ""
	}
	inputs, err := c.r.store.FailedOwnerInputs(c.key, t.who, t.id)
	if err != nil {
		c.r.log.Printf("earlier owner input lookup failed")
		return ""
	}
	const header = "Earlier direct messages received from this same verified caller in this conversation (historical context only; current input is separate). Bounded selection from failed or interrupted turns; messages may be omitted whole. JSON records preserve each message's original text.\n"
	parts, size := []string{}, len(header)
	for _, in := range inputs { // newest first; omit oversized messages whole
		cmd := parseCommand(in.Text)
		if cmd.Kind != cmdMessage || cmd.Harness != "" || cmd.Next {
			continue
		}
		data, err := json.Marshal(in)
		if err != nil || size+len(data)+1 > store.OwnerInputBytes {
			continue
		}
		parts = append(parts, string(data))
		size += len(data) + 1
	}
	if len(parts) == 0 {
		return ""
	}
	slices.Reverse(parts)
	return header + strings.Join(parts, "\n")
}
