package router

import (
	"context"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

const mcpNotice = "MCP sign-in through Heimdall is available only to owners in private chats using Pi. Do not paste OAuth codes elsewhere."

func (r *Router) mcpControl(ctx context.Context, in *core.Inbound, role core.Role) bool {
	callback := core.MCPCallback(in.Text)
	text := strings.TrimSpace(in.Text)
	command := strings.TrimPrefix(text, "/")
	isCommand := strings.HasPrefix(command, "mcp ") && (strings.HasPrefix(command, "mcp login ") || command == "mcp logout" || strings.HasPrefix(command, "mcp logout ") || command == "mcp reconnect" || strings.HasPrefix(command, "mcp reconnect "))
	if !callback && !isCommand {
		return false
	}
	eligible := role == core.RoleOwner && in.Private && !in.Ambient && in.Origin == core.OriginLive && (in.Conv.Interface == "telegram" || in.Conv.Interface == "slack")
	if callback {
		if in.Ambient {
			return true
		}
		if eligible {
			r.runsMu.Lock()
			runs := make([]*harness.Run, 0, len(r.runs))
			for run := range r.runs {
				runs = append(runs, run)
			}
			r.runsMu.Unlock()
			for _, run := range runs {
				if run.RespondMCP(ctx, in.From, in.Conv, in.Text) {
					return true
				}
			}
		}
		// Callback never persists even if no pending login or sender is ineligible.
		if iface := r.ifaces[in.Conv.Interface]; iface != nil {
			_ = iface.Send(ctx, core.Outbound{Conv: in.Conv, ReplyTo: in.MsgID, Text: mcpNotice})
		}
		return true
	}
	if eligible {
		if strings.Contains(command, "slack") && (r.opt.OwnerSlack == nil || !r.opt.OwnerSlack.Enabled) {
			in.Text = ""
			in.InterfaceReply = "Owner Slack is disabled."
			return false
		}
		in.Text = "!pi /" + command
	} else {
		in.Text = "[Private native MCP sign-in UI omitted]"
		in.InterfaceReply = mcpNotice
	}
	return false
}
