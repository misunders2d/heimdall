package router

import (
	"context"
	"slices"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/mcpoauth"
)

const mcpNotice = "MCP sign-in through Heimdall is available only in private DMs. DM the bot `mcp login mellanni` to sign in. Do not paste OAuth codes elsewhere."

// brokerEngines: an enabled employee engine other than Pi signs in through
// Heimdall's own broker; Pi keeps its native store (`!pi mcp login …`).
func (r *Router) brokerEngines() bool {
	return r.opt.MCP != nil && slices.ContainsFunc(r.opt.EmployeeHarnesses, func(h string) bool { return h != "pi" })
}

// mcpBroker answers an employee's broker sign-in control. It never enters a
// queue, session, log or model context; the reply carries only the link or
// a fixed status line.
func (r *Router) mcpBroker(ctx context.Context, in core.Inbound, command string, callback bool) {
	caller := mcpoauth.Caller{Principal: in.From, Actor: in.OAuthActor, Installation: in.OAuthInstallation}
	approved := harness.EmployeeMCPServers(harness.Caller{Role: core.RoleEmployee, Principal: in.From, OAuthActor: in.OAuthActor, OAuthInstallation: in.OAuthInstallation, Capabilities: r.opt.Access.Capabilities(in.From, in.Conv)})
	fields := strings.Fields(command)
	server := ""
	if len(fields) == 3 {
		server = fields[2]
	}
	var reply string
	var err error
	switch {
	case callback:
		server, err = r.opt.MCP.Complete(ctx, caller, in.Conv, strings.TrimSpace(in.Text))
		reply = server + " connected with your own account for your future turns."
	case len(fields) == 2 && fields[1] == "status":
		var connected []string
		connected, err = r.opt.MCP.Status(caller)
		reply = "No business MCP connected. DM the bot `mcp login mellanni` to sign in."
		if len(connected) > 0 {
			reply = "Connected with your own account: " + strings.Join(connected, ", ") + "."
		}
	case !slices.Contains(approved, server):
		reply = "Business MCP sign-in is available for: " + strings.Join(approved, ", ") + ". DM the bot `mcp login <server>`."
	case fields[1] == "login":
		var link string
		link, err = r.opt.MCP.Login(ctx, caller, in.Conv, server)
		reply = "Open this link and sign in to " + server + " with your own account:\n" + link + "\nThe browser then shows a page that cannot load (an address starting http://127.0.0.1); that is expected. Copy that page's entire address and paste it here in this same DM within 10 minutes, as the only text in the message. Do not post it elsewhere."
	case fields[1] == "logout":
		err = r.opt.MCP.Logout(caller, server)
		reply = server + " disconnected for future turns. Access already handed to an active turn expires with the provider's token; stop that turn to end it sooner."
	default:
		reply = "Send `mcp login <server>`, `mcp logout <server>` or `mcp status` in a DM."
	}
	if err != nil {
		reply = err.Error()
	}
	// Metadata only: never links, callbacks, codes or provider responses.
	r.log.Printf("employee MCP control: caller=%s result_ok=%t", in.From.Key(), err == nil)
	if iface := r.ifaces[in.Conv.Interface]; iface != nil {
		_ = iface.Send(ctx, core.Outbound{Conv: in.Conv, ReplyTo: in.MsgID, Text: reply})
	}
}

func (r *Router) mcpControl(ctx context.Context, in *core.Inbound, role core.Role) bool {
	callback := core.MCPCallback(in.Text)
	text := strings.TrimSpace(in.Text)
	piNative := strings.HasPrefix(text, "!pi ")
	if piNative {
		text = strings.TrimSpace(strings.TrimPrefix(text, "!pi "))
	}
	command := strings.TrimPrefix(text, "/")
	isCommand := strings.HasPrefix(command, "mcp ") && (strings.HasPrefix(command, "mcp login ") || command == "mcp logout" || strings.HasPrefix(command, "mcp logout ") || command == "mcp reconnect" || strings.HasPrefix(command, "mcp reconnect "))
	isStatus := command == "mcp status"
	if !callback && !isCommand && !isStatus {
		return false
	}
	private := in.Private && !in.Ambient && in.Origin == core.OriginLive && (in.Conv.Interface == "telegram" || in.Conv.Interface == "slack") && in.From.Platform == in.Conv.Interface
	if role == core.RoleEmployee && r.brokerEngines() && (callback && r.opt.MCP.Owns(text) || (isCommand || isStatus) && !piNative && !strings.HasPrefix(command, "mcp reconnect")) {
		if in.Ambient {
			return true
		}
		if private {
			r.mcpBroker(ctx, *in, command, callback)
			return true
		}
		if callback {
			// Never kept, never relayed: the one-time code dies here.
			if iface := r.ifaces[in.Conv.Interface]; iface != nil {
				_ = iface.Send(ctx, core.Outbound{Conv: in.Conv, ReplyTo: in.MsgID, Text: mcpNotice})
			}
			return true
		}
		in.Text = "[Private native MCP sign-in UI omitted]"
		in.InterfaceReply = mcpNotice
		return false
	}
	if isStatus {
		return false // an ordinary message where no broker route applies
	}
	eligible := (role == core.RoleOwner || role == core.RoleEmployee && slices.Contains(r.opt.EmployeeHarnesses, "pi")) && in.Private && !in.Ambient && in.Origin == core.OriginLive && (in.Conv.Interface == "telegram" || in.Conv.Interface == "slack")
	if in.Conv.Interface == "slack" && role == core.RoleOwner {
		eligible = eligible && in.SlackPrivateOwnerVerified
	}
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
	if eligible && role == core.RoleEmployee {
		fields := strings.Fields(command)
		approved := harness.EmployeeMCPServers(harness.Caller{Role: role, Principal: in.From, OAuthActor: in.OAuthActor, OAuthInstallation: in.OAuthInstallation, Capabilities: r.opt.Access.Capabilities(in.From, in.Conv)})
		eligible = len(fields) == 3 && slices.Contains(approved, fields[2])
	}
	if eligible {
		if strings.Contains(command, "slack") && (r.opt.OwnerSlack == nil || !r.opt.OwnerSlack.Enabled) {
			in.Text = ""
			in.InterfaceReply = "Owner Slack is disabled."
			return false
		}
		in.MCPControl = true
		in.Text = "!pi /" + command
	} else {
		in.Text = "[Private native MCP sign-in UI omitted]"
		in.InterfaceReply = mcpNotice
	}
	return false
}
