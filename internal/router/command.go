package router

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/misunders2d/heimdall/internal/harness"
)

// Command kinds. Every interface uses the ! prefix: Slack forbids / in threads.
const (
	cmdMessage = "message" // plain text, or !<harness> / !next with Text
	cmdHelp    = "help"
	cmdStatus  = "status"
	cmdStop    = "stop"
	cmdCancel  = "cancel" // clear queued messages
	cmdReset   = "reset"  // forget the caller's native sessions here
	cmdChatID  = "chatid" // show the conversation's and caller's IDs
	cmdUse     = "use"
	cmdInvalid = "invalid" // answered with Error, never sent to a harness
)

type command struct {
	Kind    string
	Harness string // !<harness> <msg>: this message only; !use <harness>: sticky ("" with !use default)
	Next    bool   // !next <msg>: follow-up instead of steering
	Text    string
	Error   string
}

// controlWords are whole messages that act as commands without the "!", as
// in the hub's chat control (chat-control.ts isExactStopCommand, Telegram
// handleCommand): case-insensitive, nothing else in the message.
var controlWords = map[string]string{
	"stop":      cmdStop,
	"cancel":    cmdCancel,
	"abort":     cmdCancel,
	"nevermind": cmdCancel,
	"reset":     cmdReset,
	"chatid":    cmdChatID,
}

// parseCommand reads a message. A control word alone is its command; other
// text that does not start with "!" followed by a letter is a plain message.
func parseCommand(text string) command {
	t := strings.TrimSpace(text)
	if kind, ok := controlWords[strings.ToLower(t)]; ok {
		return command{Kind: kind}
	}
	if len(t) < 2 || t[0] != '!' || !isLetter(t[1]) {
		return command{Kind: cmdMessage, Text: text}
	}
	word, rest := t[1:], ""
	if i := strings.IndexFunc(word, unicode.IsSpace); i >= 0 {
		word, rest = word[:i], word[i:]
	}
	word, rest = strings.ToLower(word), strings.TrimSpace(rest)
	switch word {
	case "help":
		return command{Kind: cmdHelp}
	case "status":
		return command{Kind: cmdStatus}
	case "stop":
		return command{Kind: cmdStop}
	case "cancel":
		return command{Kind: cmdCancel}
	case "reset":
		return command{Kind: cmdReset}
	case "chatid":
		return command{Kind: cmdChatID}
	case "use":
		switch arg := strings.ToLower(rest); {
		case arg == "":
			return command{Kind: cmdInvalid, Error: "Usage: !use <harness> or !use default (harnesses: " + strings.Join(harness.Names(), ", ") + ")."}
		case arg == "default":
			return command{Kind: cmdUse}
		case harness.Known(arg):
			return command{Kind: cmdUse, Harness: arg}
		default:
			return command{Kind: cmdInvalid, Error: fmt.Sprintf("Unknown harness %q. Harnesses: %s.", arg, strings.Join(harness.Names(), ", "))}
		}
	case "next":
		if rest == "" {
			return command{Kind: cmdInvalid, Error: "Usage: !next <message>."}
		}
		return command{Kind: cmdMessage, Next: true, Text: rest}
	}
	if harness.Known(word) {
		if rest == "" {
			return command{Kind: cmdInvalid, Error: fmt.Sprintf("Usage: !%s <message>.", word)}
		}
		return command{Kind: cmdMessage, Harness: word, Text: rest}
	}
	return command{Kind: cmdInvalid, Error: fmt.Sprintf("Unknown command !%s. Send !help for the list.", word)}
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

func helpText(order []string) string {
	return strings.Join([]string{
		"Heimdall commands:",
		"!help — this list",
		"!status — what is running and queued in this conversation (chat + thread)",
		"!stop (or stop) — stop the current turn and clear your queued messages",
		"!cancel (or cancel, abort, nevermind) — clear your queued messages; the turn keeps running",
		"!reset (or reset) — your next message here starts a fresh session (stops your running turn)",
		"!chatid (or chatid) — show this conversation's and your IDs",
		"!use <harness> — this conversation (chat + thread) uses <harness> (no fallback) until !use default",
		"!use default — back to the default order: " + strings.Join(order, " → "),
		"!<harness> <msg> — send only this message to <harness>",
		"!next <msg> — queue as a follow-up instead of steering the running turn",
		"Harnesses: " + strings.Join(harness.Names(), ", ") + ".",
		"Messages sent while a turn runs steer it by default; replies are tagged with the harness that produced them.",
		"The owner's stop and cancel clear everyone's queued messages; only the turn's sender or the owner can stop it.",
	}, "\n")
}
