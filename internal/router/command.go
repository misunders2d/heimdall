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

// parseCommand reads a message. Text that does not start with "!" followed
// by a letter is a plain message.
func parseCommand(text string) command {
	t := strings.TrimSpace(text)
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
		"!stop — cancel the current turn",
		"!use <harness> — this conversation (chat + thread) uses <harness> (no fallback) until !use default",
		"!use default — back to the default order: " + strings.Join(order, " → "),
		"!<harness> <msg> — send only this message to <harness>",
		"!next <msg> — queue as a follow-up instead of steering the running turn",
		"Harnesses: " + strings.Join(harness.Names(), ", ") + ".",
		"Messages sent while a turn runs steer it by default; replies are tagged with the harness that produced them.",
	}, "\n")
}
