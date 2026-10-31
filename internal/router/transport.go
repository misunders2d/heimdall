package router

import (
	"context"
	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"strings"
)

// TurnObserver is optional; implementations enqueue updates without blocking
// the conversation loop. Native telemetry has no effect on routing/identity.
type TurnObserver interface {
	ObserveTurn(context.Context, TurnEvent)
}
type TurnEvent struct {
	ID       int64
	Conv     core.ConvKey
	Phase    string
	Progress harness.Progress
}

func (c *conv) observe(ctx context.Context, t *turn, phase string, p harness.Progress) {
	if i, ok := c.r.ifaces[c.key.Interface].(TurnObserver); ok {
		i.ObserveTurn(ctx, TurnEvent{ID: t.id, Conv: c.key, Phase: phase, Progress: p})
	}
}
func IsCommand(text string) bool {
	cmd := parseCommand(text)
	return cmd.Kind != cmdMessage || cmd.Next || cmd.Harness != ""
}
func Scrub(text string) string             { return scrub(text) }
func Untrusted(source, text string) string { return untrusted(source, text) }

// UnmentionedCommand implements the shared transport policy. Nil means the
// default; an explicit empty list means every command needs addressing.
func UnmentionedCommand(raw, normalized, bot string, allowed []string) bool {
	if allowed == nil {
		allowed = []string{"stop", "cancel"}
	}
	for _, name := range allowed {
		if name == "all" {
			return IsCommand(normalized)
		}
	}
	if !IsCommand(normalized) {
		return false
	}
	words := strings.Fields(raw)
	if len(words) != 1 {
		return false
	}
	word := strings.ToLower(words[0])
	if strings.HasPrefix(word, "!") {
		return false
	}
	word = strings.TrimPrefix(word, "/")
	if name, target, ok := strings.Cut(word, "@"); ok {
		if bot == "" || !strings.EqualFold(target, bot) {
			return false
		}
		word = name
	}
	switch word {
	case "abort", "nevermind":
		word = "cancel"
	}
	for _, name := range allowed {
		if name == word {
			return true
		}
	}
	return false
}
func NormalizeSlash(text, bot string) string {
	fields := strings.Fields(text)
	if len(fields) != 1 || !strings.HasPrefix(fields[0], "/") {
		return text
	}
	word := strings.TrimPrefix(fields[0], "/")
	if name, target, ok := strings.Cut(word, "@"); ok {
		if bot == "" || !strings.EqualFold(target, bot) {
			return text
		}
		word = name
	}
	switch strings.ToLower(word) {
	case "stop", "cancel", "reset", "chatid", "help", "status":
		return "!" + strings.ToLower(word)
	case "start":
		return "!help"
	}
	return text
}

// AnswerCleaner may remove only known per-turn path prefixes from display
// text; it never selects files. Checked outbox collection remains generic.
type AnswerCleaner interface {
	CleanAnswer(text, inDir, outDir string) string
}
