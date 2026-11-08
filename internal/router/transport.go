package router

import (
	"context"
	"path"
	"regexp"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
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
	if t.job != nil {
		return // scheduled runs show no progress
	}
	if i, ok := c.r.ifaces[c.key.Interface].(TurnObserver); ok {
		p.Text, p.Tool, p.Phase = c.r.scrub(p.Text), c.r.scrub(p.Tool), c.r.scrub(p.Phase)
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
	// help/status/which require their command spelling; bare words remain prompts.
	if !IsCommand(normalized) {
		return false
	}
	words := strings.Fields(raw)
	if len(words) != 1 {
		return false
	}
	word := strings.ToLower(words[0])
	if strings.HasPrefix(word, "!") {
		word = strings.TrimPrefix(word, "!")
		if word != "help" && word != "status" && word != "which" {
			return false
		}
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
	case "abort", "nevermind":
		return "!cancel"
	case "stop", "cancel", "reset", "chatid", "help", "status", "which":
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

var emptyFence = regexp.MustCompile("```[A-Za-z0-9_-]*\\s*\\n\\s*```")

// CleanTurnPaths is the AnswerCleaner contract shared by interfaces: a line
// that is only a file directly in outDir (sent as a real attachment) goes;
// elsewhere the exact inDir/outDir prefix goes, keeping the file name, and
// the bare directory becomes its base name. Only whole path components
// match, so other paths and similarly named directories stay. Fences left
// empty by this cleanup go too.
func CleanTurnPaths(text, inDir, outDir string) string {
	original := text
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		bare := strings.Trim(strings.TrimSpace(line), "`'\".,;:")
		if outDir != "" && strings.HasPrefix(bare, outDir+"/") && !strings.ContainsAny(strings.TrimPrefix(bare, outDir+"/"), "/\n") {
			continue
		}
		kept = append(kept, line)
	}
	text = strings.Join(kept, "\n")
	for _, prefix := range []string{inDir, outDir} {
		if prefix != "" {
			text = cutPrefix(text, prefix)
		}
	}
	if text != original {
		text = emptyFence.ReplaceAllString(text, "")
	}
	return strings.TrimSpace(text)
}

// pathByte reports whether b continues a path component.
func pathByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.IndexByte("._~-/", b) >= 0
}

func cutPrefix(text, prefix string) string {
	var b strings.Builder
	for {
		i := strings.Index(text, prefix)
		if i < 0 {
			b.WriteString(text)
			return b.String()
		}
		end := i + len(prefix)
		switch {
		case i > 0 && pathByte(text[i-1]):
			b.WriteString(text[:end]) // inside a longer path
		case end < len(text) && text[end] == '/':
			b.WriteString(text[:i])
			end++
		case end == len(text) || !pathByte(text[end]):
			b.WriteString(text[:i] + path.Base(prefix))
		default:
			b.WriteString(text[:end]) // a similarly named directory
		}
		text = text[end:]
	}
}

// IsControl uses the same parser as router admission.
func IsControl(text string) bool { k := parseCommand(text).Kind; return k == cmdStop || k == cmdCancel }

// IsTurnInput shares the router command classification with ingress fences.
func IsTurnInput(text string) bool { return parseCommand(text).Kind == cmdMessage }
