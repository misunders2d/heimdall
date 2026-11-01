package router

import (
	"os"
	"regexp"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
)

// secretPatterns are the hub's outbound token patterns (misunderstood-pi-pack
// ec72c7d, extensions/.shared/access-control.ts TOKEN_PATTERNS).
var secretPatterns = regexp.MustCompile(`(?:github_pat_[A-Za-z0-9_]{20,})|(?:gh[pous]_[A-Za-z0-9]{36,})|(?:sk-[A-Za-z0-9_-]{20,})|(?:AIzaSy[A-Za-z0-9_-]{33})|(?:xoxb-[A-Za-z0-9-]+)|(?:xapp-[A-Za-z0-9-]+)|(?:[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,})`)

// secretEnv are the hub's SECRET_ENV_KEYS; every HEIMDALL_* variable counts
// too.
var secretEnv = []string{"SLACK_CLIENT_SECRET", "TELEGRAM_BOT_TOKEN", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "OPENAI_API_KEY", "OPENROUTER_API_KEY", "GOOGLE_API_KEY", "SP_API_CLIENT_SECRET", "SP_API_REFRESH_TOKEN", "ADS_API_CLIENT_SECRET", "ADS_API_REFRESH_TOKEN", "KEEPA_API_KEY", "PLANE_API_KEY", "NEO4J_PASSWORD", "PI_WEBSITE_CHANNEL_SECRET"}

func secretValues() []string {
	var out []string
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if len(v) > 8 && (strings.HasPrefix(k, "HEIMDALL_") || contains(secretEnv, k)) {
			out = append(out, v)
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// scrub redacts secrets from text sent to a chat, as the hub's scrub does
// for every outbound message: the values of Heimdall's own secret variables
// and anything shaped like a known token.
func scrub(s string) string {
	s = core.RedactMCPCallbacks(s)
	for _, v := range secretValues() {
		s = strings.ReplaceAll(s, v, "[REDACTED]")
	}
	return secretPatterns.ReplaceAllString(s, "[REDACTED]")
}

var (
	internalIDs = regexp.MustCompile(`\b(?:turn|fail)_[A-Za-z0-9_:-]+\b`)
	providerIDs = regexp.MustCompile(`(?i)\b(?:resp|req|request|trace|run|session)[_-][A-Za-z0-9_:-]{8,}\b`)
	brackets    = regexp.MustCompile(`[{}\[\]<>]`)
	spaces      = regexp.MustCompile(`\s+`)
	failureKind = []struct {
		re   *regexp.Regexp
		text string // "%s" is the cleaned reason
	}{
		{regexp.MustCompile(`timeout|timed out|deadline|abort`), "model/provider timeout — %s"},
		{regexp.MustCompile(`overload|service_unavailable|unavailable|rate limit|429`), "provider unavailable — %s"},
		{regexp.MustCompile(`server_error|server error|5\d\d`), "provider server error — %s"},
		{regexp.MustCompile(`context window|context length|too many tokens|maximum context|input exceeds|exceeds the context|token limit`), "context too large — send reset to start a fresh session"},
		{regexp.MustCompile(`enoent|eacces|eperm|lstat|mcp config|no such file or directory`), "local harness or runtime configuration error"},
		{regexp.MustCompile(`connection|network|fetch failed|econn|socket|sse`), "connection/provider stream error — %s"},
	}
)

// failureReason is the hub's user-facing failure classification
// (external-pi-runner.ts userFacingSuppressionReason): secrets and internal
// or provider IDs removed, clipped, with the kind of failure first.
func failureReason(why string) string {
	// Brackets go (the reason is shown inline) except around Heimdall's own
	// placeholders.
	raw := strings.ReplaceAll(scrub(strings.TrimSpace(why)), "[REDACTED]", "\x00")
	raw = brackets.ReplaceAllString(raw, " ")
	raw = strings.ReplaceAll(raw, "\x00", "[REDACTED]")
	raw = internalIDs.ReplaceAllString(raw, "[internal-id]")
	raw = providerIDs.ReplaceAllString(raw, "[provider-id]")
	raw = strings.TrimSpace(spaces.ReplaceAllString(raw, " "))
	if r := []rune(raw); len(r) > 180 {
		raw = string(r[:180])
	}
	if raw == "" {
		return "the harness returned no final answer"
	}
	lower := strings.ToLower(raw)
	for _, k := range failureKind {
		if k.re.MatchString(lower) {
			return strings.Replace(k.text, "%s", raw, 1)
		}
	}
	return "provider/model error — " + raw
}

func (r *Router) scrub(text string) string {
	if cfg := r.opt.OwnerSlack; cfg != nil {
		if value := os.Getenv(cfg.ClientSecretEnv); value != "" {
			text = strings.ReplaceAll(text, value, "[REDACTED]")
		}
	}
	return scrub(text)
}
