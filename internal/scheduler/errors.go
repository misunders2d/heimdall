package scheduler

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
)

// Ported from router/text.go's outbound scrub/failureReason. Keep the same
// token/env, callback, ID and 180-rune bounds without importing the router.
var (
	errorSecrets = regexp.MustCompile(`(?:github_pat_[A-Za-z0-9_]{20,})|(?:gh[pous]_[A-Za-z0-9]{36,})|(?:sk-[A-Za-z0-9_-]{20,})|(?:AIzaSy[A-Za-z0-9_-]{33})|(?:xoxb-[A-Za-z0-9-]+)|(?:xapp-[A-Za-z0-9-]+)|(?:[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,})`)
	errorIDs     = regexp.MustCompile(`(?i)\b(?:(?:turn|fail)_[A-Za-z0-9_:-]+|(?:resp|req|request|trace|run|session)[_-][A-Za-z0-9_:-]{8,})\b`)
	errorURLs    = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s<>|"']+`)
	errorHeaders = regexp.MustCompile(`(?i)\b(?:authorization|cookie|set-cookie)["']?\s*[:=]\s*[^\r\n]+`)
	errorAuth    = regexp.MustCompile(`(?i)\b(?:bearer\s+|(?:token|api[_ -]?key|secret|password)(?:["']?\s*[=:]\s*["']?|\s+))[^\s,;"']+`)
	errorEcho    = regexp.MustCompile(`(?i)\b(?:prompt|input|messages|transcript)["']?\s*[:=].*`)
	errorBraces  = regexp.MustCompile(`[{}\[\]<>]`)
	errorSpaces  = regexp.MustCompile(`\s+`)
)

var errorSecretEnv = []string{"SLACK_CLIENT_SECRET", "TELEGRAM_BOT_TOKEN", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "OPENAI_API_KEY", "OPENROUTER_API_KEY", "GOOGLE_API_KEY", "SP_API_CLIENT_SECRET", "SP_API_REFRESH_TOKEN", "ADS_API_CLIENT_SECRET", "ADS_API_REFRESH_TOKEN", "KEEPA_API_KEY", "PLANE_API_KEY", "NEO4J_PASSWORD", "PI_WEBSITE_CHANNEL_SECRET"}

// Native details are presentation only, never input to retry policy.
func causeLabel(cause string) string {
	label, _, _ := strings.Cut(cause, " — ")
	return label
}

// failureCause stores only the error field, never the answer or transcript.
// Keep the first line, redact before clipping (including all URLs, which may
// carry callbacks or signatures), and omit any echoed job prompt.
func failureCause(why, prompt string) string {
	// Short generic prompts can be substrings of otherwise useful errors.
	if len(prompt) >= 8 {
		why = strings.ReplaceAll(why, prompt, "[job input omitted]")
	}
	why = strings.ReplaceAll(why, `\/`, `/`)
	why = core.RedactMCPCallbacks(why)
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if len(v) > 8 && (strings.HasPrefix(k, "HEIMDALL_") || slices.Contains(errorSecretEnv, k)) {
			why = strings.ReplaceAll(why, v, "[REDACTED]")
		}
	}
	why = errorURLs.ReplaceAllString(why, "[REDACTED]")
	kind := category(why)
	why = errorHeaders.ReplaceAllString(why, "[REDACTED]")
	why = errorAuth.ReplaceAllString(why, "[REDACTED]")
	why = errorSecrets.ReplaceAllString(why, "[REDACTED]")
	why = errorEcho.ReplaceAllString(why, "[job input omitted]")
	why = strings.TrimSpace(why)
	if i := strings.IndexAny(why, "\r\n\u2028\u2029"); i >= 0 {
		why = why[:i]
	}
	why = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, why)
	// Preserve redaction placeholders while dropping native JSON/markup.
	why = strings.ReplaceAll(why, "[REDACTED]", "\x00")
	why = errorBraces.ReplaceAllString(why, " ")
	why = strings.ReplaceAll(why, "\x00", "[REDACTED]")
	why = errorIDs.ReplaceAllString(why, "[internal-id]")
	why = strings.TrimSpace(errorSpaces.ReplaceAllString(why, " "))
	full := why
	if r := []rune(why); len(r) > 180 {
		why = string(r[:180])
	}
	// The existing durable cause column clips at 300 bytes; leave room for
	// the class and startup prefix without splitting a UTF-8 character.
	for len(why) > 230 {
		_, n := utf8.DecodeLastRuneInString(why)
		why = why[:len(why)-n]
	}
	if why != full {
		why += "…"
	}
	if why == "" {
		return kind
	}
	return kind + " — " + why
}
