package slack

// Formatting baseline: misunderstood-pi-pack ec72c7d,
// extensions/.shared/slack-mrkdwn.ts:34-54. Artifact selection is handled by
// Heimdall's checked outbox, never by model-authored path selectors.
import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

func escapeText(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

var markdownLink = regexp.MustCompile(`^\[([^\]\n]+)\]\((https?://[^\s)]+)\)`)

// mrkdwn preserves code and existing underscores. It emits angle brackets
// only for a validated HTTP(S) Markdown link; model mentions are escaped.
func mrkdwn(s string) string {
	var out strings.Builder
	for len(s) > 0 {
		if strings.HasPrefix(s, "```") {
			end := strings.Index(s[3:], "```")
			if end < 0 {
				out.WriteString(escapeText(s))
				break
			}
			end += 6
			out.WriteString(escapeText(s[:end]))
			s = s[end:]
			continue
		}
		if s[0] == '`' {
			if end := strings.IndexByte(s[1:], '`'); end >= 0 && !strings.Contains(s[1:end+1], "\n") {
				end += 2
				out.WriteString(escapeText(s[:end]))
				s = s[end:]
				continue
			}
		}
		// Process only text outside code, preserving delimiter state locally.
		end := strings.IndexByte(s, '`')
		if end < 0 {
			end = len(s)
		}
		if end == 0 {
			end = 1
		}
		out.WriteString(plainMrkdwn(s[:end]))
		s = s[end:]
	}
	return out.String()
}
func plainMrkdwn(s string) string {
	var out strings.Builder
	lineStart := true
	previous := rune(0)
	for len(s) > 0 {
		if lineStart && s[0] == '#' {
			n := 0
			for n < len(s) && s[n] == '#' {
				n++
			}
			if n <= 6 && n < len(s) && s[n] == ' ' {
				end := strings.IndexByte(s, '\n')
				if end < 0 {
					end = len(s)
				}
				out.WriteString("*" + escapeText(strings.TrimSpace(s[n:end])) + "*")
				s = s[end:]
				lineStart = false
				continue
			}
		}
		if m := markdownLink.FindStringSubmatch(s); m != nil {
			out.WriteString("<" + escapeText(m[2]) + "|" + escapeText(strings.ReplaceAll(m[1], "|", "¦")) + ">")
			s = s[len(m[0]):]
			previous = ')'
			lineStart = false
			continue
		}
		handled := false
		for _, d := range []string{"**", "__", "*"} {
			if !strings.HasPrefix(s, d) {
				continue
			}
			if d == "*" && !(previous == 0 || unicode.IsSpace(previous) || strings.ContainsRune("([{", previous)) {
				continue
			}
			rest := s[len(d):]
			end := strings.Index(rest, d)
			if end <= 0 || strings.Contains(rest[:end], "\n") || unicode.IsSpace([]rune(rest[:end])[0]) {
				continue
			}
			mark := "*"
			if d == "*" {
				mark = "_"
			}
			out.WriteString(mark + escapeText(rest[:end]) + mark)
			s = rest[end+len(d):]
			previous = '*'
			lineStart = false
			handled = true
			break
		}
		if handled {
			continue
		}
		r, size := utf8.DecodeRuneInString(s)
		out.WriteString(escapeText(string(r)))
		s = s[size:]
		previous = r
		lineStart = r == '\n'
	}
	return out.String()
}
