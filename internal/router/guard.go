package router

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// The hub's prompt-injection scoring, ported from misunderstood-pi-pack
// ec72c7d, extensions/.shared/prompt-injection-guard.ts
// (scanPromptInjection, shouldBlockUserPrompt, maybeLogDirectPromptGuard).
// Heimdall only logs it (owner, 2026-10-30): a flagged message is never
// blocked or altered, and the log never holds its text.

type injectionPattern struct {
	name   string
	weight int
	re     *regexp.Regexp
}

// jsSpace is JavaScript's \s (WhiteSpace and LineTerminator); RE2's \s is
// ASCII only.
const jsSpace = `[\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]`

// hubPattern compiles one of the hub's /i patterns (JavaScript, no u flag)
// for RE2. JavaScript then folds case only between ASCII letters; RE2's
// (?i) also folds ſ to s and the Kelvin sign to k. So each ASCII letter
// becomes a class of its two cases, and \s becomes jsSpace. The hub's
// patterns have no character classes, which this does not handle.
func hubPattern(re string) *regexp.Regexp {
	var b strings.Builder
	for i := 0; i < len(re); i++ {
		switch ch := re[i]; {
		case ch == '\\' && i+1 < len(re):
			i++
			if re[i] == 's' {
				b.WriteString(jsSpace)
			} else {
				b.WriteByte(ch)
				b.WriteByte(re[i])
			}
		case ch == '[':
			panic("hubPattern: character class in " + re)
		case 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z':
			b.WriteByte('[')
			b.WriteByte(ch | 0x20)
			b.WriteByte(ch &^ 0x20)
			b.WriteByte(']')
		default:
			b.WriteByte(ch)
		}
	}
	return regexp.MustCompile(b.String())
}

var injectionPatterns = []injectionPattern{
	{"ignore-prior-instructions", 4, hubPattern(`\b(?:ignore|disregard|forget|override|bypass)\s+(?:(?:all|any|your|the)\s+)?(?:previous|prior|above|system|developer)?\s*(?:instructions?|directives?|rules?|context|prompts?|guidelines?)\b`)},
	{"role-redefinition", 3, hubPattern(`\b(?:you\s+are\s+now|act\s+as\s+if|pretend\s+you\s+are|new\s+system\s+prompt)\b`)},
	{"secret-exfiltration", 4, hubPattern(`\b(?:print|reveal|show|output|dump|exfiltrate)\s+(?:your|the|all)?\s*(?:system\s+)?(?:prompt|instructions?|rules?|secrets?|tokens?|api\s*keys?|environment|env)\b`)},
	{"instruction-tags", 3, hubPattern(`<\s*(?:system|instruction|prompt|developer)\s*>|\[INST\]|\[/INST\]|<<SYS>>|<\|im_start\|>`)},
	{"developer-impersonation", 3, hubPattern(`\b(?:important\s+message\s+from\s+the\s+developer|begin\s+admin\s+override|end\s+of\s+document\.?\s+new\s+system\s+message)\b`)},
	{"tool-control", 3, hubPattern(`\b(?:do\s+not\s+call|never\s+use|disable|bypass)\s+(?:tools?|guardrails?|safety|validation|checks?)\b`)},
	{"data-boundary-break", 2, hubPattern(`\b(?:this\s+document|this\s+page|the\s+following\s+text)\s+(?:is|contains)\s+(?:the\s+new\s+)?(?:system|developer)\s+(?:prompt|instructions?)\b`)},
}

var discussionContext = hubPattern(`\b(?:example|test|testing|detect|detection|guardrail|prompt\s+injection|what\s+is|explain|write\s+a\s+regex|scan\s+for)\b`)

// injectionFlag is the score at which the hub blocked a message; Heimdall
// logs it instead.
const injectionFlag = 4

// injectionScore is the hub's shouldBlockUserPrompt: the matched patterns'
// weights, less 3 (not below 0) for a message that seems to discuss prompt
// injection and scores under 6.
func injectionScore(text string) (int, []string) {
	score, names := 0, []string{}
	for _, p := range injectionPatterns {
		if p.re.MatchString(text) {
			score += p.weight
			names = append(names, p.name)
		}
	}
	if score > 0 && score < 6 && discussionContext.MatchString(text) {
		score = max(0, score-3)
	}
	return score, names
}

// DefaultGuardLogMax is the prompt-injection log's size before it is
// rotated to a single ".1" file: 1 MiB.
const DefaultGuardLogMax = 1 << 20

// guardLog appends one JSON line per flagged message to path, rotated to
// path+".1" before it would grow past max bytes.
type guardLog struct {
	mu   sync.Mutex
	path string
	max  int64
}

type guardEntry struct {
	TS        string   `json:"ts"`
	Interface string   `json:"interface"`
	Chat      string   `json:"chat"`
	Thread    string   `json:"thread,omitempty"`
	Caller    string   `json:"caller"`
	MsgID     string   `json:"msg_id"`
	Score     int      `json:"score"`
	Patterns  []string `json:"patterns"`
}

func (g *guardLog) write(e guardEntry) error {
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	g.mu.Lock()
	defer g.mu.Unlock()
	if fi, err := os.Stat(g.path); err == nil && fi.Size() > 0 && fi.Size()+int64(len(line)) > g.max {
		if err := os.Rename(g.path, g.path+".1"); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(g.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(line)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// guard scores an admitted message's text as the hub does and logs a
// flagged one: metadata only, never the text. The message goes on
// unchanged either way.
func (r *Router) guard(in core.Inbound) {
	if r.guardLog == nil {
		return
	}
	score, names := injectionScore(in.Text)
	if score < injectionFlag {
		return
	}
	err := r.guardLog.write(guardEntry{
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		Interface: in.Conv.Interface,
		Chat:      in.Conv.Chat,
		Thread:    in.Conv.Thread,
		Caller:    in.From.Key(),
		MsgID:     in.MsgID,
		Score:     score,
		Patterns:  names,
	})
	if err != nil {
		r.log.Printf("prompt-injection flag for message %s in %s not logged: %v", in.MsgID, in.Conv, err)
	}
}
