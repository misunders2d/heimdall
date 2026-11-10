package telegram

import (
	"net/url"
	"strings"
	"unicode/utf8"
)

// Reply markup as entities. Answers are written in Telegram's legacy
// Markdown (PromptNote), which Telegram parses itself (parse_mode
// Markdown). That mode has no spoiler and its caption limit counts the
// text after parsing, so for captions, and for replies with a spoiler,
// Heimdall parses the same markup itself, plus ||spoiler||, into plain
// text and entities (core.telegram.org/bots/api, Bot API 10.4 read
// 2026-11-10: "Markdown style"; MessageEntity offsets and lengths are
// UTF-16 code units; entities "can be specified instead of parse_mode").
//
// The legacy rules: *bold*, _italic_, `code`, ```pre``` (an optional
// language right after the opening fence), [label](url); entities do not
// nest; outside an entity '\' escapes _ * ` [ (and here |). Where Telegram
// rejects an unclosed entity, render keeps the delimiter as text: a
// caption is never refused for its markup.

// outEntity is a MessageEntity Heimdall sends.
type outEntity struct {
	Type     string `json:"type"`
	Offset   int    `json:"offset"`
	Length   int    `json:"length"`
	URL      string `json:"url,omitempty"`
	Language string `json:"language,omitempty"`
}

// render parses reply markup into plain text and its entities.
func render(s string) (string, []outEntity) {
	var b strings.Builder
	var ents []outEntity
	n := 0 // UTF-16 units written
	emit := func(t string) {
		b.WriteString(t)
		n += units(t)
	}
	add := func(typ, body, link, lang string) {
		if body == "" {
			return
		}
		ents = append(ents, outEntity{Type: typ, Offset: n, Length: units(body), URL: link, Language: lang})
		emit(body)
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s) && strings.IndexByte("_*`[|", s[i+1]) >= 0:
			emit(s[i+1 : i+2])
			i += 2
			continue
		case strings.HasPrefix(s[i:], "```"):
			rest := s[i+3:]
			if end := strings.Index(rest, "```"); end >= 0 {
				body, lang := rest[:end], ""
				if nl := strings.IndexAny(body, " \n"); nl > 0 && body[nl] == '\n' {
					lang, body = body[:nl], body[nl:]
				}
				body = strings.TrimPrefix(strings.TrimPrefix(body, "\r"), "\n")
				add("pre", body, "", lang)
				i += 3 + end + 3
				continue
			}
		case c == '`' || c == '*' || c == '_':
			if end := strings.IndexByte(s[i+1:], c); end >= 0 {
				typ := map[byte]string{'`': "code", '*': "bold", '_': "italic"}[c]
				add(typ, s[i+1:i+1+end], "", "")
				i += end + 2
				continue
			}
		case strings.HasPrefix(s[i:], "||"):
			if end := strings.Index(s[i+2:], "||"); end >= 0 {
				add("spoiler", s[i+2:i+2+end], "", "")
				i += end + 4
				continue
			}
		case c == '[':
			if end := strings.IndexByte(s[i+1:], ']'); end >= 0 {
				label, after := s[i+1:i+1+end], s[i+2+end:]
				if strings.HasPrefix(after, "(") {
					if close := strings.IndexByte(after, ')'); close >= 0 {
						link := after[1:close]
						if linkOK(link) {
							add("text_link", label, link, "")
						} else {
							emit(label)
						}
						i += 2 + end + close + 1
						continue
					}
				}
			}
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		emit(s[i : i+size])
		i += size
	}
	return b.String(), ents
}

// linkOK is a URL Telegram takes for a text_link.
func linkOK(link string) bool {
	u, err := url.Parse(link)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http" || u.Scheme == "tg") && (u.Host != "" || u.Scheme == "tg")
}

// hasSpoiler reports whether render finds a spoiler in s.
func hasSpoiler(s string) bool {
	if !strings.Contains(s, "||") {
		return false
	}
	_, ents := render(s)
	for _, e := range ents {
		if e.Type == "spoiler" {
			return true
		}
	}
	return false
}

// maxCaption is Telegram's caption limit: "0-1024 characters after
// entities parsing" (sendAnimation, sendPhoto, sendDocument).
const maxCaption = 1024

// CaptionFits reports whether a caption fits in its file's message once
// rendered (router.CaptionFitter).
func (t *Interface) CaptionFits(caption string) bool {
	plain, _ := render(caption)
	return units(plain) <= maxCaption
}
