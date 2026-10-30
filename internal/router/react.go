package router

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
)

// Reactor is an optional Interface whose messages the harness may react
// to, as the hub's Telegram reactions: each message the harness is given
// gets a random ref and the interface's note, and an answer may end with
// [react: <ref> <emoji>] lines.
type Reactor interface {
	// ReactionNote tells the harness how to react to one message.
	ReactionNote(ref string) string
	// Reaction normalizes an emoji and reports whether the interface can
	// set it as a reaction.
	Reaction(emoji string) (string, bool)
}

// newRef is a random UUID: a message ref the harness cannot guess or copy
// from earlier text.
func newRef() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// react gives each live message in items its reaction note, and the turn a
// ref for it. An album's parts share one ref, which targets the first part.
// It returns the refs added.
func (c *conv) react(t *turn, items []item) []string {
	r, ok := c.r.ifaces[c.key.Interface].(Reactor)
	if !ok {
		return nil
	}
	var added []string
	album := map[string]string{}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.refs == nil {
		t.refs = map[string]string{}
	}
	for i := range items {
		row := items[i].row
		if row.Origin != core.OriginLive {
			continue
		}
		ref := album[row.Group]
		if ref == "" {
			ref = newRef()
			t.refs[ref] = row.MsgID
			added = append(added, ref)
			if row.Group != "" {
				album[row.Group] = ref
			}
		}
		items[i].react = r.ReactionNote(ref)
	}
	return added
}

func (c *conv) unreact(t *turn, refs []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ref := range refs {
		delete(t.refs, ref)
	}
}

// answerReactions takes the reaction directives off the end of each answer
// of a turn (the prompt's, then each steer's), so a directive that ended
// one answer counts though another answer follows it. A message reacted to
// in an earlier answer keeps that reaction. It returns the answers' text,
// joined.
func answerReactions(answers []string, refs map[string]string, valid func(string) (string, bool)) (string, bool, []core.Reaction) {
	var texts []string
	var out []core.Reaction
	found := false
	seen := map[string]bool{}
	for _, a := range answers {
		text, directives, rs := reactions(strings.TrimSpace(a), refs, valid)
		found = found || directives
		if text != "" {
			texts = append(texts, text)
		}
		for _, r := range rs {
			if !seen[r.MsgID] {
				seen[r.MsgID] = true
				out = append(out, r)
			}
		}
	}
	return strings.Join(texts, "\n\n"), found, out
}

var reactLine = regexp.MustCompile(`^\[react: ([a-f0-9-]{36}) ([^\s\p{Z}\[\]]+)\]$`)

// reactions takes the hub's reaction directives off the end of an answer:
// the lines starting with "[react:" are always removed; they become
// reactions only if they are all the answer's last lines (outside a code
// block) and every one names a ref of this turn, an emoji the interface
// takes, and a message not reacted to yet. Otherwise none is set.
func reactions(text string, refs map[string]string, valid func(string) (string, bool)) (string, bool, []core.Reaction) {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	first := -1
	var kept []string
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "[react:") {
			if first < 0 {
				first = i
			}
			continue
		}
		kept = append(kept, line)
	}
	if first < 0 {
		return text, false, nil
	}
	clean := strings.TrimRight(strings.Join(kept, "\n"), " \t\r\n")
	fences := 0
	for _, line := range lines[:first] {
		if l := strings.TrimLeft(line, " \t"); strings.HasPrefix(l, "```") || strings.HasPrefix(l, "~~~") {
			fences++
		}
	}
	if fences%2 != 0 {
		return clean, true, nil
	}
	var out []core.Reaction
	seen := map[string]bool{}
	for _, line := range lines[first:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := reactLine.FindStringSubmatch(line)
		if m == nil {
			return clean, true, nil
		}
		msg, ok := refs[m[1]]
		emoji, valid := valid(m[2])
		if !ok || !valid || seen[msg] {
			return clean, true, nil
		}
		seen[msg] = true
		out = append(out, core.Reaction{MsgID: msg, Emoji: emoji})
	}
	return clean, true, out
}
