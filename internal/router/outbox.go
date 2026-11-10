package router

import (
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

// FileSender is an optional Interface that can send files: each turn then
// gets an outbox, out/ in its turn directory, and the files the harness
// leaves there are sent after its answer (docs/HANDOFF.md, outbound files).
type FileSender interface {
	SendsFiles() bool
}

// CaptionFitter is an optional FileSender that says whether a file's
// caption (reply markup) fits in the file's own message. A caption that
// does not is never cut: the file goes bare and the caption follows as a
// normal reply (splitCaptions).
type CaptionFitter interface {
	CaptionFits(caption string) bool
}

// Outbox limits: the hub's MAX_CHAT_ARTIFACTS_PER_TURN, and the Bot API's
// upload limit.
const (
	maxOutFiles     = 5
	maxOutFile      = 50 << 20
	outboxRetention = 7 * 24 * time.Hour
)

func (c *conv) sendsFiles() bool {
	f, ok := c.r.ifaces[c.key.Interface].(FileSender)
	return ok && f.SendsFiles() && c.r.opt.OutboxDir != ""
}

// outboxNote tells the harness where to leave files for the chat; "" when
// the interface sends none or the turn directory cannot be made.
func (c *conv) outboxNote(t *turn) string {
	if !c.sendsFiles() {
		return ""
	}
	dir, err := c.turnDir(t)
	if err != nil {
		c.r.log.Printf("turn %d: turn directory not created, no outbox: %v", t.id, err)
		return ""
	}
	return fmt.Sprintf("[Files: to send files to this chat, save them in %s before your final answer. Heimdall sends the files it finds there after your answer: at most %d, up to %d MB each. To send a file with a caption as one message, also save the caption text, written like your answer, as <file name>%s beside it (clip.gif and clip.gif%s); a caption too long for one message (Telegram: 1024 characters after formatting) is sent in full as a separate message right after the file. Never put anything there the chat should not see.]", dir.Outbox(), maxOutFiles, maxOutFile>>20, harness.CaptionSuffix, harness.CaptionSuffix)
}

// collect copies the files the harness left in the turn's outbox into
// Heimdall's outbox directory.
func (c *conv) collect(t *turn) ([]harness.OutFile, []string) {
	t.mu.Lock()
	dir := t.dir
	t.mu.Unlock()
	if dir == nil || !c.sendsFiles() {
		return nil, nil
	}
	files, skipped, ignored := dir.Collect(c.r.opt.OutboxDir, maxOutFiles, maxOutFile)
	if len(ignored) > 0 {
		c.r.log.Printf("turn %d: %d outbox caption(s) without a matching file ignored", t.id, len(ignored))
	}
	for i := range files {
		files[i].Caption = c.caption(t, files[i].Caption)
	}
	return files, skipped
}

// caption gives a file's caption a reply's treatment: this turn's
// directory paths removed (AnswerCleaner) and secrets scrubbed.
func (c *conv) caption(t *turn, text string) string {
	if text == "" {
		return ""
	}
	t.mu.Lock()
	dir := t.dir
	t.mu.Unlock()
	if cleaner, ok := c.r.ifaces[c.key.Interface].(AnswerCleaner); ok && dir != nil {
		text = cleaner.CleanAnswer(text, dir.Path()+"/in", dir.Outbox())
	}
	return strings.TrimSpace(c.r.scrub(text))
}

// fileReplies are the outbound rows that send a turn's collected files.
func (c *conv) fileReplies(t *turn, files []harness.OutFile) []core.Outbound {
	out := make([]core.Outbound, len(files))
	for i, f := range files {
		out[i] = core.Outbound{Conv: c.key, ReplyTo: t.replyTo, File: &core.File{Path: f.Path, Name: f.Name, MIME: mimeOf(f.Name), Caption: f.Caption}}
	}
	return out
}

// splitCaptions sends a caption its interface cannot fit in the file's
// message as a normal reply right after the file, which then goes bare.
// The store delivers a chat's rows in order, so the caption follows its
// file.
func (r *Router) splitCaptions(rows []core.Outbound) []core.Outbound {
	var out []core.Outbound
	for _, row := range rows {
		if row.File == nil || row.File.Caption == "" {
			out = append(out, row)
			continue
		}
		fit, ok := r.ifaces[row.Conv.Interface].(CaptionFitter)
		if !ok || fit.CaptionFits(row.File.Caption) {
			out = append(out, row)
			continue
		}
		file := *row.File
		caption := file.Caption
		file.Caption = ""
		row.File = &file
		out = append(out, row, core.Outbound{Window: row.Window, Conv: row.Conv, ReplyTo: row.ReplyTo, Text: caption, Formatted: true})
	}
	return out
}

func mimeOf(name string) string {
	if m := mime.TypeByExtension(strings.ToLower(filepath.Ext(name))); m != "" {
		m, _, _ = strings.Cut(m, ";")
		return m
	}
	return "application/octet-stream"
}

func removeFiles(files []harness.OutFile) {
	for _, f := range files {
		os.Remove(f.Path)
	}
}

// pruneOutbox removes copies older than outboxRetention: files whose turn
// ended without recording them.
func (r *Router) pruneOutbox() {
	if r.opt.OutboxDir == "" {
		return
	}
	entries, _ := os.ReadDir(r.opt.OutboxDir)
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() && time.Since(info.ModTime()) > outboxRetention {
			os.Remove(filepath.Join(r.opt.OutboxDir, e.Name()))
		}
	}
}
