package telegram

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// maxFile is the hub's download cap, which is also the public Bot API's
// getFile limit.
const maxFile = 20 << 20

// uploadRetention is how long a download is kept at most. The router
// removes a message's files once the message is final; what is left is from
// messages a restart interrupted.
const uploadRetention = 7 * 24 * time.Hour

// attachments downloads the files m and the message it replies to carry
// (once each, as the hub's collectTelegramFileAttachments), into the upload
// directory. A file that cannot be downloaded is described instead, as the
// hub's describeAttachmentForModel does.
func (t *Interface) attachments(ctx context.Context, m *message) []core.Attachment {
	var out []core.Attachment
	seen := map[string]bool{}
	for _, c := range []struct {
		source string
		m      *message
	}{{"message", m}, {"reply_to_message", repliedTo(m)}} {
		a := detect(c.m)
		if a == nil || a.FileID == "" {
			continue
		}
		// An animation's still preview follows it: harnesses that take
		// only images see that frame, labelled as a preview, not the motion.
		for _, f := range []*attachment{a, a.Preview} {
			if f == nil || f.FileID == "" {
				continue
			}
			key := or(f.FileUniqueID, f.FileID)
			if seen[key] {
				continue
			}
			seen[key] = true
			at := core.Attachment{Kind: f.Kind, Name: safeName(f.Name), MIME: f.MIME, Size: f.Size, Source: c.source}
			stored, size, err := t.download(ctx, f)
			if err != nil {
				t.logf("message %d in chat %d: %s attachment not downloaded: %v", m.MessageID, m.Chat.ID, f.Kind, err)
				at.Note = describe(f, err)
			} else {
				at.Stored, at.Size = stored, size
			}
			out = append(out, at)
		}
	}
	return out
}

// download fetches one file into the upload directory: getFile, then the
// file itself, streamed and capped at maxFile. Errors never carry the
// token, which is part of the file URL.
func (t *Interface) download(ctx context.Context, a *attachment) (string, int64, error) {
	if t.opt.UploadDir == "" {
		return "", 0, errors.New("downloads are not configured")
	}
	if a.Size > t.opt.MaxFileBytes {
		return "", 0, fmt.Errorf("telegram reported file too large: %d bytes > %d", a.Size, t.opt.MaxFileBytes)
	}
	var f struct {
		FileSize int64  `json:"file_size"`
		FilePath string `json:"file_path"`
	}
	if err := t.call(ctx, "getFile", map[string]any{"file_id": a.FileID}, &f, 20*time.Second); err != nil {
		return "", 0, err
	}
	if f.FileSize > t.opt.MaxFileBytes {
		return "", 0, fmt.Errorf("file too large: %d bytes > %d", f.FileSize, t.opt.MaxFileBytes)
	}
	if f.FilePath == "" {
		return "", 0, errors.New("telegram getFile returned no file_path")
	}
	if err := os.MkdirAll(t.opt.UploadDir, 0o700); err != nil {
		return "", 0, err
	}
	if err := os.Chmod(t.opt.UploadDir, 0o700); err != nil {
		return "", 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	reader, err := t.fileReader(ctx, f.FilePath)
	if err != nil {
		return "", 0, err
	}
	defer reader.Close()
	name := or(a.Name, path.Base(f.FilePath))
	dst := filepath.Join(t.opt.UploadDir, fmt.Sprintf("%d-%s-%s", time.Now().UnixMilli(), randHex(4), safeName(name)))
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", 0, err
	}
	n, err := io.Copy(out, io.LimitReader(reader, t.opt.MaxFileBytes+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	switch {
	case err != nil:
		err = t.scrubbed("file download", err)
	case n > t.opt.MaxFileBytes:
		err = fmt.Errorf("file too large: more than %d bytes", t.opt.MaxFileBytes)
	}
	if err != nil {
		os.Remove(dst)
		return "", 0, err
	}
	return dst, n, nil
}

// Local files are opt-in and only trusted when getFile came from a loopback
// API. os.Root confines resolution, including symlinks, throughout open.
func localAPI(base string) bool {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}
func (t *Interface) fileReader(ctx context.Context, name string) (io.ReadCloser, error) {
	if filepath.IsAbs(name) {
		if t.opt.LocalFileRoot == "" || !localAPI(t.opt.API) {
			return nil, errors.New("absolute Telegram file path requires an approved local API root")
		}
		base := filepath.Clean(t.opt.LocalFileRoot)
		resolved, err := filepath.EvalSymlinks(base)
		if err != nil || resolved != base {
			return nil, errors.New("local Telegram file root unavailable or indirect")
		}
		rel, err := filepath.Rel(base, name)
		if err != nil || !filepath.IsLocal(rel) {
			return nil, errors.New("local Telegram file path outside approved root")
		}
		root, err := os.OpenRoot(base)
		if err != nil {
			return nil, errors.New("local Telegram file root unavailable")
		}
		defer root.Close()
		file, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, errors.New("local Telegram file unavailable or outside approved root")
		}
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			file.Close()
			return nil, errors.New("local Telegram file is not a regular file")
		}
		if info.Size() > t.opt.MaxFileBytes {
			file.Close()
			return nil, errors.New("local Telegram file too large")
		}
		return file, nil
	}
	if !filepath.IsLocal(name) || strings.ContainsAny(name, `\?#`) || path.Clean(name) != name {
		return nil, errors.New("invalid relative Telegram file path")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.opt.FileBase+t.opt.Token+"/"+name, nil)
	if err != nil {
		return nil, t.scrubbed("file download", err)
	}
	res, err := t.http.Do(req)
	if err != nil {
		return nil, t.scrubbed("file download", err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("file download HTTP %d", res.StatusCode)
	}
	return res.Body, nil
}

// pruneUploads removes downloads older than uploadRetention: messages that
// never reached a turn.
func (t *Interface) pruneUploads() {
	if t.opt.UploadDir == "" {
		return
	}
	entries, _ := os.ReadDir(t.opt.UploadDir)
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() && time.Since(info.ModTime()) > uploadRetention {
			os.Remove(filepath.Join(t.opt.UploadDir, e.Name()))
		}
	}
}

// describe is the hub's describeAttachmentForModel.
func describe(a *attachment, err error) string {
	lines := []string{
		"[Telegram file download unavailable]",
		"kind: " + a.Kind,
		"name: " + or(a.Name, "unknown"),
		"mime: " + or(a.MIME, "unknown"),
	}
	if a.Size > 0 {
		lines = append(lines, fmt.Sprintf("reported_size: %d bytes", a.Size))
	}
	if a.Duration > 0 {
		lines = append(lines, fmt.Sprintf("duration: %ds", a.Duration))
	}
	if a.Width > 0 && a.Height > 0 {
		lines = append(lines, fmt.Sprintf("dimensions: %dx%d", a.Width, a.Height))
	}
	if a.FileUniqueID != "" {
		lines = append(lines, "file_unique_id: "+a.FileUniqueID)
	}
	if err != nil {
		lines = append(lines, "error: "+err.Error())
	}
	note := "File contents unavailable. This is a download or server configuration failure, not evidence of excessive size; do not claim to have inspected the file."
	if err != nil && strings.Contains(err.Error(), "file too large") {
		note = "File exceeds the configured size limit; a smaller file is required."
	}
	lines = append(lines, "note: "+note)
	return strings.Join(lines, "\n")
}

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// safeName is the hub's safeFilename: a base name of letters, digits, dot,
// underscore and dash, at most 160 bytes.
func safeName(name string) string {
	s := unsafeName.ReplaceAllString(path.Base(strings.ReplaceAll(name, `\`, "/")), "_")
	if len(s) > 160 {
		s = s[:160]
	}
	if s == "" || s == "." || s == ".." {
		return "telegram-file"
	}
	return s
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
