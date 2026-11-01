package slack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

type slackFile struct {
	ID, Name, Title, Mode string
	MIME                  string `json:"mimetype"`
	FileType              string `json:"filetype"`
	Size                  int64  `json:"size"`
	URL                   string `json:"url_private"`
	URLDownload           string `json:"url_private_download"`
	External              bool   `json:"is_external"`
	Tombstoned            bool   `json:"is_tombstoned"`
	Deleted               bool   `json:"is_deleted"`
	DeletedAt             int64  `json:"deleted_timestamp"`
}

func (f slackFile) deleted() bool {
	return f.Tombstoned || f.Deleted || f.DeletedAt != 0 || f.Mode == "tombstone"
}
func (f slackFile) image() bool {
	return strings.HasPrefix(strings.ToLower(f.MIME), "image/") || regexp.MustCompile(`(?i)^(png|jpg|jpeg|gif|webp)$`).MatchString(f.FileType) || regexp.MustCompile(`(?i)\.(png|jpe?g|gif|webp)$`).MatchString(first(f.Name, f.Title))
}
func slackFileHost(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" {
		return false
	}
	switch u.Hostname() {
	case "files.slack.com", "files-origin.slack.com", "files.slack-gov.com":
		return true
	}
	return false
}
func availableDisk(path string) (int64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	return int64(stat.Bavail) * stat.Bsize, err
}

var filenameChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func safeFilename(s string) string {
	s = filenameChars.ReplaceAllString(filepath.Base(strings.ReplaceAll(s, "\\", "/")), "_")
	s = clip(s, 160)
	if s == "" || s == "." || s == ".." {
		s = "file"
	}
	return s
}
func (s *Slack) download(ctx context.Context, f slackFile) core.Attachment {
	a := core.Attachment{Kind: "document", Name: safeFilename(first(f.Name, f.Title, f.ID, "file")), MIME: f.MIME, Size: f.Size}
	if f.image() {
		a.Kind = "photo"
		if !strings.HasPrefix(a.MIME, "image/") {
			ext := f.FileType
			if ext == "" {
				ext = strings.TrimPrefix(filepath.Ext(a.Name), ".")
			}
			if ext == "jpg" {
				ext = "jpeg"
			}
			if regexp.MustCompile(`^(jpeg|png|gif|webp)$`).MatchString(ext) {
				a.MIME = "image/" + ext
			}
		}
	}
	fail := func(note string) core.Attachment { a.Note = "Slack file download failed: " + note; return a }
	if f.External || f.Mode == "external" || f.FileType == "remote" || f.deleted() {
		return fail("external or unavailable file skipped")
	}
	raw := first(f.URLDownload, f.URL)
	if !s.fileHost(raw) {
		return fail("file host is not an allowed Slack file host")
	}
	if f.Size < 0 || f.Size > s.opt.MaxFileBytes {
		return fail("file exceeds configured size cap")
	}
	reservation := f.Size
	if reservation == 0 {
		reservation = s.opt.MaxFileBytes
	}
	s.diskMu.Lock()
	free, err := s.freeBytes(s.UploadDir())
	if err != nil || free-s.reserved-reservation < diskReserve {
		s.diskMu.Unlock()
		return fail("insufficient disk space with reserved free space")
	}
	s.reserved += reservation
	s.diskMu.Unlock()
	defer func() { s.diskMu.Lock(); s.reserved -= reservation; s.diskMu.Unlock() }()
	ctx, cancel := context.WithTimeout(ctx, downloadCap(s.opt.MaxFileBytes))
	defer cancel()
	idle := time.AfterFunc(s.downloadIdle(), cancel)
	defer idle.Stop()
	client := *s.bot.http
	client.Timeout = 0
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 || !s.fileHost(req.URL.String()) {
			return errors.New("untrusted file redirect")
		}
		req.Header.Set("Authorization", "Bearer "+s.opt.BotToken)
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return fail("invalid file request")
	}
	req.Header.Set("Authorization", "Bearer "+s.opt.BotToken)
	response, err := client.Do(req)
	if err != nil {
		return fail("file request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fail(fmt.Sprintf("HTTP %d", response.StatusCode))
	}
	if response.ContentLength > s.opt.MaxFileBytes {
		return fail("file exceeds configured size cap")
	}
	file, err := os.CreateTemp(s.UploadDir(), "*-"+a.Name+".part")
	if err != nil {
		return fail("private storage unavailable")
	}
	part := file.Name()
	defer os.Remove(part)
	// Limit every read; reserve cap for unknown sizes, and reject a body that
	// exceeds its declared reservation instead of consuming other downloads' space.
	limit := min(s.opt.MaxFileBytes, reservation)
	n, err := io.Copy(&downloadWriter{file: file, s: s, remaining: &reservation, idle: idle}, io.LimitReader(response.Body, limit+1))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return fail("file write failed")
	}
	if n > limit {
		return fail("file exceeds size reservation")
	}
	saved := strings.TrimSuffix(part, ".part")
	if err = os.Rename(part, saved); err != nil {
		return fail("file finalization failed")
	}
	a.Stored = saved
	a.Size = n
	return a
}
func (s *Slack) pruneUploads() {
	entries, _ := os.ReadDir(s.UploadDir())
	for _, e := range entries {
		if st, err := e.Info(); err == nil && st.Mode().IsRegular() && time.Since(st.ModTime()) > 7*24*time.Hour {
			_ = os.Remove(filepath.Join(s.UploadDir(), e.Name()))
		}
	}
}
func (s *Slack) upload(ctx context.Context, out core.Outbound) error {
	f, err := os.Open(out.File.Path)
	if err != nil {
		return errors.New("slack: outbound file unavailable")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	var grant struct {
		URL string `json:"upload_url"`
		ID  string `json:"file_id"`
	}
	api := *s.bot
	api.timeout = 60 * time.Second
	if err = api.call(ctx, "files.getUploadURLExternal", map[string]any{"filename": out.File.Name, "length": stat.Size()}, &grant); err != nil {
		return err
	}
	if !s.fileHost(grant.URL) || grant.ID == "" {
		return errors.New("slack: untrusted file upload endpoint")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, grant.URL, f)
	if err != nil {
		return errors.New("slack: invalid file upload endpoint")
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", first(out.File.MIME, mime.TypeByExtension(filepath.Ext(out.File.Name)), "application/octet-stream"))
	uploadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req = req.WithContext(uploadCtx)
	response, err := api.http.Do(req)
	if err != nil {
		return errors.New("slack: file upload failed")
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("slack: file upload rejected")
	}
	return api.call(ctx, "files.completeUploadExternal", map[string]any{"files": []map[string]string{{"id": grant.ID, "title": out.File.Name}}, "channel_id": out.Conv.Chat, "thread_ts": out.Conv.Thread}, nil)
}

func uploadReason(err error) string {
	var ae *apiError
	if errors.As(err, &ae) {
		if ae.Ambiguous {
			return "completion uncertain; check thread before retrying"
		}
		return ae.Error()
	}
	switch err.Error() {
	case "slack: outbound file unavailable":
		return "outbound file unavailable"
	case "slack: untrusted file upload endpoint", "slack: invalid file upload endpoint":
		return "untrusted upload endpoint"
	case "slack: file upload rejected":
		return "upload endpoint rejected transfer"
	}
	return "transfer could not be confirmed"
}

// At least 10 minutes, plus one second per 16 KiB of configured capacity.
func downloadCap(size int64) time.Duration {
	return 10*time.Minute + time.Duration(size/16384)*time.Second
}
func (s *Slack) downloadIdle() time.Duration {
	if s.fileIdle > 0 {
		return s.fileIdle
	}
	return 60 * time.Second
}

type downloadWriter struct {
	file      io.Writer
	s         *Slack
	remaining *int64
	idle      *time.Timer
}

func (w *downloadWriter) Write(p []byte) (int, error) {
	w.s.diskMu.Lock()
	defer w.s.diskMu.Unlock()
	n, err := w.file.Write(p)
	used := min(int64(n), *w.remaining)
	*w.remaining -= used
	w.s.reserved -= used
	if n > 0 {
		w.idle.Reset(w.s.downloadIdle())
	}
	return n, err
}

// Reserve allocation briefly; disk I/O must not hold the download lock.
func (s *Slack) WithUploadCopy(dir string, size int64, copyFile func() error) error {
	s.diskMu.Lock()
	free, err := s.freeBytes(dir)
	if err != nil {
		s.diskMu.Unlock()
		return err
	}
	var uploadStat, destStat syscall.Stat_t
	if err := syscall.Stat(s.UploadDir(), &uploadStat); err != nil {
		s.diskMu.Unlock()
		return err
	}
	if err := syscall.Stat(dir, &destStat); err != nil {
		s.diskMu.Unlock()
		return err
	}
	if s.copyReserved == nil {
		s.copyReserved = map[uint64]int64{}
	}
	dev := uint64(destStat.Dev)
	reserved := s.copyReserved[dev]
	if uploadStat.Dev == destStat.Dev {
		reserved += s.reserved
	}
	if free-reserved-size < diskReserve {
		s.diskMu.Unlock()
		return errors.New("insufficient disk space for turn copy")
	}
	s.copyReserved[dev] += size
	// Downloads on the upload filesystem must also see this reservation.
	if uploadStat.Dev == destStat.Dev {
		s.reserved += size
		s.copyReserved[dev] -= size
	}
	s.diskMu.Unlock()
	defer func() {
		s.diskMu.Lock()
		if uploadStat.Dev == destStat.Dev {
			s.reserved -= size
		} else {
			s.copyReserved[dev] -= size
		}
		s.diskMu.Unlock()
	}()
	return copyFile()
}
