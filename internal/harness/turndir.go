package harness

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TurnsDir is where turn directories live, relative to the run-as user's
// home, next to the pi/omp session directories.
const TurnsDir = ".local/state/heimdall/turns"

// TurnRetention is how long a turn directory is kept: a resumed session may
// still refer to its files.
const TurnRetention = 7 * 24 * time.Hour

// TurnDir is one turn's directory, ~/.local/state/heimdall/turns/NAME in
// the turn's OS user's home: in/ holds copies of the message's attachments,
// out/ the files the harness leaves to be sent to the chat.
// Everything Heimdall creates there belongs to that user, so the harness can
// read it. Heimdall works on the user's home only through os.Root, so a
// symbolic link the user made can never send it outside that home.
type TurnDir struct {
	collectMu sync.Mutex
	collected map[string][32]byte

	home string
	rel  string // relative to home, slash-separated
	uid  int    // -1: Heimdall's own user, nothing to hand over
	gid  int
}

// NewTurnDir creates a fresh turn directory for a turn launched with l, and
// removes turn directories older than TurnRetention.
func NewTurnDir(l Launch, name string) (*TurnDir, error) {
	if name == "" || name != path.Base(name) || name == "." || name == ".." {
		return nil, fmt.Errorf("invalid turn directory name %q", name)
	}
	d := &TurnDir{home: l.Home, rel: path.Join(TurnsDir, name), uid: -1, gid: -1}
	if l.Cred != nil {
		d.uid, d.gid = int(l.Cred.Uid), int(l.Cred.Gid)
	}
	root, err := os.OpenRoot(l.Home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for _, p := range []string{".local", ".local/state", ".local/state/heimdall", TurnsDir} {
		if err := d.mkdir(root, p, false); err != nil {
			return nil, err
		}
	}
	d.prune(root)
	for _, p := range []string{d.rel, d.rel + "/in", d.rel + "/out"} {
		if err := d.mkdir(root, p, true); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// mkdir creates p for the turn's user. An existing p is used only when it
// need not be fresh.
func (d *TurnDir) mkdir(root *os.Root, p string, fresh bool) error {
	err := root.Mkdir(p, 0o700)
	switch {
	case err == nil:
		if d.uid >= 0 {
			return root.Lchown(p, d.uid, d.gid)
		}
		return nil
	case !fresh && errors.Is(err, fs.ErrExist):
		return nil
	}
	return err
}

// prune removes turn directories older than TurnRetention; best effort.
func (d *TurnDir) prune(root *os.Root) {
	f, err := root.Open(TurnsDir)
	if err != nil {
		return
	}
	entries, _ := f.ReadDir(-1)
	f.Close()
	for _, e := range entries {
		if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > TurnRetention {
			_ = root.RemoveAll(path.Join(TurnsDir, e.Name()))
		}
	}
}

// Outbox is the absolute path of out/.
func (d *TurnDir) Outbox() string { return filepath.Join(d.Path(), "out") }

// OutFile is a file collected from out/: Heimdall's own copy.
type OutFile struct {
	Path    string // the copy, in Heimdall's directory
	Name    string // the name the harness gave it
	Size    int64
	Caption string // from the file's CaptionSuffix sidecar, if any; in reply markup
}

// CaptionSuffix names a caption sidecar: out/<name>.caption holds the
// caption of out/<name>. A sidecar is never a file to send and never
// counts toward the file limit.
const CaptionSuffix = ".caption"

// MaxCaption is the largest caption sidecar Collect reads.
const MaxCaption = 16 << 10

// Collect copies the files the harness left in out/ into dst, Heimdall's
// own directory: top level only, in name order, at most max files, each 1
// to limit bytes. A file counts only if, once opened (through os.Root, so
// within the user's home), it is a regular file owned by the turn's user
// with a single link: never a hard link to a file the user cannot read.
// Dot files are ignored; every other file not taken is listed in skipped
// as "name: reason". A file's caption sidecar (CaptionSuffix) is read with
// the same checks, at most MaxCaption bytes, and set as its Caption; a
// sidecar that fails them is listed in skipped and its file goes without a
// caption. Sidecars whose file is not in out/ are listed in ignored.
func (d *TurnDir) Collect(dst string, max int, limit int64) (files []OutFile, skipped, ignored []string) {
	d.collectMu.Lock()
	defer d.collectMu.Unlock()
	if d.collected == nil {
		d.collected = map[string][32]byte{}
	}
	root, err := os.OpenRoot(d.home)
	if err != nil {
		return nil, []string{"outbox: " + err.Error()}, nil
	}
	defer root.Close()
	dir, err := root.Open(path.Join(d.rel, "out"))
	if err != nil {
		return nil, []string{"outbox: " + err.Error()}, nil
	}
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return nil, []string{"outbox: " + err.Error()}, nil
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	uid := d.uid
	if uid < 0 {
		uid = os.Getuid()
	}
	present := map[string]bool{}
	for _, e := range entries {
		present[e.Name()] = true
	}
	for _, e := range entries {
		name := e.Name()
		if base, ok := strings.CutSuffix(name, CaptionSuffix); ok && !strings.HasPrefix(name, ".") && (base == "" || strings.HasPrefix(base, ".") || strings.HasSuffix(base, CaptionSuffix) || !present[base]) {
			ignored = append(ignored, name)
		}
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") || strings.HasSuffix(name, CaptionSuffix) {
			continue
		}
		if len(files) == max {
			skipped = append(skipped, fmt.Sprintf("%s: more than %d files", name, max))
			continue
		}
		f, err := d.collect(root, dst, name, uid, limit)
		if err != nil {
			skipped = append(skipped, name+": "+err.Error())
			continue
		}
		copy, err := os.Open(f.Path)
		if err != nil {
			os.Remove(f.Path)
			skipped = append(skipped, name+": cannot check copied file")
			continue
		}
		hash := sha256.New()
		_, err = io.Copy(hash, copy)
		copy.Close()
		if err != nil {
			os.Remove(f.Path)
			skipped = append(skipped, name+": cannot check copied file")
			continue
		}
		var sum [32]byte
		copySum := hash.Sum(nil)
		for i := range sum {
			sum[i] = copySum[i]
		}
		if previous, ok := d.collected[name]; ok && previous == sum {
			os.Remove(f.Path)
			continue
		}
		d.collected[name] = sum
		if present[name+CaptionSuffix] {
			caption, err := d.caption(root, name+CaptionSuffix, uid)
			if err != nil {
				skipped = append(skipped, name+CaptionSuffix+": "+err.Error())
			}
			f.Caption = caption
		}
		files = append(files, f)
	}
	return files, skipped, ignored
}

// caption reads a caption sidecar with the checks collect makes of a file:
// a regular file owned by the turn's user with a single link, opened
// within the user's home. Surrounding whitespace is dropped.
func (d *TurnDir) caption(root *os.Root, name string, uid int) (string, error) {
	in, err := root.OpenFile(path.Join(d.rel, "out", name), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", errors.New("cannot be opened")
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return "", err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !info.Mode().IsRegular():
		return "", errors.New("not a regular file")
	case !ok || int(st.Uid) != uid:
		return "", errors.New("not owned by the turn's user")
	case st.Nlink != 1:
		return "", errors.New("has more than one link")
	case info.Size() > MaxCaption:
		return "", errors.New("larger than " + sizeLabel(MaxCaption))
	}
	b, err := io.ReadAll(io.LimitReader(in, MaxCaption+1))
	if err != nil {
		return "", err
	}
	if len(b) > MaxCaption {
		return "", errors.New("larger than " + sizeLabel(MaxCaption))
	}
	return strings.TrimSpace(strings.ToValidUTF8(string(b), "\uFFFD")), nil
}

func (d *TurnDir) collect(root *os.Root, dst, name string, uid int, limit int64) (OutFile, error) {
	// O_NONBLOCK: opening a FIFO must not wait for a writer.
	in, err := root.OpenFile(path.Join(d.rel, "out", name), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return OutFile{}, errors.New("cannot be opened")
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return OutFile{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !info.Mode().IsRegular():
		return OutFile{}, errors.New("not a regular file")
	case !ok || int(st.Uid) != uid:
		return OutFile{}, errors.New("not owned by the turn's user")
	case st.Nlink != 1:
		return OutFile{}, errors.New("has more than one link")
	case info.Size() == 0:
		return OutFile{}, errors.New("empty")
	case info.Size() > limit:
		return OutFile{}, errors.New("larger than " + sizeLabel(limit))
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return OutFile{}, err
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	copyPath := filepath.Join(dst, fmt.Sprintf("%d-%x", time.Now().UnixMilli(), b))
	out, err := os.OpenFile(copyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return OutFile{}, err
	}
	n, err := io.Copy(out, io.LimitReader(in, limit+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil && n > limit {
		err = errors.New("larger than " + sizeLabel(limit))
	}
	if err != nil {
		os.Remove(copyPath)
		return OutFile{}, err
	}
	return OutFile{Path: copyPath, Name: name, Size: n}, nil
}

func sizeLabel(n int64) string {
	if n >= 1<<20 && n%(1<<20) == 0 {
		return fmt.Sprintf("%d MB", n>>20)
	}
	return fmt.Sprintf("%d bytes", n)
}

// Path is the turn directory's absolute path.
func (d *TurnDir) Path() string { return filepath.Join(d.home, filepath.FromSlash(d.rel)) }

// Add copies src, a file of Heimdall's own, into in/ as name, owned by the
// turn's user and readable only by it, and returns the copy's path.
func (d *TurnDir) Add(src, name string) (string, error) {
	if name == "" || name != path.Base(name) || name == "." || name == ".." {
		return "", fmt.Errorf("invalid file name %q", name)
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	root, err := os.OpenRoot(d.home)
	if err != nil {
		return "", err
	}
	defer root.Close()
	rel := path.Join(d.rel, "in", name)
	out, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if d.uid >= 0 {
		if err := out.Chown(d.uid, d.gid); err != nil {
			out.Close()
			return "", err
		}
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return filepath.Join(d.home, filepath.FromSlash(rel)), nil
}
