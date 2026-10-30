package harness

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"
)

// TurnsDir is where turn directories live, relative to the run-as user's
// home, next to the pi/omp session directories.
const TurnsDir = ".local/state/heimdall/turns"

// TurnRetention is how long a turn directory is kept: a resumed session may
// still refer to its files.
const TurnRetention = 7 * 24 * time.Hour

// TurnDir is one turn's directory, ~/.local/state/heimdall/turns/NAME in
// the turn's OS user's home: in/ holds copies of the message's attachments.
// Everything Heimdall creates there belongs to that user, so the harness can
// read it. Heimdall works on the user's home only through os.Root, so a
// symbolic link the user made can never send it outside that home.
type TurnDir struct {
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
	for _, p := range []string{d.rel, d.rel + "/in"} {
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
