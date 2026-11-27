package harness

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Startup removes only stale leases, never through a planted symlink.
func TestSweepLeases(t *testing.T) {
	home, outside := t.TempDir(), t.TempDir()
	now := time.Now()
	old := now.Add(-time.Hour)
	agents := filepath.Join(home, ".heimdall", "turn-agents")
	for _, d := range []string{"claude-stale/projects", "codex-live"} {
		if err := os.MkdirAll(filepath.Join(agents, d), 0700); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(outside, "keep"), []byte("x"), 0600)
	// A stale entry that is a symlink out: only the link goes.
	os.Symlink(outside, filepath.Join(agents, "omp-link"))
	// A planted parent symlink is never followed.
	os.MkdirAll(filepath.Join(home, ".pi"), 0700)
	os.Mkdir(filepath.Join(outside, "victim"), 0700)
	os.Symlink(outside, filepath.Join(home, ".pi", "turn-agents"))
	for _, p := range []string{filepath.Join(agents, "claude-stale"), filepath.Join(outside, "victim")} {
		os.Chtimes(p, old, old)
	}
	ts := []unix.Timespec{unix.NsecToTimespec(old.UnixNano()), unix.NsecToTimespec(old.UnixNano())}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, filepath.Join(agents, "omp-link"), ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	n, err := sweepLeases(home, now)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	for _, gone := range []string{"claude-stale", "omp-link"} {
		if _, err := os.Lstat(filepath.Join(agents, gone)); !os.IsNotExist(err) {
			t.Fatal("stale lease kept", gone)
		}
	}
	for _, kept := range []string{filepath.Join(agents, "codex-live"), filepath.Join(outside, "keep"), filepath.Join(outside, "victim")} {
		if _, err := os.Lstat(kept); err != nil {
			t.Fatal("removed", kept)
		}
	}
	if _, err := sweepLeases("/root", now); err == nil {
		t.Fatal("owner home swept")
	}
}
