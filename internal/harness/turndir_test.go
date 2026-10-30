package harness

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A turn directory is created in the run-as user's home, owned by that user
// and private; attachments are copied in, and old turn directories go.
func TestTurnDir(t *testing.T) {
	home := t.TempDir()
	old := filepath.Join(home, TurnsDir, "old")
	if err := os.MkdirAll(old, 0o700); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-TurnRetention - time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	// Chown to Heimdall's own uid exercises the hand-over path without root.
	cred := &syscall.Credential{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}
	d, err := NewTurnDir(Launch{Home: home, Cred: cred}, "7-abc")
	if err != nil {
		t.Fatal(err)
	}
	if d.Path() != filepath.Join(home, TurnsDir, "7-abc") {
		t.Fatalf("path %s", d.Path())
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old turn directory kept: %v", err)
	}
	for _, p := range []string{d.Path(), filepath.Join(d.Path(), "in")} {
		st, err := os.Stat(p)
		if err != nil || st.Mode().Perm() != 0o700 || st.Sys().(*syscall.Stat_t).Uid != cred.Uid {
			t.Fatalf("%s: %v %v", p, st, err)
		}
	}
	src := filepath.Join(t.TempDir(), "upload")
	if err := os.WriteFile(src, []byte("photo bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := d.Add(src, "1-photo.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(got); err != nil || string(b) != "photo bytes" || got != filepath.Join(d.Path(), "in", "1-photo.jpg") {
		t.Fatalf("copy %s: %q %v", got, b, err)
	}
	if st, _ := os.Stat(got); st.Mode().Perm() != 0o600 {
		t.Fatalf("copy mode %v", st.Mode())
	}
	if _, err := d.Add(src, "1-photo.jpg"); err == nil {
		t.Fatal("an existing file was overwritten")
	}
	for _, bad := range []string{"", "../x", "a/b", ".."} {
		if _, err := d.Add(src, bad); err == nil {
			t.Fatalf("name %q accepted", bad)
		}
	}
	if _, err := NewTurnDir(Launch{Home: home}, "7-abc"); err == nil {
		t.Fatal("an existing turn directory was reused")
	}
}

// A symbolic link in the user's home never takes Heimdall outside it.
func TestTurnDirStaysInHome(t *testing.T) {
	home, outside := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".local", "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, ".local", "state", "heimdall")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTurnDir(Launch{Home: home}, "1-x"); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("followed a link out of the home: %v", err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("wrote outside the home: %v", entries)
	}
}
