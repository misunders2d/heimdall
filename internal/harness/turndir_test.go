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

// Collect takes only regular files of the turn's user with one link, from
// out/ itself, into Heimdall's directory, and says why it left the rest.
func TestTurnDirCollect(t *testing.T) {
	home, outside, dst := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "outbox")
	d, err := NewTurnDir(Launch{Home: home}, "3-abc")
	if err != nil {
		t.Fatal(err)
	}
	out := d.Outbox()
	if st, err := os.Stat(out); err != nil || st.Mode().Perm() != 0o700 || out != filepath.Join(d.Path(), "out") {
		t.Fatalf("outbox %s: %v %v", out, st, err)
	}
	write := func(p, body string) {
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(out, "b-report.txt"), "report")
	write(filepath.Join(out, "a-chart.png"), "png")
	write(filepath.Join(out, ".hidden"), "x")
	write(filepath.Join(out, "c-empty"), "")
	write(filepath.Join(out, "d-big"), strings.Repeat("x", 11))
	write(filepath.Join(outside, "secret"), "secret")
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(out, "e-link")); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(home, "notes"), "notes")
	if err := os.Link(filepath.Join(home, "notes"), filepath.Join(out, "f-hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(out, "g-fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(out, "h-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(out, "i-third"), "third")
	write(filepath.Join(out, "j-fourth"), "fourth")

	done := make(chan struct{})
	var files []OutFile
	var skipped []string
	go func() { files, skipped = d.Collect(dst, 3, 10); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Collect blocked (on the FIFO?)")
	}
	var names []string
	for _, f := range files {
		b, err := os.ReadFile(f.Path)
		if err != nil || filepath.Dir(f.Path) != dst || int64(len(b)) != f.Size {
			t.Fatalf("copy %+v: %q %v", f, b, err)
		}
		names = append(names, f.Name+"="+string(b))
	}
	if strings.Join(names, ",") != "a-chart.png=png,b-report.txt=report,i-third=third" {
		t.Fatalf("collected %v", names)
	}
	want := []string{"c-empty: empty", "d-big: larger than 10 bytes", "e-link: cannot be opened", "f-hardlink: has more than one link", "g-fifo: not a regular file", "h-dir: not a regular file", "j-fourth: more than 3 files"}
	if strings.Join(skipped, "\n") != strings.Join(want, "\n") {
		t.Fatalf("skipped:\n%s", strings.Join(skipped, "\n"))
	}
	if st, _ := os.Stat(dst); st.Mode().Perm() != 0o700 {
		t.Fatalf("outbox copy dir mode %v", st.Mode())
	}
}
