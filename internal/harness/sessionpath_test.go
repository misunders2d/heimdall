package harness

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSessionPath(t *testing.T) {
	id := "12345678-abcd-1234-abcd-123456789abc"
	for _, name := range []string{"claude", "codex", "pi", "omp"} {
		t.Run(name, func(t *testing.T) {
			l := Launch{Home: t.TempDir()}
			rel := filepath.Join(".claude", "projects", "project", id+".jsonl")
			if name == "codex" {
				rel = filepath.Join(".codex", "sessions", "2026", "01", "01", "rollout-"+id+".jsonl")
			}
			if name == "pi" || name == "omp" {
				rel = filepath.Join(".local", "state", "heimdall", "sessions", name, id, "native.jsonl")
			}
			path := filepath.Join(l.Home, rel)
			if got := SessionPath(name, l, id); got != "" {
				t.Fatal(got)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			want := path
			if name == "pi" || name == "omp" {
				want = filepath.Dir(path)
			}
			if got := SessionPath(name, l, id); got != want {
				t.Fatalf("got %q want %q", got, want)
			}
			for _, bad := range []string{"", "../escape", "*"} {
				if got := SessionPath(name, l, bad); got != "" {
					t.Fatal(got)
				}
			}
		})
	}
}
