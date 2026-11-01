package router

import "testing"

func TestReviewSlashControlsSharedTransports(t *testing.T) {
	for word, kind := range controlWords {
		for _, prefix := range []string{"", "/"} {
			if got := parseCommand(prefix + word); got.Kind != kind {
				t.Fatalf("%s%s routed as %s", prefix, word, got.Kind)
			}
		}
		if got := NormalizeSlash("/"+word+"@TestBot", "TestBot"); parseCommand(got).Kind != kind {
			t.Fatal("targeted Telegram control", word)
		}
	}
	for _, word := range []string{"help", "status"} {
		for _, prefix := range []string{"!", "/"} {
			raw := prefix + word
			if !UnmentionedCommand(raw, NormalizeSlash(raw, ""), "", []string{word}) {
				t.Fatalf("configured %s cannot match", raw)
			}
		}
	}
}
