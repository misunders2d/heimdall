package durable

import (
	"strings"
	"testing"
)

// The owner sees only the first evidence line, capped; the full evidence
// and verdict stay in state (!work show).
func TestLongEvidenceDeliversOnlyFirstLine(t *testing.T) {
	e := open(t, t.TempDir(), Options{})
	defer e.close()
	e.cmd("add MEL-72 Ship the parser fix")
	e.phase("MEL-72", PhaseWorking, 1)
	first := "Parser fix merged in PR 41; nothing for you to do. " + strings.Repeat("détail ", 60)
	ev := "\n\n  " + first + "\r\n\nProof:\n- go test ./... passed (412 tests)\n- CI run 9981 green\n\nLong reasoning paragraph that the owner never needs to read in chat."
	if err := e.rt.Signal("MEL-72", Event{Kind: "report", By: "test:owner", Evidence: ev}, "MEL-72/r"); err != nil {
		t.Fatal(err)
	}
	e.phase("MEL-72", PhaseReported, 1)
	e.cmdAs("a1", "accept MEL-72 checked CI run 9981")
	e.phase("MEL-72", PhaseClosed, 1)
	got := outbound(t, e.dir, "MEL-72")
	for _, m := range got {
		t.Logf("delivered (%d bytes):\n%s", len(m), m)
	}
	sum := summary(ev)
	if !strings.HasPrefix(sum, "Parser fix merged in PR 41; nothing for you to do. détail") || !strings.HasSuffix(sum, "…") || len([]rune(sum)) > summaryCap+1 || strings.Contains(sum, "  …") {
		t.Fatalf("summary %q", sum)
	}
	want := []string{
		"sent: MEL-72 attempt 1: UNACCEPTED worker report: " + sum + "\nNot done until verified: !work accept MEL-72 or !work reject MEL-72 <reason>\n— heimdall",
		"sent: MEL-72 done: " + sum + "\n— heimdall",
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("deliveries %q", got)
	}
	st, _ := e.rt.State("MEL-72")
	if st.Evidence != ev || st.Verdict != "accepted by test:owner: checked CI run 9981" {
		t.Fatalf("state lost the full record: %+v", st)
	}
}

func TestSummary(t *testing.T) {
	for in, want := range map[string]string{
		"":                       "(no summary line)",
		" \n\t\r\n":              "(no summary line)",
		"\r\n  done\r\nproof":    "done",
		"one line":               "one line",
		strings.Repeat("x", 400): strings.Repeat("x", summaryCap) + "…",
	} {
		if got := summary(in); got != want {
			t.Errorf("summary(%q) = %q, want %q", in, got, want)
		}
	}
}
