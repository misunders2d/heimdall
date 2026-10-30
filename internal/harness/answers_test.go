package harness

import (
	"slices"
	"strings"
	"testing"
)

// The answers kept stay within the bound when joined; the one that passes
// it is cut and marked, later ones dropped.
func TestClipAnswers(t *testing.T) {
	const mark = "\n[answer truncated by Heimdall]"
	for _, c := range []struct {
		in   []string
		n    int
		want []string
	}{
		{[]string{"abc", "def"}, 8, []string{"abc", "def"}},
		{[]string{"abcdef"}, 4, []string{"abcd" + mark}},
		{[]string{"abc", "defgh", "ij"}, 7, []string{"abc", "de" + mark}},
		{[]string{"abc", "def"}, 4, []string{"abc" + mark}},
		{[]string{"abc", "def"}, 6, []string{"abc", "d" + mark}},
	} {
		got := clipAnswers(c.in, c.n)
		if !slices.Equal(got, c.want) {
			t.Errorf("clipAnswers(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
		if j := joinAnswers(got); len(strings.TrimSuffix(j, mark)) > c.n {
			t.Errorf("%q: joined %d bytes", c.in, len(j))
		}
	}
}
