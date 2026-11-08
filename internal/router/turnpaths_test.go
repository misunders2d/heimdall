package router

import "testing"

// CleanTurnPaths removes only this turn's exact in/out directories, by whole
// path component; everything else in the answer stays.
func TestCleanTurnPathsExactCurrentTurnOnly(t *testing.T) {
	in, out := "/home/u/.local/state/heimdall/turns/7-ab12/in", "/home/u/.local/state/heimdall/turns/7-ab12/out"
	for _, c := range []struct{ name, text, want string }{
		{"standalone outbox file line", "Here is the chart.\n" + out + "/chart.png", "Here is the chart."},
		{"quoted standalone line", "`" + out + "/chart.png`.", ""},
		{"path-only fence", "```\n" + out + "/chart.png\n```", ""},
		{"prose keeps file name", "Saved " + out + "/report.pdf and read " + in + "/input.txt.", "Saved report.pdf and read input.txt."},
		{"bare directory", "save them in " + out + " before", "save them in out before"},
		{"nested outbox path is prose", out + "/sub/x.txt", "sub/x.txt"},
		{"unrelated absolute path", "See /other/private/report.txt and /home/u/notes.", "See /other/private/report.txt and /home/u/notes."},
		{"neighboring turn", "/home/u/.local/state/heimdall/turns/7-ab123/out/x.png", "/home/u/.local/state/heimdall/turns/7-ab123/out/x.png"},
		{"similarly named dir", out + "2/x.png " + out + "-old/y", out + "2/x.png " + out + "-old/y"},
		{"prefix inside longer path", "/backup" + out + "/x.png", "/backup" + out + "/x.png"},
		{"unaffected fence kept", "```\n```\nplain", "```\n```\nplain"},
		{"no prefix supplied", "keep " + out + "/x", "keep " + out + "/x"},
	} {
		inDir, outDir := in, out
		if c.name == "no prefix supplied" {
			inDir, outDir = "", ""
		}
		if got := CleanTurnPaths(c.text, inDir, outDir); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
