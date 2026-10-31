package slack

import "testing"

func TestMrkdwnEscapesMentionsAndPreservesCode(t *testing.T) {
	cases := map[string]string{
		"<!channel> & <@U123>":                  "&lt;!channel&gt; &amp; &lt;@U123&gt;",
		"**bold** __bold__ *italic* _native_":   "*bold* *bold* _italic_ _native_",
		"# Heading":                             "*Heading*",
		"* item\n* next":                        "* item\n* next",
		"word*middle*":                          "word*middle*",
		"(*italic*)":                            "(_italic_)",
		"`**code**`\n```go\n**code**\n```":      "`**code**`\n```go\n**code**\n```",
		"[label](https://example.com/?a=1&b=2)": "<https://example.com/?a=1&amp;b=2|label>",
		"[ping](javascript:alert(1))":           "[ping](javascript:alert(1))",
		"[<!here>](https://example.com)":        "<https://example.com|&lt;!here&gt;>",
	}
	for in, want := range cases {
		if got := mrkdwn(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}
