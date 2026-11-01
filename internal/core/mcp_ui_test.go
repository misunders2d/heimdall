package core

import (
	"strings"
	"testing"
)

func TestMCPCallbackLoopbackControlOnly(t *testing.T) {
	for _, value := range []string{"http://localhost:18765/callback?state=s&code=c", " http://127.0.0.1:18765/callback?state=s&error=denied\n", "http://[::1]/callback?state=s&code=c"} {
		if !MCPCallback(value) {
			t.Fatal(value)
		}
	}
	for _, value := range []string{"https://slack.com/?state=s&code=c", "http://example.com/?state=s&code=c", "http://localhost/?code=c", "http://user@localhost/?state=s&code=c", "hello http://localhost/?state=s&code=c"} {
		if MCPCallback(value) {
			t.Fatal(value)
		}
	}
}
func TestMCPCallbackRedactionInErrorsAndEmbeddedText(t *testing.T) {
	for _, value := range []string{"before http://localhost:18765/callback?state=s&code=SYNTHETIC after", `{"error":"http://127.0.0.1/callback?state=s\u0026code=SYNTHETIC"}`, "http://[::1]/callback?code=SYNTHETIC"} {
		if strings.Contains(RedactMCPCallbacks(value), "SYNTHETIC") {
			t.Fatal("callback code survived redaction")
		}
	}
	ordinary := "https://example.com/document"
	if RedactMCPCallbacks(ordinary) != ordinary {
		t.Fatal("ordinary URL changed")
	}
}
