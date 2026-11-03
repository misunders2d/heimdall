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

func TestGoogleCallbacksRecognizedAndRedacted(t *testing.T) {
	for _, raw := range []string{
		"https://example.test/oauth/google/callback?state=hmg_1&code=SYNTHETIC_CALLBACK",
		"https://other.test/oauth/google/callback?error=access_denied&state=unknown",
		"http://example.test/oauth/google/callback?state=unknown&code=SYNTHETIC_CALLBACK",
	} {
		if !GoogleCallback(raw) {
			t.Fatal("Google callback unrecognized")
		}
		if strings.Contains(RedactMCPCallbacks("before "+raw+" after"), "SYNTHETIC_CALLBACK") {
			t.Fatal("Google callback leaked")
		}
	}
	if GoogleCallback("https://example.test/ordinary?code=public") {
		t.Fatal("ordinary URL captured")
	}
}
