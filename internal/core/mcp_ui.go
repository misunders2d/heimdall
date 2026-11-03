package core

import (
	"net"
	"net/url"
	"regexp"
	"strings"
)

// MCPCallback recognizes a whole pasted loopback callback. It is secret
// ephemeral control input, never an inbound queue row or model message.
func MCPCallback(text string) bool {
	text = strings.TrimSpace(text)
	u, err := url.Parse(text)
	if err != nil || u.Scheme != "http" || u.User != nil || strings.ContainsAny(text, "\r\n\t ") {
		return false
	}
	host := strings.TrimRight(strings.ToLower(u.Hostname()), ".")
	q, _ := url.ParseQuery(strings.ReplaceAll(u.RawQuery+"&"+u.Fragment, ";", "&"))
	return (host == "localhost" || strings.HasPrefix(host, "127.") || net.ParseIP(host).IsLoopback()) && q.Has("state") && (q.Has("code") || q.Has("error"))
}

// RedactMCPCallbacks also covers a callback embedded in other text and partially
// formed callbacks. Their codes must never enter context, errors or storage.
func RedactMCPCallbacks(text string) string {
	text = RedactGoogleCallbacks(text)
	return callbackURL.ReplaceAllStringFunc(text, func(raw string) string {
		normalized := strings.ReplaceAll(raw, `\u0026`, "&")
		u, err := url.Parse(normalized)
		if err != nil || u.Scheme != "http" {
			return raw
		}
		host := strings.TrimRight(strings.ToLower(u.Hostname()), ".")
		q, _ := url.ParseQuery(strings.ReplaceAll(u.RawQuery+"&"+u.Fragment, ";", "&"))
		if (host == "localhost" || strings.HasPrefix(host, "127.") || net.ParseIP(host).IsLoopback()) && (q.Has("code") || q.Has("state") || q.Has("error")) {
			return "[Private native MCP sign-in UI omitted]"
		}
		return raw
	})
}

var callbackURL = regexp.MustCompile(`(?i)https?://[^\s<>|"']+`)

// GoogleCallback recognizes the existing public callback path as ephemeral
// control cargo. Host/state validation belongs to the configured root broker.
// Recognition deliberately also catches unknown states and hosts for redaction.
func GoogleCallback(text string) bool {
	text = strings.TrimSpace(text)
	u, err := url.Parse(text)
	return err == nil && !strings.ContainsAny(text, "\r\n\t ") && googleCallbackURL(u)
}
func googleCallbackURL(u *url.URL) bool {
	return (u.Scheme == "https" || u.Scheme == "http") && strings.EqualFold(u.Path, "/oauth/google/callback")
}

// GoogleCommand returns only exact native controls, never a model instruction.
func GoogleCommand(text string) string {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "!pi ") {
		text = strings.TrimSpace(strings.TrimPrefix(text, "!pi "))
	}
	text = strings.TrimPrefix(text, "/")
	switch text {
	case "google login", "google status", "google logout":
		return text
	}
	return ""
}

// RedactGoogleCallbacks adds only Google's fixed callback path to consumers
// whose existing native-MCP redaction rules must remain unchanged.
func RedactGoogleCallbacks(text string) string {
	return callbackURL.ReplaceAllStringFunc(text, func(raw string) string {
		normalized := strings.ReplaceAll(raw, `\u0026`, "&")
		u, err := url.Parse(normalized)
		if err == nil && googleCallbackURL(u) {
			return "[Private native MCP sign-in UI omitted]"
		}
		return raw
	})
}
