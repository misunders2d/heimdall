package slack

// Web API transport behavior ported from misunderstood-pi-pack ec72c7d,
// extensions/slack.ts. Requests have real deadlines and bounded retries.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/router"
)

const BotTokenEnv = "HEIMDALL_SLACK_BOT_TOKEN"
const AppTokenEnv = "HEIMDALL_SLACK_APP_TOKEN"

// apiError contains only allowlisted protocol metadata, never response text,
// request URLs (Socket Mode tickets), or credentials.
type apiError struct {
	Code      string
	Status    int
	Ambiguous bool
}

func (e *apiError) Error() string { return fmt.Sprintf("slack API: %s (HTTP %d)", e.Code, e.Status) }

var safeErrorCode = regexp.MustCompile(`^[a-z_]{1,64}$`)
var transientCodes = map[string]bool{"fatal_error": true, "internal_error": true, "request_timeout": true, "service_unavailable": true, "ratelimited": true}

type apiClient struct {
	pacer       *apiPacer
	base, token string
	authScopes  string
	http        *http.Client
	timeout     time.Duration
	pause       func(context.Context, time.Duration) error
}

func newAPI(base, token string, client *http.Client) *apiClient {
	var pacer *apiPacer
	if base == "" {
		pacer = tier4
		base = "https://slack.com/api"
	}
	if client == nil {
		client = &http.Client{}
	}
	// Never send API credentials through redirects, including same-host ones.
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &apiClient{pacer: pacer, base: strings.TrimRight(base, "/"), token: token, http: &c, timeout: 15 * time.Second, pause: pause}
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// call retries idempotent reads/updates and explicit rate-limit rejections.
// An ambiguous post is returned immediately so Send can reconcile metadata
// before any resend. Retrying it blindly can duplicate a landed message.
func (a *apiClient) call(ctx context.Context, method string, args any, out any) error {
	data, err := json.Marshal(args)
	if err != nil {
		return errors.New("slack: invalid API arguments")
	}
	for attempt := 0; attempt < 5; attempt++ {
		delay := time.Duration(1<<attempt) * 200 * time.Millisecond
		err, retry, after := a.once(ctx, method, data, out)
		if err == nil {
			return nil
		}
		if !retry || attempt == 4 {
			return err
		}
		if method == "chat.postMessage" || method == "files.completeUploadExternal" {
			var ae *apiError
			if !errors.As(err, &ae) || ae.Status != 429 {
				return err
			}
		}
		if after > 0 {
			delay = after
		}
		if err = a.pause(ctx, delay); err != nil {
			return err
		}
	}
	return errors.New("slack: retry limit reached")
}
func (a *apiClient) once(parent context.Context, method string, data []byte, out any) (error, bool, time.Duration) {
	if a.pacer != nil {
		if err := a.pacer.wait(parent, method); err != nil {
			return err, false, 0
		}
	}
	ctx, cancel := context.WithTimeout(parent, a.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/"+method, bytes.NewReader(data))
	if err != nil {
		return errors.New("slack: invalid API endpoint"), false, 0
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	res, err := a.http.Do(req)
	if err != nil {
		if parent.Err() != nil {
			return parent.Err(), false, 0
		}
		return &apiError{Code: "transport_error", Ambiguous: true}, true, 0
	}
	defer res.Body.Close()
	if method == "auth.test" {
		a.authScopes = res.Header.Get("x-oauth-scopes")
	}
	if res.StatusCode != 200 {
		ambiguous := res.StatusCode == 408 || res.StatusCode == 429 || res.StatusCode >= 500
		e := &apiError{Code: "http_error", Status: res.StatusCode, Ambiguous: ambiguous}
		if !ambiguous {
			return router.PermanentError{Err: e}, false, 0
		}
		after, _ := strconv.Atoi(res.Header.Get("Retry-After"))
		after = min(max(after, 0), 300)
		return e, true, time.Duration(after) * time.Second
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 {
		return &apiError{Code: "invalid_response", Ambiguous: true}, true, 0
	}
	var result struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &result) != nil {
		return &apiError{Code: "invalid_response", Ambiguous: true}, true, 0
	}
	if !result.OK {
		code := result.Error
		if !safeErrorCode.MatchString(code) {
			code = "unknown_error"
		}
		e := &apiError{Code: code, Status: 200, Ambiguous: transientCodes[code]}
		if e.Ambiguous {
			after := 0
			if code == "ratelimited" {
				after, _ = strconv.Atoi(res.Header.Get("Retry-After"))
			}
			return e, true, time.Duration(min(max(after, 0), 300)) * time.Second
		}
		return router.PermanentError{Err: e}, false, 0
	}
	if out != nil && json.Unmarshal(raw, out) != nil {
		return &apiError{Code: "invalid_response", Ambiguous: true}, true, 0
	}
	return nil, false, 0
}
