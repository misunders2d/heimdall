package slack

import (
	"context"
	"errors"
	"fmt"
	"github.com/misunders2d/heimdall/internal/router"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIAuthRetryAndPermanentErrors(t *testing.T) {
	for _, method := range []string{"users.info", "chat.postMessage"} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-credential" {
					t.Error("wrong token header")
				}
				if r.URL.Path != "/"+method {
					t.Error("wrong path")
				}
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "900")
					w.WriteHeader(429)
					return
				}
				fmt.Fprint(w, `{"ok":false,"error":"not_in_channel"}`)
			}))
			defer server.Close()
			a := newAPI(server.URL, "test-credential", nil)
			a.pause = func(_ context.Context, d time.Duration) error {
				if d != 300*time.Second {
					t.Errorf("delay %v", d)
				}
				return nil
			}
			err := a.call(context.Background(), method, map[string]string{}, nil)
			var permanent router.PermanentError
			if !errors.As(err, &permanent) || calls.Load() != 2 {
				t.Fatalf("error %v, calls %d", err, calls.Load())
			}
		})
	}
}
func TestAPIAmbiguousPostsAreNeverBlindlyRetried(t *testing.T) {
	for _, response := range []string{"http", "json", "broken"} {
		t.Run(response, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch response {
				case "http":
					w.WriteHeader(503)
				case "json":
					fmt.Fprint(w, `{"ok":false,"error":"internal_error"}`)
				default:
					fmt.Fprint(w, "secret-test-credential")
				}
			}))
			defer s.Close()
			a := newAPI(s.URL, "secret-test-credential", nil)
			err := a.call(context.Background(), "chat.postMessage", nil, nil)
			var ae *apiError
			if !errors.As(err, &ae) || !ae.Ambiguous || calls.Load() != 1 || strings.Contains(err.Error(), "secret-test-credential") {
				t.Fatalf("error %v calls %d", err, calls.Load())
			}
		})
	}
}
func TestAPIRequestDeadlineCancelsRequest(t *testing.T) {
	cancelled := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume body so net/http notices disconnect while handler waits.
		defer close(cancelled)
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer s.Close()
	a := newAPI(s.URL, "test", nil)
	a.timeout = 30 * time.Millisecond
	if err := a.call(context.Background(), "chat.postMessage", nil, nil); err == nil {
		t.Fatal("no timeout")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("request still running")
	}
}
func TestAPIRejectsRedirectWithoutLeakingToken(t *testing.T) {
	var landed atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { landed.Store(true) }))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 307) }))
	defer source.Close()
	err := newAPI(source.URL, "test", nil).call(context.Background(), "auth.test", nil, nil)
	if err == nil || landed.Load() {
		t.Fatal("followed credential redirect")
	}
}
