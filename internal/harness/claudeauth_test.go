package harness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClaudeStatusExpiryBoundary(t *testing.T) {
	expiry := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		now     time.Time
		waitErr error
		want    error
	}{
		{name: "just before deadline", now: expiry.Add(-time.Nanosecond)},
		{name: "at deadline", now: expiry, want: errClaudeLoginExpired},
		{name: "just after deadline", now: expiry.Add(time.Nanosecond), want: errClaudeLoginExpired},
		{name: "unsuccessful command", now: expiry, waitErr: errors.New("fixture status failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout := limitedBuffer{max: 4096}
			_, _ = stdout.Write([]byte(`{"loggedIn":true,"authMethod":"claude.ai","loginExpiresAt":"2026-11-26T08:00:00.000Z"}`))
			if got := claudeStatusRejection(tc.waitErr, &stdout, tc.now); got != tc.want {
				t.Fatalf("status rejection = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClaudeAuthProbeFinishesSaveAfterCallerCancels(t *testing.T) {
	dir := t.TempDir()
	command := filepath.Join(dir, "claude")
	script := `#!/bin/sh
set -eu
test "$1 $2" = "auth status"
touch "$TEST_PROBE.started"
if [ "$TEST_PROBE" = first ]; then
    tries=0
    while [ ! -f release ]; do
        tries=$((tries + 1))
        test "$tries" -le 500
        sleep 0.01
    done
    touch first.saved
fi
echo '{"loggedIn": false}'
exit 1
`
	if err := os.WriteFile(command, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	release := func() { _ = os.WriteFile(filepath.Join(dir, "release"), nil, 0o600) }
	defer release()
	launch := Launch{Path: command, Dir: dir}
	probe := func(ctx context.Context, name string) error {
		return checkClaudeAuth(ctx, launch, []string{"PATH=/usr/bin:/bin", "TEST_PROBE=" + name})
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	first, cancel := context.WithCancel(ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- probe(first, "first")
	}()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, "first.started")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("first probe did not start")
		case <-tick.C:
		}
	}
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait = %v", err)
	}

	second, stopSecond := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopSecond()
	if err := probe(second, "second"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overlapping probe = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "second.started")); !os.IsNotExist(err) {
		t.Fatalf("second probe launched before first exited: %v", err)
	}

	release()
	if err := probe(ctx, "third"); !errors.Is(err, ErrClaudeSignedOut) {
		t.Fatalf("probe after release = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "first.saved")); err != nil {
		t.Fatalf("first probe did not finish its save: %v", err)
	}
}
