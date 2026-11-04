package slack

import (
	"strings"
	"testing"
)

func TestFailureStatusDoesNotClaimDeliveryFailure(t *testing.T) {
	s := &Slack{name: "bot"}
	for _, phase := range []string{"failed", "needs_human"} {
		text := s.statusText(&statusUpdate{phase: phase})
		if strings.Contains(text, "not sent") || !strings.Contains(text, "details follow") {
			t.Fatalf("misleading failure status: %s", text)
		}
	}
}

// Time limits persist as failed turns, so both live and recovered status use
// the failed phase. Only an explicit cancellation claims stopped by request.
func TestTimeoutFailureStatusDoesNotClaimUserStop(t *testing.T) {
	s := &Slack{name: "bot"}
	text := s.statusText(&statusUpdate{phase: "failed"})
	if strings.Contains(text, "by request") || !strings.Contains(text, "Work failed") {
		t.Fatal(text)
	}
	text = s.statusText(&statusUpdate{phase: "cancelled"})
	if !strings.Contains(text, "by request") {
		t.Fatal(text)
	}
}
