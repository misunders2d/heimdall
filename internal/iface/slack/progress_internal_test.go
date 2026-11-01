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
