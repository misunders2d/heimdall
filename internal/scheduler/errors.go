package scheduler

import (
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
)

// Native details are presentation only, never input to retry policy.
func causeLabel(cause string) string {
	label, _, _ := strings.Cut(cause, " — ")
	return label
}

func failureCause(why, prompt string) string {
	detail := core.SanitizeNativeError(why, prompt)
	if detail == "" {
		return category(why)
	}
	return category(detail) + " — " + detail
}
