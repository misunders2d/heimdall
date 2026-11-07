package core

import (
	"testing"
	"time"
)

func TestDeliveryWindowKyivWeekendDSTAndCutoffs(t *testing.T) {
	w := &DeliveryWindow{TZ: "Europe/Kyiv", Days: []time.Weekday{1, 2, 3, 4, 5}, Start: "12:00", End: "21:00"}
	if err := w.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ in, want string }{
		{"2026-10-23T18:00:00Z", "2026-10-26T10:00:00Z"}, // fall DST, Friday 21:00 -> Monday noon
		{"2026-03-27T19:00:00Z", "2026-03-30T09:00:00Z"}, // spring DST
		{"2026-11-05T09:59:59Z", "2026-11-05T10:00:00Z"},
		{"2026-11-05T10:00:00Z", "2026-11-05T10:00:00Z"},
		{"2026-11-05T18:59:59Z", "2026-11-05T18:59:59Z"},
		{"2026-11-05T19:00:00Z", "2026-11-06T10:00:00Z"},
	} {
		at, _ := time.Parse(time.RFC3339, c.in)
		want, _ := time.Parse(time.RFC3339, c.want)
		if got := w.Next(at); !got.Equal(want) {
			t.Errorf("%s: %s, want %s", c.in, got, want)
		}
	}
}
