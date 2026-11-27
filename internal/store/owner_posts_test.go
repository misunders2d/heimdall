package store

import "testing"

func TestPlatformBefore(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"41", "42", true}, {"42", "42", false}, {"43", "42", false}, {"9", "10", true},
		{"1700000000.000100", "1700000000.000101", true},
		{"1700000000.9", "1700000000.10", false}, // .900000 vs .100000, never string order
		{"1699999999.999999", "1700000000.000000", true},
		{"1700000000.000100", "1700000000.000100", false},
		{"", "42", false}, {"42", "", false}, {"41", "1700000000.1", false}, {"x", "y", false}, {"-1", "2", false},
	} {
		if got := PlatformBefore(c.a, c.b); got != c.want {
			t.Errorf("PlatformBefore(%q, %q) = %t", c.a, c.b, got)
		}
	}
}
