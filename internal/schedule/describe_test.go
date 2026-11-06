package schedule

import "testing"

func TestDescribeExactUnionAndFallback(t *testing.T) {
	for _, tc := range []struct {
		spec Spec
		want string
	}{
		{Spec{Cron: []string{"0,30 12-20 * * MON-FRI", "45 20 * * 1-5"}}, "Weekdays, every 30 minutes from 12:00–20:30, plus 20:45"},
		{Spec{Cron: []string{"*/30 12-20 * * 1-5", "45 20 * * MON-FRI", "0 12 * * MON-FRI"}}, "Weekdays, every 30 minutes from 12:00–20:30, plus 20:45"},
		{Spec{Cron: []string{"0 9 * * MON-FRI"}}, "Weekdays, 09:00"},
		{Spec{Cron: []string{"0 9 * * *"}}, "Daily, 09:00"},
		{Spec{Cron: []string{"0 9 1 * MON"}}, "Custom schedule"},
		{Spec{Cron: []string{"0 9 * JAN MON"}}, "Custom schedule"},
		{Spec{Cron: []string{"0 9 * * MON", "0 17 * * FRI"}}, "Custom schedule"},
		{Spec{Cron: []string{"0 9 * * MON#2"}}, "Custom schedule"},
		{Spec{Every: "90m"}, "Every 90 minutes"},
		{Spec{Every: "2w", At: "09:00", From: "2026-11-05"}, "Every 2 weeks at 09:00, starting 2026-11-05"},
		{Spec{At: "2026-11-05T09:00"}, "Once, 2026-11-05 09:00"},
	} {
		if got := Describe(tc.spec); got != tc.want {
			t.Errorf("%+v: %q != %q", tc.spec, got, tc.want)
		}
	}
}
