package schedule

import (
	"strings"
	"testing"
	"time"
)

func mustCompile(t *testing.T, sp Spec, tz string, anchor time.Time) *Schedule {
	t.Helper()
	s, err := Compile(sp, tz, anchor)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func local(t *testing.T, tz, wall string) time.Time {
	t.Helper()
	loc, err := Zone(tz)
	if err != nil {
		t.Fatal(err)
	}
	at, err := time.ParseInLocation(KeyLayout, wall, loc)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func keys(slots []Slot) string {
	var out []string
	for _, s := range slots {
		out = append(out, s.Key+"@"+s.At.UTC().Format("15:04Z"))
	}
	return strings.Join(out, " ")
}

func TestCompileErrors(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		sp Spec
		tz string
	}{
		{Spec{Cron: []string{"0 9 * *"}}, "UTC"},
		{Spec{Cron: []string{"0 25 * * *"}}, "UTC"},
		{Spec{Cron: []string{"@daily"}}, "UTC"},
		{Spec{Cron: []string{"0 9 * * MON"}, At: "09:00"}, "UTC"},
		{Spec{Every: "30s"}, "UTC"},
		{Spec{Every: "90s"}, "UTC"},
		{Spec{Every: "2w"}, "UTC"},
		{Spec{Every: "15m", At: "10:00"}, "UTC"},
		{Spec{At: "tomorrow"}, "UTC"},
		{Spec{}, "UTC"},
		{Spec{Cron: []string{"0 9 * * *"}}, ""},
		{Spec{Cron: []string{"0 9 * * *"}}, "Local"},
		{Spec{Cron: []string{"0 9 * * *"}}, "Mars/Olympus"},
	} {
		if _, err := Compile(c.sp, c.tz, now); err == nil {
			t.Errorf("%+v in %q compiled", c.sp, c.tz)
		}
	}
}

func TestCronSemantics(t *testing.T) {
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC) // a Sunday
	for _, c := range []struct{ expr, want string }{
		// Day of month and day of week combine with OR, as in Vixie cron.
		{"0 9 13 * FRI", "2026-11-06T09:00 2026-11-13T09:00 2026-11-20T09:00"},
		{"0 9 L * *", "2026-11-30T09:00 2026-12-31T09:00 2027-01-31T09:00"},
		{"0 9 15W * *", "2026-11-16T09:00 2026-12-15T09:00 2027-01-15T09:00"},
		{"0 9 * * MON#1", "2026-11-02T09:00 2026-12-07T09:00 2027-01-04T09:00"},
		{"30 8 * JAN,JUL MON-FRI", "2027-01-01T08:30 2027-01-04T08:30 2027-01-05T08:30"},
	} {
		s := mustCompile(t, Spec{Cron: []string{c.expr}}, "UTC", from)
		var got []string
		for _, f := range s.Fires(from, 3) {
			got = append(got, f.Key)
		}
		if strings.Join(got, " ") != c.want {
			t.Errorf("%s: %v, want %s", c.expr, got, c.want)
		}
	}
}

func TestNextThreeFiresInZone(t *testing.T) {
	s := mustCompile(t, Spec{Cron: []string{"0 9 * * MON", "0 18 * * FRI"}}, "Europe/Kyiv", time.Time{})
	got := keys(s.Fires(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC), 3))
	if want := "2026-11-06T18:00@16:00Z 2026-11-09T09:00@07:00Z 2026-11-13T18:00@16:00Z"; got != want {
		t.Fatalf("fires %s, want %s", got, want)
	}
}

// America/Los_Angeles 2027-03-14: 02:00 PST jumps to 03:00 PDT; 2027-11-07:
// 02:00 PDT falls back to 01:00 PST.
func TestDSTFixedTimes(t *testing.T) {
	la := "America/Los_Angeles"
	gap := mustCompile(t, Spec{Cron: []string{"30 2 * * *"}}, la, time.Time{})
	got := keys(gap.Fires(local(t, la, "2027-03-13T12:00"), 3))
	// Once at 03:00 PDT (10:00Z) on the day of the change.
	if want := "2027-03-14T02:30@10:00Z 2027-03-15T02:30@09:30Z 2027-03-16T02:30@09:30Z"; got != want {
		t.Fatalf("spring forward: %s, want %s", got, want)
	}
	back := mustCompile(t, Spec{Cron: []string{"30 1 * * *"}}, la, time.Time{})
	got = keys(back.Fires(local(t, la, "2027-11-06T12:00"), 3))
	// Once, at the first 01:30 (PDT, 08:30Z), never again at 01:30 PST.
	if want := "2027-11-07T01:30@08:30Z 2027-11-08T01:30@09:30Z 2027-11-09T01:30@09:30Z"; got != want {
		t.Fatalf("fall back: %s, want %s", got, want)
	}
	if w := gap.Warning(local(t, la, "2027-03-01T00:00")); !strings.Contains(w, "02:30 is skipped") {
		t.Fatalf("warning %q", w)
	}
	if w := back.Warning(local(t, la, "2027-11-01T00:00")); !strings.Contains(w, "01:30 is repeated") {
		t.Fatalf("warning %q", w)
	}
	if w := mustCompile(t, Spec{Cron: []string{"0 9 * * *"}}, la, time.Time{}).Warning(local(t, la, "2027-01-01T00:00")); w != "" {
		t.Fatalf("09:00 warned: %q", w)
	}
}

// Frequent jobs run normally: missing wall minutes are not made up, and the
// repeated hour gives one run per local slot.
func TestDSTFrequent(t *testing.T) {
	la := "America/Los_Angeles"
	s := mustCompile(t, Spec{Cron: []string{"*/15 * * * *"}}, la, time.Time{})
	got := keys(s.Fires(local(t, la, "2027-03-14T01:30"), 4))
	if want := "2027-03-14T01:45@09:45Z 2027-03-14T03:00@10:00Z 2027-03-14T03:15@10:15Z 2027-03-14T03:30@10:30Z"; got != want {
		t.Fatalf("spring forward: %s, want %s", got, want)
	}
	got = keys(s.Fires(local(t, la, "2027-11-07T00:30"), 7))
	if want := "2027-11-07T00:45@07:45Z 2027-11-07T01:00@08:00Z 2027-11-07T01:15@08:15Z 2027-11-07T01:30@08:30Z 2027-11-07T01:45@08:45Z 2027-11-07T02:00@10:00Z 2027-11-07T02:15@10:15Z"; got != want {
		t.Fatalf("fall back: %s, want %s", got, want)
	}
	// From inside the second pass of the repeated hour, nothing repeats.
	if next, _ := s.Next(time.Date(2027, 11, 7, 9, 20, 0, 0, time.UTC)); next.Key != "2027-11-07T02:00" {
		t.Fatalf("second pass: next %s", next.Key)
	}
}

// The configured default zone's transitions come from the zone data.
func TestDefaultZoneTransitions(t *testing.T) {
	loc, _ := Zone("Europe/Kyiv")
	trs := Transitions(loc, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
	if len(trs) != 2 {
		t.Fatalf("transitions %v", trs)
	}
	for _, tr := range trs {
		lo := tr.At.Add(time.Duration(min(tr.Before, tr.After)) * time.Second)
		w := lo.Add(30 * time.Minute) // a wall minute inside the window
		spec := Spec{Cron: []string{w.Format("4 15 2 1 *")}}
		s := mustCompile(t, spec, "Europe/Kyiv", time.Time{})
		fires := s.Fires(tr.At.Add(-24*time.Hour), 2)
		if fires[0].Key != w.Format(KeyLayout) || fires[1].Key[:4] == fires[0].Key[:4] {
			t.Fatalf("%v: fires %v, want one run keyed %s", tr, keys(fires), w.Format(KeyLayout))
		}
		if tr.After > tr.Before && !fires[0].At.Equal(tr.At) {
			t.Fatalf("gap slot at %s, want the change %s", fires[0].At, tr.At)
		}
		if tr.After < tr.Before && !fires[0].At.Before(tr.At) {
			t.Fatalf("repeated slot at %s, want its first occurrence before %s", fires[0].At, tr.At)
		}
	}
}

func TestEveryForms(t *testing.T) {
	la := "America/Los_Angeles"
	created := local(t, la, "2027-03-01T10:00")
	weeks := mustCompile(t, Spec{Every: "2w", At: "12:15", From: "2027-03-06"}, la, created)
	got := keys(weeks.Fires(created, 3))
	if want := "2027-03-06T12:15@20:15Z 2027-03-20T12:15@19:15Z 2027-04-03T12:15@19:15Z"; got != want {
		t.Fatalf("every 2w: %s, want %s", got, want)
	}
	quarter := mustCompile(t, Spec{Every: "15m"}, la, created.Add(20*time.Second))
	got = keys(quarter.Fires(local(t, la, "2027-03-14T01:50"), 3))
	if want := "2027-03-14T10:00Z@10:00Z 2027-03-14T10:15Z@10:15Z 2027-03-14T10:30Z@10:30Z"; got != want {
		t.Fatalf("every 15m: %s, want %s", got, want)
	}
	if g := quarter.MinGap(created); g != 15*time.Minute {
		t.Fatalf("gap %s", g)
	}
	once := mustCompile(t, Spec{At: "2027-03-14T02:30"}, la, created)
	if slot, ok := once.Next(created); !ok || slot.At.UTC().Format("15:04") != "10:00" || !once.OneOff() {
		t.Fatalf("one-off in the gap: %v %v", slot, ok)
	}
	if _, ok := once.Next(local(t, la, "2027-03-15T00:00")); ok {
		t.Fatal("a past one-off has a next slot")
	}
}

func TestJitter(t *testing.T) {
	span := 5 * time.Minute
	a, b := Jitter("job1", span), Jitter("job1", span)
	if a != b || a < 0 || a >= span || Jitter("job1", 0) != 0 {
		t.Fatalf("jitter %s %s", a, b)
	}
}
