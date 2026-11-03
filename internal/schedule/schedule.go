// Package schedule computes a job's run slots (docs/scheduler.md §2.4):
// 5-field cron specs parsed by gronx, fixed intervals, every N days or weeks
// at a wall time, and one-offs, each in the job's IANA zone.
//
// Cron is evaluated on the zone's wall clock, so DST is the wrapper's job
// (neither cron library handles it), following cronie's cron(8): a job with
// a fixed minute and hour that falls in a skipped hour runs once, at the
// first valid wall minute after the change; in a repeated hour every wall
// minute runs once, at its first occurrence. Frequent jobs (minute or hour
// field with * or a step) skip wall minutes that do not exist. A slot's key
// is its local wall minute, so a second pass through a repeated hour can
// never make a second run.
package schedule

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // zones never depend on the host's zone files

	"github.com/adhocore/gronx"
)

// KeyLayout is a slot key: the local wall minute.
const KeyLayout = "2006-01-02T15:04"

// Spec is a job's schedule: exactly one of Cron, Every (with At and From for
// days and weeks), or At alone (a one-off, local time in the job's zone).
type Spec struct {
	Cron  []string `json:"cron,omitempty"`
	Every string   `json:"every,omitempty"`
	At    string   `json:"at,omitempty"`
	From  string   `json:"from,omitempty"`
}

// Slot is one scheduled fire.
type Slot struct {
	Key string    // run key: the local wall minute (UTC, with "Z", for fixed durations)
	At  time.Time // when it fires
}

// Schedule is a compiled Spec.
type Schedule struct {
	loc    *time.Location
	cron   []string
	fixed  []bool        // per cron spec: fixed minute and hour
	step   time.Duration // every <duration>: from anchor, UTC arithmetic
	anchor time.Time
	days   int       // every N days at a wall time
	start  time.Time // wall clock (as UTC) of the first day's slot
	once   time.Time // wall clock (as UTC) of a one-off
}

// Zone loads an IANA zone; "" and "Local" are refused.
func Zone(name string) (*time.Location, error) {
	if name == "" || name == "Local" {
		return nil, errors.New("time zone must be an IANA name such as Europe/Kyiv")
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("unknown time zone %q", name)
	}
	return loc, nil
}

// Compile checks sp in zone tz. anchor is when a fixed interval starts
// (the job's creation) and the first day of a days or weeks schedule
// without From.
func Compile(sp Spec, tz string, anchor time.Time) (*Schedule, error) {
	loc, err := Zone(tz)
	if err != nil {
		return nil, err
	}
	s := &Schedule{loc: loc}
	switch {
	case len(sp.Cron) > 0:
		if sp.Every != "" || sp.At != "" || sp.From != "" {
			return nil, errors.New("schedule: cron cannot be combined with every, at or from")
		}
		if len(sp.Cron) > 10 {
			return nil, errors.New("schedule: at most 10 cron specs")
		}
		for _, c := range sp.Cron {
			f := strings.Fields(c)
			if len(f) != 5 || !gronx.IsValid(c) {
				return nil, fmt.Errorf("schedule: cron %q must be 5 valid fields: minute hour day-of-month month day-of-week", c)
			}
			s.cron = append(s.cron, strings.Join(f, " "))
			s.fixed = append(s.fixed, !strings.ContainsAny(f[0]+f[1], "*/"))
		}
	case sp.Every != "":
		n, unit := strings.TrimRightFunc(sp.Every, isLetter), strings.TrimLeftFunc(sp.Every, isDigit)
		if days := map[string]int{"d": 1, "w": 7}[unit]; days > 0 {
			k, err := strconv.Atoi(n)
			if err != nil || k < 1 || k > 366 {
				return nil, fmt.Errorf("schedule: every %q: use 1-366 days (d) or weeks (w)", sp.Every)
			}
			h, m, ok := clock(sp.At)
			if !ok {
				return nil, errors.New(`schedule: every N days or weeks needs "at": "HH:MM"`)
			}
			from := wallOf(anchor, loc)
			if sp.From != "" {
				if from, err = time.Parse("2006-01-02", sp.From); err != nil {
					return nil, errors.New(`schedule: "from" must be a date, YYYY-MM-DD`)
				}
			}
			s.days, s.start = k*days, time.Date(from.Year(), from.Month(), from.Day(), h, m, 0, 0, time.UTC)
			break
		}
		d, err := time.ParseDuration(sp.Every)
		if err != nil || d < time.Minute || d%time.Minute != 0 {
			return nil, fmt.Errorf("schedule: every %q: a whole number of minutes (15m, 2h) or days/weeks (1d, 2w)", sp.Every)
		}
		if sp.At != "" || sp.From != "" {
			return nil, errors.New("schedule: at and from go only with every N days or weeks")
		}
		s.step, s.anchor = d, anchor.UTC().Truncate(time.Minute)
	case sp.At != "":
		if sp.From != "" {
			return nil, errors.New("schedule: from goes only with every")
		}
		once, err := time.Parse(KeyLayout, strings.Replace(sp.At, " ", "T", 1))
		if err != nil {
			return nil, errors.New(`schedule: a one-off "at" is local time, YYYY-MM-DDTHH:MM`)
		}
		s.once = once
	default:
		return nil, errors.New("schedule: needs cron, every or at")
	}
	return s, nil
}

func isLetter(r rune) bool { return r >= 'a' && r <= 'z' }
func isDigit(r rune) bool  { return r >= '0' && r <= '9' }

func clock(s string) (h, m int, ok bool) {
	t, err := time.Parse("15:04", s)
	return t.Hour(), t.Minute(), err == nil
}

// Location is the schedule's zone.
func (s *Schedule) Location() *time.Location { return s.loc }

// OneOff reports a schedule with a single slot.
func (s *Schedule) OneOff() bool { return !s.once.IsZero() }

// Next is the first slot strictly after after, if any.
func (s *Schedule) Next(after time.Time) (Slot, bool) {
	switch {
	case s.step > 0:
		k := int64(1)
		if after.After(s.anchor) {
			k = int64(after.Sub(s.anchor)/s.step) + 1
		}
		at := s.anchor.Add(time.Duration(k) * s.step)
		return Slot{Key: at.Format(KeyLayout) + "Z", At: at}, true
	case s.days > 0:
		k := int(wallOf(after, s.loc).Sub(s.start)/(24*time.Hour)) / s.days
		for k = max(k-1, 0); k < 1<<20; k++ {
			w := s.start.AddDate(0, 0, k*s.days)
			if at, ok := instant(w, s.loc, true); ok && at.After(after) {
				return Slot{Key: w.Format(KeyLayout), At: at}, true
			}
		}
	case !s.once.IsZero():
		if at, ok := instant(s.once, s.loc, true); ok && at.After(after) {
			return Slot{Key: s.once.Format(KeyLayout), At: at}, true
		}
	default:
		var best Slot
		for i, expr := range s.cron {
			if slot, ok := s.cronNext(expr, s.fixed[i], after); ok && (best.At.IsZero() || slot.At.Before(best.At)) {
				best = slot
			}
		}
		return best, !best.At.IsZero()
	}
	return Slot{}, false
}

// cronNext walks the zone's wall clock as if it were UTC (which has no DST)
// and maps each matching wall minute to its instant.
func (s *Schedule) cronNext(expr string, fixed bool, after time.Time) (Slot, bool) {
	w := wallOf(after, s.loc)
	for range 2000 {
		next, err := gronx.NextTickAfter(expr, w, false)
		if err != nil || !next.After(w) {
			return Slot{}, false
		}
		w = next
		if at, ok := instant(w, s.loc, fixed); ok && at.After(after) {
			return Slot{Key: w.Format(KeyLayout), At: at}, true
		}
	}
	return Slot{}, false
}

// Fires are the next n slots after after.
func (s *Schedule) Fires(after time.Time, n int) []Slot {
	var out []Slot
	for len(out) < n {
		slot, ok := s.Next(after)
		if !ok {
			break
		}
		out, after = append(out, slot), slot.At
	}
	return out
}

// MinGap is the shortest time between two of the next 48 slots after
// from; 0 for a one-off.
func (s *Schedule) MinGap(from time.Time) time.Duration {
	var gap time.Duration
	fires := s.Fires(from, 48)
	for i := 1; i < len(fires); i++ {
		if g := fires[i].At.Sub(fires[i-1].At); gap == 0 || g < gap {
			gap = g
		}
	}
	return gap
}

// Jitter is a job's fixed offset, hash(id) mod span, to start before its
// next slot: for watches (phase 3b); agent and notify jobs use none.
func Jitter(id string, span time.Duration) time.Duration {
	if span <= 0 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(id))
	return time.Duration(h.Sum64() % uint64(span))
}

// wallOf is t's wall clock in loc, as a UTC time.
func wallOf(t time.Time, loc *time.Location) time.Time {
	l := t.In(loc)
	return time.Date(l.Year(), l.Month(), l.Day(), l.Hour(), l.Minute(), l.Second(), l.Nanosecond(), time.UTC)
}

// instant is when wall clock w (as UTC) happens in loc: its first
// occurrence. A wall time skipped by a DST change maps to the first valid
// wall minute after it for fixed-time slots, and to none otherwise.
func instant(w time.Time, loc *time.Location, fixed bool) (time.Time, bool) {
	if t, ok := first(w, loc); ok || !fixed {
		return t, ok
	}
	for m := w.Add(time.Minute); m.Sub(w) <= 26*time.Hour; m = m.Add(time.Minute) {
		if t, ok := first(m, loc); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

// first tries the offsets in effect around w (time.Date picks either one
// during a transition) and keeps the earliest instant showing w.
func first(w time.Time, loc *time.Location) (time.Time, bool) {
	var best time.Time
	for _, probe := range []time.Duration{-26 * time.Hour, 0, 26 * time.Hour} {
		_, off := w.Add(probe).In(loc).Zone()
		t := w.Add(-time.Duration(off) * time.Second)
		if wallOf(t, loc).Equal(w) && (best.IsZero() || t.Before(best)) {
			best = t
		}
	}
	return best, !best.IsZero()
}

// Transition is a change of the zone's UTC offset.
type Transition struct {
	At            time.Time
	Before, After int // offsets, seconds east of UTC
}

// Transitions finds loc's offset changes between from and to from the zone
// data, to the minute.
func Transitions(loc *time.Location, from, to time.Time) []Transition {
	var out []Transition
	offset := func(t time.Time) int { _, o := t.In(loc).Zone(); return o }
	for t := from; t.Before(to); t = t.Add(24 * time.Hour) {
		a, b := offset(t), offset(t.Add(24*time.Hour))
		if a == b {
			continue
		}
		lo, hi := t, t.Add(24*time.Hour)
		for hi.Sub(lo) > time.Minute {
			mid := lo.Add(hi.Sub(lo) / 2)
			if offset(mid) == a {
				lo = mid
			} else {
				hi = mid
			}
		}
		out = append(out, Transition{At: hi.Truncate(time.Minute), Before: a, After: b})
	}
	return out
}

// Warning names the first DST change in the year after from that one of
// the schedule's wall-clock slots falls into; "" when none does.
func (s *Schedule) Warning(from time.Time) string {
	if s.step > 0 {
		return ""
	}
	for _, tr := range Transitions(s.loc, from, from.AddDate(1, 0, 0)) {
		lo := tr.At.Add(time.Duration(min(tr.Before, tr.After)) * time.Second)
		hi := tr.At.Add(time.Duration(max(tr.Before, tr.After)) * time.Second)
		what := "is skipped by the DST change"
		if tr.After < tr.Before {
			what = "is repeated by the DST change"
		}
		for m := lo.UTC(); m.Before(hi.UTC()); m = m.Add(time.Minute) {
			w := time.Date(m.Year(), m.Month(), m.Day(), m.Hour(), m.Minute(), 0, 0, time.UTC)
			if s.wallSlot(w) {
				return fmt.Sprintf("%s %s on %s (%s); it runs once, at its first valid time.", w.Format("15:04"), what, w.Format("2006-01-02"), s.loc)
			}
		}
	}
	return ""
}

// wallSlot reports whether wall minute w (as UTC) is one of the slots.
func (s *Schedule) wallSlot(w time.Time) bool {
	switch {
	case s.days > 0:
		d := w.Sub(s.start)
		return d >= 0 && d%(24*time.Hour) == 0 && int(d/(24*time.Hour))%s.days == 0
	case !s.once.IsZero():
		return w.Equal(s.once)
	}
	for _, expr := range s.cron {
		if due, _ := gronx.New().IsDue(expr, w); due {
			return true
		}
	}
	return false
}
