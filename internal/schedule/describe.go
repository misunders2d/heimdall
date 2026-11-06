package schedule

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Describe renders only schedules whose complete set of wall times is known.
// Unsupported cron forms stay honest; callers expose exact cron in Details.
func Describe(sp Spec) string {
	if len(sp.Cron) == 0 {
		if sp.Every == "" {
			return "Once, " + strings.ReplaceAll(sp.At, "T", " ")
		}
		every := sp.Every
		if d, err := time.ParseDuration(every); err == nil {
			switch {
			case d%time.Hour == 0:
				every = quantity(int(d/time.Hour), "hour")
			default:
				every = quantity(int(d/time.Minute), "minute")
			}
		} else if n, err := strconv.Atoi(strings.TrimRight(every, "dw")); err == nil {
			unit := "day"
			if strings.HasSuffix(every, "w") {
				unit = "week"
			}
			every = quantity(n, unit)
		}
		text := "Every " + every
		if sp.At != "" {
			text += " at " + sp.At
		}
		if sp.From != "" {
			text += ", starting " + sp.From
		}
		return text
	}
	var dayset []int
	times := map[int]bool{}
	for _, expr := range sp.Cron {
		f := strings.Fields(strings.ToUpper(expr))
		if len(f) != 5 || f[2] != "*" || f[3] != "*" {
			return "Custom schedule"
		}
		for i, name := range []string{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"} {
			f[4] = strings.ReplaceAll(f[4], name, strconv.Itoa(i))
		}
		days, ok := cronValues(f[4], 6)
		if !ok {
			return "Custom schedule"
		}
		if dayset != nil && !slices.Equal(dayset, days) {
			return "Custom schedule"
		}
		dayset = days
		hours, ok := cronValues(f[1], 23)
		if !ok {
			return "Custom schedule"
		}
		minutes, ok := cronValues(f[0], 59)
		if !ok {
			return "Custom schedule"
		}
		for _, h := range hours {
			for _, m := range minutes {
				times[h*60+m] = true
			}
		}
	}
	var ts []int
	for m := range times {
		ts = append(ts, m)
	}
	slices.Sort(ts)
	if len(ts) == 0 {
		return "Custom schedule"
	}
	day := ""
	switch {
	case slices.Equal(dayset, []int{0, 1, 2, 3, 4, 5, 6}):
		day = "Daily"
	case slices.Equal(dayset, []int{1, 2, 3, 4, 5}):
		day = "Weekdays"
	case slices.Equal(dayset, []int{0, 6}):
		day = "Weekends"
	default:
		var names []string
		for _, d := range dayset {
			names = append(names, []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}[d])
		}
		day = strings.Join(names, ", ")
	}
	clock := func(m int) string { return fmt.Sprintf("%02d:%02d", m/60, m%60) }
	var parts []string
	for i := 0; i < len(ts); {
		end := i + 1
		if i+2 < len(ts) {
			step := ts[i+1] - ts[i]
			for end < len(ts) && ts[end]-ts[end-1] == step {
				end++
			}
			if end-i >= 3 {
				parts = append(parts, fmt.Sprintf("every %s from %s–%s", quantity(step, "minute"), clock(ts[i]), clock(ts[end-1])))
				i = end
				continue
			}
		}
		parts = append(parts, clock(ts[i]))
		i++
	}
	if len(parts) > 8 {
		return "Custom schedule"
	}
	return day + ", " + strings.Join(parts, ", plus ")
}

func quantity(n int, unit string) string {
	if n != 1 {
		unit += "s"
	}
	return fmt.Sprintf("%d %s", n, unit)
}

// Deliberately small subset. Never infer meaning for an unknown cron operator.
func cronValues(field string, max int) ([]int, bool) {
	values := map[int]bool{}
	for _, term := range strings.Split(field, ",") {
		base, stepText, stepped := strings.Cut(term, "/")
		step := 1
		if stepped {
			var err error
			step, err = strconv.Atoi(stepText)
			if err != nil || step < 1 {
				return nil, false
			}
		}
		lo, hi := 0, max
		if base != "*" {
			a, b, ranged := strings.Cut(base, "-")
			var err error
			lo, err = strconv.Atoi(a)
			if err != nil {
				return nil, false
			}
			hi = lo
			if ranged {
				hi, err = strconv.Atoi(b)
				if err != nil {
					return nil, false
				}
			} else if stepped {
				return nil, false
			}
		}
		if lo < 0 || hi > max || hi < lo {
			return nil, false
		}
		for n := lo; n <= hi; n += step {
			values[n] = true
		}
	}
	var out []int
	for n := range values {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, len(out) > 0
}
