package core

import (
	"errors"
	"slices"
	"time"
)

// DeliveryWindow limits attempts, including retries, in the destination's zone.
// End is exclusive. Windows crossing midnight are deliberately unsupported.
type DeliveryWindow struct {
	TZ    string         `json:"tz"`
	Days  []time.Weekday `json:"days"`
	Start string         `json:"start"`
	End   string         `json:"end"`
}

func (w *DeliveryWindow) Validate() error {
	if w == nil {
		return nil
	}
	_, err := time.LoadLocation(w.TZ)
	a, ea := time.Parse("15:04", w.Start)
	b, eb := time.Parse("15:04", w.End)
	if w.TZ == "" || w.TZ == "Local" || err != nil || ea != nil || eb != nil || !a.Before(b) || len(w.Days) == 0 || len(w.Days) > 7 {
		return errors.New("delivery_window: IANA tz, distinct days 0-6, start < end (HH:MM) required")
	}
	for i, d := range w.Days {
		if d < 0 || d > 6 || slices.Contains(w.Days[:i], d) {
			return errors.New("delivery_window: invalid or duplicate weekday")
		}
	}
	return nil
}

// Next returns now inside the window, otherwise the next opening. Validate first.
func (w *DeliveryWindow) Next(now time.Time) time.Time {
	if w == nil {
		return now
	}
	loc, err := time.LoadLocation(w.TZ)
	if err != nil {
		return now.Add(24 * time.Hour)
	} // corrupt persisted policy fails closed
	t := now.In(loc)
	start, _ := time.Parse("15:04", w.Start)
	end, _ := time.Parse("15:04", w.End)
	for i := 0; i < 8; i++ {
		day := t.AddDate(0, 0, i)
		if !slices.Contains(w.Days, day.Weekday()) {
			continue
		}
		a := time.Date(day.Year(), day.Month(), day.Day(), start.Hour(), start.Minute(), 0, 0, loc)
		b := time.Date(day.Year(), day.Month(), day.Day(), end.Hour(), end.Minute(), 0, 0, loc)
		if now.Before(a) {
			return a
		}
		if now.Before(b) {
			return now
		}
	}
	return now.Add(7 * 24 * time.Hour)
}

// EndAt bounds an in-window attempt and its internal transport retries.
func (w *DeliveryWindow) EndAt(now time.Time) time.Time {
	if w == nil || w.Validate() != nil || w.Next(now).After(now) {
		return now
	}
	loc, _ := time.LoadLocation(w.TZ)
	t := now.In(loc)
	end, _ := time.Parse("15:04", w.End)
	return time.Date(t.Year(), t.Month(), t.Day(), end.Hour(), end.Minute(), 0, 0, loc)
}
