package scheduler

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/schedule"
)

func TestMonitorTemplateScheduleAndCatchUpContext(t *testing.T) {
	raw, err := os.ReadFile("../../docs/examples/owner-slack-monitor.json")
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t)
	j, err := r.s.proposal(asBoss, raw)
	if err != nil {
		t.Fatal(err)
	}
	d, _, err := parse(j)
	if err != nil {
		t.Fatal(err)
	}
	if d.Harness != "" || !d.Checkpoint || !d.Quiet || !d.Paused || d.To != "here" {
		t.Fatal(d)
	}
	if timeout, err := time.ParseDuration(d.Timeout); err != nil || timeout >= 15*time.Minute {
		t.Fatal("20:30 scan can consume closing slot", d.Timeout)
	}
	s, err := schedule.Compile(d.Schedule, d.TZ, r.now)
	if err != nil {
		t.Fatal(err)
	}
	for _, date := range []string{"2026-03-30", "2026-10-26", "2026-11-09"} {
		loc, _ := time.LoadLocation("Europe/Kyiv")
		midnight, _ := time.ParseInLocation("2006-01-02", date, loc)
		fires := s.Fires(midnight, 20)
		if len(fires) < 20 {
			t.Fatal("missing fires")
		}
		for i, f := range fires[:19] {
			want := midnight.Add(12*time.Hour + time.Duration(i)*30*time.Minute)
			if i == 18 {
				want = midnight.Add(20*time.Hour + 45*time.Minute)
			}
			if !f.At.Equal(want) {
				t.Fatalf("slot %d: %s, want %s", i, f.At, want)
			}
		}
		if !fires[19].At.Equal(midnight.AddDate(0, 0, 1).Add(12 * time.Hour)) {
			t.Fatal("late alert slot", fires[19])
		}
		friday := midnight.AddDate(0, 0, 4).Add(20*time.Hour + 45*time.Minute)
		next, ok := s.Next(friday)
		if !ok || !next.At.Equal(midnight.AddDate(0, 0, 7).Add(12*time.Hour)) {
			t.Fatal("weekend slot", next)
		}
	}
	if !strings.Contains(d.Prompt, "last successful") || strings.Contains(d.Prompt, "last 24 hours") {
		t.Fatal("fixed lookback loses overnight/weekend")
	}
}

func TestPreparedProbeAndCatalog(t *testing.T) {
	r := newRig(t)
	raw, err := os.ReadFile("../../docs/examples/scheduled-agent-probe.json")
	if err != nil {
		t.Fatal(err)
	}
	j, err := r.s.proposal(asBoss, raw)
	if err != nil {
		t.Fatal(err)
	}
	d, _, _ := parse(j)
	if !d.Paused || !d.Checkpoint || d.Harness != "" || !strings.Contains(d.Prompt, "<THREAD_REFERENCE>") {
		t.Fatal("unsafe or incomplete probe", d)
	}
	raw, err = os.ReadFile("../../docs/examples/scheduler-catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Servers map[string]struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err = json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	entry, ok := catalog.Servers["heimdall-scheduler"]
	if !ok || len(catalog.Servers) != 1 || entry.Command != "/path/to/heimdall" || len(entry.Args) != 4 || entry.Args[0] != "sched" || entry.Args[1] != "mcp" || entry.Args[2] != "-socket" {
		t.Fatal("invalid shared catalog", catalog)
	}
}
