package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// The admin export is fixed fields only: an 80-character whitespace-
// collapsed title, origin, phase, since, attempt, needs_owner and
// next_wake; no brief, note, evidence, verdict, blocked reason, next step,
// worker or route. Closed tasks older than 7 days are hidden, open ones
// all listed, newest since first, at most 200 rows, never null.
func TestExportRows(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	const leak = "SECRET-MARKER"
	full := func(issue, title, phase string, since time.Time) View {
		return View{Issue: issue, Title: title, Status: string(dbos.WorkflowStatusPending), Worker: leak,
			State: State{Phase: phase, Attempt: 2, Since: since, Evidence: leak, Verdict: leak, Next: leak, Blocked: leak, Note: leak, Runs: []string{leak}, Dispatched: leak},
			cm:    Commitment{Issue: issue, Title: title, Brief: leak, Next: leak, Worker: leak, ReplyTo: leak}}
	}
	long := "  Confirm\tthe   supplier\nPO " + strings.Repeat("é", 100) + " " + leak
	t3 := full("T3-4", "Fix the build", PhaseTracking, now.Add(-time.Minute))
	t3.cm.Intake, t3.cm.Origin = &Intake{Thread: leak, Run: leak}, OriginTrustedT3
	queued := View{Issue: "MEL-9", Title: "Queued one", Status: string(dbos.WorkflowStatusEnqueued), Created: now.Add(-time.Hour)}
	vs := []View{
		full("MEL-1", long, PhaseWorking, now.Add(-3*time.Hour)),
		full("MEL-2", "Held one", PhaseHeld, now.Add(-2*time.Hour)),
		full("MEL-3", "Closed recently", PhaseClosed, now.Add(-6*24*time.Hour)),
		full("MEL-4", "Closed long ago", PhaseClosed, now.Add(-8*24*time.Hour)),
		full("MEL-5", "Old but open", PhaseWorking, now.Add(-30*24*time.Hour)),
		t3, queued,
	}
	raw := exportJSON(vs, now)
	if bytes.Contains(raw, []byte(leak)) || bytes.Contains(raw, []byte("SECRET")) {
		t.Fatalf("free text leaked: %s", raw)
	}
	var out struct {
		Tasks []TaskRow `json:"tasks"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	var shape struct {
		Tasks []map[string]any `json:"tasks"`
	}
	_ = json.Unmarshal(raw, &shape)
	for _, row := range shape.Tasks {
		var keys []string
		for k := range row {
			keys = append(keys, k)
		}
		if len(row) != 8 {
			t.Fatalf("row fields %v", keys)
		}
	}
	var ids []string
	for _, r := range out.Tasks {
		ids = append(ids, r.ID)
		if r.ID == "MEL-4" {
			t.Fatal("closed more than 7 days ago listed")
		}
		if _, err := time.Parse(time.RFC3339, r.Since); err != nil {
			t.Fatalf("%s since %q", r.ID, r.Since)
		}
		if r.NextWake != "" {
			t.Fatalf("next_wake %q", r.NextWake)
		}
		if r.NeedsOwner != (r.ID == "MEL-2") {
			t.Fatalf("%s needs_owner %t", r.ID, r.NeedsOwner)
		}
	}
	if got := strings.Join(ids, " "); got != "T3-4 MEL-9 MEL-2 MEL-1 MEL-3 MEL-5" {
		t.Fatalf("order %q", got)
	}
	rows := map[string]TaskRow{}
	for _, r := range out.Tasks {
		rows[r.ID] = r
	}
	if title := rows["MEL-1"].Title; len([]rune(title)) != 80 || strings.ContainsAny(title, "\t\n") || strings.Contains(title, "  ") || !strings.HasPrefix(title, "Confirm the supplier PO éé") {
		t.Fatalf("title %q", title)
	}
	if r := rows["T3-4"]; r.Origin != "t3" || r.Phase != PhaseTracking || r.Attempt != 2 {
		t.Fatalf("%+v", r)
	}
	if r := rows["MEL-1"]; r.Origin != "chat" || r.Phase != PhaseWorking || r.Since != now.Add(-3*time.Hour).Format(time.RFC3339) {
		t.Fatalf("%+v", r)
	}
	if r := rows["MEL-9"]; r.Phase != "queued" || r.Since != now.Add(-time.Hour).Format(time.RFC3339) || r.Attempt != 0 {
		t.Fatalf("queued %+v", r)
	}

	// At most 200 rows, newest first; closed rows are dropped before the cap.
	var many []View
	for i := range 250 {
		many = append(many, full(fmt.Sprintf("MEL-%d", 100+i), "Open", PhaseWorking, now.Add(-time.Duration(i)*time.Minute)))
	}
	many = append(many, full("MEL-999", "Closed old", PhaseClosed, now.Add(-9*24*time.Hour)))
	_ = json.Unmarshal(exportJSON(many, now), &out)
	if len(out.Tasks) != 200 || out.Tasks[0].ID != "MEL-100" || out.Tasks[199].ID != "MEL-299" {
		t.Fatalf("%d rows, first %s", len(out.Tasks), out.Tasks[0].ID)
	}
	// Recently closed rows never push out an old open or held task.
	many = nil
	for i := range 250 {
		many = append(many, full(fmt.Sprintf("T3-%d", i+1), "Done", PhaseClosed, now.Add(-time.Duration(i)*time.Minute)))
	}
	many = append(many, full("MEL-50", "Old held", PhaseHeld, now.Add(-20*24*time.Hour)))
	_ = json.Unmarshal(exportJSON(many, now), &out)
	if n := len(out.Tasks); n != 200 || out.Tasks[0].ID != "T3-1" || out.Tasks[199].ID != "MEL-50" || !out.Tasks[199].NeedsOwner {
		t.Fatalf("%d rows, last %+v", n, out.Tasks[n-1])
	}
	// A cancelled or failed workflow that never published state is not queued.
	_ = json.Unmarshal(exportJSON([]View{{Issue: "MEL-60", Status: string(dbos.WorkflowStatusCancelled), Created: now}, {Issue: "MEL-61", Status: string(dbos.WorkflowStatusError), Created: now}}, now), &out)
	if len(out.Tasks) != 2 || out.Tasks[0].Phase != "cancelled" || out.Tasks[1].Phase != "failed" {
		t.Fatalf("%+v", out.Tasks)
	}
	if got := string(exportJSON(nil, now)); got != `{"tasks":[]}` {
		t.Fatalf("empty export %s", got)
	}
}

// Export reads the running runtime's own views: an admitted task appears
// with its stored title and a defined phase and time.
func TestExportRuntime(t *testing.T) {
	e := open(t, t.TempDir(), Options{Silence: -1})
	defer e.close()
	if reply, changed := e.rt.Task(owner, "add", "MEL-70", "Ship the   export", "req-70"); !changed {
		t.Fatal(reply)
	}
	raw, err := e.rt.Export(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Tasks []TaskRow `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || len(out.Tasks) != 1 {
		t.Fatalf("%v %s", err, raw)
	}
	r := out.Tasks[0]
	if _, err := time.Parse(time.RFC3339, r.Since); err != nil || r.ID != "MEL-70" || r.Title != "Ship the export" || r.Origin != "chat" || r.Phase == "" || r.NeedsOwner {
		t.Fatalf("%+v", r)
	}
}

// A cancelled task ages out like a closed one and, over the cap, goes
// before open ones; a failed task needs the owner, a paused one does not.
func TestExportCancelledAndFailed(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	row := func(issue, status, pausedBy string, since time.Time) View {
		return View{Issue: issue, Title: issue, Status: status, PausedBy: pausedBy, State: State{Phase: PhaseWorking, Since: since}}
	}
	cancelled, failed := string(dbos.WorkflowStatusCancelled), string(dbos.WorkflowStatusError)
	var out struct {
		Tasks []TaskRow `json:"tasks"`
	}
	_ = json.Unmarshal(exportJSON([]View{
		row("MEL-1", cancelled, "", now.Add(-8*24*time.Hour)),
		row("MEL-2", cancelled, "", now.Add(-6*24*time.Hour)),
		row("MEL-3", failed, "", now.Add(-30*24*time.Hour)),
		row("MEL-4", cancelled, "owner", now.Add(-30*24*time.Hour)),
	}, now), &out)
	got := map[string]TaskRow{}
	for _, r := range out.Tasks {
		got[r.ID] = r
	}
	if _, ok := got["MEL-1"]; ok {
		t.Fatal("cancelled more than 7 days ago listed")
	}
	if r, ok := got["MEL-2"]; !ok || r.Phase != "cancelled" || r.NeedsOwner {
		t.Fatalf("recent cancelled %+v", r)
	}
	if r := got["MEL-3"]; r.Phase != "failed" || !r.NeedsOwner {
		t.Fatalf("failed %+v", r)
	}
	if r := got["MEL-4"]; r.Phase != "paused" || r.NeedsOwner {
		t.Fatalf("paused %+v", r)
	}

	// Over the cap, recently cancelled rows never push out an old open task.
	var many []View
	for i := range 250 {
		many = append(many, row(fmt.Sprintf("T3-%d", i+1), cancelled, "", now.Add(-time.Duration(i)*time.Minute)))
	}
	many = append(many, row("MEL-50", string(dbos.WorkflowStatusPending), "", now.Add(-20*24*time.Hour)))
	_ = json.Unmarshal(exportJSON(many, now), &out)
	if n := len(out.Tasks); n != 200 || out.Tasks[199].ID != "MEL-50" {
		t.Fatalf("%d rows, last %+v", n, out.Tasks[n-1])
	}
}
