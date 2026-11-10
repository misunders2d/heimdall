package durable

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAssistant answers each wake by its key: a WAIT or a REPORT.
type fakeAssistant struct {
	mu      sync.Mutex
	calls   []string          // keys, in order
	prompts map[string]string // key → prompt
	plan    map[string]WorkResult
}

func (a *fakeAssistant) Work(_ context.Context, q WorkRequest) (WorkResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, q.Key)
	a.prompts[q.Key] = q.Prompt
	return a.plan[q.Key], nil
}

func (a *fakeAssistant) seen() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

// A non-coding commitment with no worker and no T3 at all is worked by
// wakes of the creator's assistant: a wait persists the owned next action
// and holds no compute (two commitments on a one-slot resource both
// progress), a restart resumes the wait, the next wake gets the next
// action back, and the report is delivered UNACCEPTED until verified.
func TestAssistantWorksWithoutT3(t *testing.T) {
	dir := t.TempDir()
	a := &fakeAssistant{prompts: map[string]string{}, plan: map[string]WorkResult{
		"MEL-70/1/work/1": {Turn: 1, Wait: 50 * time.Millisecond, Next: "check the supplier reply"},
		"MEL-70/1/work/2": {Turn: 2, Wait: 2 * time.Second, Next: "send the PO summary"},
		"MEL-70/1/work/3": {Turn: 3, Report: "PO 551 confirmed by supplier email 2026-11-08."},
		"MEL-71/1/work/1": {Turn: 4, Wait: time.Hour, Next: "recheck stock"},
	}}
	opt := Options{Assistant: a, Resources: map[string]Resource{defaultRes: {Concurrency: 1}}}
	e := open(t, dir, opt)
	e.cmd("add MEL-70 Confirm the supplier PO")
	e.cmd("add MEL-71 Watch the restock")
	waitFor(t, 20*time.Second, "both wakes", func() bool {
		s := a.seen()
		return slices.Contains(s, "MEL-70/1/work/2") && slices.Contains(s, "MEL-71/1/work/1")
	})
	// The fake records the wake when it is called; the workflow persists
	// its WAIT just after. Wait for that state, not only the call.
	var st State
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, _ = e.rt.State("MEL-71")
		if st.Phase == PhaseWorking && st.Next == "next wake in 1h0m0s: recheck stock" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.close() // restart during MEL-70's second wait

	e = open(t, dir, opt)
	defer e.close()
	e.phase("MEL-70", PhaseReported, 1)
	var got []string // each wake once, in order, across the restart
	for _, k := range a.seen() {
		if strings.HasPrefix(k, "MEL-70/") {
			got = append(got, k)
		}
	}
	if !slices.Equal(got, []string{"MEL-70/1/work/1", "MEL-70/1/work/2", "MEL-70/1/work/3"}) {
		t.Fatalf("wakes %q", got)
	}
	a.mu.Lock()
	p := a.prompts["MEL-70/1/work/3"]
	a.mu.Unlock()
	if !strings.Contains(p, "Goal: MEL-70: Confirm the supplier PO") || !strings.Contains(p, "Your persisted next action: send the PO summary") {
		t.Fatalf("prompt %q", p)
	}
	st, _ = e.rt.State("MEL-70")
	if st.Evidence != "PO 551 confirmed by supplier email 2026-11-08." || st.Verdict != "" {
		t.Fatalf("%+v", st)
	}
	waitFor(t, 5*time.Second, "unaccepted report", func() bool {
		return len(outbound(t, dir, "MEL-70 attempt 1: UNACCEPTED worker report. Evidence: PO 551")) == 1
	})
	e.cmdAs("v1", "accept MEL-70 checked the email")
	e.phase("MEL-70", PhaseClosed, 1)
	if len(outbound(t, dir, "MEL-70 accepted (attempt 1")) != 1 {
		t.Fatal("acceptance not delivered")
	}
}

// A wake that failed, or ended without a valid REPORT or WAIT, may already
// have acted: it is held with its key, turn and reason, never replayed
// under a new key while waiting or across a restart; only !work retry
// wakes the assistant again.
//
// That is the old path (before patchWakeRules), kept for workflows that
// started before it; TestWakeClassesAreBounded covers the new rules.
func TestAssistantAmbiguousWakeHolds(t *testing.T) {
	forceOldPath = true
	defer func() { forceOldPath = false }()
	dir := t.TempDir()
	a := &fakeAssistant{prompts: map[string]string{}, plan: map[string]WorkResult{
		"MEL-72/1/work/1": {Turn: 7, Failure: "failed: harness exited after sending the email"},
		"MEL-73/1/work/1": {Turn: 8}, // malformed: no report, no wait
		"MEL-72/1/work/2": {Turn: 9, Report: "Email to supplier confirmed sent once."},
	}}
	opt := Options{Assistant: a}
	e := open(t, dir, opt)
	e.cmd("add MEL-72 Email the supplier")
	e.cmd("add MEL-73 Post the update")
	held := func(issue, want string) {
		t.Helper()
		waitFor(t, 20*time.Second, issue+" held", func() bool {
			st, _ := e.rt.State(issue)
			return st.Phase == PhaseHeld && strings.Contains(st.Blocked, want)
		})
	}
	held("MEL-72", "work MEL-72/1/work/1 (turn 7): failed: harness exited after sending the email; it may have acted")
	held("MEL-73", "work MEL-73/1/work/1 (turn 8): no report and no valid WAIT")
	time.Sleep(300 * time.Millisecond)
	e.close()
	e = open(t, dir, opt)
	defer e.close()
	held("MEL-72", "MEL-72/1/work/1 (turn 7)")
	time.Sleep(300 * time.Millisecond)
	if got := a.seen(); !slices.Equal(got, []string{"MEL-72/1/work/1", "MEL-73/1/work/1"}) && !slices.Equal(got, []string{"MEL-73/1/work/1", "MEL-72/1/work/1"}) {
		t.Fatalf("replayed: %q", got)
	}
	e.cmdAs("r1", "retry MEL-72")
	e.phase("MEL-72", PhaseReported, 1)
	if st, _ := e.rt.State("MEL-72"); st.Evidence != "Email to supplier confirmed sent once." {
		t.Fatalf("%+v", st)
	}
}
