package router

import (
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// replies collects n replies by chat.
func (e *env) replies(n int) map[string][]core.Outbound {
	e.t.Helper()
	got := map[string][]core.Outbound{}
	for range n {
		out := e.reply()
		got[out.Conv.Chat] = append(got[out.Conv.Chat], out)
	}
	return got
}

// Item 10: one turn slot is kept for the owner. With max_parallel 2 an
// employee's running turn takes the one general slot; a second employee's
// message waits and its sender is told once; the owner's message runs at
// once in the reserved slot.
func TestOwnerHasAReservedSlotAndOthersAreTold(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.MaxParallel = 2
	e.start()
	e.send(employee, "a", "long WAIT_STEER")
	time.Sleep(300 * time.Millisecond)
	waiting := e.send(coworker, "b", "second")
	note := e.reply()
	if note.Conv.Chat != "b" || note.ReplyTo != waiting || note.Text != "Queued — starting when a slot frees. — heimdall" {
		t.Fatalf("queued note = %+v", note)
	}
	e.send(owner, "c", "owner first")
	out := e.reply()
	if out.Conv.Chat != "c" {
		t.Fatalf("owner waited behind others: got %+v first", out)
	}
	want(t, out, "omp#1", "role owner")
	e.noReply(200 * time.Millisecond) // the coworker still waits
	e.send(employee, "a", "FINISH")
	got := e.replies(2)
	want(t, got["a"][0], "steers=1")
	want(t, got["b"][0], `last="second"`)
	for _, outs := range got {
		for _, o := range outs {
			if strings.Contains(o.Text, "Queued") {
				t.Fatalf("second queued note: %+v", got)
			}
		}
	}
}

// With max_parallel 1 there is no reserved slot: the owner waits too, and
// is told.
func TestSingleSlotHasNoReservation(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.MaxParallel = 1
	e.start()
	e.send(employee, "a", "long WAIT_STEER")
	time.Sleep(300 * time.Millisecond)
	e.send(owner, "c", "owner")
	note := e.reply()
	if note.Conv.Chat != "c" || note.Text != "Queued — starting when a slot frees. — heimdall" {
		t.Fatalf("queued note = %+v", note)
	}
	e.send(employee, "a", "FINISH")
	got := e.replies(2)
	want(t, got["c"][0], "role owner")
}
