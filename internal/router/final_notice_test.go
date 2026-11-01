package router

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestFinalOwnerPartialResolutionAsync(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	// Sorted first owner fails; second must still receive durable notice.
	e.r.opt.Access.Owners["fake:a-unreachable"] = true
	var calls atomic.Int32
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	e.f.mu.Lock()
	e.f.ownerHook = func(id string) (core.ConvKey, bool) {
		if id == "a-unreachable" {
			calls.Add(1)
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return core.ConvKey{}, false
		}
		return core.ConvKey{Interface: "fake", Chat: "dm-" + id}, true
	}
	e.f.mu.Unlock()
	defer close(release)
	e.send(employee, "c1", "!help")
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no async lookup")
	}
	// Lookup blocked: turn and stop still complete.
	if out := e.reply(); out.Conv.Chat != "c1" {
		t.Fatal("turn delayed")
	}
	e.send(employee, "c1", "stop")
	e.reply()
	release <- struct{}{}
	out := e.reply()
	if out.Conv.Chat != "dm-owner" {
		t.Fatalf("reachable owner skipped: %+v", out)
	}
	p := core.Principal{Platform: "fake", ID: "employee"}
	if ok, _ := e.st.CallerSeen(p); !ok {
		t.Fatal("partial success not marked seen")
	}
	if ok, _ := e.st.OwnerNotified(p, "fake:owner"); !ok {
		t.Fatal("owner not durably marked")
	}
	// Repeat messages don't re-resolve failed owner before retry deadline.
	for range 4 {
		e.send(employee, "c1", "!help")
		e.reply()
	}
	if calls.Load() != 1 {
		t.Fatal("owner retries not rate limited")
	}
	// Direct retry simulates later worker attempt and commits only failed owner.
	e.f.mu.Lock()
	e.f.ownerHook = func(id string) (core.ConvKey, bool) { return core.ConvKey{Interface: "fake", Chat: "dm-" + id}, true }
	e.f.mu.Unlock()
	if !e.r.notifyCaller(core.Inbound{From: p, Conv: core.ConvKey{Interface: "fake", Chat: "c1"}}) {
		t.Fatal("retry failed")
	}
	if out := e.reply(); out.Conv.Chat != "dm-a-unreachable" {
		t.Fatal("duplicate successful notice or failed owner lost")
	}
	e.noReply(100 * time.Millisecond)
}

func TestFinalOwnerBackgroundRetryBounded(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	var calls atomic.Int32
	e.f.mu.Lock()
	e.f.ownerHook = func(id string) (core.ConvKey, bool) { calls.Add(1); return core.ConvKey{}, false }
	e.f.mu.Unlock()
	r := New(e.r.opt, e.st, e.f)
	r.noticeRetry = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.callerNotices(ctx) }()
	defer func() { cancel(); <-done }()
	in := core.Inbound{From: core.Principal{Platform: "fake", ID: "retry"}}
	r.newCaller(in)
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 3 {
		t.Fatal("failed owner not retried")
	}
	for range 5 {
		r.newCaller(in)
	}
	time.Sleep(40 * time.Millisecond)
	if calls.Load() != 3 {
		t.Fatal("owner retries unbounded")
	}
}
