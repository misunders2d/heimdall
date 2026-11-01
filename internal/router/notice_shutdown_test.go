package router

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

type cancelNoticeIface struct {
	*fakeIface
	entered chan struct{}
}

func (f *cancelNoticeIface) OwnerChatContext(ctx context.Context, _ string) (core.ConvKey, bool) {
	close(f.entered)
	<-ctx.Done()
	return core.ConvKey{}, false
}
func TestCallerNoticeWorkerCancelsResolution(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	f := &cancelNoticeIface{fakeIface: e.f, entered: make(chan struct{})}
	r := New(e.r.opt, e.st, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.callerNotices(ctx) }()
	defer func() { cancel(); <-done }()
	r.newCaller(core.Inbound{From: core.Principal{Platform: "fake", ID: "shutdown"}})
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("context resolver not used")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notice worker ignored shutdown")
	}
}

func TestCallerNoticeMapFullLogged(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	r := New(e.r.opt, e.st, e.f)
	logs := &syncBuffer{}
	r.log = log.New(logs, "", 0)
	r.noticeRetry = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.callerNotices(ctx) }()
	defer func() { cancel(); <-done }()
	for i := 0; i < 1025; i++ {
		select {
		case r.noticeQueue <- core.Inbound{From: core.Principal{Platform: "fake", ID: fmt.Sprint("full-", i)}}:
		case <-time.After(5 * time.Second):
			t.Fatal("worker stopped consuming")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "pending map full") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !strings.Contains(logs.String(), "pending map full") {
		t.Fatal("map-full drop silent")
	}
}
