package slack

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestOwnerStopDoesNotResolveQueuedUsers(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	s.opt.Owners = []string{"slack:uowner@example.test"}
	for range 10 {
		stageReview(t, s, f.event("D1", "im", "Uemployee", "earlier", "root"))
	}
	f.setHook(func(w http.ResponseWriter, r *http.Request, args map[string]any) bool {
		if r.URL.Path == "/users.info" && args["user"] != "Uowner" {
			fmt.Fprint(w, `{"ok":false,"error":"internal_error"}`)
			return true
		}
		return false
	})
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "stop", "root"))
	called := false
	if err := s.process(context.Background(), e, func(_ context.Context, in core.Inbound) error {
		called = true
		if in.ControlIngress != 10 {
			t.Fatalf("count %d", in.ControlIngress)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called || len(f.all("users.info")) != 1 {
		t.Fatal("stop resolved queued users")
	}
}

func TestControlIdentityPacerPriority(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	p := &apiPacer{interval: 20 * time.Millisecond, next: map[string]time.Time{"users.info": time.Now().Add(time.Second)}}
	s.bot.pacer = p
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	// Waiting ordinary reads must not own the pacer mutex or reserve future slots.
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); s.liveUser(ctx, "Uemployee") }()
	}
	time.Sleep(10 * time.Millisecond)
	e := stageReview(t, s, f.event("D1", "im", "Uowner", "stop", "root"))

	result := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		result <- s.process(ctx, e, func(context.Context, core.Inbound) error { return nil })
	}()
	registered := false
	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p.mu.TryLock() {
			registered = p.priority["users.info"] > 0
			p.mu.Unlock()
			if registered {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !registered {
		t.Fatal("control identity did not register priority while ordinary readers waited")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	calls := f.all("users.info")
	if len(calls) == 0 || calls[0].Body["user"] != "Uowner" {
		t.Fatal("ordinary identity reads preceded control")
	}
}

func TestLookupEmailTier3Pacing(t *testing.T) {
	p := &apiPacer{interval: time.Millisecond, next: map[string]time.Time{}}
	if tier4.interval != 600*time.Millisecond {
		t.Fatal("wrong production interval")
	}
	before := time.Now()
	if err := p.wait(context.Background(), "users.lookupByEmail"); err != nil {
		t.Fatal(err)
	}
	if delay := p.next["users.lookupByEmail"].Sub(before); delay < 2*time.Millisecond {
		t.Fatal("lookupByEmail not paced at 50/minute")
	}
}

func TestOwnerChatHonorsCallerCancellation(t *testing.T) {
	f := newSlackFake(t)
	s, _ := testSlack(t, f)
	close(s.ready)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := s.OwnerChatContext(ctx, "uowner@example.test"); ok {
		t.Fatal("canceled owner resolution succeeded")
	}
	if len(f.all("users.lookupByEmail")) != 0 {
		t.Fatal("canceled resolution made API call")
	}
}

// Guard the dispatch-loop integration, separately from drainIngressDone's unit
// behavior: every scan must release ALL buffered worker slots before dispatch.
func TestProcessEventsDrainsBeforeScan(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	tree, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(file), "slack.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range tree.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "processEvents" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			loop, ok := n.(*ast.ForStmt)
			if !ok {
				return true
			}
			if len(loop.Body.List) == 0 {
				return true
			}
			stmt, ok := loop.Body.List[0].(*ast.ExprStmt)
			if !ok {
				return true
			}
			call, ok := stmt.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && id.Name == "drainIngressDone" {
				found = true
			}
			return true
		})
	}
	if !found {
		t.Fatal("dispatcher must drain completions before scanning backlog")
	}
}
