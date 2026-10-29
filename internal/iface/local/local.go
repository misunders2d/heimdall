// Package local is a terminal interface for exercising Heimdall end to end
// without a chat platform. It trusts its operator: whoever types a line
// chooses the sender. Use it for local testing only, never in production.
//
// Each input line is a message. Optional leading tokens pick the sender and
// conversation for that line:
//
//	@alice #team/42 hello      sender local:alice, chat "team", thread "42"
//	!status                    default sender and chat
package local

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

const Platform = "local"

type Interface struct {
	In     io.Reader
	Out    io.Writer
	User   string // default sender ID
	Chat   string // default chat
	mu     sync.Mutex
	seq    atomic.Int64
	prefix string
	eof    chan struct{}
}

func New(in io.Reader, out io.Writer, user, chat string) *Interface {
	return &Interface{In: in, Out: out, User: user, Chat: chat, prefix: strconv.FormatInt(time.Now().UnixNano(), 36), eof: make(chan struct{})}
}

func (l *Interface) Name() string { return Platform }

// EOF is closed when the input ends.
func (l *Interface) EOF() <-chan struct{} { return l.eof }

func (l *Interface) Run(ctx context.Context, accept func(context.Context, core.Inbound) error) error {
	lines := make(chan string)
	go func() {
		defer close(lines)
		s := bufio.NewScanner(l.In)
		s.Buffer(make([]byte, 64<<10), 1<<20)
		for s.Scan() {
			select {
			case lines <- s.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-lines:
			if !ok {
				close(l.eof)
				<-ctx.Done()
				return nil
			}
			in, ok := l.parse(line)
			if !ok {
				continue
			}
			if err := accept(ctx, in); err != nil {
				l.print("heimdall", "message not accepted: "+err.Error())
			}
		}
	}
}

func (l *Interface) parse(line string) (core.Inbound, bool) {
	user, chat, thread := l.User, l.Chat, ""
	rest := strings.TrimSpace(line)
	for {
		tok, after, _ := strings.Cut(rest, " ")
		switch {
		case len(tok) > 1 && tok[0] == '@':
			user = tok[1:]
		case len(tok) > 1 && tok[0] == '#':
			chat, thread, _ = strings.Cut(tok[1:], "/")
		default:
			if rest == "" {
				return core.Inbound{}, false
			}
			id := fmt.Sprintf("%s-%d", l.prefix, l.seq.Add(1))
			return core.Inbound{
				Conv:   core.ConvKey{Interface: Platform, Chat: chat, Thread: thread},
				MsgID:  id,
				From:   core.Principal{Platform: Platform, ID: user, Name: user},
				Origin: core.OriginLive,
				Text:   rest,
				At:     time.Now(),
			}, true
		}
		rest = strings.TrimSpace(after)
	}
}

func (l *Interface) Send(_ context.Context, out core.Outbound) error {
	l.print(out.Conv.String(), out.Text)
	return nil
}

func (l *Interface) print(where, text string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.Out, "── %s\n%s\n", where, text)
}
