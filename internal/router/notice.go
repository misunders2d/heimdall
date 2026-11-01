package router

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// OwnerNotifier is an optional Interface that can write to an owner
// privately: OwnerChat is the owner's one-to-one conversation with the bot
// for the owner's ID on that platform.
type OwnerNotifier interface {
	OwnerChat(id string) (core.ConvKey, bool)
}

// newCaller tells every owner it can reach, once, that a non-root caller
// wrote for the first time: who (verified key and display name) and
// where, never what. Every identified caller is non-root without an
// allowlist, so this is how the owner learns who uses the bot.
func (r *Router) newCaller(in core.Inbound) {
	select {
	case r.noticeQueue <- in:
	default:
		r.log.Print("new-caller notice queue full; retry on next input")
	}
}

// One worker, bounded pending callers and three attempts spaced one minute.
// Resolution never owns the accept/conversation lock.
func (r *Router) callerNotices(ctx context.Context) {
	type pending struct {
		in       core.Inbound
		attempts int
		next     time.Time
	}
	jobs := map[string]pending{}
	tick := time.NewTicker(min(time.Second, r.noticeRetry))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case in := <-r.noticeQueue:
			if _, ok := jobs[in.From.Key()]; !ok && len(jobs) < 1024 {
				jobs[in.From.Key()] = pending{in: in}
			}
		case <-tick.C:
		}
		for key, j := range jobs {
			if j.attempts >= 3 || time.Now().Before(j.next) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			complete := r.notifyCaller(j.in)
			j.attempts++
			j.next = time.Now().Add(r.noticeRetry)
			if complete {
				delete(jobs, key)
			} else {
				jobs[key] = j
			}
		}
	}
}

func (r *Router) notifyCaller(in core.Inbound) bool {
	where := in.Conv.String()
	if in.Private {
		where += " (their private chat with the bot)"
	}
	name := sanitizeName(in.From.Name)
	if name == "" {
		name = "no name"
	}
	text := fmt.Sprintf("ℹ New caller: %q (%s) wrote for the first time, in %s. Their turns run as the non-root employee account.\n— heimdall", name, in.From.Key(), where)
	owners := make([]string, 0, len(r.opt.Access.Owners))
	for key := range r.opt.Access.Owners {
		owners = append(owners, key)
	}
	slices.Sort(owners)
	complete := true
	for _, key := range owners {
		platform, id, _ := strings.Cut(key, ":")
		n, ok := r.ifaces[platform].(OwnerNotifier)
		if !ok {
			continue
		}
		seen, err := r.store.OwnerNotified(in.From, key)
		if err != nil {
			complete = false
			continue
		}
		if seen {
			continue
		}
		if conv, ok := n.OwnerChat(id); ok {
			if err := r.store.NotifyCallerOwner(in.From, key, core.Outbound{Conv: conv, Text: text}); err != nil {
				r.log.Print("new-caller notice storage failed")
				complete = false
				continue
			}
			r.wake(conv.Interface)
		} else {
			complete = false
		}
	}
	return complete
}
