package router

import (
	"fmt"
	"slices"
	"strings"

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
	first, err := r.store.FirstMessage(in.From)
	if err != nil {
		r.log.Printf("new-caller check for a %s caller failed: %v", in.From.Platform, err)
		return
	}
	if !first {
		return
	}
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
	for _, key := range owners {
		platform, id, _ := strings.Cut(key, ":")
		n, ok := r.ifaces[platform].(OwnerNotifier)
		if !ok {
			continue
		}
		if conv, ok := n.OwnerChat(id); ok {
			r.enqueue(core.Outbound{Conv: conv, Text: text})
		}
	}
}
