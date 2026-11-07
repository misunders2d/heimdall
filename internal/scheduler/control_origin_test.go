package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestControlExactOriginAcrossPlatformsAndRetries(t *testing.T) {
	r, c := controlRig(t)
	slack := core.SlackPrincipal("owner@example.test", "Ttest", "")
	r.s.access.Owners[slack.Key()] = true
	r.s.Reverify = func(context.Context, core.Principal) (Verified, bool, error) {
		return Verified{Principal: slack}, true, nil
	}
	callers := []Caller{asBoss,
		{Principal: slack, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "Dother", Thread: "1000.1"}, Private: true},
		{Principal: slack, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "Cshared", Thread: "1000.2"}},
		{Principal: boss, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "telegram", Chat: "1", Thread: "3"}, Private: true},
	}
	for i, who := range callers {
		if _, _, err := r.st.AddInbound(core.Inbound{Conv: who.Conv, From: who.Principal, Origin: core.OriginLive, Private: who.Private, MsgID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Reproduce historical selection: deployment's Slack selector resolves an
	// unrelated Slack thread even though this request is from Telegram.
	_, wrong, err := r.st.ControlBinding(slack.Key())
	if err != nil || wrong == asBoss.Conv {
		t.Fatal(wrong, err)
	}
	c.owner = slack.Key()
	c.source = &wrong // neither deployment field may override ingress
	ctx := context.Background()
	req := func(id string) ControlRequest {
		return ControlRequest{Action: "create", RequestID: id, Definition: json.RawMessage(`{"name":"origin","schedule":{"every":"1h"},"prompt":"private prompt","to":"tg:999","paused":true}`)}
	}
	if got := c.execute(ctx, 1234, req("absent")); got.Error == "" {
		t.Fatal("unbound request admitted")
	}
	var wg sync.WaitGroup
	for i, who := range callers {
		wg.Add(1)
		go func(i int, who Caller) {
			defer wg.Done()
			request := req(fmt.Sprint("create-", i))
			got := c.execute(ctx, 1234, request, who)
			if got.Error != "" {
				t.Error(got)
				return
			}
			job, ok, err := r.st.Job(got.DraftID)
			if err != nil || !ok || job.CreatedIn != who.Conv || job.Creator.Key() != who.Principal.Key() {
				t.Error(job, err)
				return
			}
			replay := c.execute(ctx, 1234, request, who)
			if replay != got {
				t.Error("retry changed", replay, got)
			}
			if _, change := r.s.Command(who, "job confirm "+got.DraftID); change == nil {
				t.Error("confirmation failed")
			} else {
				r.apply(change)
			}
			// Another thread of the same creator cannot reuse a completed operation.
			other := who
			if i == 0 {
				other = callers[3]
			} else if i == 3 {
				other = callers[0]
			} else if i == 1 {
				other = callers[2]
			} else {
				other = callers[1]
			}
			if cross := c.execute(ctx, 1234, request, other); cross.Error == "" {
				t.Error("cross-origin replay accepted", cross)
			}
			for _, action := range []string{"edit", "remove"} {
				change := ControlRequest{Action: action, ID: got.DraftID, RequestID: fmt.Sprintf("%s-%d", action, i)}
				if action == "edit" {
					change.Definition = json.RawMessage(`{"name":"edited"}`)
				}
				result := c.execute(ctx, 1234, change, who)
				if result.Error != "" {
					t.Error(action, result)
				}
			}
		}(i, who)
	}
	wg.Wait()
	outs := r.sent()
	for {
		row, ok, err := r.st.NextOutbound(core.PlatformSlack)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := r.st.Sent(row.ID); err != nil {
			t.Fatal(err)
		}
		outs = append(outs, row.Outbound)
	}
	if len(outs) != 12 {
		t.Fatalf("unexpected cards %d", len(outs))
	}
	counts := map[core.ConvKey]int{}
	for _, out := range outs {
		counts[out.Conv]++
		if out.Conv.Chat == "999" {
			t.Fatal("result target redirected approval")
		}
		if out.Conv.Chat == "Cshared" && containsPrivate(out.Text) {
			t.Fatal("shared prompt leak")
		}
	}
	for _, who := range callers {
		if counts[who.Conv] != 3 {
			t.Fatal(counts)
		}
	}
	unknown := asBoss
	unknown.Conv.Thread = "unknown"
	if got := c.execute(ctx, 1234, req("unknown"), unknown); got.Error == "" {
		t.Fatal("unknown origin admitted")
	}
	employee := asBoss
	employee.Role = core.RoleEmployee
	if got := c.execute(ctx, 1234, req("employee"), employee); got.Error == "" {
		t.Fatal("employee admitted")
	}
	for _, raw := range []string{`{"action":"list","source":{}}`, `{"action":"list","origin":{}}`, `{"action":"list","approved":true}`} {
		var v ControlRequest
		if strictJSON([]byte(raw), &v) == nil {
			t.Fatal("spoof accepted", raw)
		}
	}
}
func containsPrivate(s string) bool { return strings.Contains(s, "private prompt") }
