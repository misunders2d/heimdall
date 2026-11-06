package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

const capacityError = "Selected model is at capacity. Please try a different model."

func TestFailureNoticeCapacityAndTerminalState(t *testing.T) {
	for _, state := range []string{"recurring", "paused", "one-off", "completed"} {
		t.Run(state, func(t *testing.T) {
			r := newRig(t)
			schedule := `{"every":"1h"}`
			if state == "one-off" {
				schedule = `{"at":"2026-11-03T11:00"}`
			}
			id := r.add(asBoss, `{"name":"capacity","tz":"UTC","schedule":`+schedule+`,"quiet":true,"prompt":"private job prompt"}`)
			if state == "paused" {
				r.s.cfg.PauseAfter = 1
			}
			r.now = r.now.Add(time.Hour)
			r.tick()
			r.sent()
			answer := "RAW ANSWER MUST NOT APPEAR"
			if state == "completed" {
				answer += "\n[job: done]"
			}
			e := r.finish(answer, Outcome{Harness: "codex", Result: harness.Result{Failure: capacityError}})
			outs := r.sent()
			if e.Run.State != store.RunFailed || !e.Run.Retry.IsZero() || e.Reply != nil || len(outs) == 0 {
				t.Fatalf("terminal failure: %+v, notices %+v", e, outs)
			}
			body := outs[0].Text
			for _, want := range []string{"Cause: provider unavailable", "Error: " + capacityError, "Attempts: 1", "Automatic retry: no", "— heimdall"} {
				if !strings.Contains(body, want) {
					t.Errorf("notice missing %q: %s", want, body)
				}
			}
			if strings.Contains(body, "Next run:") || strings.Contains(body, "RAW ANSWER") || strings.Contains(body, "private job prompt") {
				t.Fatalf("ambiguous or private notice: %s", body)
			}
			if got := strings.Contains(body, "Next scheduled run: 2026-11-03 12:00 UTC"); got != (state == "recurring") {
				t.Fatalf("next slot in %s notice: %s", state, body)
			}
			wantState := map[string]string{"recurring": store.JobActive, "paused": store.JobPaused, "one-off": store.JobDone, "completed": store.JobDone}[state]
			if r.job(id).State != wantState || !strings.Contains(r.runs(id)[0].Cause, capacityError) {
				t.Fatalf("durable result: %+v %+v", r.job(id), r.runs(id))
			}
			if history := r.cmd(asBoss, "job history "+id); !strings.Contains(history, capacityError) || !strings.Contains(history, "retry: no") {
				t.Fatalf("history: %s", history)
			}
		})
	}
}

func TestRetryNoticeMatchesQueuedSameSlot(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"retry","tz":"UTC","schedule":{"every":"1h"},"prompt":"p"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	r.sent()
	before := r.runs(id)[0]
	e := r.finish("", Outcome{Notes: []string{"codex failed before starting: " + capacityError}})
	after := r.runs(id)[0]
	if e.Run.Retry.IsZero() || after.State != store.RunPending || after.Attempt != 1 || !after.NotBefore.Equal(e.Run.Retry.Truncate(time.Millisecond)) || after.ID != before.ID || after.Slot != before.Slot || !after.Due.Equal(before.Due) {
		t.Fatalf("retry changed: before %+v, after %+v, end %+v", before, after, e.Run)
	}
	outs := r.sent()
	if len(outs) != 1 || outs[0].Conv != bossDM {
		t.Fatalf("retry not persisted to creator: %+v", outs)
	}
	for _, want := range []string{"will retry the same slot", capacityError, "Attempts: 1", "Automatic retry: queued for " + when(Def{TZ: "UTC"}, after.NotBefore) + " (attempt 2)", "Next scheduled run: 2026-11-03 12:00 UTC"} {
		if !strings.Contains(outs[0].Text, want) {
			t.Errorf("retry notice missing %q: %s", want, outs[0].Text)
		}
	}
	if h := r.cmd(asBoss, "job history "+id); !strings.Contains(h, capacityError) || !strings.Contains(h, "retry automatically at ") || !strings.Contains(h, "(attempt 2)") {
		t.Fatalf("pending retry history: %s", h)
	}
	r.rows = nil
	r.now = after.NotBefore
	r.s.start(context.Background(), r.now)
	if len(r.rows) != 1 || r.rows[0].Run.Run != before.ID || r.rows[0].Run.Attempt != 1 {
		t.Fatalf("retry dispatch: %+v", r.rows)
	}
	r.finish("done", Outcome{Harness: "codex"})
	if r.runs(id)[0].State != store.RunOK {
		t.Fatal("same run did not complete")
	}
}

func TestFailureDetailsStayPrivateInNoticesAndHistory(t *testing.T) {
	for _, caller := range []Caller{asBoss, inGroup} {
		t.Run(fmt.Sprint(caller.Private), func(t *testing.T) {
			r := newRig(t)
			id := r.add(caller, `{"name":"shared","schedule":{"every":"1h"},"to":"tg:-100","prompt":"p"}`)
			r.now = r.now.Add(time.Hour)
			r.tick()
			r.sent()
			e := r.finish("", Outcome{Harness: "codex", Result: harness.Result{Failure: capacityError}})
			if len(e.Notices) != map[bool]int{true: 2, false: 1}[caller.Private] {
				t.Fatalf("audience changed: %+v", e.Notices)
			}
			for _, o := range e.Notices {
				if o.Conv != bossDM && o.Conv != team || strings.Contains(o.Text, capacityError) != (o.Conv == bossDM) || !strings.Contains(o.Text, "Cause: provider unavailable") || !strings.Contains(o.Text, "Automatic retry: no") {
					t.Fatalf("wrong disclosure: %+v", o)
				}
			}
			for _, cmd := range []string{"job history ", "job show "} {
				if text := r.cmd(inGroup, cmd+id); strings.Contains(text, capacityError) || !strings.Contains(text, "provider unavailable") {
					t.Fatalf("shared history: %s", text)
				}
			}
		})
	}
}

func TestFailureDetailsRedactionAndBounds(t *testing.T) {
	t.Setenv("HEIMDALL_TEST_SECRET", "synthetic-env-secret")
	t.Setenv("OPENAI_API_KEY", "synthetic-api-secret")
	for _, leak := range []string{
		"synthetic-env-secret", "synthetic-api-secret", "sk-" + strings.Repeat("a", 32), "ghp_" + strings.Repeat("a", 36),
		"Bearer synthetic-bearer", "token=synthetic-token", "Cookie: synthetic-cookie", "password synthetic-password",
		"Authorization: Bearer synthetic-bearer", `"token":"synthetic-token"`, "Cookie: first=synthetic-cookie; second=synthetic-cookie",
		`"cookie":"synthetic-cookie"`, `https:\/\/example.com\/report?signature=SYNTHETIC`,
		"https://example.com/report?X-Amz-Signature=SYNTHETIC", "http://localhost:123/callback?code=SYNTHETIC&state=SYNTHETIC",
		"https://example.com/oauth/google/callback?code=SYNTHETIC", "req_1234567890", "turn_abcdef",
	} {
		got := failureCause("provider rejected: "+leak, "")
		if strings.Contains(got, leak) || strings.Contains(got, "SYNTHETIC") || strings.Contains(got, "synthetic-") {
			t.Errorf("leak in %q", got)
		}
	}
	got := failureCause(capacityError+"\nRAW TRANSCRIPT\nprivate job prompt", "private job prompt")
	if !strings.Contains(got, capacityError) || strings.Contains(got, "RAW TRANSCRIPT") || strings.Contains(got, "private job prompt") {
		t.Fatalf("error field boundary: %s", got)
	}
	if got := failureCause("refused private job prompt", "private job prompt"); strings.Contains(got, "private job prompt") {
		t.Fatalf("echoed input: %s", got)
	}
	for _, echo := range []string{"\rSYNTHETIC TRANSCRIPT", "\u2028SYNTHETIC TRANSCRIPT", `; "prompt":"SYNTHETIC INPUT"`, "; transcript=SYNTHETIC TRANSCRIPT"} {
		if got := failureCause(capacityError+echo, ""); strings.Contains(got, "SYNTHETIC") || !strings.Contains(got, capacityError) {
			t.Fatalf("echoed transcript: %s", got)
		}
	}
	for _, detail := range []string{strings.Repeat("x", 500), strings.Repeat("é", 500), strings.Repeat("🦉", 500)} {
		r := newRig(t)
		id := r.add(asBoss, `{"name":"bound","schedule":{"every":"1h"},"prompt":"p"}`)
		r.run(id)
		e := r.finish("", Outcome{Notes: []string{detail}})
		stored := r.runs(id)[0].Cause
		if e.Run.Cause != stored || !utf8.ValidString(stored) || len(stored) > 300 || !strings.HasSuffix(stored, "…") {
			t.Fatalf("durable clipping mismatch: %q %q", e.Run.Cause, stored)
		}
	}
}

func TestRetryNoticeSuppressedWhenCommitFenceWins(t *testing.T) {
	for _, fence := range []string{"stop", "pause", "restart"} {
		for _, inTurn := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/turn=%t", fence, inTurn), func(t *testing.T) {
				r, id := timeoutJob(t)
				row := r.rows[0]
				turn, ok := r.s.Begin(row)
				if !ok {
					t.Fatal("begin")
				}
				turnID, err := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
				if err != nil {
					t.Fatal(err)
				}
				e := r.s.End(Outcome{Turn: turn, Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
				if e.Run.Retry.IsZero() || len(e.Notices) == 0 {
					t.Fatalf("no proposed retry: %+v", e)
				}
				switch fence {
				case "stop":
					r.cmd(asBoss, "job stop "+id)
				case "pause":
					r.cmd(asBoss, "job pause "+id)
					r.cmd(asBoss, "job resume "+id)
				case "restart":
					if err := r.st.TurnReady(turnID); err != nil {
						t.Fatal(err)
					}
					if _, err := r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
						t.Fatal(err)
					}
					if err := r.s.Recover(); err != nil {
						t.Fatal(err)
					}
				}
				r.sent()
				if inTurn {
					_, err = r.st.EndTurn(turnID, row.Conv, row.From.Key(), e)
				} else {
					err = r.st.EndRun(*e.Run, e.Notices...)
				}
				if err != nil {
					t.Fatal(err)
				}
				if after := r.runs(id)[0]; after.State == store.RunPending || strings.Contains(texts(r.sent()), "Retry: automatically") {
					t.Fatalf("vetoed retry announced or revived: %+v", after)
				}
			})
		}
	}
}

func TestNativeErrorDetailsNeverControlRetryPolicy(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"ordinary","schedule":{"every":"1h"},"prompt":"p","idempotent":true}`)
	r.run(id)
	run := r.runs(id)[0]
	cause := failureCause("provider reported timeout retry unavailable", "")
	if err := r.st.EndRun(store.RunEnd{ID: run.ID, Job: id, Retry: r.now, Replay: true, Cause: cause}); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Recover(); err != nil {
		t.Fatal(err)
	}
	run = r.runs(id)[0]
	if run.State != store.RunPending {
		t.Fatalf("native detail changed restart policy: %+v", run)
	}
	d, sched, _ := parse(r.job(id))
	if end := r.s.retry(r.job(id), d, sched, run, r.now, cause); end.Retry.IsZero() {
		t.Fatalf("native detail changed startup retry policy: %+v", end)
	}
}

func TestRetryNoticesForIdentityTimeoutAndDrain(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		r := newRig(t)
		id := r.add(asBoss, `{"name":"identity","schedule":{"every":"1h"},"prompt":"p"}`)
		r.run(id)
		r.sent()
		e := r.finish("", Outcome{Identity: ErrTemporary})
		if e.Run.Retry.IsZero() || !strings.Contains(texts(r.sent()), "Automatic retry: queued for ") {
			t.Fatalf("silent identity retry: %+v", e)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		r, _ := timeoutJob(t)
		r.now = r.now.Add(time.Minute)
		r.sent()
		e := r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
		if e.Run.Retry.IsZero() || !strings.Contains(texts(r.sent()), "Cause: repeat-safe timeout retry\nAttempts: 1\nAutomatic retry: queued for ") {
			t.Fatalf("silent timeout retry: %+v", e)
		}
	})
	t.Run("pre-dispatch", func(t *testing.T) {
		r := newRig(t)
		p := core.SlackPrincipal("boss@example.com", "T1", "boss")
		r.s.cfg.Interfaces = append(r.s.cfg.Interfaces, core.PlatformSlack)
		r.s.access.Owners[p.Key()] = true
		c := Caller{Principal: p, Role: core.RoleOwner, Conv: core.ConvKey{Interface: core.PlatformSlack, Chat: "D1"}, Private: true}
		id := r.add(c, `{"name":"identity","schedule":{"every":"1h"},"prompt":"p"}`)
		r.s.Reverify = func(context.Context, core.Principal) (Verified, bool, error) {
			return Verified{}, false, errors.New("synthetic timeout")
		}
		r.now = r.now.Add(time.Hour)
		r.tick()
		out, ok, err := r.st.NextOutbound(core.PlatformSlack)
		if err != nil || !ok || out.Conv != c.Conv || !strings.Contains(out.Text, "Automatic retry: queued for ") || r.runs(id)[0].State != store.RunPending || len(r.rows) != 0 {
			t.Fatalf("pre-dispatch retry: %+v %v", out, err)
		}
	})
	t.Run("drain", func(t *testing.T) {
		r := newRig(t)
		id := r.add(asBoss, `{"name":"held","schedule":{"every":"1h"},"prompt":"p"}`)
		r.run(id)
		r.sent()
		_ = r.st.SetMeta(MetaHold, r.now.Add(time.Hour).UnixMilli())
		if _, ok := r.s.Begin(r.rows[0]); ok || len(r.sent()) != 0 || r.runs(id)[0].Attempt != 0 {
			t.Fatal("drain hold misreported as a retry")
		}
	})
}
