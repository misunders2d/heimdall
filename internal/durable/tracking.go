package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Tracking-only tasks (durable.t3_auto_track, off by default;
// docs/durable.md). Registering T3 work is the model's optional choice
// (task_add); a turn that starts T3 work and registers nothing loses it.
// When an owner's live turn's native events show a T3 launch, delegation
// or send and that turn made no task_add, Heimdall admits a tracking-only
// task T3-<n>, once per thread. It observes the thread read-only and never
// works: no wake, dispatch or review runs for it. It tells the owner one
// line when the thread's run ends. `!work promote T3-<n>` hands it to hub
// work (assistant wakes), an explicit owner choice. Tracking grants no
// permission.

// Track is the T3 thread a tracking-only task follows.
type Track struct {
	Thread string `json:"thread"`
	Env    string `json:"env,omitempty"` // another environment: not observed here
	Run    string `json:"run,omitempty"`
	Kind   string `json:"kind"` // launch | delegate | send
}

func (t Track) ref() string {
	if t.Env != "" {
		return "t3:" + t.Thread + "@" + t.Env
	}
	return "t3:" + t.Thread
}

// adds remembers, per conversation, when task_add last admitted a task
// there: a live turn that did is not auto-tracked.
type adds struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (a *adds) note(conv string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.at == nil {
		a.at = map[string]time.Time{}
	}
	a.at[conv] = time.Now()
}

func (a *adds) since(conv string, t time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	at, ok := a.at[conv]
	return ok && !at.Before(t)
}

// AutoTracks reports whether tracking-only admission is on: the router
// then decodes a live owner turn's native T3 calls (TrackUntracked).
func (r *Runtime) AutoTracks() bool { return r.opt.AutoTrack }

// TrackUntracked is a live owner turn's end: children are the T3 work its
// native results show, since its start. If the turn admitted a task
// (task_add) nothing happens; otherwise each child thread not tracked yet
// becomes a tracking-only task T3-<n> bound to the turn's verified caller
// and route. It returns the admitted ids.
func (r *Runtime) TrackUntracked(c Caller, since time.Time, children []Child) ([]string, error) {
	if !r.opt.AutoTrack || !c.Live || c.Origin != "" || len(children) == 0 || r.added.since(c.Conv.String(), since) {
		return nil, nil
	}
	var out []string
	for _, ch := range children {
		if ch.Thread == "" {
			continue
		}
		id, err := r.admitTrack(c, Track{Thread: ch.Thread, Env: ch.Env, Run: ch.Run, Kind: ch.Kind})
		if err != nil {
			return out, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	return out, nil
}

// admitTrack numbers and admits one thread's tracking-only task, once per
// thread and environment ("" when it is tracked already).
func (r *Runtime) admitTrack(c Caller, t Track) (string, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var n int64
	err = tx.QueryRow(`SELECT n FROM heimdall_t3_intakes WHERE thread=? AND message='track' AND run=?`, t.Thread, t.Env).Scan(&n)
	if err == nil {
		return "", nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	res, err := tx.Exec(`INSERT INTO heimdall_t3_intakes(thread,message,run,creator,probe,at) VALUES(?,'track',?,?,?,?)`, t.Thread, t.Env, c.Principal.Key(), "turn:"+c.MsgID, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}
	if n, err = res.LastInsertId(); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	issue := fmt.Sprintf("T3-%d", n)
	cm := Commitment{Issue: issue, Title: clip("T3 "+t.Kind+" "+t.ref()+" (started in chat; tracked, not worked)", 120), Creator: c.Principal.Key(), Owner: c.Principal,
		OAuthActor: c.OAuthActor, OAuthInstallation: c.OAuthInstallation, Route: c.Conv, Private: c.Private, ReplyTo: c.MsgID, Resource: defaultRes, Track: &t,
		Brief: fmt.Sprintf("%s: T3 work (%s) the owner's assistant started from chat in %s without tracking it. The owner promoted it to you: read the thread, check what is done, finish it or report what is left. It was not your request; the thread holds the request.", issue, t.Kind, t.ref())}
	if _, err := r.Admit(cm); err != nil {
		return "", err
	}
	return issue, nil
}

// trackOnly runs a tracking-only task: it observes, never works. When the
// thread's run ends it tells the owner one fixed line; a finished run
// closes it, a failed or interrupted one leaves it open for the owner
// (promote or close). promoted=true hands it to hub work.
func (r *Runtime) trackOnly(ctx dbos.Context, w *run) (promoted bool, err error) {
	t := w.c.Track
	o := r.opt.Observer
	watch := t.Env == "" && o != nil && o.Observable("t3:"+t.Thread)
	next := "observes " + t.ref() + "; never works; !work promote " + w.c.Issue + " hands it to your assistant"
	if !watch {
		next = "not observed here (" + t.ref() + "); never works; !work promote " + w.c.Issue + " or !work close " + w.c.Issue
	}
	if err := r.publish(ctx, w, PhaseTracking, next); err != nil {
		return false, err
	}
	poll := r.opt.Observe
	run := t.Run
	for {
		wait := poll
		if !watch || w.st.Phase == PhaseOpen {
			wait = maxWait
		}
		ev, ok, err := r.next(ctx, w, wait)
		if err != nil {
			return false, err
		}
		if ok {
			switch ev.Kind {
			case "close":
				return false, closed{ev}
			case "promote":
				return true, nil
			}
			continue
		}
		if !watch || w.st.Phase == PhaseOpen {
			continue
		}
		poll = min(poll*2, 10*time.Minute)
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (childRead, error) {
			obs, err := o.Observe(sctx, "t3:"+t.Thread)
			if err != nil {
				return childRead{Err: clip(err.Error(), 200)}, nil
			}
			return childRead{Obs: obs}, nil
		}, dbos.WithStepName("track-observe"))
		if err != nil {
			return false, err
		}
		note := ""
		if got.Err != "" {
			note = "T3 unreadable: " + got.Err
		}
		if note != w.st.Note {
			w.st.Note = note
			if err := r.publish(ctx, w, w.st.Phase, w.st.Next); err != nil {
				return false, err
			}
		}
		if got.Err != "" {
			continue
		}
		obs := got.Obs
		if run == "" {
			if run = or(obs.ActiveRun, obs.LatestRun); run == "" {
				continue
			}
		}
		if obs.ActiveRun == run || obs.LatestRun == run && !terminalRun(obs.Status) || obs.LatestRun == "" {
			continue
		}
		status := obs.Status
		if obs.LatestRun != run {
			status = "ended; a later run followed"
		}
		if status == "failed" || status == "interrupted" {
			text := fmt.Sprintf("T3 work %s (%s) ended %s. Reply \"!work promote %s\" to have your assistant finish it, or \"!work close %s\". — heimdall", t.ref(), w.c.Issue, status, w.c.Issue, w.c.Issue)
			if err := r.send(ctx, w, "track-ended", text, false); err != nil {
				return false, err
			}
			if err := r.publish(ctx, w, PhaseOpen, "the T3 run ended "+status+"; !work promote or !work close (owner)"); err != nil {
				return false, err
			}
			continue
		}
		text := fmt.Sprintf("T3 work %s (%s) finished (%s); no longer tracked. — heimdall", t.ref(), w.c.Issue, status)
		if err := r.send(ctx, w, "track-ended", text, false); err != nil {
			return false, err
		}
		w.st.Verdict = "finished in T3 (" + status + ")"
		return false, r.publish(ctx, w, PhaseClosed, "none: the T3 run finished")
	}
}

// Promote hands a tracking-only task to hub work (!work promote): from
// then on the creator's assistant works it under the wake rules.
func (r *Runtime) promote(c Caller, issue string) string {
	cm, ok := r.input(issue)
	if !ok || !c.sees(cm) {
		return issue + " is not tracked here."
	}
	if !c.controls(cm) {
		return issue + ": only its creator can change it."
	}
	if cm.Track == nil {
		return issue + ": only a tracking-only T3 task can be promoted."
	}
	if st, _ := r.State(issue); st.Phase != PhaseTracking && st.Phase != PhaseOpen {
		return issue + ": already promoted or closed."
	}
	if err := r.Signal(issue, Event{Kind: "promote", By: c.Principal.Key()}, c.Conv.Interface+":"+c.Conv.Chat+":"+c.MsgID); err != nil {
		return issue + ": " + err.Error()
	}
	return "Promoted " + issue + ": your assistant works it from now on."
}
