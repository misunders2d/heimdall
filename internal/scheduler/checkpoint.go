package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

var checkpointBlock = regexp.MustCompile("(?s)^```heimdall-checkpoint[ \\t]*\\r?\\n(.*?)\\r?\\n```$")

func (s *Scheduler) checkpointResult(t *Turn, text string, files []core.File, tools []string) (*store.CheckpointUpdate, error) {
	quiet := strings.HasPrefix(text, QuietToken+"\n")
	if quiet {
		text = strings.TrimSpace(strings.TrimPrefix(text, QuietToken+"\n"))
	}
	m := checkpointBlock.FindStringSubmatch(strings.TrimSpace(text))
	if len(m) != 2 || len(text) > 2200000 {
		return nil, errors.New("one bounded result block required; scan not committed")
	}
	var u store.CheckpointUpdate
	dec := json.NewDecoder(strings.NewReader(m[1]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&u); err != nil || dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid result schema")
	}
	if !u.Complete {
		if quiet || u.Through != t.Checkpoint.Through {
			return nil, errors.New("partial scan must preserve watermark and cannot claim quiet")
		}
		switch u.Cause {
		case "pagination", "budget", "coverage", "overflow":
		default:
			return nil, errors.New("partial scan requires cause: pagination, budget, coverage or overflow")
		}
	}
	through, err := time.Parse(time.RFC3339Nano, u.Through)
	upper, _ := time.Parse(time.RFC3339Nano, t.ScanThrough)
	prior, _ := time.Parse(time.RFC3339Nano, t.Checkpoint.Through)
	if (u.Complete && (err != nil || through.After(upper) || through.Before(prior))) || u.Version != t.Checkpoint.Version {
		return nil, errors.New("stale version or unsafe watermark")
	}
	if len(u.State) > 24576 || len(u.State) == 0 || u.State[0] != '{' || !json.Valid(u.State) || len(u.Alerts) > 512 || len(u.Retire) > 20 || len(files) > 0 {
		return nil, errors.New("state/alert limit or unsupported file")
	}
	if quiet && len(u.Alerts) > 0 {
		return nil, errors.New("quiet result contains alerts")
	}
	if check := expectFail(t.Def.Expect, text, 0, tools); check != "" {
		return nil, errors.New(check)
	}
	keys := map[string]bool{}
	for _, key := range u.Retire {
		if key == "" || len(key) > 128 || keys[key] {
			return nil, errors.New("invalid retired key")
		}
		keys[key] = true
		confirmed := false
		unresolved := false
		for _, r := range t.Checkpoint.Receipts {
			if r.Key == key {
				if r.Status == store.OutSent {
					confirmed = true
				} else {
					unresolved = true
				}
			}
		}
		if !confirmed || unresolved {
			return nil, errors.New("only confirmed resolved receipts may retire")
		}
	}
	for _, a := range u.Alerts {
		if a.Key == "" || len(a.Key) > 128 || a.Revision == "" || len(a.Revision) > 128 || strings.ContainsAny(a.Key+a.Revision, "\x00\r\n") || strings.TrimSpace(a.Text) == "" || len(a.Text) > 4000 || keys[a.Key] {
			return nil, errors.New("invalid or duplicate alert")
		}
		keys[a.Key] = true
	}
	all := map[string]bool{}
	for _, r := range t.Checkpoint.Receipts {
		all[r.Key+"\x00"+r.Revision] = true
	}
	for _, key := range u.Retire {
		for _, r := range t.Checkpoint.Receipts {
			if r.Key == key {
				delete(all, r.Key+"\x00"+r.Revision)
			}
		}
	}
	// Compare JSON values, not whitespace/key order. Version and moving clock
	// are not progress: unchanged partials must eventually stall/pause.
	canonical := func(raw json.RawMessage) []byte {
		var v any
		_ = json.Unmarshal(raw, &v)
		b, _ := json.Marshal(v)
		return b
	}
	u.Progress = !bytes.Equal(canonical(u.State), canonical(t.Checkpoint.State))
	for _, a := range u.Alerts {
		pair := a.Key + "\x00" + a.Revision
		if !all[pair] {
			known, err := s.st.AlertKnown(t.Job, a.Key, a.Revision)
			if err != nil {
				return nil, errors.New("receipt history unavailable")
			}
			if !known {
				u.Progress = true
				all[pair] = true
			}
		}
	}
	if len(all) > 512 {
		return nil, errors.New("receipt limit reached; retire confirmed resolved keys first")
	}

	if !u.Complete {
		u.ScanThrough = t.ScanThrough
	} else {
		u.Cause = ""
	}
	u.Target, u.Window = t.Target.Conv, t.Def.DeliveryWindow
	return &u, nil
}
