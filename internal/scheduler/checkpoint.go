package scheduler

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	if len(m) != 2 || len(text) > 110000 {
		return nil, errors.New("one bounded result block required; scan not committed")
	}
	var u store.CheckpointUpdate
	dec := json.NewDecoder(strings.NewReader(m[1]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&u); err != nil || dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid result schema")
	}
	if !u.Complete {
		return nil, errors.New("scan incomplete; watermark unchanged")
	}
	through, err := time.Parse(time.RFC3339Nano, u.Through)
	upper, _ := time.Parse(time.RFC3339Nano, t.ScanThrough)
	prior, _ := time.Parse(time.RFC3339Nano, t.Checkpoint.Through)
	if err != nil || through.After(upper) || through.Before(prior) || u.Version != t.Checkpoint.Version {
		return nil, errors.New("stale version or unsafe watermark")
	}
	if len(u.State) > 24576 || len(u.State) == 0 || u.State[0] != '{' || !json.Valid(u.State) || len(u.Alerts) > 20 || len(u.Retire) > 20 || len(files) > 0 {
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
		for _, r := range t.Checkpoint.Receipts {
			if r.Key == key && r.Status == store.OutSent {
				confirmed = true
			}
		}
		if !confirmed {
			return nil, errors.New("only confirmed resolved receipts may retire")
		}
	}
	for _, a := range u.Alerts {
		if a.Key == "" || len(a.Key) > 128 || a.Revision == "" || len(a.Revision) > 128 || strings.TrimSpace(a.Text) == "" || len(a.Text) > 4000 || keys[a.Key] {
			return nil, errors.New("invalid or duplicate alert")
		}
		keys[a.Key] = true
		for _, r := range t.Checkpoint.Receipts {
			if r.Key == a.Key && r.Revision == a.Revision && r.ContentHash != fmt.Sprintf("%x", sha256.Sum256([]byte(a.Text))) {
				return nil, errors.New("alert text changed without a material revision")
			}
		}
	}
	all := map[string]bool{}
	for _, r := range t.Checkpoint.Receipts {
		all[r.Key] = true
	}
	for _, key := range u.Retire {
		delete(all, key)
	}
	for _, a := range u.Alerts {
		all[a.Key] = true
	}
	if len(all) > 512 {
		return nil, errors.New("alert key limit reached; retire confirmed resolved keys first")
	}
	u.Target, u.Window = t.Target.Conv, t.Def.DeliveryWindow
	return &u, nil
}
