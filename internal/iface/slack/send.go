package slack

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

func metadata(id, event string) map[string]any {
	return map[string]any{"event_type": event, "event_payload": map[string]string{"delivery_id": id}}
}
func (s *Slack) findDelivery(ctx context.Context, k core.ConvKey, id, event string, oldest ...string) (string, error) {
	cursor := ""
	for page := 0; page < 100; page++ {
		args := map[string]any{"channel": k.Chat, "ts": k.Thread, "include_all_metadata": true, "limit": 100, "cursor": cursor}
		// The stable thread root is a lower bound for every final part, across
		// arbitrarily delayed retries. Slack timestamps are epoch seconds.
		if k.Thread != "" {
			args["oldest"] = k.Thread
		}
		if len(oldest) > 0 && oldest[0] != "" {
			args["oldest"] = oldest[0]
		}
		var result struct {
			Messages []struct {
				TS       string `json:"ts"`
				Metadata struct {
					Type    string `json:"event_type"`
					Payload struct {
						ID string `json:"delivery_id"`
					} `json:"event_payload"`
				} `json:"metadata"`
			} `json:"messages"`
			Meta struct {
				Cursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		method := "conversations.replies"
		if k.Thread == "" {
			method = "conversations.history"
			delete(args, "ts")
		}
		if err := s.bot.call(ctx, method, args, &result); err != nil {
			return "", err
		}
		for _, m := range result.Messages {
			if m.Metadata.Type == event && m.Metadata.Payload.ID == id {
				return m.TS, nil
			}
		}
		next := strings.TrimSpace(result.Meta.Cursor)
		if next == "" {
			return "", nil
		}
		if next == cursor {
			return "", errors.New("slack: repeated history cursor")
		}
		cursor = next
	}
	return "", errors.New("slack: reconciliation page limit reached")
}
func (s *Slack) Send(ctx context.Context, out core.Outbound) error {
	if err := s.await(ctx); err != nil {
		return err
	}
	if out.File != nil {
		err := s.upload(ctx, out)
		if err != nil {
			warning := out
			warning.File = nil
			warning.Text = "File upload failed: " + safeFilename(out.File.Name)
			warning.Formatted = false
			warning.TurnID = 0
			if sent := s.sendText(ctx, warning); sent == nil {
				return nil
			}
		}
		return err
	}
	s.mu.Lock()
	u := s.progress[out.TurnID]
	s.mu.Unlock()
	if u != nil {
		select {
		case <-u.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if out.Text != "" {
		if err := s.sendText(ctx, out); err != nil {
			return err
		}
	}
	var failures atomic.Int32
	var reactions sync.WaitGroup
	for _, r := range out.Reactions {
		reactions.Add(1)
		go func() {
			defer reactions.Done()
			err := s.bot.call(ctx, "reactions.add", map[string]string{"channel": out.Conv.Chat, "timestamp": r.MsgID, "name": r.Emoji}, nil)
			if err != nil {
				var ae *apiError
				if !errors.As(err, &ae) || ae.Code != "already_reacted" {
					failures.Add(1)
					s.opt.Log.Print("slack: reaction delivery failed")
				}
			}
		}()
	}
	reactions.Wait()
	if out.Text == "" && failures.Load() > 0 {
		s.setDeliveryStatus(ctx, out, "could not complete this request. Could not confirm Slack reaction delivery.")
		warning := out
		warning.Reactions = nil
		warning.Text = "Could not confirm Slack reaction delivery."
		warning.Formatted = false
		if err := s.sendText(ctx, warning); err != nil {
			return err
		}
	}
	if out.TurnID != 0 {
		return s.cleanupStatus(ctx, out, out.Text == "" && len(out.Reactions) > 0 && failures.Load() == 0)
	}
	return nil
}
func (s *Slack) sendText(ctx context.Context, out core.Outbound) error {
	text := router.Scrub(out.Text)
	if out.Formatted {
		text = mrkdwn(text)
	} else {
		text = escapeText(text)
	}
	parts := splitReply(text)
	if out.Attempt > 0 && len(parts) > 1 {
		ts, err := s.findDelivery(ctx, out.Conv, strconv.FormatInt(out.ID, 10), "heimdall_reply", reconciliationOldest(out))
		if err != nil {
			return err
		}
		if ts != "" {
			return nil
		}
	}
	for index, part := range parts {
		id := strconv.FormatInt(out.ID, 10)
		if len(parts) > 1 {
			id += fmt.Sprintf("#%d", index+1)
		}
		// Reconcile each part before a router-driven resend. Fresh parts do
		// not depend on history availability; ambiguous posts reconcile below.
		if out.Attempt > 0 {
			ts, err := s.findDelivery(ctx, out.Conv, id, "heimdall_reply", reconciliationOldest(out))
			if err != nil {
				return err
			}
			if ts != "" {
				continue
			}
		}
		if index > 0 {
			if err := s.bot.pause(ctx, s.partEvery); err != nil {
				return err
			}
		}
		args := map[string]any{"channel": out.Conv.Chat, "thread_ts": out.Conv.Thread, "text": part, "unfurl_links": false, "unfurl_media": false, "metadata": metadata(id, "heimdall_reply")}
		if out.Conv.Thread == "" {
			delete(args, "thread_ts")
		}
		var response struct {
			TS string `json:"ts"`
		}
		err := s.bot.call(ctx, "chat.postMessage", args, &response)
		if err == nil && response.TS != "" {
			continue
		}
		if err == nil {
			err = &apiError{Code: "missing_timestamp", Ambiguous: true}
		}
		var ae *apiError
		if errors.As(err, &ae) && ae.Ambiguous {
			found := false
			for _, delay := range []time.Duration{250 * time.Millisecond, 750 * time.Millisecond, 1500 * time.Millisecond} {
				if e := s.bot.pause(ctx, delay); e != nil {
					return e
				}
				ts, _ := s.findDelivery(ctx, out.Conv, id, "heimdall_reply", reconciliationOldest(out))
				if ts != "" {
					found = true
					break
				}
			}
			if found {
				continue
			}
			s.setDeliveryStatus(ctx, out, "final delivery is uncertain. Check this thread before retrying.")
			return err // durable router retry remains at-least-once
		}
		// Clear rejection: preserve the complete answer in the existing status
		// when Slack can hold it. A separate whole-row receipt lets restart
		// reconciliation skip every part after this fallback, not just one chunk.
		var perm router.PermanentError
		if errors.As(err, &perm) && out.TurnID != 0 && len([]rune(text)) <= 40000 {
			if p, ok, _ := s.opt.Store.Progress(out.TurnID); ok && p.TS != "" {
				if updateErr := s.bot.call(ctx, "chat.update", map[string]any{"channel": p.Conv.Chat, "ts": p.TS, "text": text, "metadata": metadata(strconv.FormatInt(out.ID, 10), "heimdall_reply")}, nil); updateErr == nil {
					_ = s.opt.Store.DeleteProgress(out.TurnID)
					return nil
				}
			}
		}
		s.setDeliveryStatus(ctx, out, "could not complete this request. Final answer not sent.")
		return err
	}
	return nil
}
func (s *Slack) setDeliveryStatus(ctx context.Context, out core.Outbound, message string) {
	if p, ok, _ := s.opt.Store.Progress(out.TurnID); ok && p.TS != "" {
		_ = s.bot.call(ctx, "chat.update", map[string]any{"channel": p.Conv.Chat, "ts": p.TS, "text": escapeText(s.name + " " + message)}, nil)
	}
}
func (s *Slack) cleanupStatus(ctx context.Context, out core.Outbound, reacted bool) error {
	p, ok, err := s.opt.Store.Progress(out.TurnID)
	if err != nil {
		return err
	}
	if !ok {
		s.mu.Lock()
		delete(s.progress, out.TurnID)
		s.mu.Unlock()
		return nil
	}
	if p.Phase == "done" && p.TS == "" && (s.opt.StatusUpdates == nil || *s.opt.StatusUpdates) {
		ts, findErr := s.findDelivery(ctx, p.Conv, fmt.Sprintf("progress-%d", p.Turn), "heimdall_progress")
		if findErr != nil || ts == "" {
			s.mu.Lock()
			delete(s.progress, out.TurnID)
			s.mu.Unlock()
			return nil
		}
		p.TS = ts
	}
	if p.Phase == "done" && p.TS != "" && (out.Text != "" || reacted) {
		err = s.bot.call(ctx, "chat.delete", map[string]string{"channel": p.Conv.Chat, "ts": p.TS}, nil)
		var ae *apiError
		if err != nil && (!errors.As(err, &ae) || ae.Code != "message_not_found") {
			message := s.name + " answered below."
			if reacted {
				message = s.name + " reacted to your message."
			}
			if e := s.bot.call(ctx, "chat.update", map[string]any{"channel": p.Conv.Chat, "ts": p.TS, "text": escapeText(message)}, nil); e != nil {
				s.opt.Log.Print("slack: final status cleanup degraded")
			}
		}
	}
	s.mu.Lock()
	delete(s.progress, out.TurnID)
	s.mu.Unlock()
	return s.opt.Store.DeleteProgress(out.TurnID)
}
func splitReply(text string) []string {
	var parts []string
	fence := ""
	prefix := ""
	for text != "" {
		runes := []rune(text)
		n := min(len(runes), 3200-len([]rune(prefix)))
		if n < len(runes) {
			for _, sep := range []string{"\n\n", "\n", " "} {
				candidate := string(runes[:n])
				if at := strings.LastIndex(candidate, sep); at >= len(candidate)/2 {
					n = len([]rune(candidate[:at+len(sep)]))
					break
				}
			}
		}
		body := string(runes[:n])
		if n < len(runes) {
			if at := strings.LastIndex(body, "&"); at >= len(body)/2 && !strings.Contains(body[at:], ";") {
				n = len([]rune(body[:at]))
				body = string(runes[:n])
			}
			if at := strings.LastIndex(body, "<"); at >= len(body)/2 && !strings.Contains(body[at:], ">") {
				n = len([]rune(body[:at]))
				body = string(runes[:n])
			}
		}
		if n == 0 {
			n = min(len(runes), 3000)
			body = string(runes[:n])
		}
		for _, line := range strings.Split(body, "\n") {
			trim := strings.TrimSpace(line)
			if strings.HasPrefix(trim, "```") {
				if fence == "" {
					fence = clip(trim, 80)
				} else {
					fence = ""
				}
			}
		}
		part := prefix + body
		text = string(runes[n:])
		prefix = ""
		if fence != "" {
			part += "\n```"
			if text != "" {
				prefix = fence + "\n"
			}
		}
		parts = append(parts, part)
	}
	if len(parts) > 1 {
		for i := range parts {
			parts[i] += fmt.Sprintf("\n(%d/%d)", i+1, len(parts))
		}
	}
	return parts
}

func reconciliationOldest(out core.Outbound) string {
	if out.CreatedAt.IsZero() {
		return out.Conv.Thread
	}
	return fmt.Sprintf("%.6f", float64(out.CreatedAt.Add(-30*time.Second).UnixMicro())/1e6)
}
