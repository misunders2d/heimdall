package slack

// Display rules ported from hub ec72c7d extensions/slack.ts:430-650 and
// .shared/external-pi-runner.ts:64-88. Only filtered summaries reach Slack.
import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/text/unicode/norm"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf16"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

type statusUpdate struct {
	mu                          sync.Mutex
	p                           store.Progress
	phase, activity, tool, last string
	disabled, terminal          bool
	signal                      chan struct{}
	done                        chan struct{}
	stop                        chan struct{}
	stopOnce                    sync.Once
}

var slackActivityID = regexp.MustCompile(`\b[UCDGW][A-Z0-9]{8,}\b`)
var privateActivity = regexp.MustCompile("(?i)https?://|www\\.|\\b[\\w.+-]+@[\\w.-]+\\.\\w{2,}\\b|<[@#!]|@(?:channel|everyone|here)\\b|(?:^|\\s)(?:/(?:[\\w.-]+/)*[\\w.-]+|~/|[A-Za-z]:\\\\)|\\b[0-9a-f]{8}-[0-9a-f-]{27,}\\b|\\b[A-Za-z0-9_+/=.-]{32,}\\b|\\b(?:api[ _-]?key|authorization|bearer|client[ _-]?secret|password|refresh[ _-]?token|secret|token|ssn|social security|medical|diagnos\\w*|patient|salary|phone number|account number)\\b|[`{}\\[\\]]|=>|\\w\\s*=\\s*\\S")

func safeActivity(value string) string {
	value = normalizeThinking(tail(value, 2000))
	lines := strings.FieldsFunc(value, func(r rune) bool { return r == '\n' || r == '\r' || r == 0x2028 || r == 0x2029 })
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if utf16Length(line) > 500 {
			continue
		}
		line = router.Scrub(line)
		line = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) || r >= 0x200b && r <= 0x200f || r >= 0x202a && r <= 0x202e || r >= 0x2060 && r <= 0x206f {
				return ' '
			}
			return r
		}, line)
		line = strings.TrimLeft(line, "-#*_~ \t")
		line = strings.NewReplacer("*", "", "_", "", "~", "").Replace(line)
		line = strings.Join(strings.Fields(line), " ")
		line = strings.TrimRight(line, ".!?:;, ")
		if line == "" || strings.Contains(line, "[REDACTED]") || (privateActivity.MatchString(line) || slackActivityID.MatchString(line)) {
			continue
		}
		if utf16Length(line) > 120 {
			line = string(utf16.Decode(utf16.Encode([]rune(line))[:117]))
			if j := strings.LastIndexByte(line, ' '); j >= 0 {
				line = line[:j]
			}
			line += "…"
		}
		return line
	}
	return ""
}

// The hub normalizes compatibility characters before its privacy filters.
func normalizeThinking(s string) string { return norm.NFKC.String(s) }
func utf16Length(s string) int          { return len(utf16.Encode([]rune(s))) }
func tail(s string, n int) string {
	units := utf16.Encode([]rune(s))
	if len(units) > n {
		return string(utf16.Decode(units[len(units)-n:]))
	}
	return s
}

func toolLabel(name string) string {
	for _, r := range []struct{ pattern, label string }{{`web|fetch|search|youtube`, "Searching web"}, {`read|grep|ls|file|drive_download|download`, "Reading files"}, {`edit|write|patch|create_tab|append|upload|calendar_create|create_task|update_task`, "Editing code"}, {`bash|shell|command`, "Running shell"}, {`subagent|writer_critic|critic|researcher|scout`, "Using sub-agent"}, {`google|sheet|drive|gmail|calendar|clickup`, "Checking workspace"}, {`spapi|amazon|ads|keepa|bigquery|mellanni`, "Querying business data"}, {`neo4j|memory`, "Checking memory"}} {
		if regexp.MustCompile(r.pattern).MatchString(strings.ToLower(name)) {
			return r.label
		}
	}
	return "Running tools"
}
func (s *Slack) ObserveTurn(ctx context.Context, e router.TurnEvent) {
	s.mu.Lock()
	u := s.progress[e.ID]
	if u == nil && e.Phase != "start" {
		s.mu.Unlock()
		return
	}
	if u == nil {
		u = &statusUpdate{p: store.Progress{Turn: e.ID, Conv: e.Conv, Phase: "working"}, phase: "queued", signal: make(chan struct{}, 1), done: make(chan struct{}), stop: make(chan struct{})}
		s.progress[e.ID] = u
		go s.runStatus(ctx, u)
	}
	s.mu.Unlock()
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.terminal {
		return
	}
	switch e.Phase {
	case "start":
	case "steer":
		u.phase = "running"
		u.activity = "Follow-up accepted"
	case "progress":
		p := e.Progress
		if p.Phase == "thinking" {
			if activity := safeActivity(p.Text); activity != "" {
				u.activity = activity
				u.phase = "running"
			} else if p.Reset {
				u.activity = ""
			}
		} else {
			u.phase = p.Phase
			if p.Tool != "" {
				u.tool = toolLabel(p.Tool)
			}
			if p.Error {
				u.phase = "finalizing"
			}
			if p.Phase == "finalizing" {
				u.activity = ""
			}
		}
	default:
		u.terminal = true
		u.phase = e.Phase
		u.p.Phase = e.Phase
	}
	select {
	case u.signal <- struct{}{}:
	default:
	}
}
func (s *Slack) statusText(u *statusUpdate) string {
	name := escapeText(s.name)
	switch u.phase {
	case "done":
		return name + " is ready.\n_Answer prepared._"
	case "failed", "needs_human":
		return name + " could not complete this request.\n_Final answer not sent._"
	case "cancelled":
		return name + " stopped.\n_Work stopped by request._"
	case "interrupted":
		return name + " was interrupted by restart.\n_Work was not replayed._"
	}
	current := "Analyzing request"
	switch u.phase {
	case "queued":
		current = "Starting work"
	case "tools":
		current = first(u.activity, u.tool, "Running tools")
	case "finalizing":
		current = "Preparing answer"
	default:
		if u.activity != "" {
			current = u.activity
		}
	}
	return name + " is working…\n_" + escapeText(current) + "…_"
}
func (s *Slack) runStatus(ctx context.Context, u *statusUpdate) {
	defer close(u.done)
	if s.await(ctx) != nil {
		return
	}
	u.mu.Lock()
	u.disabled = s.opt.StatusUpdates != nil && !*s.opt.StatusUpdates
	text := s.statusText(u)
	p := u.p
	u.mu.Unlock()
	if err := s.opt.Store.SaveProgress(p); err != nil {
		s.opt.Log.Print("slack: progress persistence failed; status disabled")
		u.mu.Lock()
		u.disabled = true
		u.mu.Unlock()
	}
	u.mu.Lock()
	disabled := u.disabled
	u.mu.Unlock()
	if !disabled {
		var response struct {
			TS string `json:"ts"`
		}
		err := s.bot.call(ctx, "chat.postMessage", map[string]any{"channel": p.Conv.Chat, "thread_ts": p.Conv.Thread, "text": text, "unfurl_links": false, "unfurl_media": false, "metadata": metadata(fmt.Sprintf("progress-%d", p.Turn), "heimdall_progress")}, &response)
		if err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.Ambiguous {
				for _, delay := range []time.Duration{250 * time.Millisecond, 750 * time.Millisecond, 1500 * time.Millisecond} {
					if s.bot.pause(ctx, delay) != nil {
						break
					}
					ts, _ := s.findDelivery(ctx, p.Conv, fmt.Sprintf("progress-%d", p.Turn), "heimdall_progress")
					if ts != "" {
						response.TS = ts
						err = nil
						break
					}
				}
			}
		}
		u.mu.Lock()
		if err != nil || response.TS == "" {
			u.disabled = true
			s.opt.Log.Print("slack: progress post failed; status disabled")
		} else {
			u.p.TS = response.TS
			u.last = text
		}
		p = u.p
		u.mu.Unlock()
		if err := s.opt.Store.SaveProgress(p); err != nil {
			s.opt.Log.Print("slack: progress timestamp persistence failed")
		}
	}
	ticker := time.NewTicker(s.statusEvery)
	defer ticker.Stop()
	for {
		u.mu.Lock()
		terminal := u.terminal
		u.mu.Unlock()
		if terminal {
			s.flushStatus(ctx, u, true)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-u.stop:
			return
		case <-u.signal:
		case <-ticker.C:
			s.flushStatus(ctx, u, false)
		}
	}
}
func (s *Slack) flushStatus(ctx context.Context, u *statusUpdate, final bool) {
	u.mu.Lock()
	text := s.statusText(u)
	p := u.p
	disabled := u.disabled
	same := text == u.last
	u.mu.Unlock()
	if final {
		if err := s.opt.Store.SaveProgress(p); err != nil {
			s.opt.Log.Print("slack: terminal status not persisted")
		}
	}
	if disabled || p.TS == "" || same {
		return
	}
	err := s.bot.call(ctx, "chat.update", map[string]any{"channel": p.Conv.Chat, "ts": p.TS, "text": text, "unfurl_links": false, "unfurl_media": false}, nil)
	u.mu.Lock()
	defer u.mu.Unlock()
	if err != nil {
		u.disabled = true
		s.opt.Log.Print("slack: status update failed; updates disabled")
	} else {
		u.last = text
	}
}
func (s *Slack) closeStatuses() {
	s.mu.Lock()
	var all []*statusUpdate
	for _, u := range s.progress {
		u.stopOnce.Do(func() { close(u.stop) })
		all = append(all, u)
	}
	s.mu.Unlock()
	for _, u := range all {
		<-u.done
	}
}
func (s *Slack) recoverProgress(ctx context.Context) error {
	ps, err := s.opt.Store.Progresses("slack")
	if err != nil {
		return err
	}
	for _, p := range ps {
		if p.TS == "" {
			ts, _ := s.findDelivery(ctx, p.Conv, fmt.Sprintf("progress-%d", p.Turn), "heimdall_progress")
			p.TS = ts
		}
		if p.TurnState == "done" && !p.Pending && p.TS != "" {
			p.Phase = "done"
			if err = s.opt.Store.SaveProgress(p); err != nil {
				return err
			}
			if err := s.cleanupStatus(ctx, core.Outbound{TurnID: p.Turn, Conv: p.Conv, Text: "delivered"}, false); err != nil {
				return err
			}
			continue
		}
		p.Phase = p.TurnState
		if p.Phase == "running" {
			p.Phase = "interrupted"
		}
		if p.TS != "" {
			u := &statusUpdate{p: p, phase: p.Phase}
			if err := s.bot.call(ctx, "chat.update", map[string]any{"channel": p.Conv.Chat, "ts": p.TS, "text": s.statusText(u)}, nil); err != nil {
				s.opt.Log.Print("slack: recovered status update failed")
			}
		}
		if err = s.opt.Store.SaveProgress(p); err != nil {
			return err
		}
	}
	return nil
}

// Compile-time adapter hook and transport checks.
var _ router.TurnObserver = (*Slack)(nil)
