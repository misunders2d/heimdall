package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
)

// Job kinds. Watches arrive in phase 3b.
const (
	KindAgent  = "agent"
	KindNotify = "notify"
)

// QuietToken is the final non-empty line of an answer with nothing to report.
const QuietToken = "NO_UPDATE"

// quietAnswer reports a scheduled answer with nothing to deliver: its last
// non-empty line is exactly QuietToken. Models often narrate before the token
// ("Marker exists.\n\nNO_UPDATE"); the owner never wants those delivered.
func quietAnswer(text string) bool {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return strings.TrimSpace(lines[len(lines)-1]) == QuietToken
}

// Def is a job definition (docs/scheduler.md §2.2): what !job add, !job
// edit and model drafts carry, checked by one validator.
type Def struct {
	Summary        string               `json:"summary,omitempty"` // bounded presentation only; never execution authority
	Checkpoint     bool                 `json:"checkpoint,omitempty"`
	DeliveryWindow *core.DeliveryWindow `json:"delivery_window,omitempty"`
	Name           string               `json:"name"`
	Kind           string               `json:"kind"`
	Schedule       schedule.Spec        `json:"schedule"`
	TZ             string               `json:"tz"`
	To             string               `json:"to"`
	Prompt         string               `json:"prompt,omitempty"`
	Text           string               `json:"text,omitempty"`
	Harness        string               `json:"harness,omitempty"`
	Quiet          bool                 `json:"quiet,omitempty"`
	Grace          string               `json:"grace,omitempty"`
	Until          string               `json:"until,omitempty"` // normalized: RFC 3339 UTC
	Expect         Expect               `json:"expect,omitzero"`
	Overlap        string               `json:"overlap"`
	Session        string               `json:"session"`
	RerunOnTimeout bool                 `json:"rerun_on_timeout,omitempty"`
	Idempotent     bool                 `json:"idempotent,omitempty"`
	Requires       []string             `json:"requires,omitempty"`
	Timeout        string               `json:"timeout,omitempty"`
	Paused         bool                 `json:"paused,omitempty"` // creation only: the job starts paused (§7.5); never stored
}

// Expect are deterministic result checks (§3.8).
type Expect struct {
	Tools  []string `json:"tools,omitempty"` // patterns (path.Match) for native tool names
	Files  int      `json:"files,omitempty"` // at least this many files in out/
	Prefix string   `json:"prefix,omitempty"`
}

// Caller is who creates, changes or runs a job: the interface-verified
// principal, its role now, and the conversation it asked in.
type Caller struct {
	Principal core.Principal
	Role      core.Role
	Conv      core.ConvKey
	Private   bool // Conv is the caller's private chat with the bot
	// Job/Run: a router-started scheduled run executing this turn (control
	// origin only; set from the harness run, never from a request). Empty for
	// person turns.
	Job string
	Run int64
	// The stored ingress's interface-verified native OAuth actor (set by the
	// router from the active turn, never from a request): carried into a
	// durable task so its wakes use the creator's own native sign-ins.
	OAuthActor, OAuthInstallation string
}

// Target is where a job delivers.
type Target struct {
	Conv     core.ConvKey // replies; for AgentNet, the conversation the job was created in
	AgentNet string       // agentnet:<address> target, or ""
	Private  bool         // the creator's own private chat
}

// decode reads one definition strictly: unknown fields are errors.
func decode(raw []byte, d *Def) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(d); err != nil {
		return fmt.Errorf("job definition: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("job definition: one JSON object only")
	}
	return nil
}

// merge applies an edit's changed fields to d.
func merge(d Def, changes map[string]json.RawMessage) (Def, error) {
	if _, ok := changes["paused"]; ok {
		return d, errors.New("paused: only when creating a job; use !job pause or !job resume")
	}
	raw, _ := json.Marshal(d)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	for k, v := range changes {
		if k != "edit" {
			fields[k] = v
		}
	}
	raw, _ = json.Marshal(fields)
	var out Def
	return out, decode(raw, &out)
}

var (
	agentnetAddr = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@/:-]{0,127}$`)
	slackChannel = regexp.MustCompile(`^[A-Z0-9]{2,32}$`)
	slackTS      = regexp.MustCompile(`^[0-9]{1,12}\.[0-9]{1,8}$`)
	tgChat       = regexp.MustCompile(`^-?[0-9]{1,20}$`)
	word         = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
)

// resolve finds where to delivers for a job created by c, without role
// rules.
func (s *Scheduler) resolve(to string, c Caller) (Target, error) {
	if to == "" || to == "here" {
		return Target{Conv: c.Conv, Private: c.Private}, nil
	}
	kind, rest, _ := strings.Cut(to, ":")
	var t Target
	switch kind {
	case "tg":
		chat, topic, _ := strings.Cut(rest, "/")
		if !tgChat.MatchString(chat) || topic != "" && !tgChat.MatchString(topic) {
			return t, fmt.Errorf("to %q: tg:<chat id>[/<topic id>]", to)
		}
		t.Conv = core.ConvKey{Interface: core.PlatformTelegram, Chat: chat, Thread: topic}
		t.Private = c.Principal.Platform == core.PlatformTelegram && chat == c.Principal.ID && topic == ""
	case "slack":
		ch, thread, _ := strings.Cut(rest, "/")
		if !slackChannel.MatchString(ch) || thread != "" && !slackTS.MatchString(thread) {
			return t, fmt.Errorf("to %q: slack:<channel id>[/<thread ts>]", to)
		}
		t.Conv = core.ConvKey{Interface: core.PlatformSlack, Chat: ch, Thread: thread}
	case "agentnet":
		if !agentnetAddr.MatchString(rest) {
			return t, fmt.Errorf("to %q: agentnet:<address>", to)
		}
		return Target{Conv: c.Conv, AgentNet: rest, Private: c.Private}, nil
	default:
		return t, fmt.Errorf(`to %q: here, tg:<chat>, slack:<channel>[/<thread>] or agentnet:<address>`, to)
	}
	if !slices.Contains(s.cfg.Interfaces, t.Conv.Interface) {
		return t, fmt.Errorf("to %q: %s is not configured here", to, t.Conv.Interface)
	}
	return t, nil
}

const untilUsage = "until: a future date (YYYY-MM-DD[THH:MM]) or a duration (72h, 30d)"

// check validates d for c, the job's creator (its role now and where the
// job was created), fills defaults and resolves the target and schedule.
// anchor is the job's creation: fixed intervals count from it. It holds at
// every fire too, so nothing in it depends on the slot being ahead.
func (s *Scheduler) check(d *Def, c Caller, anchor, now time.Time) (Target, *schedule.Schedule, error) {
	owner := c.Role == core.RoleOwner
	fail := func(format string, args ...any) (Target, *schedule.Schedule, error) {
		return Target{}, nil, fmt.Errorf(format, args...)
	}
	if d.Name = strings.TrimSpace(d.Name); d.Name == "" || utf8.RuneCountInString(d.Name) > 80 || strings.ContainsFunc(d.Name, unicode.IsControl) {
		return fail("name: 1-80 characters on one line")
	}
	d.Summary = strings.TrimSpace(d.Summary)
	if utf8.RuneCountInString(d.Summary) > 160 || strings.ContainsFunc(d.Summary, func(r rune) bool {
		return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029'
	}) {
		return fail("summary: at most 160 characters on one line")
	}
	switch d.Kind {
	case "", KindAgent:
		d.Kind = KindAgent
		if strings.TrimSpace(d.Prompt) == "" || len(d.Prompt) > 8000 || d.Text != "" {
			return fail("an agent job needs a prompt (at most 8000 bytes) and no text")
		}
	case KindNotify:
		if strings.TrimSpace(d.Text) == "" || len(d.Text) > 4000 || d.Prompt != "" {
			return fail("a notify job needs a text (at most 4000 bytes) and no prompt")
		}
	case "watch":
		return fail("watches are not available yet")
	default:
		return fail("kind: agent or notify")
	}
	if d.TZ == "" {
		d.TZ = s.cfg.DefaultTZ
	}
	sched, err := schedule.Compile(d.Schedule, d.TZ, anchor)
	if err != nil {
		return fail("%v", err)
	}
	minGap := time.Minute
	if !owner {
		minGap = time.Hour
	}
	if gap := sched.MinGap(now); gap > 0 && gap < minGap {
		return fail("schedule: runs at most every %s for your role", minGap)
	}
	if d.To == "" {
		d.To = "here"
	}
	t, err := s.resolve(d.To, c)
	if err != nil {
		return fail("%v", err)
	}
	if err := d.DeliveryWindow.Validate(); err != nil {
		return fail("%v", err)
	}
	if d.Checkpoint && (!owner || d.Kind != KindAgent || !t.Private || t.AgentNet != "" || !d.Quiet || d.Session != "" && d.Session != "fresh") {
		return fail("checkpoint: owner agent jobs only; quiet, fresh session and creator's private target required")
	}
	if !owner && (d.To != "here" && !t.Private || t.AgentNet != "") {
		return fail("to: employee jobs deliver here or to your own private chat")
	}
	if t.AgentNet != "" && d.Kind != KindAgent {
		return fail("to: an agentnet target needs an agent job: the agent delivers")
	}
	if d.Harness != "" && (!harness.Known(d.Harness) || !owner && !slices.Contains(s.cfg.EmployeeHarnesses, d.Harness)) {
		return fail("harness %q is not available for this job", d.Harness)
	}
	if d.Grace != "" {
		if g, err := span(d.Grace); err != nil || g < time.Minute || g > 7*24*time.Hour {
			return fail("grace: 1m to 7d")
		}
	}
	if d.Until != "" {
		until, err := s.until(d.Until, sched.Location(), now)
		if err != nil {
			return fail(untilUsage)
		}
		d.Until = until.UTC().Format(time.RFC3339)
	}
	if len(d.Expect.Tools) > 10 || d.Expect.Files < 0 || d.Expect.Files > 5 || len(d.Expect.Prefix) > 200 {
		return fail("expect: at most 10 tool patterns, 0-5 files, a prefix of at most 200 bytes")
	}
	for _, p := range d.Expect.Tools {
		if _, err := path.Match(p, ""); err != nil || p == "" {
			return fail("expect.tools: bad pattern %q", p)
		}
	}
	switch d.Overlap {
	case "":
		d.Overlap = "skip"
	case "skip", "queue_one":
	default:
		return fail("overlap: skip or queue_one")
	}
	switch d.Session {
	case "":
		d.Session = "fresh"
	case "fresh", "job":
	default:
		return fail("session: fresh or job")
	}
	if d.RerunOnTimeout && (!owner || !d.Idempotent || d.Kind != KindAgent || d.Timeout == "" || d.Session != "fresh" || t.AgentNet != "") {
		return fail("rerun_on_timeout: owner agent jobs only; requires idempotent, explicit timeout, fresh session and a platform target")
	}
	if d.Idempotent && !owner {
		return fail("idempotent: owner jobs only")
	}
	if len(d.Requires) > 5 {
		return fail("requires: at most 5 names")
	}
	for _, r := range d.Requires {
		if !word.MatchString(r) {
			return fail("requires: short lowercase names such as mellanni or google")
		}
	}
	if d.Timeout != "" {
		if to, err := span(d.Timeout); !owner || err != nil || to < time.Minute || to > 2*time.Hour {
			return fail("timeout: 1m to 2h, owner jobs only")
		} else if d.RerunOnTimeout && s.grace(*d, sched) < to {
			return fail("rerun_on_timeout: grace must be at least timeout")
		}
	}
	return t, sched, nil
}

// span is a duration: Go's units, or whole days (d) and weeks (w).
func span(s string) (time.Duration, error) {
	for unit, d := range map[string]time.Duration{"d": 24 * time.Hour, "w": 7 * 24 * time.Hour} {
		if n, ok := strings.CutSuffix(s, unit); ok {
			k, err := strconv.Atoi(n)
			if err != nil || k < 1 || k > 3660 {
				return 0, fmt.Errorf("bad duration %q", s)
			}
			return time.Duration(k) * d, nil
		}
	}
	return time.ParseDuration(s)
}

// until reads an expiry: an RFC 3339 instant (normalized), a local date
// (its end) or wall time in loc, or a duration from now.
func (s *Scheduler) until(v string, loc *time.Location, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation(schedule.KeyLayout, v, loc); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", v, loc); err == nil {
		return t.AddDate(0, 0, 1), nil
	}
	d, err := span(v)
	return now.Add(d), err
}

// grace is how late a slot may start.
func (s *Scheduler) grace(d Def, sched *schedule.Schedule) time.Duration {
	if g, err := span(d.Grace); err == nil && d.Grace != "" {
		return g
	}
	if sched != nil && sched.OneOff() {
		return s.cfg.GraceOneOff
	}
	return s.cfg.GraceRecurring
}
