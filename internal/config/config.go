// Package config loads Heimdall's one JSON configuration file. Secrets are
// never stored here: interfaces read them from the environment.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/googleoauth"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
)

// DefaultOrder is the harness order used when a chat has no `!use` choice.
var DefaultOrder = []string{"codex", "claude", "pi"}

type Config struct {
	EmployeeGoogle    *googleoauth.Config `json:"employee_google,omitempty"`
	EmployeeHarnesses []string            `json:"employee_harnesses,omitempty"` // enable only after native isolation validation
	AccessDir         string              `json:"access_dir,omitempty"`
	StandaloneAccess  bool                `json:"standalone_access,omitempty"` // non-hub installs only: legacy owners fallback
	warnings          []string
	Harness           harness.WarmConfig       `json:"harness,omitempty"`
	OwnerSlack        *harness.OwnerSlack      `json:"owner_slack,omitempty"`
	Commands          *Commands                `json:"commands,omitempty"`
	DataDir           string                   `json:"data_dir"`
	Mode              core.Mode                `json:"mode"`
	Owners            []string                 `json:"owners"` // principal keys, platform:id; everyone else is non-root
	HarnessOrder      []string                 `json:"harness_order,omitempty"`
	MaxParallel       int                      `json:"max_parallel,omitempty"`
	RunAs             map[core.Role]core.RunAs `json:"run_as"`
	Slack             *Slack                   `json:"slack,omitempty"`
	Telegram          *Telegram                `json:"telegram,omitempty"`
	PromptGuard       *PromptGuard             `json:"prompt_guard,omitempty"`
	Scheduler         *Scheduler               `json:"scheduler,omitempty"`
	Durable           *Durable                 `json:"durable,omitempty"`
}

// Durable configures durable commitments (docs/durable.md). Disabled by
// default; slice 1 admits only explicit owner !work commitments.
type Durable struct {
	Enabled        bool                       `json:"enabled"`
	SilenceSeconds int                        `json:"silence_seconds,omitempty"` // one no-signal notice per attempt and phase after this; default 86400; negative = none
	Linear         string                     `json:"linear_workspace,omitempty"`
	T3Command      []string                   `json:"t3_command,omitempty"`        // installed T3 host CLI (absolute path first); enables read-only t3:<thread> observation
	ObserveSeconds int                        `json:"observe_seconds,omitempty"`   // first observation interval; default 30, doubling to 600
	Resources      map[string]DurableResource `json:"resources,omitempty"`         // admission queues; "default" always exists
	Review         bool                       `json:"supervisor_review,omitempty"` // verdicts by a server-initiated review turn of the creator's harness; default off (live owner verdicts)
	Assistant      bool                       `json:"assistant_work,omitempty"`    // a commitment without a worker is worked by server-initiated wakes of the creator's own assistant (normal native tools); default off (!work report)
	Dispatch       bool                       `json:"t3_dispatch,omitempty"`       // each attempt's brief is sent by a server-initiated turn of the creator's harness with its native t3_thread_send; default off (owner-dispatched)
}

// DurableResource is one capacity class (a host or provider).
type DurableResource struct {
	Concurrency       int `json:"concurrency,omitempty"` // active commitments; 0 = unlimited
	PerWorker         int `json:"per_worker,omitempty"`  // default 1
	RateLimit         int `json:"rate_limit,omitempty"`  // starts per rate_period_seconds
	RatePeriodSeconds int `json:"rate_period_seconds,omitempty"`
}

// On reports whether durable commitments run.
func (d *Durable) On() bool { return d != nil && d.Enabled }

// Scheduler configures scheduled jobs (docs/scheduler.md §10). Jobs are
// kept in the store whether it is enabled or not.
type Scheduler struct {
	Control               *SchedulerControl `json:"control,omitempty"`
	Enabled               bool              `json:"enabled"`
	DefaultTZ             string            `json:"default_tz"`                        // IANA zone; required when enabled
	MaxConcurrent         int               `json:"max_concurrent,omitempty"`          // scheduled turns at once; default 2, at most max_parallel-1
	PerCreator            int               `json:"per_creator,omitempty"`             // running runs per creator; default 1
	GraceRecurringSeconds int               `json:"grace_recurring_seconds,omitempty"` // default 7200
	GraceOneOffSeconds    int               `json:"grace_oneoff_seconds,omitempty"`    // default 86400
	PauseAfter            int               `json:"pause_after,omitempty"`             // failed or unverified runs in a row; default 3
}

// SchedulerControl binds local owner tools to one existing platform identity.
// OS authorization always derives from run_as.owner, never from this selector.
type SchedulerControl struct {
	Owner  string        `json:"owner"`
	Source *core.ConvKey `json:"source,omitempty"` // protected source conversation, never tool arguments
	// External trusted local launchers that vouch for the runs they start
	// (docs/external-origin.md). Tried only when no Heimdall turn owns the peer.
	External []ExternalOrigin `json:"external,omitempty"`
	// TrustedT3: the owner-trusted T3 launcher; its descendants may only
	// register durable tasks (task_add/list/show) for Owner and, with
	// Manage, manage Owner's existing jobs and tasks. Absent = off.
	TrustedT3 *TrustedLauncher `json:"trusted_t3,omitempty"`
}

// TrustedLauncher names the trusted T3 launcher executable (a stable path
// such as its installed symlink, resolved per request) and its OS user.
type TrustedLauncher struct {
	Launcher string `json:"launcher"`
	User     string `json:"user"`
	// Manage (owner opt-in, default off): also list/show/history/run/pause/
	// resume/stop Owner's existing jobs and task_pause/task_resume/task_close
	// Owner's tasks. Never create, edit, remove, confirm or reroute.
	Manage bool `json:"manage,omitempty"`
}

// ExternalOrigin is one trusted local launcher and the people it may vouch
// for, each mapped to an existing Heimdall principal (role evaluated live).
type ExternalOrigin struct {
	Name   string                    `json:"name"`
	Socket string                    `json:"socket"` // absolute path of the launcher's own socket
	User   string                    `json:"user"`   // OS user the launcher runs as
	People map[string]ExternalPerson `json:"people"`
}

var externalName = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)

// ExternalPerson is the Heimdall identity a launcher person maps to.
type ExternalPerson struct {
	Principal string        `json:"principal"`        // platform:id (telegram or slack)
	Source    *core.ConvKey `json:"source,omitempty"` // which registered private conversation
}

// On reports whether the scheduler runs.
func (s *Scheduler) On() bool { return s != nil && s.Enabled }

// PromptGuard sizes the prompt-injection log, <data_dir>/prompt-guard.log:
// the hub's scoring of every admitted message, logged as metadata only. It
// never blocks or alters a message.
type PromptGuard struct {
	// LogMaxBytes is the log's size before it is rotated to a single ".1"
	// file; 0 means the router's default, 1 MiB.
	LogMaxBytes int64 `json:"log_max_bytes,omitempty"`
}

// GuardLogMax is the prompt-injection log's cap; 0 for the default.
func (c *Config) GuardLogMax() int64 {
	if c.PromptGuard == nil {
		return 0
	}
	return c.PromptGuard.LogMaxBytes
}

// Telegram configures the Telegram interface. Its bot token is read from
// the HEIMDALL_TELEGRAM_TOKEN environment variable, never from this file.
type Telegram struct {
	LocalFileRoot string `json:"local_file_root,omitempty"` // opt-in; approved local Bot API root only
	// BotUsername is the bot the token must belong to: getMe is checked
	// against it before polling, so a wrong token is never used.
	BotUsername  string `json:"bot_username"`
	APIBase      string `json:"api_base,omitempty"`  // origin, without /bot
	FileBase     string `json:"file_base,omitempty"` // prefix, including /file/bot
	MaxFileBytes int64  `json:"max_file_bytes,omitempty"`
}

// Commands controls addressing on every shared interface.
type Commands struct {
	Unmentioned []string `json:"unmentioned"`
}

func (c *Config) UnmentionedCommands() []string {
	if c.Commands == nil || c.Commands.Unmentioned == nil {
		return []string{"stop", "cancel"}
	}
	return c.Commands.Unmentioned
}

// Slack configures Socket Mode; both tokens come only from HEIMDALL_SLACK_*.
type Slack struct {
	DispatchConcurrency int    `json:"dispatch_concurrency,omitempty"`
	IngressRetrySeconds int    `json:"ingress_retry_seconds,omitempty"`
	EventDedupeHours    int    `json:"event_dedupe_hours,omitempty"`
	TeamID              string `json:"team_id"`

	MaxFileBytes  int64  `json:"max_file_bytes,omitempty"`
	StatusName    string `json:"status_name,omitempty"`
	StatusUpdates *bool  `json:"status_updates,omitempty"`
}

// Load reads and validates the config at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	if err = c.validateAuthority(); err != nil {
		return nil, err
	}
	for _, w := range c.warnings {
		log.Print("config: " + w)
	}
	return c, nil
}

// Parse validates a config document and fills defaults without runtime authority
// checks. Load and CheckFile additionally validate the startup environment.
func Parse(raw []byte) (*Config, error) { return parse(raw, false) }

// CheckFile validates without opening stores, launching harnesses or connecting.
// Only a missing employee account is downgraded for a staged installation.
func CheckFile(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := parse(raw, true)
	if err != nil {
		return nil, err
	}
	if err := c.validateAuthority(); err != nil {
		return nil, err
	}
	return c.warnings, nil
}

func parse(raw []byte, staged bool) (*Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("config: trailing data after the JSON object")
	}
	if err := c.fill(staged); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

func (c *Config) fill(staged bool) error {
	if err := c.EmployeeGoogle.Validate(); err != nil {
		return err
	}
	for _, h := range c.EmployeeHarnesses {
		if h != "codex" && h != "claude" && h != "pi" {
			return errors.New("employee_harnesses supports only codex, claude, pi after native validation")
		}
	}
	if c.OwnerSlack != nil {
		if err := c.OwnerSlack.Validate(); err != nil {
			return err
		}
	}
	if c.DataDir == "" {
		return errors.New("data_dir is required")
	}
	dir, err := expandHome(c.DataDir)
	if err != nil {
		return err
	}
	c.DataDir = dir
	if c.Mode != core.ModeCorporate && c.Mode != core.ModePersonal {
		return fmt.Errorf("mode must be %q or %q", core.ModeCorporate, core.ModePersonal)
	}
	if c.StandaloneAccess && len(c.Owners) == 0 {
		return errors.New("owners must list at least one verified principal (platform:id)")
	}
	for i, k := range c.Owners {
		p, err := core.ParsePrincipalKey(k)
		if err != nil {
			return fmt.Errorf("owners: %w", err)
		}
		c.Owners[i] = p.Key()
		if slices.Contains(c.Owners[:i], p.Key()) {
			return fmt.Errorf("owners: %q listed twice", k)
		}
	}
	if len(c.HarnessOrder) == 0 {
		c.HarnessOrder = slices.Clone(DefaultOrder)
	}
	for i, h := range c.HarnessOrder {
		if !harness.Known(h) {
			return fmt.Errorf("harness_order: unknown harness %q (known: %s)", h, strings.Join(harness.Names(), ", "))
		}
		if slices.Contains(c.HarnessOrder[:i], h) {
			return fmt.Errorf("harness_order: %q listed twice", h)
		}
	}
	if c.Commands != nil {
		for _, name := range c.Commands.Unmentioned {
			if !slices.Contains([]string{"stop", "cancel", "reset", "chatid", "help", "status", "which", "all"}, name) {
				return errors.New("commands.unmentioned contains an unknown command")
			}
		}
	}
	if c.Slack != nil {
		if c.Slack.TeamID == "" || strings.ContainsAny(c.Slack.TeamID, " \t\r\n") {
			return errors.New("slack.team_id is required")
		}
		if c.Slack.DispatchConcurrency == 0 {
			c.Slack.DispatchConcurrency = 8
		}
		if c.Slack.DispatchConcurrency < 1 || c.Slack.DispatchConcurrency > 128 {
			return errors.New("slack.dispatch_concurrency must be 1-128")
		}
		if c.Slack.IngressRetrySeconds == 0 {
			c.Slack.IngressRetrySeconds = 600
		}
		if c.Slack.IngressRetrySeconds < 1 || c.Slack.IngressRetrySeconds > 3600 {
			return errors.New("slack.ingress_retry_seconds must be 1-3600")
		}
		if c.Slack.EventDedupeHours == 0 {
			c.Slack.EventDedupeHours = 72
		}
		if c.Slack.EventDedupeHours < 1 || c.Slack.EventDedupeHours > 720 {
			return errors.New("slack.event_dedupe_hours must be 1-720")
		}
		if c.Slack.MaxFileBytes == 0 {
			c.Slack.MaxFileBytes = 200_000_000
		}
		if c.Slack.MaxFileBytes < 1 {
			return errors.New("slack.max_file_bytes must be positive")
		}
	}
	if c.Telegram != nil {
		if c.Telegram.LocalFileRoot != "" && c.Telegram.LocalFileRoot != "/var/lib/telegram-bot-api" {
			return errors.New("telegram.local_file_root must be /var/lib/telegram-bot-api")
		}
		if c.Telegram.MaxFileBytes < 0 || c.Telegram.MaxFileBytes == 1<<63-1 {
			return errors.New("telegram.max_file_bytes must be positive")
		}
		for _, base := range []string{c.Telegram.APIBase, c.Telegram.FileBase} {
			if base != "" {
				u, err := url.Parse(base)
				if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
					return errors.New("telegram bases must be HTTP(S) URLs without credentials, query or fragment")
				}
			}
		}
		c.Telegram.BotUsername = strings.TrimPrefix(strings.TrimSpace(c.Telegram.BotUsername), "@")
		if !botUsername(c.Telegram.BotUsername) {
			return errors.New("telegram.bot_username must be the bot's Telegram username (5-32 letters, digits or _)")
		}
	}
	if g := c.PromptGuard; g != nil && g.LogMaxBytes != 0 && g.LogMaxBytes < 4096 {
		return errors.New("prompt_guard.log_max_bytes must be at least 4096 (bytes)")
	}
	if err := c.Harness.Validate(); err != nil {
		return err
	}
	if c.MaxParallel == 0 {
		c.MaxParallel = 4
	}
	if c.MaxParallel < 1 {
		return errors.New("max_parallel must be at least 1")
	}
	if s := c.Scheduler; s != nil {
		if s.Control != nil {
			p, err := core.ParsePrincipalKey(s.Control.Owner)
			if !s.Enabled || err != nil || (p.Platform != core.PlatformTelegram && p.Platform != core.PlatformSlack) {
				return errors.New("scheduler.control requires enabled scheduler and a platform owner binding")
			}
			if k := s.Control.Source; k != nil && (k.Interface != p.Platform || k.Chat == "" || strings.ContainsAny(k.Chat+k.Thread, "\r\n")) {
				return errors.New("scheduler.control.source must name an owner platform conversation")
			}
			if t := s.Control.TrustedT3; t != nil && (!filepath.IsAbs(t.Launcher) || filepath.Clean(t.Launcher) != t.Launcher || t.User == "") {
				return errors.New("scheduler.control.trusted_t3 needs a clean absolute launcher path and its user")
			}
			names := map[string]bool{}
			for i, o := range s.Control.External {
				at := fmt.Sprintf("scheduler.control.external[%d]", i)
				if !externalName.MatchString(o.Name) || names[o.Name] {
					return fmt.Errorf("%s.name must be a unique word (a-z, 0-9, _ or -)", at)
				}
				names[o.Name] = true
				if !filepath.IsAbs(o.Socket) || filepath.Clean(o.Socket) != o.Socket || len(o.Socket) > 107 {
					return fmt.Errorf("%s.socket must be a clean absolute path", at)
				}
				if o.User == "" {
					return fmt.Errorf("%s.user is required", at)
				}
				if len(o.People) == 0 {
					return fmt.Errorf("%s.people must map at least one person", at)
				}
				for person, m := range o.People {
					q, err := core.ParsePrincipalKey(m.Principal)
					if person == "" || len(person) > 128 || strings.ContainsFunc(person, func(r rune) bool { return r < 0x20 || r == 0x7f }) || err != nil || (q.Platform != core.PlatformTelegram && q.Platform != core.PlatformSlack) {
						return fmt.Errorf("%s.people: %q must map to a telegram or slack principal", at, person)
					}
					if k := m.Source; k != nil && (k.Interface != q.Platform || k.Chat == "" || strings.ContainsAny(k.Chat+k.Thread, "\r\n")) {
						return fmt.Errorf("%s.people: %q source must name that principal's platform conversation", at, person)
					}
				}
			}
		}
		for _, d := range []struct {
			v   *int
			def int
		}{{&s.MaxConcurrent, 2}, {&s.PerCreator, 1}, {&s.GraceRecurringSeconds, 7200}, {&s.GraceOneOffSeconds, 86400}, {&s.PauseAfter, 3}} {
			if *d.v == 0 {
				*d.v = d.def
			}
			if *d.v < 1 {
				return errors.New("scheduler: counts and seconds must be positive")
			}
		}
		if s.Enabled {
			if _, err := schedule.Zone(s.DefaultTZ); err != nil {
				return fmt.Errorf("scheduler.default_tz: %w", err)
			}
			if s.MaxConcurrent > c.MaxParallel-1 {
				return fmt.Errorf("scheduler.max_concurrent must be at most max_parallel-1 (%d), so live chat keeps a slot", c.MaxParallel-1)
			}
		}
	}
	for role, r := range c.RunAs {
		if role != core.RoleOwner && role != core.RoleEmployee {
			return fmt.Errorf("run_as: unknown role %q", role)
		}
		if r.User == "" {
			return fmt.Errorf("run_as.%s.user is required", role)
		}
		if r.Dir != "" {
			if r.Dir, err = expandHome(r.Dir); err != nil {
				return err
			}
		}
		c.RunAs[role] = r
	}
	if _, ok := c.RunAs[core.RoleOwner]; !ok {
		return errors.New("run_as.owner is required")
	}
	// Every verified caller who is not an owner runs as the employee account.
	if _, ok := c.RunAs[core.RoleEmployee]; !ok {
		return errors.New("run_as.employee is required: every caller who is not an owner runs as it")
	}
	// Offline staging may lack the employee account; all runtime paths stay strict.
	if staged {
		_, lookupErr := lookupUser(c.RunAs[core.RoleEmployee].User)
		var missing user.UnknownUserError
		if errors.As(lookupErr, &missing) {
			if _, err := lookupUser(c.RunAs[core.RoleOwner].User); err != nil {
				return err
			}
			c.warnings = append(c.warnings, "employee OS account missing; create it before first start")
			return nil
		}
	}
	// The employee's account must be neither root nor the owner's, by uid.
	if err := (harness.Accounts{RunAs: c.RunAs, Lookup: lookupUser}).Check(); err != nil {
		return fmt.Errorf("run_as: %w", err)
	}
	return nil
}

func botUsername(s string) bool {
	if len(s) < 5 || len(s) > 32 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}

// lookupUser resolves OS accounts; tests replace it.
var lookupUser = user.Lookup

// Access selects the hub Pi authority, or explicit non-hub owners fallback.
func (c *Config) Access() core.Access {
	a := core.Access{Mode: c.Mode, Owners: map[string]bool{}}
	if !c.StandaloneAccess {
		a.Dir = c.AccessDir
		if a.Dir == "" {
			a.Dir = "/root/.pi/agent/access"
		}
		return a
	}
	for _, k := range c.Owners {
		a.Owners[k] = true
	}
	return a
}

func expandHome(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, p[1:])
	}
	return filepath.Abs(p)
}

func (c *Config) validateAuthority() error {
	warnings, err := c.Access().ValidateAuthority()
	if err != nil {
		return err
	}
	c.warnings = append(c.warnings, warnings...)
	if !c.StandaloneAccess && len(c.Owners) > 0 {
		c.warnings = append(c.warnings, "owners ignored in Pi access mode; authority comes only from ADMIN_USER_IDS")
	}
	return nil
}
