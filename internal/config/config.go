// Package config loads Heimdall's one JSON configuration file. Secrets are
// never stored here: interfaces read them from the environment.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
)

// DefaultOrder is the harness order used when a chat has no `!use` choice.
var DefaultOrder = []string{"omp", "codex", "claude", "pi"}

type Config struct {
	Commands     *Commands                `json:"commands,omitempty"`
	DataDir      string                   `json:"data_dir"`
	Mode         core.Mode                `json:"mode"`
	Owners       []string                 `json:"owners"` // principal keys, platform:id; everyone else is non-root
	HarnessOrder []string                 `json:"harness_order,omitempty"`
	MaxParallel  int                      `json:"max_parallel,omitempty"`
	RunAs        map[core.Role]core.RunAs `json:"run_as"`
	Slack        *Slack                   `json:"slack,omitempty"`
	Telegram     *Telegram                `json:"telegram,omitempty"`
	PromptGuard  *PromptGuard             `json:"prompt_guard,omitempty"`
}

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
	// BotUsername is the bot the token must belong to: getMe is checked
	// against it before polling, so a wrong token is never used.
	BotUsername string `json:"bot_username"`
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
	return Parse(raw)
}

// Parse validates a config document and fills defaults.
func Parse(raw []byte) (*Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("config: trailing data after the JSON object")
	}
	if err := c.fill(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

func (c *Config) fill() error {
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
	if len(c.Owners) == 0 {
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
			if !slices.Contains([]string{"stop", "cancel", "reset", "chatid", "help", "status", "all"}, name) {
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
		c.Telegram.BotUsername = strings.TrimPrefix(strings.TrimSpace(c.Telegram.BotUsername), "@")
		if !botUsername(c.Telegram.BotUsername) {
			return errors.New("telegram.bot_username must be the bot's Telegram username (5-32 letters, digits or _)")
		}
	}
	if g := c.PromptGuard; g != nil && g.LogMaxBytes != 0 && g.LogMaxBytes < 4096 {
		return errors.New("prompt_guard.log_max_bytes must be at least 4096 (bytes)")
	}
	if c.MaxParallel == 0 {
		c.MaxParallel = 4
	}
	if c.MaxParallel < 1 {
		return errors.New("max_parallel must be at least 1")
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

// Access is the config's owner list.
func (c *Config) Access() core.Access {
	a := core.Access{Mode: c.Mode, Owners: map[string]bool{}}
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
