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
	DataDir      string                   `json:"data_dir"`
	Mode         core.Mode                `json:"mode"`
	Owners       []string                 `json:"owners"`    // principal keys, platform:id
	Employees    []string                 `json:"employees"` // principal keys, platform:id
	HarnessOrder []string                 `json:"harness_order,omitempty"`
	MaxParallel  int                      `json:"max_parallel,omitempty"`
	RunAs        map[core.Role]core.RunAs `json:"run_as"`
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
	seen := map[string]string{}
	for _, list := range []struct {
		name string
		keys []string
	}{{"owners", c.Owners}, {"employees", c.Employees}} {
		for _, k := range list.keys {
			if _, err := core.ParsePrincipalKey(k); err != nil {
				return fmt.Errorf("%s: %w", list.name, err)
			}
			if prev, dup := seen[k]; dup {
				return fmt.Errorf("principal %q is listed in both %s and %s", k, prev, list.name)
			}
			seen[k] = list.name
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
	if _, ok := c.RunAs[core.RoleEmployee]; len(c.Employees) > 0 && !ok {
		return errors.New("run_as.employee is required when employees are listed")
	}
	// The employee's account must be neither root nor the owner's, by uid.
	if err := (harness.Accounts{RunAs: c.RunAs, Lookup: lookupUser}).Check(); err != nil {
		return fmt.Errorf("run_as: %w", err)
	}
	return nil
}

// lookupUser resolves OS accounts; tests replace it.
var lookupUser = user.Lookup

// Access is the config's verified-principal access list.
func (c *Config) Access() core.Access {
	a := core.Access{Mode: c.Mode, Owners: map[string]bool{}, Employees: map[string]bool{}}
	for _, k := range c.Owners {
		a.Owners[k] = true
	}
	for _, k := range c.Employees {
		a.Employees[k] = true
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
