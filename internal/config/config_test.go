package config

import (
	"os/user"
	"strconv"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// OS accounts the tests' configs name, by uid.
var uids = map[string]int{"root": 0, "toor": 0, "boss": 1000, "boss2": 1000, "worker": 1001}

func init() {
	lookupUser = func(name string) (*user.User, error) {
		uid, ok := uids[name]
		if !ok {
			return nil, user.UnknownUserError(name)
		}
		id := strconv.Itoa(uid)
		return &user.User{Uid: id, Gid: id, Username: name, HomeDir: "/home/" + name}, nil
	}
}

func TestParseDefaultsAndAccess(t *testing.T) {
	c, err := Parse([]byte(`{"data_dir":"/tmp/h","mode":"personal","owners":["telegram:1"],"employees":["slack:T/U"],
"run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(c.HarnessOrder, ",") != "omp,codex,claude,pi" || c.MaxParallel != 4 {
		t.Fatalf("defaults: %+v", c)
	}
	a := c.Access()
	if r, ok := a.Role(core.Principal{Platform: "telegram", ID: "1", Name: "anything"}); !ok || r != core.RoleOwner {
		t.Fatal("owner not recognised by platform ID")
	}
	if r, ok := a.Role(core.Principal{Platform: "slack", ID: "T/U"}); !ok || r != core.RoleEmployee {
		t.Fatal("employee not recognised")
	}
	if _, ok := a.Role(core.Principal{Platform: "telegram", ID: "2", Name: "telegram:1"}); ok {
		t.Fatal("unknown caller accepted (display name must never count)")
	}
}

func TestParseRejects(t *testing.T) {
	base := `"data_dir":"/tmp/h","mode":"corporate","run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}`
	for name, doc := range map[string]string{
		"no owners":        `{` + base + `}`,
		"bad principal":    `{` + base + `,"owners":["nocolon"]}`,
		"owner+employee":   `{` + base + `,"owners":["t:1"],"employees":["t:1"]}`,
		"unknown harness":  `{` + base + `,"owners":["t:1"],"harness_order":["vim"]}`,
		"unknown field":    `{` + base + `,"owners":["t:1"],"token":"x"}`,
		"bad mode":         `{"data_dir":"/tmp/h","mode":"x","owners":["t:1"],"run_as":{"owner":{"user":"root"}}}`,
		"same OS user":     `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"employees":["t:2"],"run_as":{"owner":{"user":"root"},"employee":{"user":"root"}}}`,
		"employee no user": `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"employees":["t:2"],"run_as":{"owner":{"user":"root"}}}`,
		"employee is root": `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"employees":["t:2"],"run_as":{"owner":{"user":"boss"},"employee":{"user":"toor"}}}`,
		"employee=owner":   `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"employees":["t:2"],"run_as":{"owner":{"user":"boss"},"employee":{"user":"boss2"}}}`,
		"unknown OS user":  `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"employees":["t:2"],"run_as":{"owner":{"user":"root"},"employee":{"user":"nobody-here"}}}`,
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
