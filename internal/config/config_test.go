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
	c, err := Parse([]byte(`{"data_dir":"/tmp/h","mode":"personal","owners":["telegram:1","slack:Owner@Example.com"],
"run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(c.HarnessOrder, ",") != "omp,codex,claude,pi" || c.MaxParallel != 4 {
		t.Fatalf("defaults: %+v", c)
	}
	a := c.Access()
	if r, ok := a.Role(core.Principal{Platform: "telegram", ID: "1", Name: "anything"}); !ok || r != core.RoleOwner {
		t.Fatal("owner not recognised by Telegram user ID")
	}
	if r, ok := a.Role(core.SlackPrincipal("owner@example.com", "T0WORK", "")); !ok || r != core.RoleOwner {
		t.Fatal("owner not recognised by verified Slack email")
	}
	if r, ok := a.Role(core.Principal{Platform: "telegram", ID: "2", Name: "telegram:1"}); !ok || r != core.RoleEmployee {
		t.Fatal("a Telegram caller who is not an owner must be non-root (display name never counts)")
	}
}

func TestParseRejects(t *testing.T) {
	base := `"data_dir":"/tmp/h","mode":"corporate","run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}`
	for name, doc := range map[string]string{
		"no owners":           `{` + base + `}`,
		"bad principal":       `{` + base + `,"owners":["nocolon"]}`,
		"telegram name":       `{` + base + `,"owners":["telegram:@boss"]}`,
		"slack user ID":       `{` + base + `,"owners":["slack:T1/U2"]}`,
		"owner twice":         `{` + base + `,"owners":["slack:a@b.c","slack:A@B.C"]}`,
		"employee allowlist":  `{` + base + `,"owners":["t:1"],"employees":["t:2"]}`,
		"unknown harness":     `{` + base + `,"owners":["t:1"],"harness_order":["vim"]}`,
		"unknown field":       `{` + base + `,"owners":["t:1"],"token":"x"}`,
		"bad mode":            `{"data_dir":"/tmp/h","mode":"x","owners":["t:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}}`,
		"same OS user":        `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"root"}}}`,
		"no employee account": `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"root"}}}`,
		"employee is root":    `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"boss"},"employee":{"user":"toor"}}}`,
		"employee=owner":      `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"boss"},"employee":{"user":"boss2"}}}`,
		"unknown OS user":     `{"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"nobody-here"}}}`,
		"telegram no bot":     `{` + base + `,"owners":["t:1"],"telegram":{}}`,
		"telegram bad bot":    `{` + base + `,"owners":["t:1"],"telegram":{"bot_username":"my bot"}}`,
		"telegram token":      `{` + base + `,"owners":["t:1"],"telegram":{"bot_username":"a_bot","token":"x"}}`,
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestTelegramBlock(t *testing.T) {
	base := `"data_dir":"/tmp/h","mode":"corporate","owners":["telegram:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}`
	c, err := Parse([]byte(`{` + base + `,"telegram":{"bot_username":"@Example_Test_Bot"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Telegram == nil || c.Telegram.BotUsername != "Example_Test_Bot" {
		t.Fatalf("telegram: %+v", c.Telegram)
	}
	if c, err = Parse([]byte(`{` + base + `}`)); err != nil || c.Telegram != nil {
		t.Fatalf("no telegram block: %+v, %v", c, err)
	}
}
