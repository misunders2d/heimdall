package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/store"
)

func TestCheckConfigOffline(t *testing.T) {
	t.Setenv("ADMIN_USER_IDS", "1")
	path := filepath.Join(t.TempDir(), "config.json")
	raw := `{"data_dir":"/no-store-created","mode":"corporate","owners":["telegram:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"heimdall-no-such-test-user"}},"slack":{"team_id":"TFAKE"},"telegram":{"bot_username":"example_bot"}}`
	raw = strings.Replace(raw, `"mode":"corporate"`, `"mode":"corporate","access_dir":"`+t.TempDir()+`"`, 1)
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkConfig([]string{"-config", path}); err != nil {
		t.Fatal(err)
	}
	if err := checkConfig(nil); err == nil {
		t.Fatal("missing config accepted")
	}
}

// Admin commands never migrate: a new binary's sched or drain must not
// upgrade the schema under the running (older) service.
func TestAdminCommandsNeverMigrate(t *testing.T) {
	t.Setenv("ADMIN_USER_IDS", "1")
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.json")
	raw := `{"data_dir":"` + data + `","mode":"corporate","access_dir":"` + t.TempDir() + `","owners":["telegram:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"nobody"}},"telegram":{"bot_username":"example_bot"}}`
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(data, "heimdall.db")
	version := func() int {
		t.Helper()
		c, err := sql.Open("sqlite", "file:"+db+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var v int
		if err := c.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	cfg := []string{"-config", cfgPath}

	// No database: refused, and none is created.
	if err := drain(append([]string{"-timeout", "1s"}, cfg...)); err == nil || !strings.Contains(err.Error(), "start heimdall run first") {
		t.Fatalf("drain without database: %v", err)
	}
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatalf("admin command created the database: %v", err)
	}

	// An older schema than the admin commands serve: refused, unchanged.
	old := store.AdminMinSchema - 1
	if err := store.CreateAtSchema(db, old); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"drain":      func() error { return drain(append([]string{"-timeout", "1s"}, cfg...)) },
		"sched list": func() error { return sched(append([]string{"list"}, cfg...)) },
		"sched show": func() error { return sched(append([]string{"show", "nope"}, cfg...)) },
	} {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "never migrate") {
			t.Fatalf("%s on schema %d: %v", name, old, err)
		}
		if v := version(); v != old {
			t.Fatalf("%s changed user_version %d -> %d", name, old, v)
		}
	}

	// The oldest served schema works and stays as it is.
	if err := os.Remove(db); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAtSchema(db, store.AdminMinSchema); err != nil {
		t.Fatal(err)
	}
	if err := drain(append([]string{"-timeout", "5s"}, cfg...)); err != nil {
		t.Fatalf("drain on schema %d: %v", store.AdminMinSchema, err)
	}
	if err := sched(append([]string{"list"}, cfg...)); err != nil {
		t.Fatalf("sched list on schema %d: %v", store.AdminMinSchema, err)
	}
	if v := version(); v != store.AdminMinSchema {
		t.Fatalf("admin commands changed user_version %d -> %d", store.AdminMinSchema, v)
	}

	// A newer schema (rollback without restoring the backup): refused.
	c, err := sql.Open("sqlite", "file:"+db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, store.Schema()+1)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := drain(append([]string{"-timeout", "1s"}, cfg...)); err == nil || !strings.Contains(err.Error(), "newer than this Heimdall") {
		t.Fatalf("drain on newer schema: %v", err)
	}
	if v := version(); v != store.Schema()+1 {
		t.Fatalf("newer schema changed to %d", v)
	}
}
