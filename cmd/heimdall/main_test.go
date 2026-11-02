package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
