package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineCheckMissingEmployeeOnly(t *testing.T) {
	t.Setenv("ADMIN_USER_IDS", "1")
	base := `{"data_dir":"/tmp/unused","mode":"corporate","owners":[],"run_as":{"owner":{"user":"root"},"employee":{"user":"MISSING"}},"telegram":{"bot_username":"example_bot","api_base":"http://127.0.0.1:8081","file_base":"http://127.0.0.1:8081/file/bot","max_file_bytes":2000000000}}`
	base = strings.Replace(base, `"mode":"corporate"`, `"mode":"corporate","access_dir":"`+t.TempDir()+`"`, 1)
	path := filepath.Join(t.TempDir(), "config.json")
	for _, tc := range []struct {
		employee    string
		ok, warning bool
	}{{"missing-employee", true, true}, {"worker", true, false}, {"toor", false, false}} {
		raw := []byte(strings.ReplaceAll(base, "MISSING", tc.employee))
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		warnings, err := CheckFile(path)
		if (err == nil) != tc.ok || (len(warnings) > 0) != tc.warning {
			t.Fatalf("%s: warnings=%v err=%v", tc.employee, warnings, err)
		}
		if tc.warning {
			if _, err := Parse(raw); err == nil {
				t.Fatal("runtime accepted missing employee")
			}
		}
	}
	for _, bad := range []string{`"api_base":"file:///tmp"`, `"max_file_bytes":-1`} {
		raw := strings.ReplaceAll(base, "MISSING", "worker")
		if strings.Contains(bad, "api_base") {
			raw = strings.ReplaceAll(raw, `"api_base":"http://127.0.0.1:8081"`, bad)
		} else {
			raw = strings.ReplaceAll(raw, `"max_file_bytes":2000000000`, bad)
		}
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestStartupAuthority(t *testing.T) {
	raw := []byte(`{"data_dir":"/tmp/unused","mode":"corporate","run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}}`)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, ids := range []string{"", " , ", "garbage"} {
		t.Setenv("ADMIN_USER_IDS", ids)
		if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "ADMIN_USER_IDS") {
			t.Fatalf("Load %q: %v", ids, err)
		}
		if _, err := CheckFile(path); err == nil {
			t.Fatal("check accepted missing authority")
		}
	}
	raw = []byte(strings.Replace(string(raw), `"mode":"corporate"`, `"mode":"corporate","standalone_access":true,"owners":["telegram:1"]`, 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}
