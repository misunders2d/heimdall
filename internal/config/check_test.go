package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineCheckMissingEmployeeOnly(t *testing.T) {
	base := `{"data_dir":"/tmp/unused","mode":"corporate","owners":["telegram:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"MISSING"}},"telegram":{"bot_username":"example_bot","api_base":"http://127.0.0.1:8081","file_base":"http://127.0.0.1:8081/file/bot","max_file_bytes":2000000000}}`
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
