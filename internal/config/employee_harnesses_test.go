package config

import "testing"

// MEL-846: every engine that meets the employee boundary may be listed;
// enabling one stays a per-engine owner decision after the hub probe.
func TestEmployeeHarnessesAllEngines(t *testing.T) {
	base := `"data_dir":"/tmp/h","mode":"corporate","owners":["t:1"],"run_as":{"owner":{"user":"root"},"employee":{"user":"worker"}}`
	for _, list := range []string{`["omp"]`, `["codex"]`, `["claude"]`, `["pi"]`, `["omp","codex","claude","pi"]`} {
		if _, err := Parse([]byte(`{` + base + `,"employee_harnesses":` + list + `}`)); err != nil {
			t.Errorf("%s rejected: %v", list, err)
		}
	}
	for _, list := range []string{`["agy"]`, `["vim"]`} {
		if _, err := Parse([]byte(`{` + base + `,"employee_harnesses":` + list + `}`)); err == nil {
			t.Errorf("%s accepted", list)
		}
	}
}
