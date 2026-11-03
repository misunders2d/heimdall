package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/googleoauth"
)

func TestEmployeeGoogleHandoffHasNoRenewalMaterial(t *testing.T) {
	dir := t.TempDir()
	launch := Launch{Cred: &syscall.Credential{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}}
	access := googleoauth.Access{Email: "employee@example.test", Token: "SYNTHETIC_ACCESS", Expires: time.Now().Add(time.Hour), Scopes: googleoauth.Scopes}
	if err := writeEmployeeGoogleAccess(dir, access, launch); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "google-access.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err = json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	if len(data) != 4 || data["email"] != access.Email || data["access_token"] != access.Token {
		t.Fatal("wrong handoff schema")
	}
	for _, forbidden := range []string{"refresh", "client_secret", "client_id"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("renewal field in handoff")
		}
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("unsafe handoff mode", err)
	}
	// Rebuilt employee env strips both legacy Google and dedicated root names.
	env := employeeEnv([]string{"GOOGLE_OAUTH_CLIENT_SECRET=SYNTHETIC_CLIENT_SECRET", "GOOGLE_OAUTH_CLIENT_ID=SYNTHETIC_ID", "HEIMDALL_GOOGLE_CLIENT_SECRET=SYNTHETIC_CLIENT_SECRET", "HEIMDALL_GOOGLE_ACCESS_FILE=/other/caller", "PI_GOOGLE_OAUTH_DB=/owner/store", "INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET=SYNTHETIC_INFISICAL", "MEMENTO_PROCESSING=0", "SAFE=kept"}, Caller{Role: core.RoleEmployee})
	for _, kv := range env {
		if strings.Contains(kv, "SYNTHETIC") || strings.Contains(kv, "GOOGLE_") {
			t.Fatal("Google/Infisical credentials in employee env")
		}
	}
	if envValue(env, "SAFE") != "kept" || envValue(env, "MEMENTO_PROCESSING") != "1" || envValue(env, "PI_RESTRICTED_EXTERNAL_USER") != "1" {
		t.Fatal("employee restrictions changed")
	}
}

func TestGoogleHandoffNeverFollowsEmployeeSymlinks(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(secret, []byte("SYNTHETIC_ROOT_REFRESH"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(dir, "google-access.json")); err != nil {
		t.Fatal(err)
	}
	launch := Launch{Cred: &syscall.Credential{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}}
	if err := writeEmployeeGoogleAccess(dir, googleoauth.Access{}, launch); err == nil {
		t.Fatal("handoff followed a credential symlink")
	}
	after, err := os.ReadFile(secret)
	if err != nil || string(after) != "SYNTHETIC_ROOT_REFRESH" {
		t.Fatal("root credential was modified")
	}
	link := filepath.Join(t.TempDir(), "agent-link")
	if err = os.Symlink(filepath.Dir(secret), link); err != nil {
		t.Fatal(err)
	}
	if err = chownEmployeeAgent(link, launch); err == nil {
		t.Fatal("agent directory chown followed a root-store symlink")
	}
}
