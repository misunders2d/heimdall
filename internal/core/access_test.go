package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPiAdmission(t *testing.T) {
	dir := t.TempDir()
	a := Access{Dir: dir, Owners: map[string]bool{"telegram:99": true}}
	write := func(name string, v any) {
		t.Helper()
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write("whitelist.json", accessList{Users: []string{"staff@example.test"}, Chats: []string{"tg_chat_-10", "sl_channel_C1"}})
	write("blacklist.json", accessList{Users: []string{"tg_user_3"}, Chats: []string{"tg_chat_-20"}})
	write("admins.json", accessList{Users: []string{"tg_user_8"}})
	write("identities.json", map[string]any{"principals": map[string]any{"staff@example.test": map[string]any{"aliases": []string{"tg_user_2", "sl_user_U2"}}}})
	write("capabilities.json", map[string][]string{"staff@example.test": {"mcp.linear", "scheduler"}})
	t.Setenv("ADMIN_USER_IDS", "1")
	for _, tc := range []struct {
		id, chat string
		want     Role
	}{{"1", "-20", RoleOwner}, {"8", "-20", RoleOwner}, {"2", "-30", RoleEmployee}, {"3", "-10", ""}, {"2", "-20", ""}, {"4", "-10", RoleEmployee}, {"99", "-30", ""}} {
		p := Principal{Platform: "telegram", ID: tc.id}
		r, _, ok := a.Decide(p, ConvKey{Chat: tc.chat})
		if r != tc.want || ok != (tc.want != "") {
			t.Errorf("%+v got %s %v", tc, r, ok)
		}
	}
	if keys := a.OwnerKeys(); !slices.Equal(keys, []string{"telegram:1", "telegram:8"}) {
		t.Fatal("derived owners", keys)
	}
	p := SlackPrincipal("STAFF@example.test", "T1", "")
	r, caps, ok := a.Decide(p, ConvKey{Chat: "C9"})
	if !ok || r != RoleEmployee || !slices.Contains(caps, "mcp.linear") {
		t.Fatalf("alias capabilities: %s %v %v", r, caps, ok)
	}
	// Live revocation, malformed and missing state all fail closed.
	write("whitelist.json", accessList{})
	if _, _, ok = a.Decide(p, ConvKey{Chat: "C9"}); ok {
		t.Fatal("revocation ignored")
	}
	if e := os.WriteFile(filepath.Join(dir, "blacklist.json"), []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, _, ok = a.Decide(Principal{Platform: "telegram", ID: "1"}, ConvKey{}); ok {
		t.Fatal("malformed snapshot admitted")
	}
	if e := os.Remove(filepath.Join(dir, "blacklist.json")); e != nil {
		t.Fatal(e)
	}
	if _, _, ok = a.Decide(p, ConvKey{}); ok {
		t.Fatal("missing snapshot admitted")
	}
}
