package core

import "testing"

func TestRoles(t *testing.T) {
	a := Access{Mode: ModeCorporate, Owners: map[string]bool{"telegram:1": true, "slack:owner@example.com": true}}
	for _, tc := range []struct {
		name string
		p    Principal
		role Role // "" = cannot be identified
	}{
		{"telegram admin", Principal{Platform: "telegram", ID: "1"}, RoleOwner},
		{"telegram non-admin is non-root", Principal{Platform: "telegram", ID: "2"}, RoleEmployee},
		{"telegram ID must be numeric", Principal{Platform: "telegram", ID: "@owner"}, ""},
		{"slack owner email", SlackPrincipal(" Owner@Example.com", "T0WORK", "x"), RoleOwner},
		{"slack verified email is non-root", SlackPrincipal("worker@example.com", "T0WORK", ""), RoleEmployee},
		{"slack without a verified email", Principal{Platform: "slack", ID: "T1/U2"}, ""},
		{"slack email without its workspace", Principal{Platform: "slack", ID: "worker@example.com", Email: "worker@example.com"}, ""},
		{"slack ID that is not its verified email", Principal{Platform: "slack", ID: "owner@example.com", Email: "worker@example.com", Team: "T0WORK"}, ""},
		{"display name is never identity", Principal{Platform: "telegram", ID: "2", Name: "telegram:1"}, RoleEmployee},
		{"slack display name is never identity", Principal{Platform: "slack", ID: "worker@example.com", Email: "worker@example.com", Team: "T0WORK", Name: "owner@example.com"}, RoleEmployee},
		{"no identity", Principal{Platform: "telegram"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, ok := a.Role(tc.p)
			if role != tc.role || ok != (tc.role != "") {
				t.Fatalf("Role(%+v) = %q, %v; want %q", tc.p, role, ok, tc.role)
			}
		})
	}
	if k := SlackPrincipal("Worker@Example.com", "T0WORK", "").Key(); k != "slack:worker@example.com" {
		t.Fatalf("slack key = %q, want the email", k)
	}
}

func TestMemory(t *testing.T) {
	tg := Principal{Platform: "telegram", ID: "2"}
	sl := SlackPrincipal("worker@example.com", "T0WORK", "")
	for _, tc := range []struct {
		mode Mode
		role Role
		p    Principal
		want bool
	}{
		{ModePersonal, RoleOwner, tg, true},
		{ModeCorporate, RoleOwner, tg, true},
		{ModePersonal, RoleEmployee, sl, false},
		{ModeCorporate, RoleEmployee, sl, true},
		{ModeCorporate, RoleEmployee, tg, false},
		{ModePersonal, RoleEmployee, tg, false},
		{ModeCorporate, RoleEmployee, Principal{Platform: "local", ID: "x"}, false},
	} {
		if got := Memory(tc.mode, tc.role, tc.p); got != tc.want {
			t.Errorf("Memory(%s, %s, %s) = %v, want %v", tc.mode, tc.role, tc.p.Key(), got, tc.want)
		}
	}
}

func TestParsePrincipalKey(t *testing.T) {
	if p, err := ParsePrincipalKey("slack:Owner@Example.com"); err != nil || p.Key() != "slack:owner@example.com" {
		t.Fatalf("slack owner = %+v, %v", p, err)
	}
	for _, bad := range []string{"telegram:abc", "slack:T1/U2", "nocolon", "telegram:"} {
		if _, err := ParsePrincipalKey(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
