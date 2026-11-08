package core

// Pi access-control.ts semantics. Files are read by Heimdall, never the child UID.
import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type accessList struct {
	Users []string `json:"users"`
	Chats []string `json:"chats"`
}
type accessFiles struct {
	White, Black accessList
	Admins       accessList
	Identities   struct {
		Principals map[string]struct {
			Aliases []string `json:"aliases"`
		} `json:"principals"`
	}
	Caps map[string][]string
}

func normID(s string) string {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "@") {
		s = strings.ToLower(s)
	}
	return s
}
func variants(platform, id string) []string {
	id = normID(id)
	if id == "" {
		return nil
	}
	raw := id
	prefixes := []string{platform + "_user_", platform + "_chat_", platform + "_"}
	if platform == "sl" {
		prefixes = []string{"sl_user_", "sl_channel_", "sl_chat_", "sl_"}
	}
	for _, p := range prefixes {
		raw = strings.TrimPrefix(raw, p)
	}
	out := []string{id, raw, platform + "_" + raw, platform + "_user_" + raw, platform + "_chat_" + raw}
	if platform == "sl" {
		out = append(out, "sl_channel_"+raw)
	}
	return out
}
func overlap(a, b []string) bool {
	for _, v := range a {
		if slices.Contains(b, normID(v)) {
			return true
		}
	}
	return false
}
func (f accessFiles) ids(platform, id string) []string {
	out := variants(platform, id)
	// Sort for reproducible alias-chain expansion; normal files have disjoint principals.
	keys := make([]string, 0, len(f.Identities.Principals))
	for k := range f.Identities.Principals {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, p := range keys {
		e := f.Identities.Principals[p]
		var expanded []string
		for _, alias := range append([]string{p}, e.Aliases...) {
			expanded = append(expanded, variants("tg", alias)...)
			expanded = append(expanded, variants("sl", alias)...)
		}
		if overlap(out, expanded) {
			out = append(out, expanded...)
		}
	}
	return out
}
func (a Access) files() (accessFiles, bool) {
	var f accessFiles
	for name, dst := range map[string]any{"whitelist.json": &f.White, "blacklist.json": &f.Black, "admins.json": &f.Admins, "identities.json": &f.Identities, "capabilities.json": &f.Caps} {
		raw, e := os.ReadFile(filepath.Join(a.Dir, name))
		if e == nil {
			e = json.Unmarshal(raw, dst)
		}
		if e != nil {
			log.Printf("access: cannot load %s; admission denied", name)
			return f, false
		}
	}
	return f, true
}
func platformID(p Principal) string {
	if p.Platform == PlatformTelegram {
		return "tg"
	}
	if p.Platform == PlatformSlack {
		return "sl"
	}
	return ""
}

// Decide includes conversation admission and capabilities in one root-side snapshot.
func (a Access) Decide(p Principal, conv ConvKey) (Role, []string, bool) {
	if _, ok := (Access{}).Role(p); !ok {
		return "", nil, false
	}
	if a.Dir == "" {
		r, ok := a.Role(p)
		return r, nil, ok
	}
	platform := platformID(p)
	if platform == "" {
		return "", nil, false
	}
	f, ok := a.files()
	if !ok {
		return "", nil, false
	}
	ids := f.ids(platform, p.ID)
	if p.Platform == PlatformSlack && p.SlackUserID != "" {
		ids = append(ids, f.ids(platform, p.SlackUserID)...)
	}
	chats := f.ids(platform, conv.Chat)
	has := func(list []string, ids []string) bool {
		for _, v := range list {
			if slices.Contains(ids, normID(v)) {
				return true
			}
		}
		return false
	}
	admins := strings.Split(os.Getenv("ADMIN_USER_IDS"), ",")
	for _, admin := range admins {
		if overlap(ids, variants(platform, admin)) {
			return RoleOwner, nil, true
		}
	}
	fileAdmin := has(f.Admins.Users, ids)
	if !fileAdmin && (has(f.Black.Users, ids) || has(f.Black.Chats, chats)) {
		return "", nil, false
	}
	if !fileAdmin && !has(f.White.Users, ids) && !has(f.White.Chats, chats) {
		return "", nil, false
	}
	var caps []string
	for _, id := range ids {
		for _, cap := range f.Caps[id] {
			if !slices.Contains(caps, cap) {
				caps = append(caps, cap)
			}
		}
	}
	slices.Sort(caps)
	return RoleEmployee, caps, true
}
func (a Access) Capabilities(p Principal, conv ConvKey) []string {
	_, caps, _ := a.Decide(p, conv)
	return caps
}

// OwnerKeys derives notice destinations from the same authority, never config owners on hub.
func (a Access) OwnerKeys() []string {
	if a.Dir == "" {
		keys := make([]string, 0, len(a.Owners))
		for k, v := range a.Owners {
			if v {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		return keys
	}
	f, ok := a.files()
	if !ok {
		return nil
	}
	candidates := strings.Split(os.Getenv("ADMIN_USER_IDS"), ",")
	for p, e := range f.Identities.Principals {
		candidates = append(candidates, p)
		candidates = append(candidates, e.Aliases...)
	}
	var keys []string
	for _, id := range candidates {
		id = normID(id)
		for _, prefix := range []string{"tg_user_", "tg_", "sl_user_", "sl_"} {
			id = strings.TrimPrefix(id, prefix)
		}
		var p Principal
		if numeric(id) {
			p = Principal{Platform: PlatformTelegram, ID: id}
		} else if strings.Contains(id, "@") {
			p = SlackPrincipal(id, "notice-destination", "")
		} else {
			continue
		}
		if r, admitted := a.Role(p); admitted && r == RoleOwner && !slices.Contains(keys, p.Key()) {
			keys = append(keys, p.Key())
		}
	}
	slices.Sort(keys)
	return keys
}

// SamePerson reports whether two verified principals are the same person: the
// same key, or principals linked by the access snapshot's identity aliases (the
// expansion Decide already uses). Without access files only equal keys match.
func (a Access) SamePerson(x, y Principal) bool {
	if x.Key() == y.Key() {
		return true
	}
	px, py := platformID(x), platformID(y)
	if a.Dir == "" || px == "" || py == "" {
		return false
	}
	f, ok := a.files()
	if !ok {
		return false
	}
	return overlap(f.ids(px, x.ID), f.ids(py, y.ID))
}

// ValidateAuthority checks startup authority without reading credentials or requiring
// the staged Pi access files to exist. The files themselves fail closed per turn.
func (a Access) ValidateAuthority() ([]string, error) {
	if a.Dir == "" {
		return nil, nil
	}
	valid := false
	for _, id := range strings.Split(os.Getenv("ADMIN_USER_IDS"), ",") {
		id = normID(id)
		for _, prefix := range []string{"tg_user_", "tg_", "sl_user_", "sl_"} {
			id = strings.TrimPrefix(id, prefix)
		}
		if numeric(id) || strings.Contains(id, "@") || (strings.HasPrefix(id, "U") || strings.HasPrefix(id, "W")) && len(id) > 1 {
			valid = true
		}
	}
	if !valid {
		return nil, fmt.Errorf("Pi access requires non-empty ADMIN_USER_IDS with a verified owner identity; non-hub installs may explicitly set standalone_access and owners")
	}
	var admins accessList
	raw, err := os.ReadFile(filepath.Join(a.Dir, "admins.json"))
	if err == nil && json.Unmarshal(raw, &admins) == nil && (len(admins.Users) > 0 || len(admins.Chats) > 0) {
		return []string{"admins.json is non-empty: entries grant admission only, never owner authority"}, nil
	}
	return nil, nil
}
