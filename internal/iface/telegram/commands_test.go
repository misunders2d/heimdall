package telegram

import (
	"encoding/json"
	"testing"
)

func TestSharedUnmentionedCommandPolicies(t *testing.T) {
	for _, policy := range []struct {
		name  string
		words []string
	}{{"default", nil}, {"all", []string{"all"}}, {"none", []string{}}} {
		t.Run(policy.name, func(t *testing.T) {
			tg, err := New(Options{Token: "test-token", BotUsername: botName, UnmentionedCommands: policy.words})
			if err != nil {
				t.Fatal(err)
			}
			tg.botID = 900
			tg.botName = botName
			for _, text := range []string{"stop", "cancel", "/stop", "/stop@" + botName, "abort", "nevermind", "/abort", "/nevermind", "reset", "chatid", "/start", "!help", "!status", "!use omp", "!next hello", "!omp hello", "stop now", "hello", "@" + botName + " reset"} {
				var u update
				raw, _ := json.Marshal(upd(1, msg(1, -1, "group", 42, text)))
				_ = json.Unmarshal(raw, &u)
				in, _, ok := tg.inbound(u)
				if !ok {
					t.Fatalf("missing inbound for %q", text)
				}
				control := text == "stop" || text == "cancel" || text == "/stop" || text == "/stop@"+botName || text == "abort" || text == "nevermind" || text == "/abort" || text == "/nevermind"
				command := text != "hello" && text != "stop now"
				mention := text == "@"+botName+" reset" || text == "/stop@"+botName
				want := mention || policy.name == "all" && command || policy.name == "default" && control
				// An explicit /stop@bot addresses the bot even when the unmentioned list
				// is empty; this is the existing Telegram mention behavior.
				if in.Ambient == want {
					t.Errorf("%q addressed=%v want=%v", text, !in.Ambient, want)
				}
				raw, _ = json.Marshal(upd(2, msg(2, 42, "private", 42, text)))
				_ = json.Unmarshal(raw, &u)
				in, _, ok = tg.inbound(u)
				if !ok || in.Ambient {
					t.Errorf("private chat gated: %q", text)
				}
			}
		})
	}
}
