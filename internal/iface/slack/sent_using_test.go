package slack

import "testing"

func TestSentUsingFooterIsNotAMention(t *testing.T) {
	s := &Slack{botID: "U07LYKB410R"}
	footer := "Quick test. For <@UKU6CV683>'s visibility. *Sent using* <@U07LYKB410R>"
	if s.mention().MatchString(sentUsing.ReplaceAllString(footer, "")) {
		t.Fatal("Slack MCP attribution footer counted as a bot mention")
	}
	real := "<@U07LYKB410R> look at this *Sent using* <@U0BQYFVMAV6|ChatGPT>"
	if !s.mention().MatchString(sentUsing.ReplaceAllString(real, "")) {
		t.Fatal("a real bot mention was removed")
	}
}
