package harness

import (
	"encoding/json"
	"github.com/misunders2d/heimdall/internal/core"
	"strings"
	"testing"
)

func TestReviewDefaultDeliveryIdentity(t *testing.T) {
	for _, c := range []Caller{
		{Principal: core.SlackPrincipal("owner@example.test", "Ttest", "Owner"), Conv: core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1000.001"}},
		{Principal: core.Principal{Platform: core.PlatformTelegram, ID: "12"}, Conv: core.ConvKey{Interface: "telegram", Chat: "-123"}},
	} {
		env, err := WithIdentity([]string{`PI_EXTERNAL_DEFAULT_DELIVERY={"channel":"spoof"}`}, c)
		if err != nil {
			t.Fatal(err)
		}
		var d map[string]any
		count := 0
		for _, v := range env {
			if strings.HasPrefix(v, "PI_EXTERNAL_DEFAULT_DELIVERY=") {
				count++
				if err := json.Unmarshal([]byte(strings.TrimPrefix(v, "PI_EXTERNAL_DEFAULT_DELIVERY=")), &d); err != nil {
					t.Fatal(err)
				}
			}
		}
		if count != 1 {
			t.Fatal("missing/duplicate delivery")
		}
		if c.Principal.Platform == core.PlatformSlack {
			if d["channel"] != "C1" || d["thread_ts"] != "1000.001" || d["sessionKey"] != "sl_channel_C1_thread_1000.001" {
				t.Fatal(d)
			}
		} else {
			if d["chatId"] != float64(-123) || d["sessionKey"] != "tg_chat_-123" {
				t.Fatal(d)
			}
		}
	}
}
