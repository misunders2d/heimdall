package telegram

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// Bot API Message/MessageEntity/LinkPreviewOptions shapes, checked 2026-11-08
// at https://core.telegram.org/bots/api#message; values are synthetic.
func TestInboundContent(t *testing.T) {
	for _, tc := range []struct {
		name, cargo string
		want        []string
		files       int
	}{
		{"forwarded_preview_gif", `{"text":"waylay","entities":[{"type":"text_link","offset":0,"length":6,"url":"https://example.test/preview.gif"}],"link_preview_options":{"url":"https://example.test/preview.gif"},"forward_origin":{"type":"channel","date":1700000000,"chat":{"id":-100123,"type":"channel","title":"News"},"message_id":7}}`, []string{"waylay", "https://example.test/preview.gif", "source News"}, 0},
		{"caption_link", `{"caption":"look","caption_entities":[{"type":"text_link","offset":0,"length":4,"url":"https://example.test/caption"}],"photo":[{"file_id":"id:photo.jpg","width":100,"height":100}]}`, []string{"look", "https://example.test/caption", "[Photo]"}, 1},
		{"utf16_url", `{"text":"😀 https://example.test","entities":[{"type":"url","offset":3,"length":20}]}`, []string{"[Link: https://example.test]"}, 0},
		{"poll", `{"poll":{"id":"p1","question":"Lunch?","options":[{"text":"Yes","voter_count":1}],"total_voter_count":1,"is_closed":false,"is_anonymous":true,"type":"regular","allows_multiple_answers":false}}`, []string{"[Poll:", "Lunch?", "Yes"}, 0},
		{"dice", `{"dice":{"emoji":"🎲","value":6}}`, []string{"[Dice:", "🎲", "6"}, 0},
		{"paid_media_items", `{"paid_media":{"star_count":5,"paid_media":[{"type":"photo","photo":[{"file_id":"id:photo.jpg","width":100,"height":100}]},{"type":"video","video":{"file_id":"id:video.mp4","width":100,"height":100,"duration":1}},{"type":"preview","width":100,"height":100}]}}`, []string{"[Paid media: 5 stars]", "[Photo]", "[Video:", "file unavailable"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]any
			if err := json.Unmarshal([]byte(tc.cargo), &fields); err != nil {
				t.Fatal(err)
			}
			m := msg(10, 42, "private", 42, "")
			for key, value := range fields {
				m[key] = value
			}
			var u update
			raw, _ := json.Marshal(upd(1, m))
			if err := json.Unmarshal(raw, &u); err != nil {
				t.Fatal(err)
			}
			api := newFakeAPI(t)
			api.files["photo.jpg"], api.files["video.mp4"] = []byte("photo"), []byte("video")
			var logs syncBuffer
			i := newIface(t, api.srv.URL, botName, &logs)
			i.opt.UploadDir = t.TempDir()
			in, message, ok := i.inbound(u)
			if !ok {
				t.Fatal("content dropped")
			}
			for _, want := range tc.want {
				if !strings.Contains(in.Text+"\n"+in.Context, want) {
					t.Errorf("missing %q in %+v", want, in)
				}
			}
			files := i.attachments(context.Background(), message)
			if len(files) != tc.files {
				t.Fatalf("files=%d want %d", len(files), tc.files)
			}
			for _, file := range files {
				if file.Stored == "" {
					t.Errorf("file not downloaded: %+v", file)
				}
			}
		})
	}
}
