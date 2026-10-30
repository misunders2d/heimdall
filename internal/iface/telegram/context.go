package telegram

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Message context for the harness, as the hub builds it
// (misunderstood-pi-pack ec72c7d, extensions/telegram/index.ts
// telegramContextPayload, extensions/.shared/telegram-attachments.ts).

// media is what a message carries besides text.
type media struct {
	Photo     []tgFile  `json:"photo"`
	Document  *tgFile   `json:"document"`
	Voice     *tgFile   `json:"voice"`
	Audio     *tgFile   `json:"audio"`
	Video     *tgFile   `json:"video"`
	VideoNote *tgFile   `json:"video_note"`
	Contact   *contact  `json:"contact"`
	Location  *location `json:"location"`
	Venue     *venue    `json:"venue"`
}

// tgFile covers PhotoSize, Document, Voice, Audio, Video and VideoNote.
type tgFile struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	FileName     string `json:"file_name"`
	MimeType     string `json:"mime_type"`
	Title        string `json:"title"`
	Duration     int    `json:"duration"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Length       int    `json:"length"` // video note diameter
}

type contact struct {
	PhoneNumber string `json:"phone_number"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	UserID      int64  `json:"user_id"`
	VCard       string `json:"vcard"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type venue struct {
	Title   string `json:"title"`
	Address string `json:"address"`
}

// attachment is one detected attachment: a file to download (FileID set)
// or a contact, location or venue, which is only its label.
type attachment struct {
	FileID, FileUniqueID string
	Size                 int64
	Label, Name, MIME    string
	Kind                 string
	Duration             int
	Width, Height        int
}

// detect is the hub's detectTelegramAttachment: the first attachment of m,
// in the hub's order, or nil.
func detect(m *message) *attachment {
	switch {
	case m == nil:
		return nil
	case len(m.Photo) > 0:
		p := m.Photo[len(m.Photo)-1] // the largest size
		return &attachment{FileID: p.FileID, FileUniqueID: p.FileUniqueID, Size: p.FileSize, Label: "[Photo]", Name: "photo.jpg", MIME: "image/jpeg", Kind: "photo", Width: p.Width, Height: p.Height}
	case m.Document != nil:
		d := m.Document
		return &attachment{FileID: d.FileID, FileUniqueID: d.FileUniqueID, Size: d.FileSize, Label: "[Document: " + or(d.FileName, "unknown") + "]", Name: or(d.FileName, "document.bin"), MIME: or(d.MimeType, "application/octet-stream"), Kind: "document"}
	case m.Voice != nil:
		v := m.Voice
		return &attachment{FileID: v.FileID, FileUniqueID: v.FileUniqueID, Size: v.FileSize, Label: "[Voice Message]", Name: "voice.ogg", MIME: or(v.MimeType, "audio/ogg"), Kind: "voice", Duration: v.Duration}
	case m.Audio != nil:
		a := m.Audio
		return &attachment{FileID: a.FileID, FileUniqueID: a.FileUniqueID, Size: a.FileSize, Label: "[Audio: " + or(a.Title, or(a.FileName, "unknown")) + "]", Name: or(a.FileName, "audio.bin"), MIME: or(a.MimeType, "audio/mpeg"), Kind: "audio", Duration: a.Duration}
	case m.Video != nil:
		v := m.Video
		return &attachment{FileID: v.FileID, FileUniqueID: v.FileUniqueID, Size: v.FileSize, Label: "[Video: " + or(v.FileName, "unknown") + "]", Name: or(v.FileName, "video.mp4"), MIME: or(v.MimeType, "video/mp4"), Kind: "video", Duration: v.Duration, Width: v.Width, Height: v.Height}
	case m.VideoNote != nil:
		v := m.VideoNote
		return &attachment{FileID: v.FileID, FileUniqueID: v.FileUniqueID, Size: v.FileSize, Label: "[Video Message]", Name: "video-note.mp4", MIME: "video/mp4", Kind: "video_note", Duration: v.Duration, Width: v.Length, Height: v.Length}
	case m.Contact != nil:
		c := m.Contact
		var parts []string
		for _, p := range []string{strings.TrimSpace(c.FirstName + " " + c.LastName), c.PhoneNumber} {
			if p != "" {
				parts = append(parts, p)
			}
		}
		if c.UserID != 0 {
			parts = append(parts, "tg_user_id="+strconv.FormatInt(c.UserID, 10))
		}
		label := "[Contact: " + strings.Join(parts, ", ") + "]"
		if c.VCard != "" {
			label += "\nvCard:\n" + c.VCard
		}
		return &attachment{Label: label, Kind: "contact"}
	case m.Location != nil:
		return &attachment{Label: fmt.Sprintf("[Location: lat=%v, lon=%v]", m.Location.Latitude, m.Location.Longitude), Kind: "location"}
	case m.Venue != nil:
		return &attachment{Label: "[Venue: " + m.Venue.Title + " — " + m.Venue.Address + "]", Kind: "venue"}
	}
	return nil
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// repliedTo is the message m replies to, or nil. In a forum topic every
// message that is not a reply carries the topic's first message as
// reply_to_message; that is not a reply (the hub treats it as one).
func repliedTo(m *message) *message {
	r := m.ReplyToMessage
	if r == nil || m.IsTopicMessage && r.MessageID == m.MessageThreadID {
		return nil
	}
	return r
}

// messageContext is the hub's reply and forward context for m: who wrote
// the message it replies to, when, and a preview of it; where a forward
// came from. Empty when m is neither.
func messageContext(m *message) string {
	var lines []string
	if r := repliedTo(m); r != nil {
		id := "?"
		if r.MessageID != 0 {
			id = strconv.FormatInt(r.MessageID, 10)
		}
		lines = append(lines, fmt.Sprintf("[Telegram reply context | replied_msg %s | from %s | date %s]", id, userLabel(r.From), isoDate(r.Date)))
		if p := preview(r); p != "" {
			lines = append(lines, p)
		}
	}
	if o := m.ForwardOrigin; o != nil {
		lines = append(lines, fmt.Sprintf("[Telegram forward context | source %s | date %s]", o.source(), isoDate(o.Date)))
	}
	return strings.Join(lines, "\n")
}

// source names where a forward came from. The hub reads only user and
// channel origins; hidden users and anonymous group admins are named too.
func (o *origin) source() string {
	switch {
	case o.Type == "user" && o.SenderUser != nil:
		return userLabel(o.SenderUser)
	case o.Type == "hidden_user" && o.SenderUserName != "":
		return o.SenderUserName + " (hidden user)"
	case o.Chat != nil:
		return chatLabel(o.Chat)
	case o.SenderChat != nil:
		return chatLabel(o.SenderChat)
	}
	return "unknown"
}

func chatLabel(c *chat) string {
	switch {
	case c.Title != "":
		return c.Title
	case c.Username != "":
		return c.Username
	}
	return strconv.FormatInt(c.ID, 10)
}

// userLabel is "First Last (tg_user_ID)", as the hub writes a sender.
func userLabel(u *user) string {
	if u == nil {
		return "unknown"
	}
	name := strings.TrimSpace(u.FirstName + " " + u.LastName)
	if name == "" {
		name = or(u.Username, "Unknown")
	}
	if u.ID != 0 {
		name += " (tg_user_" + strconv.FormatInt(u.ID, 10) + ")"
	}
	return name
}

// isoDate is a Telegram date as JavaScript's toISOString writes it.
func isoDate(unix int64) string {
	if unix == 0 {
		return "unknown"
	}
	return time.Unix(unix, 0).UTC().Format("2006-01-02T15:04:05.000Z")
}

var spaceRun = regexp.MustCompile(`\s+`)

// preview is a replied message's text or caption and attachment label, on
// one line and at most 1200 characters; native MCP sign-in text is
// omitted, as in the hub.
func preview(m *message) string {
	text := strings.TrimSpace(or(m.Text, m.Caption))
	if mcpSignIn.MatchString(text) {
		text = "[Private native MCP sign-in UI omitted]"
	}
	var parts []string
	if text != "" {
		parts = append(parts, text)
	}
	if a := detect(m); a != nil {
		parts = append(parts, a.Label)
	}
	s := spaceRun.ReplaceAllString(strings.Join(parts, " "), " ")
	if r := []rune(s); len(r) > 1200 {
		s = string(r[:1200])
	}
	return s
}

// mcpSignIn is the hub's isMcpOAuthUiText (chat-control.ts).
var mcpSignIn = regexp.MustCompile(`(?i)https?://[^\s<>]*[?&](?:code|state|error)=|Sign in to MCP server .* in your browser:`)
