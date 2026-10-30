// Package telegram is Heimdall's Telegram interface: it long-polls the Bot
// API for messages, hands each verified one to the router and sends
// replies as plain text. Only the standard library is used.
//
// Identity is the sender's numeric Telegram user ID, which the Bot API
// reports for the message; display names and text are never identity.
// Group behaviour copies the Pi hub (misunderstood-pi-pack ec72c7d,
// extensions/telegram/index.ts isMentioned): a private chat is always
// handled; in a group a message is handled only when it mentions
// @<bot username> or replies to one of the bot's own messages, and every
// other message is ignored without being stored.
//
// The bot token is a secret: it is part of every request URL, so it is
// scrubbed from every error and log line, and it never reaches a harness
// (HEIMDALL_* variables are stripped from harness environments).
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

// TokenEnv is the environment variable the bot token is read from. Its
// HEIMDALL_ prefix keeps it out of every harness environment.
const TokenEnv = "HEIMDALL_TELEGRAM_TOKEN"

const (
	// DefaultAPI is the Bot API base URL.
	DefaultAPI = "https://api.telegram.org"
	// maxText is Telegram's message limit in UTF-16 code units.
	maxText = 4096
)

// Options configure the interface.
type Options struct {
	Token       string        // bot token; never logged
	BotUsername string        // the bot getMe must report, without "@"
	API         string        // Bot API base URL; default DefaultAPI
	Log         *log.Logger   // metadata only, never message text
	PollTimeout time.Duration // getUpdates long-poll timeout; default 50s
}

// Interface is one Telegram bot.
type Interface struct {
	opt     Options
	http    *http.Client
	botID   int64
	botName string // as getMe reported it

	backoffMin, backoffMax time.Duration // getUpdates failures
	maxRetryWait           time.Duration // longest 429 retry_after honoured inside Send
}

// New checks the options. The token is verified against BotUsername by
// Run, before anything is polled.
func New(opt Options) (*Interface, error) {
	if opt.Token == "" {
		return nil, fmt.Errorf("telegram: %s is not set", TokenEnv)
	}
	opt.BotUsername = strings.TrimPrefix(strings.TrimSpace(opt.BotUsername), "@")
	if opt.BotUsername == "" {
		return nil, errors.New("telegram: bot_username is required")
	}
	if opt.API == "" {
		opt.API = DefaultAPI
	}
	opt.API = strings.TrimRight(opt.API, "/")
	if opt.Log == nil {
		opt.Log = log.Default()
	}
	if opt.PollTimeout <= 0 {
		opt.PollTimeout = 50 * time.Second
	}
	return &Interface{opt: opt, http: &http.Client{}, backoffMin: time.Second, backoffMax: time.Minute, maxRetryWait: time.Minute}, nil
}

func (t *Interface) Name() string { return core.PlatformTelegram }

// Bot API types: only the fields Heimdall reads.
type user struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
}

type chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"` // private, group, supergroup, channel
}

type message struct {
	MessageID          int64  `json:"message_id"`
	MessageThreadID    int64  `json:"message_thread_id"`
	IsTopicMessage     bool   `json:"is_topic_message"`
	From               *user  `json:"from"`
	SenderChat         *chat  `json:"sender_chat"`
	IsAutomaticForward bool   `json:"is_automatic_forward"`
	Chat               chat   `json:"chat"`
	Date               int64  `json:"date"`
	Text               string `json:"text"`
	Caption            string `json:"caption"`
	ReplyToMessage     *struct {
		From *user `json:"from"`
	} `json:"reply_to_message"`
}

type update struct {
	UpdateID int64    `json:"update_id"`
	Message  *message `json:"message"`
}

// apiError is an unsuccessful Bot API answer.
type apiError struct {
	method     string
	status     int
	desc       string
	retryAfter int
}

func (e *apiError) Error() string {
	return fmt.Sprintf("telegram %s: HTTP %d: %s", e.method, e.status, e.desc)
}

// call posts one Bot API method and decodes its result. Every error it
// returns has the token scrubbed.
func (t *Interface) call(ctx context.Context, method string, body any, result any, timeout time.Duration) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.opt.API+"/bot"+t.opt.Token+"/"+method, bytes.NewReader(raw))
	if err != nil {
		return t.scrubbed(method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := t.http.Do(req)
	if err != nil {
		return t.scrubbed(method, err)
	}
	defer res.Body.Close()
	var env struct {
		OK          bool            `json:"ok"`
		Result      json.RawMessage `json:"result"`
		Description string          `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 32<<20)).Decode(&env); err != nil {
		if res.StatusCode >= 300 {
			return &apiError{method: method, status: res.StatusCode, desc: http.StatusText(res.StatusCode)}
		}
		return t.scrubbed(method, fmt.Errorf("unreadable response: %w", err))
	}
	if res.StatusCode >= 300 || !env.OK {
		status := res.StatusCode
		if status < 300 {
			status = http.StatusBadGateway // ok:false with a success status: treat as a server fault
		}
		return &apiError{method: method, status: status, desc: t.scrub(env.Description), retryAfter: env.Parameters.RetryAfter}
	}
	if result != nil {
		if err := json.Unmarshal(env.Result, result); err != nil {
			return fmt.Errorf("telegram %s: unreadable result: %v", method, err)
		}
	}
	return nil
}

// scrubbed turns a transport error into one without the token: Go's
// *url.Error text contains the request URL, which contains the token.
func (t *Interface) scrubbed(method string, err error) error {
	return fmt.Errorf("telegram %s: %s", method, t.scrub(err.Error()))
}

func (t *Interface) scrub(s string) string {
	if t.opt.Token == "" {
		return s
	}
	return strings.ReplaceAll(s, t.opt.Token, "<token>")
}

func (t *Interface) logf(format string, args ...any) {
	t.opt.Log.Print(t.scrub(fmt.Sprintf("telegram: "+format, args...)))
}

// Run verifies the token belongs to the configured bot, then long-polls
// getUpdates until ctx ends. An update is acknowledged upstream (the next
// offset moves past it) only after accept returned nil for it, or after it
// was deliberately ignored. Heimdall never calls deleteWebhook or
// setWebhook: a webhook or another poller on the same token shows as a 409
// conflict, which is logged and retried with backoff.
func (t *Interface) Run(ctx context.Context, accept func(context.Context, core.Inbound) error) error {
	var me user
	if err := t.call(ctx, "getMe", struct{}{}, &me, 20*time.Second); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("token check failed, not polling: %w", err)
	}
	if !strings.EqualFold(me.Username, t.opt.BotUsername) {
		return fmt.Errorf("the token belongs to @%s, not the configured @%s; refusing to poll", me.Username, t.opt.BotUsername)
	}
	t.botID, t.botName = me.ID, me.Username
	t.logf("polling as @%s", t.botName)
	var offset int64
	backoff := t.backoffMin
	for ctx.Err() == nil {
		var updates []update
		err := t.call(ctx, "getUpdates", map[string]any{
			"offset":          offset,
			"timeout":         int(t.opt.PollTimeout / time.Second),
			"allowed_updates": []string{"message"},
		}, &updates, t.opt.PollTimeout+10*time.Second)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			var api *apiError
			wait := backoff
			switch {
			case errors.As(err, &api) && (api.status == http.StatusUnauthorized || api.status == http.StatusNotFound):
				return fmt.Errorf("the bot token was rejected: %w", err)
			case errors.As(err, &api) && api.status == http.StatusConflict:
				t.logf("getUpdates conflict: another poller or a webhook uses this token; retry in %s: %v", wait, err)
			case errors.As(err, &api) && api.status == http.StatusTooManyRequests && api.retryAfter > 0:
				wait = time.Duration(api.retryAfter) * time.Second
				t.logf("getUpdates rate limited; retry in %s", wait)
			default:
				t.logf("getUpdates failed; retry in %s: %v", wait, err)
			}
			sleep(ctx, wait)
			backoff = min(backoff*2, t.backoffMax)
			continue
		}
		backoff = t.backoffMin
		for _, u := range updates {
			if u.UpdateID < offset {
				continue
			}
			if in, ok := t.inbound(u); ok {
				if err := accept(ctx, in); err != nil {
					if ctx.Err() != nil {
						return nil
					}
					// Not acknowledged: polled again from this update.
					t.logf("update %d in chat %s not accepted; retry in %s: %v", u.UpdateID, in.Conv.Chat, t.backoffMin, err)
					sleep(ctx, t.backoffMin)
					break
				}
			}
			offset = u.UpdateID + 1
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// inbound turns an update into a message for the router, or reports that
// Heimdall ignores it.
func (t *Interface) inbound(u update) (core.Inbound, bool) {
	m := u.Message
	switch {
	case m == nil:
		return core.Inbound{}, false
	case m.From == nil || m.From.IsBot || m.SenderChat != nil || m.IsAutomaticForward || m.Chat.Type == "channel":
		// Not a person writing as themselves: channel posts, anonymous
		// admins, linked-channel forwards and bots.
		return core.Inbound{}, false
	}
	private := m.Chat.Type == "private"
	text := m.Text
	if text == "" {
		text = m.Caption
	}
	addressed := private || t.mentions(text) || m.ReplyToMessage != nil && m.ReplyToMessage.From != nil && m.ReplyToMessage.From.ID == t.botID
	if !addressed {
		return core.Inbound{}, false // group traffic not for the bot: not stored
	}
	text = t.command(text)
	if strings.TrimSpace(text) == "" {
		t.logf("update %d in chat %d ignored: no text or caption", u.UpdateID, m.Chat.ID)
		return core.Inbound{}, false
	}
	in := core.Inbound{
		Conv:    core.ConvKey{Interface: core.PlatformTelegram, Chat: strconv.FormatInt(m.Chat.ID, 10)},
		MsgID:   strconv.FormatInt(m.MessageID, 10),
		From:    core.Principal{Platform: core.PlatformTelegram, ID: strconv.FormatInt(m.From.ID, 10), Name: strings.TrimSpace(m.From.FirstName + " " + m.From.LastName)},
		Origin:  core.OriginLive,
		Text:    text,
		At:      time.Unix(m.Date, 0),
		Private: private,
	}
	if m.IsTopicMessage && m.MessageThreadID != 0 {
		in.Conv.Thread = strconv.FormatInt(m.MessageThreadID, 10)
	}
	if err := in.Validate(); err != nil {
		t.logf("update %d in chat %d ignored: %v", u.UpdateID, m.Chat.ID, err)
		return core.Inbound{}, false
	}
	return in, true
}

// mentions reports whether text mentions @<bot username>: not merely a
// longer username that starts with it. Telegram usernames are
// case-insensitive.
func (t *Interface) mentions(text string) bool {
	if t.botName == "" {
		return false
	}
	mention := "@" + t.botName
	for i := 0; i+len(mention) <= len(text); i++ {
		if strings.EqualFold(text[i:i+len(mention)], mention) && !usernameByte(text, i+len(mention)) {
			return true
		}
	}
	return false
}

// usernameByte reports whether s[i] continues a Telegram username.
func usernameByte(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	c := s[i]
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

// command removes one leading or trailing @<bot username> (as the hub's
// chat-control.ts does), so "@bot !status" and "!status @bot" are commands,
// and maps /start and /start@<bot> (with or without a deep-link payload) to
// !help. Other text is returned unchanged.
func (t *Interface) command(text string) string {
	s := strings.TrimSpace(text)
	if t.botName != "" {
		mention := "@" + t.botName
		n := len(mention)
		switch {
		case len(s) >= n && strings.EqualFold(s[:n], mention) && !usernameByte(s, n):
			s = strings.TrimSpace(s[n:])
		case len(s) >= n && strings.EqualFold(s[len(s)-n:], mention):
			s = strings.TrimSpace(s[:len(s)-n])
		}
	}
	first, _, _ := strings.Cut(s, " ")
	if strings.EqualFold(first, "/start") || t.botName != "" && strings.EqualFold(first, "/start@"+t.botName) {
		return "!help"
	}
	if s == strings.TrimSpace(text) {
		return text
	}
	return s
}

// Send delivers one reply as plain text, split at Telegram's limit, in the
// chat and topic it belongs to and as a reply to the message it answers.
// A 429 waits out retry_after (bounded) before reporting a transient
// failure; any other 4xx is permanent. Delivery is at-least-once: if a
// later part of a split reply fails, the retry sends every part again.
func (t *Interface) Send(ctx context.Context, out core.Outbound) error {
	var chatID any = out.Conv.Chat
	if id, err := strconv.ParseInt(out.Conv.Chat, 10, 64); err == nil {
		chatID = id
	}
	first := true
	for _, part := range split(out.Text, maxText) {
		if strings.TrimSpace(part) == "" {
			continue
		}
		body := map[string]any{"chat_id": chatID, "text": part}
		if out.Conv.Thread != "" {
			if id, err := strconv.ParseInt(out.Conv.Thread, 10, 64); err == nil {
				body["message_thread_id"] = id
			}
		}
		if first && out.ReplyTo != "" {
			if id, err := strconv.ParseInt(out.ReplyTo, 10, 64); err == nil {
				body["reply_parameters"] = map[string]any{"message_id": id, "allow_sending_without_reply": true}
			}
		}
		if err := t.call(ctx, "sendMessage", body, nil, 30*time.Second); err != nil {
			return t.sendError(ctx, err)
		}
		first = false
	}
	return nil
}

func (t *Interface) sendError(ctx context.Context, err error) error {
	var api *apiError
	if !errors.As(err, &api) {
		return err // network: transient
	}
	switch {
	case api.status == http.StatusTooManyRequests:
		if api.retryAfter > 0 {
			sleep(ctx, min(time.Duration(api.retryAfter)*time.Second, t.maxRetryWait))
		}
		return err
	case api.status >= 400 && api.status < 500:
		return router.PermanentError{Err: err}
	}
	return err
}

// split cuts text into parts of at most max UTF-16 code units, at a line
// break in the second half of a part when there is one.
func split(text string, max int) []string {
	var parts []string
	for units(text) > max {
		n, cut, newline := 0, len(text), -1
		for i, r := range text {
			w := utf16.RuneLen(r)
			if w < 0 {
				w = 1 // invalid UTF-8 is sent as U+FFFD
			}
			if n+w > max {
				cut = i
				break
			}
			n += w
			if r == '\n' {
				newline = i + 1
			}
		}
		if newline > cut/2 {
			cut = newline
		}
		parts = append(parts, text[:cut])
		text = text[cut:]
	}
	if text != "" {
		parts = append(parts, text)
	}
	return parts
}

func units(s string) int {
	n := 0
	for _, r := range s {
		if w := utf16.RuneLen(r); w > 0 {
			n += w
		} else {
			n++
		}
	}
	return n
}
