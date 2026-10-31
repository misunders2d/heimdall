package slack

// Socket Mode contract: https://docs.slack.dev/apis/events-api/using-socket-mode/
// (checked 2026-10-31). Hub baseline: ec72c7d extensions/slack.ts:1082-1087.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type socketEnvelope struct {
	Type         string          `json:"type"`
	ID           string          `json:"envelope_id"`
	Reason       string          `json:"reason"`
	Payload      json.RawMessage `json:"payload"`
	RetryAttempt int             `json:"retry_attempt"`
	RetryReason  string          `json:"retry_reason"`
}

var errLinkDisabled = errors.New("slack: Socket Mode link disabled")

// socketTransport owns only delivery/acknowledgement. persist must commit
// the event before returning nil. History fetches/downloads/agent routing
// belong after this durable boundary, outside the receiver callback.
type socketTransport struct {
	log func(string)

	acknowledged    func(json.RawMessage) error
	app             *apiClient
	allowTestSocket bool // injected only by package tests, never configuration
}

func (s *socketTransport) connect(ctx context.Context) (*websocket.Conn, error) {
	var response struct {
		URL string `json:"url"`
	}
	if err := s.app.call(ctx, "apps.connections.open", map[string]any{}, &response); err != nil {
		return nil, err
	}
	u, err := url.Parse(response.URL)
	if err != nil || u.User != nil || u.Host == "" {
		return nil, errors.New("slack: invalid Socket Mode URL")
	}
	if !s.allowTestSocket && (u.Scheme != "wss" || !strings.HasSuffix(u.Hostname(), ".slack.com")) {
		return nil, errors.New("slack: untrusted Socket Mode URL")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{HTTPClient: s.app.http})
	if err != nil {
		return nil, errors.New("slack: Socket Mode connection failed")
	}
	conn.SetReadLimit(8 << 20)
	return conn, nil
}

// run refreshes by opening the replacement before closing the old socket.
// Unacknowledged envelopes on a closing connection are redelivered by Slack;
// their stable event IDs must be deduped by the durable inbox.
func (s *socketTransport) run(ctx context.Context, persist func(context.Context, json.RawMessage) error) error {
	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			_ = conn.CloseNow()
		}
	}()
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		if conn == nil {
			var err error
			conn, err = s.connect(ctx)
			if err != nil {
				var ae *apiError
				if errors.As(err, &ae) && !ae.Ambiguous {
					return err
				}
				if err = s.app.pause(ctx, backoff); err != nil {
					return ctx.Err()
				}
				backoff = min(backoff*2, 10*time.Second)
				continue
			}
		}
		_, raw, err := conn.Read(ctx)
		if err != nil {
			_ = conn.CloseNow()
			conn = nil
			if err = s.app.pause(ctx, backoff); err != nil {
				return ctx.Err()
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		var envelope socketEnvelope
		if json.Unmarshal(raw, &envelope) != nil {
			continue
		}
		switch envelope.Type {
		case "hello":
			backoff = 200 * time.Millisecond
		case "disconnect":
			if envelope.Reason == "link_disabled" {
				return errLinkDisabled
			}
			replacement, err := s.connect(ctx)
			if err != nil {
				continue
			} // keep old socket alive until Slack closes it
			_ = conn.CloseNow()
			conn = replacement
		case "events_api":
			if envelope.RetryAttempt > 0 && s.log != nil {
				s.log(fmt.Sprintf("slack: event redelivery attempt %d", envelope.RetryAttempt))
			}
			if envelope.ID == "" || len(envelope.Payload) == 0 {
				continue
			}
			if err = persist(ctx, envelope.Payload); err != nil {
				return errors.New("slack: event persistence failed; envelope not acknowledged")
			}
			ack, _ := json.Marshal(map[string]string{"envelope_id": envelope.ID})
			ackCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = conn.Write(ackCtx, websocket.MessageText, ack)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				conn = nil
			}
			// A failed write has unknown acknowledgement status. The event is
			// durable and must still be processed; redelivery is deduped.
			if s.acknowledged != nil {
				if err = s.acknowledged(envelope.Payload); err != nil {
					return err
				}
			}
		}
	}
	return ctx.Err()
}
