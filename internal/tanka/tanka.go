// Package tanka holds the parts of the Tanka Open Platform this project uses:
// request signing, the app_mention event, and bot replies.
package tanka

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	HeaderSignature = "X-Tanka-Signature"
	HeaderTimestamp = "X-Tanka-Request-Timestamp"
)

var ErrBadSignature = errors.New("tanka: bad signature")

// Sign returns the X-Tanka-Signature value: "v0=" + hex(HMAC-SHA256(secret, "v0:"+ts+":"+body)).
func Sign(secret []byte, ts string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("v0:" + ts + ":"))
	m.Write(body)
	return "v0=" + hex.EncodeToString(m.Sum(nil))
}

// Verify checks a request signature and rejects timestamps more than maxSkew from now,
// which bounds how long a captured request can be replayed.
func Verify(secret []byte, ts, sig string, body []byte, now time.Time, maxSkew time.Duration) error {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: missing or malformed timestamp", ErrBadSignature)
	}
	if d := now.Sub(time.Unix(sec, 0)); d > maxSkew || d < -maxSkew {
		return fmt.Errorf("%w: timestamp outside the %s window", ErrBadSignature, maxSkew)
	}
	if !hmac.Equal([]byte(Sign(secret, ts, body)), []byte(sig)) {
		return ErrBadSignature
	}
	return nil
}

// Envelope is the outer body of every request Tanka posts to the Request URL.
type Envelope struct {
	Type      string        `json:"type"`
	Challenge string        `json:"challenge,omitempty"`
	TeamID    string        `json:"team_id,omitempty"`
	APIAppID  string        `json:"api_app_id,omitempty"`
	EventID   string        `json:"event_id,omitempty"`
	EventTime int64         `json:"event_time,omitempty"`
	Event     *MentionEvent `json:"event,omitempty"`
}

// MentionEvent is the app_mention event: the bot was @-mentioned in a group.
type MentionEvent struct {
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	UserName    string `json:"user_name"`
	MessageID   string `json:"message_id"`
	Text        string `json:"text"`
	RoomID      string `json:"room_id"`
	RoomName    string `json:"room_name"`
	ResponseURL string `json:"response_url"`
	// SenderType is not in Tanka's docs; it arrived on 2026-11-05.
	SenderType string `json:"sender_type"`
	// Attachments has one item per file or image sent with the mention (or with the batch it
	// was sent in), [] for text only. Each url is a download link that needs no login.
	Attachments []Attachment `json:"attachments"`
	// Message is the mention's own message snapshot, sent since late 2026-11; older events
	// have none. It never holds the text of a message it quotes.
	Message *MessageSnapshot `json:"message"`
}

// MessageSnapshot is the part of Tanka's public message structure this project uses.
type MessageSnapshot struct {
	// ReplyMessageID is the id of the message this one quotes; Tanka leaves it out otherwise.
	ReplyMessageID string `json:"reply_message_id"`
}

// ReplyMessageID is the id of the message the mention quotes, or "" for one that quotes
// nothing or comes from before Tanka sent event.message.
func (e MentionEvent) ReplyMessageID() string {
	if e.Message == nil {
		return ""
	}
	return e.Message.ReplyMessageID
}

// Attachment is the part of an attachments item this project uses. Tanka also sends
// file_id, file_type, size, width, height, message_id and msg_type.
type Attachment struct {
	URL  string `json:"url"`
	Name string `json:"name"`
}

// Message is a BotMessagePayload, accepted by response_url and incoming webhooks.
type Message struct {
	Text     string `json:"text"`
	TextType string `json:"text_type,omitempty"`
	Theme    string `json:"theme,omitempty"` // one of Themes; Tanka ignores other values
}

// Themes are the message background colors Tanka accepts: idea is yellow, important red,
// celebration green and announcement blue. No theme is a plain white message.
var Themes = []string{"idea", "important", "celebration", "announcement"}

// Post sends m to a response_url or an incoming-webhook URL. Both URLs carry a secret
// token, so errors never include the URL.
func Post(ctx context.Context, c *http.Client, target string, m Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return errInvalidURL
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("tanka: post: %w", err)
	}
	defer resp.Body.Close()
	var r struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = json.Unmarshal(raw, &r)
	if resp.StatusCode != http.StatusOK || r.Code != 0 {
		return &HTTPError{Status: resp.StatusCode, Code: r.Code, Msg: r.Msg}
	}
	return nil
}

var errInvalidURL = errors.New("tanka: invalid reply URL")

// HTTPError is Tanka answering a post with anything but HTTP 200 and code 0.
type HTTPError struct {
	Status, Code int
	Msg          string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("tanka: post: HTTP %d code %d %s", e.Status, e.Code, e.Msg)
}

// Retryable reports whether a failed Post may go through if sent again: Tanka answered 429
// or 5xx, or the request got no answer at all. A refusal (another status, or a non-zero code
// on HTTP 200) and a bad URL are final.
func Retryable(err error) bool {
	if err == nil || errors.Is(err, errInvalidURL) {
		return false
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusTooManyRequests || he.Status >= 500
	}
	return true
}

// Truncate cuts s to at most n runes, marking the cut.
func Truncate(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	const mark = "\n\n…(truncated)"
	r := []rune(s)
	keep := n - utf8.RuneCountInString(mark)
	if keep < 0 {
		keep = 0
	}
	return string(r[:keep]) + mark
}
