// Package proto is the wire format between agentlink-server and agentlink-worker.
package proto

import (
	"strings"
	"time"
	"unicode"
)

// Task is one @-mention routed to a worker device.
type Task struct {
	ID        string    `json:"id"`
	RoomID    string    `json:"room_id"`
	RoomName  string    `json:"room_name"`
	UserID    string    `json:"user_id"`
	UserName  string    `json:"user_name"`
	Text      string    `json:"text"` // message text with the leading bot mention removed
	CreatedAt time.Time `json:"created_at"`
}

// Result is the worker's reply to a task, posted to the task's room as markdown.
type Result struct {
	Text string `json:"text"`
}

type RegisterRequest struct {
	Name string `json:"name"`
}

type RegisterResponse struct {
	DeviceID    string `json:"device_id"`
	DeviceToken string `json:"device_token"`
}

// CutWord splits s into its first whitespace-separated word and the trimmed rest.
func CutWord(s string) (word, rest string) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, unicode.IsSpace)
	if i < 0 {
		return s, ""
	}
	return s[:i], strings.TrimSpace(s[i:])
}

// Keyword normalizes a command word: case-insensitive, with an optional leading "/".
func Keyword(word string) string {
	return strings.ToLower(strings.TrimPrefix(word, "/"))
}
