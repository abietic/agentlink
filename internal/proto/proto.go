// Package proto is the wire format between agentlink-server and agentlink-worker.
package proto

import (
	"strings"
	"time"
	"unicode"
)

// Task is one @-mention routed to a worker device. It travels sealed (see Envelope).
type Task struct {
	ID        string    `json:"id"`
	RoomID    string    `json:"room_id"`
	RoomName  string    `json:"room_name"`
	UserID    string    `json:"user_id"`
	UserName  string    `json:"user_name"`
	Text      string    `json:"text"` // message text with the leading bot mention removed
	CreatedAt time.Time `json:"created_at"`
	// Trigger names the source of a task nobody typed: a worker trigger, or a server hook
	// such as "sentry". It runs in a new session, which becomes the room's current one.
	Trigger string `json:"trigger,omitempty"`
	// Key says what a triggered task is about; the worker skips one whose key it already
	// ran within the trigger's dedupe window, so a hook and a poller can watch one source.
	Key string `json:"key,omitempty"`
}

// Result is the worker's reply to a task, posted to the task's room as markdown.
// An empty Text posts nothing.
type Result struct {
	Text string `json:"text"`
}

// TriggerRequest asks the server to queue a triggered task for the device paired with
// RoomID. Sealed holds a TriggerBody under TriggerContext.
type TriggerRequest struct {
	RoomID string `json:"room_id"`
	Sealed []byte `json:"sealed"`
}

type TriggerBody struct {
	Trigger string `json:"trigger"`
	Key     string `json:"key"`
	Text    string `json:"text"`
}

// Envelope carries a Task (server to worker) or a Result (worker to server) sealed
// between the server's and the device's X25519 keys, under TaskContext / ResultContext.
type Envelope struct {
	ID     string `json:"id"`
	Sealed []byte `json:"sealed"`
}

func TaskContext(deviceID, taskID string) string    { return "task " + deviceID + " " + taskID }
func ResultContext(deviceID, taskID string) string  { return "result " + deviceID + " " + taskID }
func TriggerContext(deviceID, roomID string) string { return "trigger " + deviceID + " " + roomID }

type RegisterRequest struct {
	Name      string `json:"name"`
	Bot       string `json:"bot,omitempty"` // empty: the server's default bot
	PublicKey []byte `json:"public_key"`    // the device's X25519 key
}

type RegisterResponse struct {
	DeviceID    string `json:"device_id"`
	DeviceToken string `json:"device_token"`
	Bot         string `json:"bot"`
	ServerKey   []byte `json:"server_key"` // the server's X25519 key; the worker pins it
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
