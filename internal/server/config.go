package server

import (
	"errors"
	"os"
	"strings"
	"time"

	"agentlink/internal/jsonfile"
)

// Config is the server's JSON config. The two secrets can come from the environment
// (AGENTLINK_SIGNING_SECRET, AGENTLINK_ENROLL_TOKEN) instead of the file.
type Config struct {
	Listen        string `json:"listen"`         // default ":8080"
	StatePath     string `json:"state_path"`     // devices and room bindings; default "agentlink-server.state.json"
	SigningSecret string `json:"signing_secret"` // the Tanka app's Signing Secret
	EnrollToken   string `json:"enroll_token"`   // shared secret a worker needs to register

	// AllowedUsers limits who may use the bot at all (Tanka user ids). Empty means anyone
	// in a room the bot is in; each worker still checks its own allowed_users.
	AllowedUsers []string `json:"allowed_users"`
	// BotNames are the bot's display names, so "@Name with spaces" is stripped whole.
	BotNames []string `json:"bot_names"`
	// RoomWebhooks maps room_id to an incoming-webhook URL, used when response_url is
	// missing, expired or already used.
	RoomWebhooks map[string]string `json:"room_webhooks"`
	// ResponseURLPrefix is the only prefix a response_url may have; default "https://api.tanka.ai/".
	ResponseURLPrefix string `json:"response_url_prefix"`

	PollWaitSec   int `json:"poll_wait_sec"`   // long-poll hold time; default 25
	LeaseSec      int `json:"lease_sec"`       // how long a worker may hold a task; default 2400, keep above the worker's timeout_sec
	MaxReplyChars int `json:"max_reply_chars"` // default 8000
}

func LoadConfig(path string) (Config, error) {
	var c Config
	found, err := jsonfile.Load(path, &c)
	if err != nil {
		return c, err
	}
	if !found {
		return c, errors.New("config file not found: " + path)
	}
	if v := os.Getenv("AGENTLINK_SIGNING_SECRET"); v != "" {
		c.SigningSecret = v
	}
	if v := os.Getenv("AGENTLINK_ENROLL_TOKEN"); v != "" {
		c.EnrollToken = v
	}
	c.applyDefaults()
	if c.SigningSecret == "" || c.EnrollToken == "" {
		return c, errors.New("signing_secret and enroll_token are required")
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.StatePath == "" {
		c.StatePath = "agentlink-server.state.json"
	}
	if c.ResponseURLPrefix == "" {
		c.ResponseURLPrefix = "https://api.tanka.ai/"
	}
	if !strings.HasSuffix(c.ResponseURLPrefix, "/") {
		c.ResponseURLPrefix += "/" // so "https://api.tanka.ai" cannot match "https://api.tanka.ai.evil.example"
	}
	if c.PollWaitSec <= 0 {
		c.PollWaitSec = 25
	}
	if c.LeaseSec <= 0 {
		c.LeaseSec = 2400
	}
	if c.MaxReplyChars <= 0 {
		c.MaxReplyChars = 8000
	}
}

func (c Config) PollWait() time.Duration { return time.Duration(c.PollWaitSec) * time.Second }
func (c Config) lease() time.Duration    { return time.Duration(c.LeaseSec) * time.Second }
