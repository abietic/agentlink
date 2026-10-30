package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"
	"time"

	"agentlink/internal/jsonfile"
)

// Bot is one Tanka app the server answers for. Its events arrive at POST /tanka/events/{name}
// (POST /tanka/events is the default bot).
type Bot struct {
	Name string `json:"name"` // lowercase letters, digits and "-"
	// AppID, when set, must match the event's api_app_id.
	AppID string `json:"app_id"`
	// SigningSecret is the app's Signing Secret; AGENTLINK_SIGNING_SECRET_<NAME> overrides it
	// (name upper-cased, "-" as "_").
	SigningSecret string `json:"signing_secret"`
	// Mentions are the bot's display names, so "@Name with spaces" is stripped whole.
	Mentions []string `json:"mentions"`
	// RoomWebhooks maps room_id to an incoming-webhook URL, used when response_url is
	// missing, expired or already used, and for triggered tasks, which have none. Webhook
	// URLs are credentials: AGENTLINK_ROOM_WEBHOOKS_<NAME>, a JSON object, adds to them.
	RoomWebhooks map[string]string `json:"room_webhooks"`
	// Rooms binds room_id to device_id at startup, as `pair` from that room would, so a
	// room can be bound without posting in it.
	Rooms map[string]string `json:"rooms"`
	// Sentry, when set, takes Sentry integration webhooks at POST /sentry/{name}.
	Sentry *SentryHook `json:"sentry"`
}

// Config is the server's JSON config. AGENTLINK_ENROLL_TOKEN overrides enroll_token.
type Config struct {
	Listen      string `json:"listen"`       // default "127.0.0.1:8080"
	DBPath      string `json:"db_path"`      // default "agentlink-server.db"
	KeyPath     string `json:"key_path"`     // storage key, created on first start; default db_path + ".key"
	EnrollToken string `json:"enroll_token"` // shared secret a worker needs to register

	Bots       []Bot  `json:"bots"`
	DefaultBot string `json:"default_bot"` // default the first bot

	// AllowedUsers limits who may use any bot at all (Tanka user ids). Empty means anyone
	// in a room a bot is in; each worker still checks its own allowed_users.
	AllowedUsers []string `json:"allowed_users"`
	// ResponseURLPrefix is the only prefix a response_url may have; default "https://api.tanka.ai/".
	ResponseURLPrefix string `json:"response_url_prefix"`

	PollWaitSec   int `json:"poll_wait_sec"`   // long-poll hold time; default 25
	LeaseSec      int `json:"lease_sec"`       // how long a worker may hold a task; default 2400, keep above the worker's timeout_sec
	QueueTTLSec   int `json:"queue_ttl_sec"`   // a task nobody picked up is dropped after this; default 600
	MaxReplyChars int `json:"max_reply_chars"` // default 8000
}

var botName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

func LoadConfig(path string) (Config, error) {
	var c Config
	found, err := jsonfile.Load(path, &c)
	if err != nil {
		return c, err
	}
	if !found {
		return c, errors.New("config file not found: " + path)
	}
	if v := os.Getenv("AGENTLINK_ENROLL_TOKEN"); v != "" {
		c.EnrollToken = v
	}
	for i := range c.Bots {
		b := &c.Bots[i]
		if v := os.Getenv(botEnv("AGENTLINK_SIGNING_SECRET_", b.Name)); v != "" {
			b.SigningSecret = v
		}
		if v := os.Getenv(botEnv("AGENTLINK_SENTRY_SECRET_", b.Name)); v != "" && b.Sentry != nil {
			b.Sentry.ClientSecret = v
		}
		if v := os.Getenv(botEnv("AGENTLINK_ROOM_WEBHOOKS_", b.Name)); v != "" {
			var hooks map[string]string
			if err := json.Unmarshal([]byte(v), &hooks); err != nil {
				return c, fmt.Errorf("%s: %w", botEnv("AGENTLINK_ROOM_WEBHOOKS_", b.Name), err)
			}
			if b.RoomWebhooks == nil {
				b.RoomWebhooks = map[string]string{}
			}
			maps.Copy(b.RoomWebhooks, hooks)
		}
	}
	return c, c.check()
}

// botEnv names a per-bot environment variable: prefix + the bot name upper-cased, "-" as "_".
func botEnv(prefix, bot string) string {
	return prefix + strings.ToUpper(strings.ReplaceAll(bot, "-", "_"))
}

func (c *Config) check() error {
	c.applyDefaults()
	if c.EnrollToken == "" {
		return errors.New("enroll_token is required")
	}
	if len(c.Bots) == 0 {
		return errors.New("at least one bot is required")
	}
	seen := map[string]bool{}
	for _, b := range c.Bots {
		switch {
		case !botName.MatchString(b.Name):
			return fmt.Errorf("bot name %q: use lowercase letters, digits and -", b.Name)
		case seen[b.Name]:
			return fmt.Errorf("bot %q is listed twice", b.Name)
		case b.SigningSecret == "":
			return fmt.Errorf("bot %q: signing_secret (or %s) is required", b.Name, botEnv("AGENTLINK_SIGNING_SECRET_", b.Name))
		}
		for room, dev := range b.Rooms {
			if room == "" || dev == "" {
				return fmt.Errorf("bot %q: rooms needs a room id and a device id in each entry", b.Name)
			}
		}
		if h := b.Sentry; h != nil {
			switch {
			case h.ClientSecret == "":
				return fmt.Errorf("bot %q: sentry.client_secret (or %s) is required", b.Name, botEnv("AGENTLINK_SENTRY_SECRET_", b.Name))
			case h.RoomID == "" || h.Prompt == "":
				return fmt.Errorf("bot %q: sentry needs room_id and prompt", b.Name)
			}
			if h.Trigger == "" {
				h.Trigger = "sentry"
			}
		}
		seen[b.Name] = true
	}
	if !seen[c.DefaultBot] {
		return fmt.Errorf("default_bot %q is not in bots", c.DefaultBot)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	if c.DBPath == "" {
		c.DBPath = "agentlink-server.db"
	}
	if c.KeyPath == "" {
		c.KeyPath = c.DBPath + ".key"
	}
	if c.DefaultBot == "" && len(c.Bots) > 0 {
		c.DefaultBot = c.Bots[0].Name
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
	if c.QueueTTLSec <= 0 {
		c.QueueTTLSec = 600
	}
	if c.MaxReplyChars <= 0 {
		c.MaxReplyChars = 8000
	}
}

func (c Config) PollWait() time.Duration { return time.Duration(c.PollWaitSec) * time.Second }
func (c Config) lease() time.Duration    { return time.Duration(c.LeaseSec) * time.Second }
func (c Config) queueTTL() time.Duration { return time.Duration(c.QueueTTLSec) * time.Second }
