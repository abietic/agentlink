package worker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agentlink/internal/jsonfile"
)

// Config is the worker's JSON config. AGENTLINK_ENROLL_TOKEN overrides enroll_token.
type Config struct {
	ServerURL   string `json:"server_url"`
	EnrollToken string `json:"enroll_token"` // only needed until the device is registered
	Name        string `json:"name"`         // shown when a room pairs; default hostname
	StateDir    string `json:"state_dir"`    // worker.db and worker.key; default ~/.agentlink
	// Bot is the server's bot this device pairs through; empty means the server's default.
	Bot string `json:"bot"`

	// AllowedUsers are the Tanka user ids that may drive this device. Everyone else is
	// refused, with their id in the reply so it can be added here.
	AllowedUsers []string `json:"allowed_users"`
	// WorkdirRoots bound `cd`: a workdir must be one of these or inside one. Default ~/Code.
	WorkdirRoots   []string `json:"workdir_roots"`
	DefaultWorkdir string   `json:"default_workdir"` // default the first root

	// Runner runs one prompt through the Claude Agent SDK, normally
	// ["node", "<repo>/sdk/runner.mjs"] after `npm ci` in sdk/.
	Runner []string `json:"runner"`
	// SDKOptions go to the SDK's query() as options; cwd and the session are set per run.
	// Default {"permissionMode": "auto"}: a model classifier approves or denies each tool
	// call; a call it cannot decide is denied, since nobody is there to answer a prompt.
	SDKOptions     json.RawMessage `json:"sdk_options"`
	TimeoutSec     int             `json:"timeout_sec"`      // per run; default 1800, keep below the server's lease_sec
	MaxConcurrency int             `json:"max_concurrency"`  // runs at once across rooms; default 2
	MaxOutputChars int             `json:"max_output_chars"` // reply body cap; default 3500
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
	if v := os.Getenv("AGENTLINK_ENROLL_TOKEN"); v != "" {
		c.EnrollToken = v
	}
	if c.ServerURL == "" {
		return c, errors.New("server_url is required")
	}
	if len(c.Runner) == 0 {
		return c, errors.New(`runner is required, e.g. ["node", "/path/to/agentlink/sdk/runner.mjs"]`)
	}
	return c, c.applyDefaults()
}

func (c *Config) applyDefaults() error {
	if c.Name == "" {
		c.Name, _ = os.Hostname()
	}
	if c.StateDir == "" {
		c.StateDir = "~/.agentlink"
	}
	if len(c.WorkdirRoots) == 0 {
		c.WorkdirRoots = []string{"~/Code"}
	}
	if c.DefaultWorkdir == "" {
		c.DefaultWorkdir = c.WorkdirRoots[0]
	}
	c.StateDir = expandHome(c.StateDir)
	c.DefaultWorkdir = expandHome(c.DefaultWorkdir)
	for i, r := range c.WorkdirRoots {
		c.WorkdirRoots[i] = expandHome(r)
	}
	dir, err := resolveWorkdir(c.WorkdirRoots, "/", c.DefaultWorkdir)
	if err != nil {
		return errors.New("default_workdir: " + err.Error())
	}
	c.DefaultWorkdir = dir
	if c.SDKOptions == nil {
		c.SDKOptions = json.RawMessage(`{"permissionMode":"auto"}`)
	}
	if c.TimeoutSec <= 0 {
		c.TimeoutSec = 1800
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 2
	}
	if c.MaxOutputChars <= 0 {
		c.MaxOutputChars = 3500
	}
	return nil
}

func (c Config) timeout() time.Duration { return time.Duration(c.TimeoutSec) * time.Second }

func expandHome(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[1:])
	}
	return p
}

// tilde shortens a path under the home directory for display.
func tilde(p string) string {
	home, _ := os.UserHomeDir()
	if home != "" && (p == home || strings.HasPrefix(p, home+string(filepath.Separator))) {
		return "~" + p[len(home):]
	}
	return p
}

// resolveWorkdir turns a `cd` argument into an existing directory inside one of the roots.
// Symlinks are resolved first, so a link cannot lead out of the roots.
func resolveWorkdir(roots []string, cur, arg string) (string, error) {
	p := expandHome(strings.TrimSpace(arg))
	if !filepath.IsAbs(p) {
		p = filepath.Join(cur, p)
	}
	p, err := filepath.EvalSymlinks(filepath.Clean(p))
	if err != nil {
		return "", errors.New("no such directory: " + arg)
	}
	if fi, err := os.Stat(p); err != nil || !fi.IsDir() {
		return "", errors.New("not a directory: " + arg)
	}
	for _, r := range roots {
		r, err := filepath.EvalSymlinks(r)
		if err != nil {
			continue
		}
		if p == r || strings.HasPrefix(p, r+string(filepath.Separator)) {
			return p, nil
		}
	}
	return "", errors.New(arg + " is outside the allowed workdir roots")
}
