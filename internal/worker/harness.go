package worker

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// legacyHarness is what sessions recorded before harnesses existed ran on.
const legacyHarness = "claude"

// Harness is one way to run a prompt, chosen per room with `harness <name>`.
type Harness struct {
	// Type is "sdk" (the Claude Agent SDK through runner, the default) or "mirasim".
	Type string `json:"type"`

	// sdk: Runner and SDKOptions default to the config's top-level runner and sdk_options.
	Runner     []string        `json:"runner"`
	SDKOptions json.RawMessage `json:"sdk_options"`

	// mirasim: runs the turn in a running mirasim (the desktop app by default) through
	// `mirasim ui-cli`, so it shows up in mirasim's sidebar and on its phone app.
	Command []string `json:"command"` // default ["mirasim"]
	Port    int      `json:"port"`    // default 4970, the desktop app; `ls ~/.mirasim/run/` lists others
	Agent   string   `json:"agent"`   // default "claude"
	Model   string   `json:"model"`
	Effort  string   `json:"effort"`
	// Route is "own" (default: the agent's own sign-in on this machine) or "platform"
	// (mirasim's account, so prompts and files pass through mirasim's servers).
	Route string `json:"route"`
}

func (c *Config) checkHarnesses() error {
	if c.Harnesses == nil {
		c.Harnesses = map[string]Harness{}
	}
	if _, ok := c.Harnesses[legacyHarness]; !ok && len(c.Runner) > 0 {
		c.Harnesses[legacyHarness] = Harness{Type: "sdk"}
	}
	for name, h := range c.Harnesses {
		switch h.Type = cmp.Or(h.Type, "sdk"); h.Type {
		case "sdk":
			if len(h.Runner) == 0 {
				h.Runner = c.Runner
			}
			if h.SDKOptions == nil {
				h.SDKOptions = c.SDKOptions
			}
			if len(h.Runner) == 0 {
				return fmt.Errorf(`harness %q: runner is required, e.g. ["node", "/path/to/agentlink/sdk/runner.mjs"]`, name)
			}
		case "mirasim":
			if len(h.Command) == 0 {
				h.Command = []string{"mirasim"}
			}
			h.Port = cmp.Or(h.Port, 4970)
			h.Agent = cmp.Or(h.Agent, "claude")
			if h.Route = cmp.Or(h.Route, "own"); h.Route != "own" && h.Route != "platform" {
				return fmt.Errorf(`harness %q: route must be "own" or "platform"`, name)
			}
		default:
			return fmt.Errorf(`harness %q: type must be "sdk" or "mirasim"`, name)
		}
		c.Harnesses[name] = h
	}
	if len(c.Harnesses) == 0 {
		return errors.New(`runner is required, e.g. ["node", "/path/to/agentlink/sdk/runner.mjs"], unless harnesses are set`)
	}
	c.Harness = cmp.Or(c.Harness, legacyHarness)
	if _, ok := c.Harnesses[c.Harness]; !ok {
		return fmt.Errorf("harness %q is not in harnesses", c.Harness)
	}
	return nil
}

func (h Harness) runner(timeout time.Duration) Runner {
	if h.Type == "mirasim" {
		return Mirasim{Command: h.Command, Port: h.Port, Agent: h.Agent, Model: h.Model, Effort: h.Effort, Route: h.Route, Timeout: timeout}
	}
	return SDK{Command: h.Runner, Options: h.SDKOptions, Timeout: timeout}
}

// Mirasim runs a prompt with `mirasim ui-cli prompt`, which blocks until the turn settles or
// its own --timeout passes. Session ids are the part of mirasim's session key after
// "<agent>:", so they look like the SDK's and work with `sessions` and id prefixes.
//
// The prompt goes on ui-cli's argv (it reads no stdin), so other users of this machine can
// see it in `ps`. Text starting with "/" may be a mirasim host verb (/branch forks).
type Mirasim struct {
	Command                     []string
	Port                        int
	Agent, Model, Effort, Route string
	Timeout                     time.Duration
}

// mirasimReply is what `ui-cli prompt` prints.
type mirasimReply struct {
	SessionKey   string          `json:"sessionKey"`
	Phase        string          `json:"phase"`
	Text         string          `json:"text"`
	Error        json.RawMessage `json:"error"`
	Settled      bool            `json:"settled"`
	Interactions []struct {
		Kind  string `json:"kind"`
		Title string `json:"title"`
	} `json:"interactions"`
}

func (m Mirasim) Run(ctx context.Context, r Run) (RunResult, error) {
	args := []string{"prompt", "--text", cmp.Or(r.Prompt, "See the attached image."), "--agent", m.Agent, "--cwd", r.Workdir, "--route", m.Route, "--quiet"}
	if r.Resume {
		args = append(args, "--session", m.Agent+":"+r.SessionID)
	}
	if m.Model != "" {
		args = append(args, "--model", m.Model)
	}
	if m.Effort != "" {
		args = append(args, "--effort", m.Effort)
	}
	if len(r.Images) > 0 {
		dir, err := os.MkdirTemp("", "agentlink-images-")
		if err != nil {
			return RunResult{}, err
		}
		defer os.RemoveAll(dir)
		for i, img := range r.Images {
			p := filepath.Join(dir, fmt.Sprintf("image-%d.%s", i+1, strings.TrimPrefix(img.MediaType, "image/")))
			if err := os.WriteFile(p, img.Data, 0o600); err != nil {
				return RunResult{}, err
			}
			args = append(args, "--file", p)
		}
	}
	// ui-cli gives up waiting after --timeout and still prints the session key; the context
	// only kills it if it hangs past that.
	ctx, cancel := context.WithTimeout(ctx, m.Timeout+time.Minute)
	defer cancel()
	start := time.Now()
	stdout, runErr := m.uiCLI(ctx, m.Timeout, args...)
	res := RunResult{Duration: time.Since(start), Stopped: ctx.Err() != nil}

	var out mirasimReply
	if runErr != nil || json.Unmarshal(stdout, &out) != nil || out.SessionKey == "" {
		return res, cmp.Or(runErr, fmt.Errorf("mirasim printed no result: %s", lastBytes(bytes.TrimSpace(stdout), 400)))
	}
	res.SessionID = strings.TrimPrefix(out.SessionKey, m.Agent+":")
	switch {
	case len(out.Interactions) > 0: // left running: someone can answer it in mirasim
		var b strings.Builder
		b.WriteString("Waiting for an answer in mirasim; the turn goes on there once it is answered:")
		for _, it := range out.Interactions {
			fmt.Fprintf(&b, "\n- %s: %s", it.Kind, it.Title)
		}
		res.Text = b.String()
	case !out.Settled:
		res.Stopped = true
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := m.uiCLI(stopCtx, 30*time.Second, "stop", "--session", out.SessionKey); err != nil {
			return res, fmt.Errorf("timed out, and stopping the turn failed: %w", err)
		}
	default:
		res.Text = out.Text
		if e := errorText(out.Error); e != "" {
			res.Text, res.IsError = strings.TrimSpace(out.Text+"\n\n"+e), true
		}
	}
	return res, nil
}

// uiCLI runs one ui-cli command against this harness's mirasim and returns its stdout.
func (m Mirasim) uiCLI(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	argv := append(append([]string{}, m.Command[1:]...), "ui-cli", "--port", strconv.Itoa(m.Port), "--timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	cmd := exec.CommandContext(ctx, m.Command[0], append(argv, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 15 * time.Second
	if err := cmd.Run(); err != nil {
		if tail := bytes.TrimSpace(lastBytes(stderr.Bytes(), 800)); len(tail) > 0 {
			err = fmt.Errorf("%w: %s", err, tail)
		}
		return stdout.Bytes(), err
	}
	return stdout.Bytes(), nil
}

// errorText reads mirasim's error field: null, a string, or an object with a message.
func errorText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var o struct{ Message string }
	if json.Unmarshal(raw, &o) == nil && o.Message != "" {
		return o.Message
	}
	if t := strings.TrimSpace(string(raw)); t != "" && t != "null" {
		return t
	}
	return ""
}
