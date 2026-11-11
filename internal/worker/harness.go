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
	"slices"
	"strconv"
	"strings"
	"time"
)

// legacyHarness is what sessions recorded before harnesses existed ran on.
const legacyHarness = "claude"

// Harness is one way to run a prompt, chosen per room with `harness <name>`.
type Harness struct {
	// Type is "sdk" (the Claude Agent SDK through runner, the default), "mirasim" or "kimi".
	Type string `json:"type"`

	// Fallback names the harnesses that take a prompt, in order, while this one is at a
	// usage limit or cannot start. A fallback starts a new session: one harness cannot
	// continue another's.
	Fallback []string `json:"fallback"`

	// sdk: Runner and SDKOptions default to the config's top-level runner and sdk_options.
	Runner     []string        `json:"runner"`
	SDKOptions json.RawMessage `json:"sdk_options"`

	// mirasim: runs the turn in a running mirasim (the desktop app by default) through
	// `mirasim ui-cli`, so it shows up in mirasim's sidebar and on its phone app.
	Command []string `json:"command"` // default ["mirasim"]; kimi: ["kimi"]
	Port    int      `json:"port"`    // default 4970, the desktop app; `ls ~/.mirasim/run/` lists others
	Agent   string   `json:"agent"`   // default "claude"
	Model   string   `json:"model"`   // mirasim and kimi: empty is their own default
	Effort  string   `json:"effort"`
	// Route is "own" (default: the agent's own sign-in on this machine) or "platform"
	// (mirasim's account, so prompts and files pass through mirasim's servers).
	Route string `json:"route"`
	// Members maps Tanka user ids to people of this mirasim space (their "usr_…" id), so a
	// mapped sender's turn is theirs: it carries their name, and only their phone is told
	// when it finishes or waits on an answer. mirasim decides which models and credit they
	// may use; this machine's own sign-ins serve only its owner and admins. Everyone else,
	// and triggers, run as the owner. While members are set, every session is shared with
	// the space, since otherwise only a session's creator may send in it.
	Members map[string]string `json:"members"`
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
		case "kimi":
			if len(h.Command) == 0 {
				h.Command = []string{"kimi"}
			}
		default:
			return fmt.Errorf(`harness %q: type must be "sdk", "mirasim" or "kimi"`, name)
		}
		c.Harnesses[name] = h
	}
	for name, h := range c.Harnesses {
		for _, f := range h.Fallback {
			if _, ok := c.Harnesses[f]; !ok || f == name {
				return fmt.Errorf("harness %q: fallback %q is not another harness", name, f)
			}
		}
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

func (h Harness) runner(timeout, stall time.Duration) Runner {
	switch h.Type {
	case "mirasim":
		return Mirasim{Command: h.Command, Port: h.Port, Agent: h.Agent, Model: h.Model, Effort: h.Effort, Route: h.Route, Members: h.Members, Timeout: timeout}
	case "kimi":
		return Kimi{Command: h.Command, Model: h.Model, Timeout: timeout}
	}
	return SDK{Command: h.Runner, Options: h.SDKOptions, Timeout: timeout, Stall: stall}
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
	Members                     map[string]string
	Timeout                     time.Duration
}

// mirasimReply is what `ui-cli prompt` prints.
type mirasimReply struct {
	SessionKey   string               `json:"sessionKey"`
	Phase        string               `json:"phase"`
	Text         string               `json:"text"`
	Error        json.RawMessage      `json:"error"`
	Settled      bool                 `json:"settled"`
	Interactions []mirasimInteraction `json:"interactions"`
}

// mirasimInteraction is a permission request or question a turn waits on. ui-cli prompt
// prints only its kind and title; the session snapshot also carries the questions.
type mirasimInteraction struct {
	Kind      string `json:"kind"`
	Title     string `json:"title"`
	Questions []struct {
		Text    string `json:"text"`
		Choices []struct {
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"choices"`
	} `json:"questions"`
}

func (m Mirasim) Run(ctx context.Context, r Run) (RunResult, error) {
	by, note := m.person(r.UserID)
	var args []string
	if by != "" {
		args = append(args, "--by", by)
	}
	args = append(args, "prompt", "--text", cmp.Or(r.Prompt, "See the attached image."), "--agent", m.Agent, "--cwd", r.Workdir, "--route", m.Route, "--quiet")
	var shareErr error
	if r.Resume {
		args = append(args, "--session", m.Agent+":"+r.SessionID)
		if len(m.Members) > 0 { // it may predate members, or have been started in mirasim
			shareErr = m.share(m.Agent + ":" + r.SessionID)
		}
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
	if len(m.Members) > 0 && !r.Resume {
		shareErr = m.share(out.SessionKey)
	}
	if shareErr != nil {
		note += fmt.Sprintf("\n\n⚠️ This session is not shared with the mirasim space (%v), so only whoever started it can send in it.", shareErr)
	}
	switch {
	case len(out.Interactions) > 0: // left running: someone can answer it in mirasim
		items := m.pending(out.SessionKey)
		if len(items) == 0 {
			items = out.Interactions
		}
		res.Text = waitingText(items)
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
	res.Text += note
	return res, nil
}

// person returns who a sender's turn runs as: the member they map to, or "" for the machine's
// owner. mirasim runs a turn named for anyone who is not a person of the space as the owner
// without saying so, so a mapping to no member comes back with a note instead.
func (m Mirasim) person(userID string) (by, note string) {
	uid := m.Members[userID]
	if uid == "" {
		return "", ""
	}
	if people, err := m.people(); err == nil && !slices.Contains(people, uid) {
		return "", fmt.Sprintf("\n\n⚠️ `%s` is not a person of this mirasim space, so this turn ran as the machine's owner.", uid)
	}
	return uid, "" // if the list cannot be read, mirasim still decides
}

// people lists the user ids of the space's human members, as mirasim looks them up for --by.
func (m Mirasim) people() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stdout, err := m.uiCLI(ctx, 15*time.Second, "call", "chatMembers", "--max-frames", "1")
	var out struct {
		Frames []struct {
			People []struct {
				UID  string `json:"uid"`
				Kind string `json:"kind"`
			} `json:"people"`
		} `json:"frames"`
	}
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(stdout, &out) != nil || len(out.Frames) == 0 {
		return nil, errors.New("mirasim listed no members")
	}
	var uids []string
	for _, p := range out.Frames[0].People {
		if p.Kind == "human" {
			uids = append(uids, p.UID)
		}
	}
	return uids, nil
}

// share opens a session to every member of the space; the machine's owner may do that for
// anyone's session.
func (m Mirasim) share(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args, _ := json.Marshal(map[string]any{"sessionKey": key, "shared": true})
	stdout, err := m.uiCLI(ctx, 15*time.Second, "call", "setSessionAccess", "--args", string(args), "--max-frames", "1")
	var out struct {
		Frames []struct {
			OK    bool            `json:"ok"`
			Error json.RawMessage `json:"error"`
		} `json:"frames"`
	}
	switch {
	case err != nil:
		return err
	case json.Unmarshal(stdout, &out) != nil || len(out.Frames) == 0:
		return errors.New("mirasim did not answer")
	case !out.Frames[0].OK:
		return errors.New(cmp.Or(errorText(out.Frames[0].Error), "refused"))
	}
	return nil
}

// pending reads the session's open interactions with their questions from its snapshot, or
// nil if that fails.
func (m Mirasim) pending(key string) []mirasimInteraction {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args, _ := json.Marshal(map[string]string{"sessionKey": key})
	stdout, err := m.uiCLI(ctx, 15*time.Second, "call", "subscribe", "--args", string(args), "--max-frames", "1")
	var out struct {
		Frames []struct {
			Snapshot struct {
				Interactions []mirasimInteraction `json:"interactions"`
			} `json:"snapshot"`
		} `json:"frames"`
	}
	if err != nil || json.Unmarshal(stdout, &out) != nil || len(out.Frames) == 0 {
		return nil
	}
	return out.Frames[0].Snapshot.Interactions
}

func waitingText(items []mirasimInteraction) string {
	var b strings.Builder
	b.WriteString("Waiting for an answer in mirasim; the turn goes on there once it is answered:")
	for _, it := range items {
		if len(it.Questions) == 0 {
			fmt.Fprintf(&b, "\n- %s: %s", it.Kind, it.Title)
		}
		for _, q := range it.Questions {
			fmt.Fprintf(&b, "\n- %s: %s", it.Title, q.Text)
			for _, c := range q.Choices {
				fmt.Fprintf(&b, "\n  - %s", c.Label)
				if c.Description != "" {
					fmt.Fprintf(&b, " (%s)", c.Description)
				}
			}
		}
	}
	return b.String()
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
