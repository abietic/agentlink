package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Kimi runs a prompt with Kimi Code's print mode, `kimi -p <prompt> --output-format
// stream-json`. Print mode asks nobody: tools run under kimi's auto permission policy, and
// the static deny rules in its config still apply. The prompt goes on argv (print mode reads
// no stdin), so other users of this machine can see it in `ps`.
//
// Kimi names sessions "session_<uuid>"; the worker keeps the uuid, so ids look like the
// other harnesses' and work with `sessions` and id prefixes. A new session takes kimi's id,
// not the one the worker chose.
type Kimi struct {
	Command []string
	Model   string
	Timeout time.Duration
}

// kimiLine is one line of stream-json output.
type kimiLine struct {
	Role      string          `json:"role"`
	Type      string          `json:"type"`
	Content   json.RawMessage `json:"content"`
	SessionID string          `json:"session_id"`
}

const kimiSessionPrefix = "session_"

func (k Kimi) Run(ctx context.Context, r Run) (RunResult, error) {
	ctx, cancel := context.WithTimeout(ctx, k.Timeout)
	defer cancel()
	prompt := r.Prompt
	args := slices.Clone(k.Command[1:])
	if len(r.Images) > 0 {
		dir, err := os.MkdirTemp("", "agentlink-images-")
		if err != nil {
			return RunResult{}, err
		}
		defer os.RemoveAll(dir)
		var paths []string
		for i, img := range r.Images {
			p := filepath.Join(dir, fmt.Sprintf("image-%d%s", i+1, imageExt(img.MediaType)))
			if err := os.WriteFile(p, img.Data, 0o600); err != nil {
				return RunResult{}, err
			}
			paths = append(paths, p)
		}
		prompt = fmt.Sprintf("The message came with %d image(s); read them with ReadMediaFile: %s\n\n%s", len(paths), strings.Join(paths, ", "), prompt)
		args = append(args, "--add-dir", dir)
	}
	if r.Resume {
		args = append(args, "-S", kimiSessionPrefix+r.SessionID)
	}
	if k.Model != "" {
		args = append(args, "-m", k.Model)
	}
	args = append(args, "--output-format", "stream-json", "-p", prompt)

	cmd := exec.CommandContext(ctx, k.Command[0], args...)
	cmd.Dir = r.Workdir
	cmd.Env = append(os.Environ(), "KIMI_DISABLE_TELEMETRY=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 15 * time.Second

	start := time.Now()
	runErr := cmd.Run()
	res := RunResult{Duration: time.Since(start), Stopped: ctx.Err() != nil}
	var texts []string
	for _, line := range bytes.Split(stdout.Bytes(), []byte("\n")) {
		var l kimiLine
		if json.Unmarshal(line, &l) != nil {
			continue
		}
		switch {
		case l.Role == "meta" && l.Type == "session.resume_hint":
			res.SessionID = strings.TrimPrefix(l.SessionID, kimiSessionPrefix)
		case l.Role == "assistant" && len(l.Content) > 0:
			var s string
			if json.Unmarshal(l.Content, &s) == nil && strings.TrimSpace(s) != "" {
				texts = append(texts, s)
			}
		case l.Role == "assistant":
			res.Turns++ // a tool call
		}
	}
	if runErr == nil {
		if len(texts) > 0 {
			res.Text = texts[len(texts)-1]
		}
		return res, nil
	}
	if res.Stopped {
		return res, runErr
	}
	if l, err := k.usedUp(context.Background()); err != nil {
		log.Printf("kimi: quota not checked: %v", err)
	} else {
		res.Limit = l
	}
	if tail := bytes.TrimSpace(lastBytes(stderr.Bytes(), 800)); len(tail) > 0 {
		runErr = fmt.Errorf("%w: %s", runErr, tail)
	}
	return res, runErr
}

func imageExt(mediaType string) string {
	switch mediaType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	}
	return ""
}

// kimiBanner is the line `kimi web` prints with its address and bearer token.
var kimiBanner = regexp.MustCompile(`(http://127\.0\.0\.1:\d+)/#token=(\S+)`)

// usedUp asks a short-lived `kimi web` for the account's plan quota and returns the window
// that is used up, if any; of several, the one that lifts last. Kimi reports quota only through its local server; the bearer
// token it prints never leaves this function.
func (k Kimi) usedUp(ctx context.Context) (*Limit, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	args := append(slices.Clone(k.Command[1:]), "web", "--no-open", "--port", "58700") // takes the next free port when it is in use
	cmd := exec.CommandContext(ctx, k.Command[0], args...)
	cmd.Env = append(os.Environ(), "KIMI_DISABLE_TELEMETRY=1")
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) } // lets it unregister itself
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	defer func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
	}()
	var base, token string
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if m := kimiBanner.FindStringSubmatch(sc.Text()); m != nil {
			base, token = m[1], m[2]
			break
		}
	}
	go io.Copy(io.Discard, out) // keep it from blocking on a full pipe
	if token == "" {
		return nil, errors.New("kimi web printed no address")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/oauth/usage", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Data struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
			Quota   struct {
				Usages map[string]struct {
					UsedRatio float64   `json:"usedRatio"`
					ResetAt   time.Time `json:"resetAt"`
				} `json:"usages"`
			} `json:"quota"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("usage: HTTP %d: %w", resp.StatusCode, err)
	}
	if body.Data.Kind != "ok" {
		return nil, fmt.Errorf("usage: HTTP %d, %s %s", resp.StatusCode, body.Data.Kind, body.Data.Message)
	}
	var l *Limit
	for _, name := range slices.Sorted(maps.Keys(body.Data.Quota.Usages)) {
		if u := body.Data.Quota.Usages[name]; u.UsedRatio >= 1 && (l == nil || u.ResetAt.After(l.ResetsAt)) {
			l = &Limit{Name: name, ResetsAt: u.ResetAt}
		}
	}
	return l, nil
}
