package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// Run is one prompt for Claude Code.
type Run struct {
	Workdir   string
	SessionID string
	Resume    bool // continue SessionID; otherwise start a new session with that id
	Prompt    string
}

type RunResult struct {
	Text      string
	SessionID string
	IsError   bool
	Stopped   bool // timed out or the worker shut down
	Turns     int
	Denials   int
	CostUSD   float64
	Duration  time.Duration
}

type Runner interface {
	Run(ctx context.Context, r Run) (RunResult, error)
}

// Claude runs the Claude Code CLI headless (`claude -p --output-format json`), the
// documented integration path for languages without an Agent SDK.
type Claude struct {
	Bin     string
	Args    []string
	Timeout time.Duration
}

func (c Claude) Run(ctx context.Context, r Run) (RunResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	args := []string{"-p", "--output-format", "json"}
	if r.Resume {
		args = append(args, "--resume", r.SessionID)
	} else {
		args = append(args, "--session-id", r.SessionID)
	}
	args = append(args, c.Args...)
	cmd := exec.CommandContext(ctx, c.Bin, args...)
	cmd.Dir = r.Workdir
	cmd.Stdin = strings.NewReader(r.Prompt) // not argv: keeps the prompt out of `ps`
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) } // the CLI's documented clean stop
	cmd.WaitDelay = 15 * time.Second

	start := time.Now()
	runErr := cmd.Run()
	res := RunResult{Duration: time.Since(start), Stopped: ctx.Err() != nil}

	var out struct {
		Type              string            `json:"type"`
		Result            string            `json:"result"`
		SessionID         string            `json:"session_id"`
		IsError           bool              `json:"is_error"`
		NumTurns          int               `json:"num_turns"`
		TotalCostUSD      float64           `json:"total_cost_usd"`
		PermissionDenials []json.RawMessage `json:"permission_denials"`
	}
	if json.Unmarshal(lastLine(stdout.Bytes()), &out) == nil && out.Type == "result" {
		res.Text, res.SessionID, res.IsError = out.Result, out.SessionID, out.IsError
		res.Turns, res.CostUSD, res.Denials = out.NumTurns, out.TotalCostUSD, len(out.PermissionDenials)
		return res, nil
	}
	if runErr == nil {
		runErr = errors.New("claude printed no result")
	}
	if tail := bytes.TrimSpace(lastBytes(stderr.Bytes(), 800)); len(tail) > 0 {
		runErr = fmt.Errorf("%w: %s", runErr, tail)
	}
	return res, runErr
}

func lastLine(b []byte) []byte {
	b = bytes.TrimSpace(b)
	if i := bytes.LastIndexByte(b, '\n'); i >= 0 {
		return b[i+1:]
	}
	return b
}

func lastBytes(b []byte, n int) []byte {
	if len(b) > n {
		return b[len(b)-n:]
	}
	return b
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
