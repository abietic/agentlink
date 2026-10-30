package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

// Run is one prompt for Claude.
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

// runRequest is what sdk/runner.mjs reads on stdin.
type runRequest struct {
	Cwd       string          `json:"cwd"`
	Prompt    string          `json:"prompt"`
	SessionID string          `json:"session_id"`
	Resume    bool            `json:"resume"`
	Options   json.RawMessage `json:"options,omitempty"`
}

// runReply is the one JSON line sdk/runner.mjs prints.
type runReply struct {
	Type         string  `json:"type"` // "result" or "error"
	Text         string  `json:"text"`
	Error        string  `json:"error"`
	SessionID    string  `json:"session_id"`
	IsError      bool    `json:"is_error"`
	NumTurns     int     `json:"num_turns"`
	TotalCostUSD float64 `json:"total_cost_usd"`
	Denials      int     `json:"denials"`
}

// SDK runs each prompt through the Claude Agent SDK's query(): Command (normally
// node sdk/runner.mjs) reads a runRequest on stdin and prints a runReply.
type SDK struct {
	Command []string
	Options json.RawMessage // passed to query() as options
	Timeout time.Duration
}

func (c SDK) Run(ctx context.Context, r Run) (RunResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	req, err := json.Marshal(runRequest{Cwd: r.Workdir, Prompt: r.Prompt, SessionID: r.SessionID, Resume: r.Resume, Options: c.Options})
	if err != nil {
		return RunResult{}, err
	}
	cmd := exec.CommandContext(ctx, c.Command[0], c.Command[1:]...)
	cmd.Dir = r.Workdir
	cmd.Stdin = bytes.NewReader(req) // not argv: keeps the prompt out of `ps`
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) } // runner.mjs aborts the query on SIGTERM
	cmd.WaitDelay = 15 * time.Second

	start := time.Now()
	runErr := cmd.Run()
	res := RunResult{Duration: time.Since(start), Stopped: ctx.Err() != nil}

	var out runReply
	if json.Unmarshal(lastLine(stdout.Bytes()), &out) == nil {
		res.SessionID = out.SessionID
		if out.Type == "result" {
			res.Text, res.IsError = out.Text, out.IsError
			res.Turns, res.CostUSD, res.Denials = out.NumTurns, out.TotalCostUSD, out.Denials
			return res, nil
		}
		if out.Error != "" {
			runErr = errors.New(out.Error)
		}
	}
	if runErr == nil {
		runErr = errors.New("the SDK runner printed no result")
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
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
