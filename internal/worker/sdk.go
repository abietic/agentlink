package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Run is one prompt for a harness.
type Run struct {
	Harness   string // picks the runner; runners ignore it
	Workdir   string
	SessionID string
	Resume    bool // continue SessionID; otherwise start a new session with that id
	Prompt    string
	Images    []Image // go in the same message as Prompt, before it
	UserID    string  // the Tanka user who sent it; empty for triggers
}

type RunResult struct {
	Text      string
	SessionID string
	IsError   bool
	Stopped   bool // timed out, stalled, or the worker shut down
	Stalled   bool // stopped because the harness showed no activity for its stall window
	Turns     int
	Denials   int
	CostUSD   float64
	Duration  time.Duration
	Limit     *Limit // the run stopped at this usage limit
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
	Images    []Image         `json:"images,omitempty"`
	Options   json.RawMessage `json:"options,omitempty"`
}

// runReply is the last JSON line sdk/runner.mjs prints. The lines before it are
// {"type":"progress"}: one when it starts and one for each message the SDK yields.
type runReply struct {
	Type         string  `json:"type"` // "result" or "error"; "progress" if it never got that far
	Text         string  `json:"text"`
	Error        string  `json:"error"`
	SessionID    string  `json:"session_id"`
	IsError      bool    `json:"is_error"`
	NumTurns     int     `json:"num_turns"`
	TotalCostUSD float64 `json:"total_cost_usd"`
	Denials      int     `json:"denials"`
	RateLimit    *struct {
		Type     string `json:"type"`
		ResetsAt int64  `json:"resets_at"` // epoch seconds; 0 if unknown
	} `json:"rate_limit"`
}

// SDK runs each prompt through the Claude Agent SDK's query(): Command (normally
// node sdk/runner.mjs) reads a runRequest on stdin and prints a runReply.
type SDK struct {
	Command []string
	Options json.RawMessage // passed to query() as options
	Timeout time.Duration
	// Stall stops a run that printed nothing for this long; 0 never does. It counts only
	// once the runner has printed its first line, so a runner from before progress lines
	// is left to Timeout alone.
	Stall time.Duration
}

// activity is a run's stdout, with the time of its last write.
type activity struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	last time.Time // zero until the first write
}

func (a *activity) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.last = time.Now()
	return a.buf.Write(p)
}

func (a *activity) idle() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.last.IsZero() {
		return 0
	}
	return time.Since(a.last)
}

func (c SDK) Run(ctx context.Context, r Run) (RunResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	req, err := json.Marshal(runRequest{Cwd: r.Workdir, Prompt: r.Prompt, SessionID: r.SessionID, Resume: r.Resume, Images: r.Images, Options: c.Options})
	if err != nil {
		return RunResult{}, err
	}
	cmd := exec.CommandContext(ctx, c.Command[0], c.Command[1:]...)
	cmd.Dir = r.Workdir
	cmd.Stdin = bytes.NewReader(req) // not argv: keeps the prompt out of `ps`
	var stdout activity
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) } // runner.mjs aborts the query on SIGTERM
	cmd.WaitDelay = 15 * time.Second

	var stalled atomic.Bool
	if c.Stall > 0 {
		done := make(chan struct{})
		defer close(done)
		go func() {
			tick := time.NewTicker(max(c.Stall/10, 10*time.Millisecond))
			defer tick.Stop()
			for {
				select {
				case <-done:
					return
				case <-tick.C:
					if stdout.idle() > c.Stall {
						stalled.Store(true)
						cancel()
						return
					}
				}
			}
		}()
	}

	start := time.Now()
	runErr := cmd.Run()
	res := RunResult{Duration: time.Since(start), Stopped: ctx.Err() != nil, Stalled: stalled.Load()}

	var out runReply
	if json.Unmarshal(lastLine(stdout.buf.Bytes()), &out) == nil {
		res.SessionID = out.SessionID
		if l := out.RateLimit; l != nil {
			res.Limit = &Limit{Name: l.Type}
			if l.ResetsAt > 0 {
				res.Limit.ResetsAt = time.Unix(l.ResetsAt, 0)
			}
		}
		if out.Type == "result" {
			if res.Stalled { // it had finished and was only slow to exit
				res.Stalled, res.Stopped = false, false
			}
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
