package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

// The run log, runs.jsonl in the state directory, has a line for each run of a harness and
// each result sent, keyed by the server's task id, so the server's task log can be matched
// with what this device did. It holds ids, harness names, times and sizes; no prompts,
// replies, error text from a run, or room ids (the worker's database keeps those encrypted).
// A result line without "error" was accepted by the server.
const runLogKeep = 90 * 24 * time.Hour

type runRecord struct {
	At      time.Time `json:"at"`
	Event   string    `json:"event"` // "run" or "result"
	Task    string    `json:"task"`
	Trigger string    `json:"trigger,omitempty"`
	// a run
	Harness string  `json:"harness,omitempty"`
	Session string  `json:"session,omitempty"`
	Seconds float64 `json:"seconds,omitempty"`
	Failed  bool    `json:"failed,omitempty"`
	Stopped bool    `json:"stopped,omitempty"`
	Limit   string  `json:"limit,omitempty"` // the usage limit it hit
	Turns   int     `json:"turns,omitempty"`
	CostUSD float64 `json:"cost_usd,omitempty"`
	// a result
	Bytes int    `json:"bytes,omitempty"`
	Error string `json:"error,omitempty"` // why the server did not take it
}

func (w *Worker) runLogPath() string { return filepath.Join(w.cfg.StateDir, "runs.jsonl") }

func (w *Worker) logRun(r runRecord) {
	r.At = w.now()
	b, _ := json.Marshal(r)
	w.runLogMu.Lock()
	defer w.runLogMu.Unlock()
	f, err := os.OpenFile(w.runLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err == nil {
		_, err = f.Write(append(b, '\n'))
		f.Close()
	}
	if err != nil {
		log.Printf("run log: %v", err)
	}
}

// pruneRunLog drops the lines older than runLogKeep.
func pruneRunLog(path string, now time.Time) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var keep []byte
	dropped := false
	for line := range bytes.Lines(b) {
		var r struct {
			At time.Time `json:"at"`
		}
		if json.Unmarshal(line, &r) == nil && now.Sub(r.At) > runLogKeep {
			dropped = true
			continue
		}
		keep = append(keep, line...)
	}
	if !dropped {
		return nil
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, keep, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
