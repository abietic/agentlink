package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"agentlink/internal/proto"
)

// stubMirasim stands in for `mirasim ui-cli`: it logs its argv as one JSON line and answers
// like ui-cli. The prompt "slow" never settles, "ask" waits on a permission, "fail" ends in
// an error; anything else is echoed. A new session's key is claude:<the prompt's sha-ish>.
func stubMirasim(logPath string, args []string) {
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	json.NewEncoder(f).Encode(args)
	f.Close()
	flag := func(name string) string {
		if i := slices.Index(args, name); i >= 0 && i+1 < len(args) {
			return args[i+1]
		}
		return ""
	}
	if slices.Contains(args, "stop") {
		os.Stdout.WriteString(`{"stopped":true}` + "\n")
		return
	}
	key := flag("--session")
	if key == "" {
		key = "claude:0d1e2f30-0000-4000-8000-000000000001"
	}
	reply := map[string]any{"sessionKey": key, "phase": "done", "text": "mirasim: " + flag("--text"), "error": nil, "settled": true}
	switch flag("--text") {
	case "slow":
		reply = map[string]any{"sessionKey": key, "phase": "running", "settled": false}
	case "ask":
		reply = map[string]any{"sessionKey": key, "phase": "running", "settled": false,
			"interactions": []map[string]string{{"promptId": "p1", "kind": "permission", "title": "Run git push?"}}}
	case "fail":
		reply["error"] = "agent crashed"
	}
	json.NewEncoder(os.Stdout).Encode(reply)
}

func stubCalls(t *testing.T, logPath string) [][]string {
	t.Helper()
	b, _ := os.ReadFile(logPath)
	var calls [][]string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var args []string
		if err := json.Unmarshal([]byte(line), &args); err != nil {
			t.Fatalf("stub log: %v", err)
		}
		calls = append(calls, args)
	}
	return calls
}

func TestMirasimRunner(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "mirasim.log")
	t.Setenv("AGENTLINK_STUB_MIRASIM", logPath)
	m := Mirasim{Command: []string{os.Args[0]}, Port: 4970, Agent: "claude", Route: "own", Timeout: 5 * time.Second}
	dir := t.TempDir()

	res, err := m.Run(context.Background(), Run{Workdir: dir, SessionID: "placeholder", Prompt: "hi", Images: []Image{{MediaType: "image/png", Data: []byte("png")}}})
	if err != nil || res.Text != "mirasim: hi" || res.SessionID != "0d1e2f30-0000-4000-8000-000000000001" || res.IsError || res.Stopped {
		t.Fatalf("new session: %+v %v", res, err)
	}
	args := stubCalls(t, logPath)[0]
	want := []string{"ui-cli", "--port", "4970", "--timeout", "5000", "prompt", "--text", "hi", "--agent", "claude", "--cwd", dir, "--route", "own", "--quiet", "--file"}
	if !slices.Equal(args[:len(want)], want) || slices.Contains(args, "--session") || !strings.HasSuffix(args[len(want)], "image-1.png") {
		t.Fatalf("new session argv: %q", args)
	}

	res, _ = m.Run(context.Background(), Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "fail"})
	if !res.IsError || !strings.Contains(res.Text, "agent crashed") || res.SessionID != "abc" {
		t.Fatalf("error: %+v", res)
	}
	if args := stubCalls(t, logPath)[1]; !slices.Contains(args, "claude:abc") {
		t.Fatalf("resume argv: %q", args)
	}

	res, err = m.Run(context.Background(), Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "ask"})
	if err != nil || res.Stopped || !strings.Contains(res.Text, "permission: Run git push?") {
		t.Fatalf("waiting on an answer: %+v %v", res, err)
	}

	res, err = m.Run(context.Background(), Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "slow"})
	calls := stubCalls(t, logPath)
	if err != nil || !res.Stopped || !slices.Equal(calls[len(calls)-1][len(calls[len(calls)-1])-3:], []string{"stop", "--session", "claude:abc"}) {
		t.Fatalf("timeout: %+v %v, last call %q", res, err, calls[len(calls)-1])
	}
}

// TestHarnessSwitch: a room's harness picks the runner, switching starts a new session,
// and a session keeps the harness it ran on when it is addressed later.
func TestHarnessSwitch(t *testing.T) {
	f := newFixture(t)
	mlog := filepath.Join(t.TempDir(), "mirasim.log")
	t.Setenv("AGENTLINK_STUB_MIRASIM", mlog)
	f.cfg.Harnesses = map[string]Harness{"mirasim": {Type: "mirasim", Command: []string{os.Args[0]}}}
	if err := f.cfg.checkHarnesses(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	task := func(text string) string {
		return w.handle(context.Background(), proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: text})
	}

	if r := task("first"); !strings.HasPrefix(r, "echo: first") || !strings.Contains(r, "claude · session") {
		t.Fatalf("default harness: %q", r)
	}
	claudeSession := w.state["R1"].SessionID
	if r := task("harness nope"); !strings.Contains(r, "only `claude`, `mirasim`") {
		t.Fatalf("unknown harness: %q", r)
	}
	if r := task("harness mirasim"); !strings.Contains(r, "now `mirasim`") || w.state["R1"].SessionID != "" {
		t.Fatalf("switch: %q, session %q", r, w.state["R1"].SessionID)
	}
	if r := task("second"); !strings.HasPrefix(r, "mirasim: second") || !strings.Contains(r, "mirasim · session `0d1e2f30`") {
		t.Fatalf("mirasim run: %q", r)
	}
	if s := w.state["R1"].Sessions[0]; s.Harness != "mirasim" || w.state["R1"].SessionID != s.ID {
		t.Fatalf("mirasim session: %+v", s)
	}
	if r := task("third"); !slices.Contains(stubCalls(t, mlog)[1], "claude:0d1e2f30-0000-4000-8000-000000000001") || !strings.HasPrefix(r, "mirasim: third") {
		t.Fatalf("mirasim resume: %q %q", r, stubCalls(t, mlog)[1])
	}
	if r := task(claudeSession[:8] + " back to the sdk"); !strings.HasPrefix(r, "echo: back to the sdk") || f.last(t).SessionID != claudeSession {
		t.Fatalf("addressed claude session: %q", r)
	}
	if r := w.query("R1", "status"); !strings.Contains(r, "**Harness** `mirasim`") {
		t.Fatalf("status: %q", r)
	}

	// The device default changes under a room that never picked one: its current session
	// belongs to the old harness, so the next prompt starts a new one rather than resuming.
	w.state["R2"] = &RoomState{Workdir: f.root, SessionID: claudeSession, Sessions: []Session{{ID: claudeSession, Workdir: f.root}}}
	w.cfg.Harness = "mirasim"
	before := len(stubCalls(t, mlog))
	w.handle(context.Background(), proto.Task{ID: "t", RoomID: "R2", UserID: "U1", Text: "hello"})
	if args := stubCalls(t, mlog)[before]; slices.Contains(args, "--session") {
		t.Fatalf("resumed another harness's session: %q", args)
	}
}

func TestHarnessConfig(t *testing.T) {
	for name, c := range map[string]Config{
		"no runner, no harnesses": {},
		"unknown default":         {Runner: []string{"node"}, Harness: "mirasim"},
		"bad type":                {Harnesses: map[string]Harness{"x": {Type: "codex"}}, Harness: "x"},
		"bad route":               {Harnesses: map[string]Harness{"m": {Type: "mirasim", Route: "cloud"}}, Harness: "m"},
	} {
		if err := c.checkHarnesses(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	c := Config{Harnesses: map[string]Harness{"m": {Type: "mirasim"}}, Harness: "m"}
	if err := c.checkHarnesses(); err != nil || c.Harnesses["m"].Port != 4970 || c.Harnesses["m"].Route != "own" || len(c.Harnesses) != 1 {
		t.Fatalf("mirasim only: %v %+v", err, c.Harnesses)
	}
}
