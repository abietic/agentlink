package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/server"
	"agentlink/internal/tanka"
)

// stubClaude stands in for the claude CLI: it logs its args, echoes the prompt and cwd,
// and reports the session id it was given. The prompt "sleep" makes it hang.
const stubClaude = `#!/bin/sh
printf '%s\n' "$*" >> "$AGENTLINK_STUB_LOG"
sid=""
while [ $# -gt 0 ]; do
  case "$1" in --session-id|--resume) sid="$2"; shift ;; esac
  shift
done
prompt=$(cat)
case "$prompt" in sleep) exec sleep 5 ;; esac
printf '{"type":"result","subtype":"success","is_error":false,"result":"echo: %s @ %s","session_id":"%s","num_turns":1,"total_cost_usd":0.01,"permission_denials":[]}\n' "$prompt" "$(pwd)" "$sid"
`

type fixture struct {
	cfg  Config
	root string
	log  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	root, _ := filepath.EvalSymlinks(dir)
	root = filepath.Join(root, "code")
	os.MkdirAll(filepath.Join(root, "sub"), 0o755)
	bin := filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte(stubClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "args.log")
	t.Setenv("AGENTLINK_STUB_LOG", log)
	cfg := Config{
		Name:         "test-laptop",
		StateDir:     filepath.Join(dir, "state"),
		AllowedUsers: []string{"U1"},
		WorkdirRoots: []string{root},
		ClaudeBin:    bin,
		TimeoutSec:   10,
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	return &fixture{cfg: cfg, root: root, log: log}
}

func (f *fixture) lastArgs(t *testing.T) string {
	t.Helper()
	b, _ := os.ReadFile(f.log)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	return lines[len(lines)-1]
}

func flagValue(args, flag string) string {
	f := strings.Fields(args)
	for i := range f[:len(f)-1] {
		if f[i] == flag {
			return f[i+1]
		}
	}
	return ""
}

func TestParseCommand(t *testing.T) {
	for in, want := range map[string]command{
		"status":                  {name: "status"},
		"/Sessions":               {name: "sessions"},
		"status of the build?":    {name: "prompt", prompt: "status of the build?"},
		"new":                     {name: "new"},
		"new  fix the flaky test": {name: "new", prompt: "fix the flaky test"},
		"resume 2":                {name: "resume", arg: "2"},
		"resume abcd1234 go on":   {name: "resume", arg: "abcd1234", prompt: "go on"},
		"cd ~/Code/my repo":       {name: "cd", arg: "~/Code/my repo"},
		"workdir":                 {name: "cd"},
		"  看一下这个报错":               {name: "prompt", prompt: "看一下这个报错"},
	} {
		if got := parseCommand(in); got != want {
			t.Errorf("parseCommand(%q) = %+v, want %+v", in, got, want)
		}
	}
}

func TestResolveWorkdirStaysInsideRoots(t *testing.T) {
	f := newFixture(t)
	outside := filepath.Join(filepath.Dir(f.root), "outside")
	os.Mkdir(outside, 0o755)
	os.Symlink(outside, filepath.Join(f.root, "escape"))
	roots := f.cfg.WorkdirRoots

	for arg, ok := range map[string]bool{
		"sub":            true,
		f.root:           true,
		"../outside":     false,
		outside:          false,
		"escape":         false,
		"missing":        false,
		"sub/../../code": true,
	} {
		_, err := resolveWorkdir(roots, f.root, arg)
		if (err == nil) != ok {
			t.Errorf("resolveWorkdir(%q): err = %v, want ok=%v", arg, err, ok)
		}
	}
}

func TestSessionLifecycle(t *testing.T) {
	f := newFixture(t)
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.state.DeviceID = "dev-test"
	ctx := context.Background()
	task := func(text string) proto.Task { return proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: text} }

	reply := w.handle(ctx, task("hello"))
	if !strings.Contains(reply, "echo: hello @ "+f.root) || !strings.Contains(reply, "`dev-test`") {
		t.Fatalf("first reply: %q", reply)
	}
	first := flagValue(f.lastArgs(t), "--session-id")
	if !isUUID(first) || !strings.Contains(f.lastArgs(t), "--permission-prompts none") {
		t.Fatalf("first run args: %q", f.lastArgs(t))
	}

	w.handle(ctx, task("again"))
	if got := flagValue(f.lastArgs(t), "--resume"); got != first {
		t.Fatalf("second run should resume %s, args: %q", first, f.lastArgs(t))
	}

	if r := w.handle(ctx, task("new")); !strings.Contains(r, "new session") {
		t.Fatalf("new reply: %q", r)
	}
	w.handle(ctx, task("fresh start"))
	second := flagValue(f.lastArgs(t), "--session-id")
	if second == "" || second == first {
		t.Fatalf("new should start another session, args: %q", f.lastArgs(t))
	}

	if r := w.handle(ctx, task("cd ../..")); !strings.Contains(r, "outside the allowed workdir roots") {
		t.Fatalf("cd outside roots: %q", r)
	}
	if r := w.handle(ctx, task("cd sub")); !strings.Contains(r, "Workdir is now") {
		t.Fatalf("cd reply: %q", r)
	}
	if r := w.handle(ctx, task("in sub")); !strings.Contains(r, "@ "+filepath.Join(f.root, "sub")) {
		t.Fatalf("run after cd: %q", r)
	}
	if flagValue(f.lastArgs(t), "--session-id") == "" {
		t.Fatalf("cd should start a new session, args: %q", f.lastArgs(t))
	}

	list := w.query("R1", "sessions")
	if strings.Count(list, "\n") != 3 || !strings.Contains(list, "3. `"+first[:8]+"`") {
		t.Fatalf("sessions:\n%s", list)
	}
	if r := w.handle(ctx, task("resume 3 back to the first one")); !strings.Contains(r, "@ "+f.root) {
		t.Fatalf("resume reply: %q", r)
	}
	if got := flagValue(f.lastArgs(t), "--resume"); got != first {
		t.Fatalf("resume 3 should continue %s, args: %q", first, f.lastArgs(t))
	}
	if r := w.handle(ctx, task("resume zzzz")); !strings.Contains(r, "No session") {
		t.Fatalf("unknown resume: %q", r)
	}

	status := w.query("R1", "status")
	if !strings.Contains(status, first[:8]) || !strings.Contains(status, "Running** nothing") {
		t.Fatalf("status:\n%s", status)
	}

	w2, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rs := w2.state.Rooms["R1"]; rs == nil || rs.SessionID != first || len(rs.Sessions) != 3 {
		t.Fatalf("room state after restart: %+v", rs)
	}
}

func TestTimeoutKeepsTheSession(t *testing.T) {
	f := newFixture(t)
	f.cfg.TimeoutSec = 1
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	reply := w.handle(context.Background(), proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: "sleep"})
	if !strings.Contains(reply, "Timed out") {
		t.Fatalf("reply: %q", reply)
	}
	sid := flagValue(f.lastArgs(t), "--session-id")
	if w.state.Rooms["R1"].SessionID != sid {
		t.Fatalf("timed-out session %s not kept: %+v", sid, w.state.Rooms["R1"])
	}
}

// TestEndToEnd wires a real server, a fake Tanka and a worker with the stub CLI.
func TestEndToEnd(t *testing.T) {
	const signing = "e2e-secret"
	replies := make(chan string, 16)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m tanka.Message
		json.NewDecoder(r.Body).Decode(&m)
		replies <- m.Text
		io.WriteString(w, `{"code":0}`)
	}))
	defer fake.Close()

	srv, err := server.New(server.Config{
		SigningSecret: signing, EnrollToken: "enroll", PollWaitSec: 1,
		StatePath: filepath.Join(t.TempDir(), "server.json"), ResponseURLPrefix: fake.URL + "/",
	})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()

	f := newFixture(t)
	f.cfg.ServerURL, f.cfg.EnrollToken = hs.URL, "enroll"
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Register(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	n := 0
	say := func(user, text string) string {
		n++
		body, _ := json.Marshal(tanka.Envelope{Type: "event_callback", EventID: fmt.Sprint("e", n), Event: &tanka.MentionEvent{
			Type: "app_mention", UserID: user, RoomID: "R1", Text: "@bot " + text, ResponseURL: fake.URL + "/hooks/response/" + strconv.Itoa(n),
		}})
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		req, _ := http.NewRequest(http.MethodPost, hs.URL+"/tanka/events", bytes.NewReader(body))
		req.Header.Set(tanka.HeaderTimestamp, ts)
		req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte(signing), ts, body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		select {
		case r := <-replies:
			return r
		case <-time.After(10 * time.Second):
			t.Fatalf("no reply to %q", text)
			return ""
		}
	}

	if r := say("U1", "pair "+w.DeviceID()); !strings.Contains(r, "Paired") {
		t.Fatalf("pair: %q", r)
	}
	// The worker's first long poll may still be in flight; the server needs to have seen it.
	deadline := time.Now().Add(5 * time.Second)
	var r string
	for r = say("U1", "ping"); strings.Contains(r, "offline") && time.Now().Before(deadline); r = say("U1", "ping") {
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(r, "echo: ping @ "+f.root) {
		t.Fatalf("ping: %q", r)
	}
	if r := say("U1", "status"); !strings.Contains(r, "**Workdir** `") {
		t.Fatalf("status: %q", r)
	}
	if r := say("U9", "rm -rf everything"); !strings.Contains(r, "`U9` may not use device") {
		t.Fatalf("stranger: %q", r)
	}

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}
}
