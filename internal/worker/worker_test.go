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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/server"
	"agentlink/internal/tanka"
)

// TestMain lets the test binary stand in for sdk/runner.mjs: with AGENTLINK_STUB_RUNNER
// set it logs the request there, echoes the prompt and cwd, and reports the session id it
// was given. The prompt "sleep" makes it hang; "wait" holds it until the file named by
// AGENTLINK_STUB_GATE exists.
func TestMain(m *testing.M) {
	if logPath := os.Getenv("AGENTLINK_STUB_MIRASIM"); logPath != "" && len(os.Args) > 1 && os.Args[1] == "ui-cli" {
		stubMirasim(logPath, os.Args[1:])
		return
	}
	if logPath := os.Getenv("AGENTLINK_STUB_RUNNER"); logPath != "" {
		var req runRequest
		json.NewDecoder(os.Stdin).Decode(&req)
		f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		json.NewEncoder(f).Encode(req)
		f.Close()
		if req.Prompt == "sleep" {
			time.Sleep(5 * time.Second)
		}
		for req.Prompt == "wait" {
			if _, err := os.Stat(os.Getenv("AGENTLINK_STUB_GATE")); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cwd, _ := os.Getwd()
		json.NewEncoder(os.Stdout).Encode(runReply{Type: "result", Text: "echo: " + req.Prompt + " @ " + cwd, SessionID: req.SessionID, NumTurns: 1, TotalCostUSD: 0.01})
		return
	}
	os.Exit(m.Run())
}

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
	log := filepath.Join(dir, "runs.log")
	t.Setenv("AGENTLINK_STUB_RUNNER", log)
	cfg := Config{
		Name:         "test-laptop",
		StateDir:     filepath.Join(dir, "state"),
		AllowedUsers: []string{"U1"},
		WorkdirRoots: []string{root},
		Runner:       []string{os.Args[0]},
		TimeoutSec:   10,
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	return &fixture{cfg: cfg, root: root, log: log}
}

// last is the request the runner got most recently.
func (f *fixture) last(t *testing.T) runRequest {
	t.Helper()
	b, _ := os.ReadFile(f.log)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	var req runRequest
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &req); err != nil {
		t.Fatalf("runner log: %v", err)
	}
	return req
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
	w.id.DeviceID = "dev-test"
	ctx := context.Background()
	task := func(text string) proto.Task { return proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: text} }

	reply := w.handle(ctx, task("hello"))
	if !strings.Contains(reply, "echo: hello @ "+f.root) || !strings.Contains(reply, "`dev-test`") {
		t.Fatalf("first reply: %q", reply)
	}
	first := f.last(t).SessionID
	if !isUUID(first) || f.last(t).Resume || string(f.last(t).Options) != `{"permissionMode":"auto"}` {
		t.Fatalf("first run: %+v", f.last(t))
	}

	w.handle(ctx, task("again"))
	if got := f.last(t); !got.Resume || got.SessionID != first {
		t.Fatalf("second run should resume %s: %+v", first, got)
	}

	if r := w.handle(ctx, task("new")); !strings.Contains(r, "new session") {
		t.Fatalf("new reply: %q", r)
	}
	w.handle(ctx, task("fresh start"))
	if got := f.last(t); got.Resume || got.SessionID == first {
		t.Fatalf("new should start another session: %+v", got)
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
	if f.last(t).Resume {
		t.Fatalf("cd should start a new session: %+v", f.last(t))
	}

	list := w.query("R1", "sessions")
	if strings.Count(list, "\n") != 3 || !strings.Contains(list, "3. `"+first[:8]+"`") {
		t.Fatalf("sessions:\n%s", list)
	}
	if r := w.handle(ctx, task("resume 3 back to the first one")); !strings.Contains(r, "@ "+f.root) {
		t.Fatalf("resume reply: %q", r)
	}
	if got := f.last(t); !got.Resume || got.SessionID != first {
		t.Fatalf("resume 3 should continue %s: %+v", first, got)
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
	if rs := w2.state["R1"]; rs == nil || rs.SessionID != first || len(rs.Sessions) != 3 {
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
	sid := f.last(t).SessionID
	if w.state["R1"].SessionID != sid {
		t.Fatalf("timed-out session %s not kept: %+v", sid, w.state["R1"])
	}
}

func TestMentionSender(t *testing.T) {
	const id = "6a7d7188238a191436b929da"
	for _, tc := range []struct {
		t    proto.Task
		in   string
		want string
	}{
		{proto.Task{UserID: id}, "done", "<@" + id + ">\n\ndone"},
		{proto.Task{UserID: id}, "", ""},                            // posts nothing
		{proto.Task{UserID: id, Trigger: "alerts"}, "done", "done"}, // nobody sent it
		{proto.Task{UserID: "all"}, "done", "done"},                 // never <@all>
		{proto.Task{UserID: "x> <@all"}, "done", "done"},            // nor anything that is not an id
		{proto.Task{UserID: ""}, "done", "done"},
	} {
		if got := mentionSender(tc.t, tc.in); got != tc.want {
			t.Errorf("mentionSender(%+v, %q) = %q, want %q", tc.t, tc.in, got, tc.want)
		}
	}
}

// TestSessionsRunSideBySide: a task addressed to another session does not wait for the room's
// current one, a task addressed to the current session does, and no session ever has two runs
// at once.
func TestSessionsRunSideBySide(t *testing.T) {
	var mu sync.Mutex
	var order []string
	posted := func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(order) }
	hs := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var env proto.Envelope
		json.NewDecoder(r.Body).Decode(&env)
		mu.Lock()
		order = append(order, env.ID)
		mu.Unlock()
	}))
	defer hs.Close()
	f := newFixture(t)
	gate := filepath.Join(t.TempDir(), "gate")
	t.Setenv("AGENTLINK_STUB_GATE", gate)
	f.cfg.ServerURL = hs.URL
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.setIdentity(Identity{DeviceID: "d", DeviceKey: seal.NewPrivateKey().Bytes(), ServerKey: seal.NewPrivateKey().PublicKey().Bytes()})
	ctx := context.Background()
	task := func(id, text string) proto.Task { return proto.Task{ID: id, RoomID: "R1", UserID: "U1", Text: text} }
	w.handle(ctx, task("t0", "first"))
	other := f.last(t).SessionID
	w.handle(ctx, task("t1", "new second"))
	current := f.last(t).SessionID
	eventually := func(what string, ok func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !ok(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
		}
	}

	w.dispatch(ctx, task("held", "wait"))
	w.dispatch(ctx, task("same", short(current)+" after"))
	w.dispatch(ctx, task("side", short(other)+" meanwhile"))
	eventually("the task on another session waited for the room's", func() bool { return len(posted()) == 1 })
	os.WriteFile(gate, nil, 0o600)
	w.wg.Wait()
	if want := []string{"side", "held", "same"}; !slices.Equal(posted(), want) {
		t.Fatalf("results in order %v, want %v", posted(), want)
	}
	if len(w.queues)+len(w.running)+len(w.locks) != 0 || w.state["R1"].SessionID != current {
		t.Fatalf("left behind: queues %v, running %v, locks %v, current %s", w.queues, w.running, w.locks, w.state["R1"].SessionID)
	}

	// Two runs on one session, however they got there, take turns.
	os.Remove(gate)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func() { defer wg.Done(); w.handle(ctx, task(id, short(other)+" wait")) }()
	}
	locked := func() (holders, running int) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if l := w.locks[other]; l != nil {
			holders = l.holders
		}
		return holders, len(w.running)
	}
	eventually("both runs never reached the session", func() bool { h, _ := locked(); return h == 2 })
	time.Sleep(100 * time.Millisecond)
	if _, n := locked(); n != 1 {
		t.Fatalf("%d runs on one session at once", n)
	}
	os.WriteFile(gate, nil, 0o600)
	wg.Wait()
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

	srvDB := filepath.Join(t.TempDir(), "server.db")
	srv, err := server.New(server.Config{
		EnrollToken: "enroll", PollWaitSec: 1, DBPath: srvDB, ResponseURLPrefix: fake.URL + "/",
		Bots: []server.Bot{{Name: "main", SigningSecret: signing}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()

	f := newFixture(t)
	const member = "6a7d7188238a191436b929da" // shaped like a Tanka member id, so replies mention it
	f.cfg.ServerURL, f.cfg.EnrollToken = hs.URL, "enroll"
	f.cfg.AllowedUsers = append(f.cfg.AllowedUsers, member)
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
			Type: "app_mention", UserID: user, RoomID: "room-canary-77", Text: "@bot " + text, ResponseURL: fake.URL + "/hooks/response/" + strconv.Itoa(n),
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
	if r := say("U1", "zebra-canary"); !strings.Contains(r, "echo: zebra-canary") {
		t.Fatalf("canary: %q", r)
	}
	if r := say(member, "who asked"); !strings.HasPrefix(r, "<@"+member+">\n\necho: who asked") {
		t.Fatalf("reply to a member does not mention them first: %q", r)
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

	// Both databases (and their WAL files) only ever held the prompt encrypted, and the
	// worker's does not reveal which rooms it serves (the server needs room ids to route).
	for _, p := range []string{srvDB, srvDB + "-wal", filepath.Join(f.cfg.StateDir, "worker.db"), filepath.Join(f.cfg.StateDir, "worker.db-wal")} {
		b, _ := os.ReadFile(p)
		if bytes.Contains(b, []byte("zebra-canary")) {
			t.Errorf("%s holds the prompt in plain text", filepath.Base(p))
		}
		if strings.HasPrefix(filepath.Base(p), "worker") && bytes.Contains(b, []byte("room-canary-77")) {
			t.Errorf("%s holds the room id in plain text", filepath.Base(p))
		}
	}
}
