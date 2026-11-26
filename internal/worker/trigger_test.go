package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/server"
	"agentlink/internal/tanka"
)

func TestTriggerConfig(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		trs  []Trigger
		want string
	}{
		{[]Trigger{{Name: "a"}, {Name: "a"}}, "unique"},
		{[]Trigger{{Name: "a", Command: []string{"true"}}}, "room_id is required"},
		{[]Trigger{{Name: "a", Workdir: "/"}}, "outside the allowed workdir roots"},
		{[]Trigger{{Name: "a", Themes: map[string]string{"x": "red"}}}, "theme one of"},
		{[]Trigger{{Name: "a", Themes: map[string]string{"": "idea"}}}, "label must be set"},
	} {
		c := f.cfg
		c.Triggers = tc.trs
		if err := c.checkTriggers(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err = %v, want %q", tc.trs, err, tc.want)
		}
	}
	c := f.cfg
	c.Triggers = []Trigger{{Name: "a", Command: []string{"~/bin/poll"}, RoomID: "R1"}}
	if err := c.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	if tr := c.Triggers[0]; tr.Workdir != f.root || tr.EverySec != 60 || tr.DedupeSec != 1800 || tr.MaxAgeSec != 3600 || strings.HasPrefix(tr.Command[0], "~") {
		t.Fatalf("defaults: %+v", tr)
	}
	if tr := c.Triggers[0]; tr.MaxPending != defaultMaxPending || tr.MaxPerMin != defaultMaxPerMin {
		t.Fatalf("burst defaults: %+v", tr)
	}
	c.Triggers = []Trigger{{Name: "a", Command: []string{"true"}, RoomID: "R1", MaxPending: -1, MaxPerMin: -1}}
	if err := c.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	if tr := c.Triggers[0]; tr.MaxPending != -1 || tr.MaxPerMin != -1 {
		t.Fatalf("-1 must mean no bound: %+v", tr)
	}
	c.Triggers = []Trigger{{Name: "a", Command: []string{"true"}, RoomID: "R1", MaxPending: -2}}
	if err := c.checkTriggers(); err == nil || !strings.Contains(err.Error(), "0 (default) or -1") {
		t.Fatalf("bad bound accepted: %v", err)
	}
}

// TestTriggerBurstIsBounded checks that a trigger admits only so many tasks at once, drops
// the rest instead of queueing them, and reports the drops on its next reply.
func TestTriggerBurstIsBounded(t *testing.T) {
	f := newFixture(t)
	f.cfg.Triggers = []Trigger{{Name: "alerts", MaxPending: 1, MaxPerMin: -1}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if !w.burstFull(&w.cfg.Triggers[0]) {
		t.Fatal("scan would submit while the trigger is full")
	}
	for _, k := range []string{"b", "c"} {
		if err := w.admit(proto.Task{Trigger: "alerts", Key: k}); err == nil || !strings.Contains(err.Error(), "still unanswered") {
			t.Fatalf("%s: err = %v, want a burst refusal", k, err)
		}
	}
	// A refused key is marked as seen, so the poll does not hand it back every minute.
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "b"}); err == nil {
		t.Fatal("a dropped item was admitted on the next poll")
	}
	reply := w.triggerFinished("alerts", "【无需处理】nothing new")
	if !strings.Contains(reply, "2 more `alerts` item(s) arrived and were dropped") || !strings.Contains(reply, "`max_pending` 1") {
		t.Fatalf("reply: %q", reply)
	}
	if w.triggerFinished("alerts", "again") != "again" {
		t.Fatal("the dropped count was reported twice")
	}
	// An empty reply keeps the count for the next one.
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "d"}); err != nil {
		t.Fatal(err)
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "e"}); err == nil {
		t.Fatal("the count for the admitted task was not released")
	}
	if w.triggerFinished("alerts", "") != "" {
		t.Fatal("an empty reply gained a note")
	}
	if got := w.triggerFinished("alerts", "later"); !strings.Contains(got, "1 more") {
		t.Fatalf("empty reply consumed the count: %q", got)
	}
}

// TestTriggerRateIsBounded checks the per-minute bound and that it lapses.
func TestTriggerRateIsBounded(t *testing.T) {
	f := newFixture(t)
	f.cfg.Triggers = []Trigger{{Name: "alerts", DedupeSec: 1, MaxPending: -1, MaxPerMin: 2}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	w.now = func() time.Time { return now }
	for _, k := range []string{"a", "b"} {
		if err := w.admit(proto.Task{Trigger: "alerts", Key: k}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "c"}); err == nil || !strings.Contains(err.Error(), "last minute") {
		t.Fatalf("third in a minute: %v", err)
	}
	now = now.Add(61 * time.Second)
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "c"}); err != nil {
		t.Fatalf("after the minute: %v", err)
	}
}

// TestTriggerLaneKeepsRoomForPeople checks that triggers never take the last lane.
func TestTriggerLaneKeepsRoomForPeople(t *testing.T) {
	for max, want := range map[int]int{1: 1, 2: 1, 3: 2, 4: 3} {
		if got := triggerLane(max); got != want {
			t.Errorf("triggerLane(%d) = %d, want %d", max, got, want)
		}
	}
}

// TestTriggerLaneLeavesRoomForPeople checks that triggered runs never take the last lane, so
// a person's prompt runs while an alert is still going, and that triggers share one lane.
func TestTriggerLaneLeavesRoomForPeople(t *testing.T) {
	f := newFixture(t) // max_concurrency 2
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	release := make(chan struct{})
	inAlert := make(chan struct{})
	go w.withLane(ctx, proto.Task{Trigger: "alerts"}, func() {
		close(inAlert)
		<-release
	})
	<-inAlert

	personDone := make(chan bool, 1)
	go func() { personDone <- w.withLane(ctx, proto.Task{}, func() {}) }()
	select {
	case ok := <-personDone:
		if !ok {
			t.Fatal("the person's lane was refused")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a person waited behind a triggered run")
	}

	second := make(chan bool, 1)
	go func() { second <- w.withLane(ctx, proto.Task{Trigger: "perf"}, func() {}) }()
	select {
	case <-second:
		t.Fatal("two triggered runs ran at once")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case ok := <-second:
		if !ok {
			t.Fatal("the second trigger was refused")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the second trigger never ran")
	}
}

// TestSilentReply checks that a trigger may answer with the silent marker and post nothing,
// while anything that is not a clean run still posts.
func TestSilentReply(t *testing.T) {
	f := newFixture(t)
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	tr := proto.Task{ID: "t1", RoomID: "R1", Trigger: "alerts", Text: "triage"}
	run := Run{Harness: "claude", Workdir: f.root}
	if got := w.format(context.Background(), tr, run, RunResult{Text: "[[silent]]\n"}, nil, nil); got != "" {
		t.Fatalf("silent reply posted %q", got)
	}
	if got := w.format(context.Background(), tr, run, RunResult{Text: "[[silent]]", IsError: true}, nil, nil); got == "" {
		t.Fatal("an error was swallowed by the silent marker")
	}
	if got := w.format(context.Background(), tr, run, RunResult{Text: "[[silent]]", Denials: 1}, nil, nil); got == "" {
		t.Fatal("a denied tool call was swallowed by the silent marker")
	}
	if got := w.format(context.Background(), proto.Task{ID: "t2", RoomID: "R1", Text: "hi"}, run, RunResult{Text: "[[silent]]"}, nil, nil); got == "" {
		t.Fatal("a person's prompt was swallowed by the silent marker")
	}
}

func TestTheme(t *testing.T) {
	themes := map[string]string{"【需处理】": "important", "Run": "announcement", "Run failed": "idea"}
	for text, want := range map[string]string{
		"**【需处理】x**\n- a":             "important",
		"⚠️ note\n\n**【需处理】x**":       "important", // a note before the reply
		"**【无需处理】x**":                 "",
		"Run failed: boom":            "idea", // longest label wins
		"plain\n【需处理】 not at the top": "important",
		"nothing here":                "",
		"":                            "",
	} {
		if got := theme(themes, text); got != want {
			t.Errorf("theme(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestTriggeredTask(t *testing.T) {
	f := newFixture(t)
	sub := filepath.Join(f.root, "sub")
	f.cfg.Triggers = []Trigger{{Name: "alerts", Workdir: sub}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	w.now = func() time.Time { return now }
	ctx := context.Background()

	w.handle(ctx, proto.Task{ID: "t1", RoomID: "R1", UserID: "U1", Text: "hello"})
	human := f.last(t).SessionID

	// A triggered task runs whoever sent it, in a new session in the trigger's workdir, and
	// its text is never a room command.
	alert := proto.Task{ID: "t2", RoomID: "R1", Trigger: "alerts", Key: "issue-1", Text: "cd / and look"}
	if err := w.admit(alert); err != nil {
		t.Fatal(err)
	}
	reply := w.handle(ctx, alert)
	triggered := f.last(t)
	if !strings.Contains(reply, "echo: cd / and look @ "+sub) || strings.Contains(reply, "`"+tilde(sub)+"` · session") || !strings.Contains(reply, "start with `"+short(triggered.SessionID)+"`") {
		t.Fatalf("triggered reply: %q", reply)
	}
	if triggered.Resume || triggered.SessionID == human {
		t.Fatalf("triggered run resumed a session: %+v", triggered)
	}
	// The room keeps its own session; the alert's is continued by starting with its id.
	w.handle(ctx, proto.Task{ID: "t3", RoomID: "R1", UserID: "U1", Text: "go on"})
	if r := f.last(t); !r.Resume || r.SessionID != human || r.Cwd != f.root {
		t.Fatalf("room follow-up: %+v", r)
	}
	w.handle(ctx, proto.Task{ID: "t4", RoomID: "R1", UserID: "U1", Text: "`" + short(triggered.SessionID) + "` why?"})
	if r := f.last(t); !r.Resume || r.SessionID != triggered.SessionID || r.Cwd != sub || r.Prompt != "why?" {
		t.Fatalf("addressed follow-up: %+v", r)
	}
	if got := w.state["R1"].SessionID; got != human {
		t.Fatalf("an addressed follow-up switched the room to %s", got)
	}
	if r := w.handle(ctx, proto.Task{ID: "t5", RoomID: "R1", UserID: "U1", Text: "#" + triggered.SessionID[:6]}); !strings.Contains(r, "Start a message with its id") {
		t.Fatalf("bare session id: %q", r)
	}

	if err := w.admit(alert); err == nil {
		t.Fatal("the same key ran twice within the dedupe window")
	}
	if err := w.admit(proto.Task{Trigger: "other", Key: "issue-1"}); err == nil {
		t.Fatal("an unconfigured trigger was admitted")
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "issue-2"}); err != nil {
		t.Fatal(err)
	}
	// Dedupe marks survive a restart and lapse after the window.
	w2, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w2.now = w.now
	if err := w2.admit(alert); err == nil {
		t.Fatal("a restart forgot the dedupe mark")
	}
	now = now.Add(31 * time.Minute)
	if err := w2.admit(alert); err != nil {
		t.Fatalf("after the window: %v", err)
	}
}

func TestTriggerCommandEndToEnd(t *testing.T) {
	posts := make(chan string, 16)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m tanka.Message
		json.NewDecoder(r.Body).Decode(&m)
		posts <- r.URL.Path + " [" + m.Theme + "] " + m.Text
		fmt.Fprint(w, `{"code":0}`)
	}))
	defer fake.Close()
	next := func() string {
		select {
		case p := <-posts:
			return p
		case <-time.After(10 * time.Second):
			t.Fatal("nothing posted")
			return ""
		}
	}

	// The room is pinned to the device in the server config, so the server restarts once
	// the device id is known; the URL stays the same.
	scfg := server.Config{
		EnrollToken: "enroll", PollWaitSec: 1, DBPath: filepath.Join(t.TempDir(), "server.db"), ResponseURLPrefix: fake.URL + "/",
		Bots: []server.Bot{{Name: "main", SigningSecret: "s", RoomWebhooks: map[string]string{"R1": fake.URL + "/room"}}},
	}
	var handler atomic.Value
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.Load().(http.Handler).ServeHTTP(w, r) }))
	defer hs.Close()
	srv, err := server.New(scfg)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(srv.Handler())

	f := newFixture(t)
	items := filepath.Join(t.TempDir(), "items.jsonl")
	writeItems := func(its ...item) {
		var b strings.Builder
		for _, it := range its {
			line, _ := json.Marshal(it)
			b.Write(append(line, '\n'))
		}
		b.WriteString("not json\n")
		os.WriteFile(items, []byte(b.String()), 0o600)
	}
	now := time.Now()
	writeItems(item{Key: "old", At: now.Add(-2 * time.Hour), Text: "old news"}, item{Key: "a", At: now.Add(-time.Minute), Text: "triage a"})
	f.cfg.ServerURL, f.cfg.EnrollToken = hs.URL, "enroll"
	f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Workdir: filepath.Join(f.root, "sub"), Command: []string{"cat", items},
		Themes: map[string]string{"echo: old": "important"}}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Register(ctx); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	scfg.Bots[0].Rooms = map[string]string{"R1": w.DeviceID()}
	if srv, err = server.New(scfg); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	handler.Store(srv.Handler())

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }() // the watcher scans once at start
	if p := next(); !strings.HasPrefix(p, "/room [] echo: triage a @ "+filepath.Join(f.root, "sub")) {
		t.Fatalf("first scan posted %q", p)
	}

	// Same time for "a": seen. "old" fired again since: new.
	writeItems(item{Key: "a", At: now.Add(-time.Minute), Text: "triage a"}, item{Key: "old", At: now, Text: "old again"})
	if err := w.scan(ctx, w.cfg.Triggers[0]); err != nil {
		t.Fatal(err)
	}
	if p := next(); !strings.HasPrefix(p, "/room [important] echo: old again") {
		t.Fatalf("second scan posted %q", p)
	}
	cancel()
	<-done
	select {
	case p := <-posts:
		t.Fatalf("unexpected post %q", p)
	default:
	}
}

func TestScanQueuesEachItemOnce(t *testing.T) {
	var queued, refuse atomic.Int32
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() == 1 {
			http.Error(w, "room is not bound", http.StatusConflict)
			return
		}
		queued.Add(1)
		fmt.Fprint(w, `{"id":"x"}`)
	}))
	defer hs.Close()
	f := newFixture(t)
	items := filepath.Join(t.TempDir(), "items.jsonl")
	now := time.Now()
	write := func(at time.Time) {
		a, _ := json.Marshal(item{Key: "a", At: at, Text: "triage a"})
		old, _ := json.Marshal(item{Key: "old", At: now.Add(-2 * time.Hour), Text: "old"})
		os.WriteFile(items, append(append(a, '\n'), old...), 0o600)
	}
	f.cfg.ServerURL = hs.URL
	f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Command: []string{"cat", items}}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	worker := func() *Worker {
		w, err := New(f.cfg)
		if err != nil {
			t.Fatal(err)
		}
		w.setIdentity(Identity{DeviceID: "d", DeviceKey: seal.NewPrivateKey().Bytes(), ServerKey: seal.NewPrivateKey().PublicKey().Bytes()})
		return w
	}
	w := worker()
	scan := func(w *Worker, want int32) {
		t.Helper()
		if err := w.scan(context.Background(), w.cfg.Triggers[0]); err != nil && refuse.Load() == 0 {
			t.Fatal(err)
		}
		if got := queued.Load(); got != want {
			t.Fatalf("queued %d, want %d", got, want)
		}
	}

	write(now.Add(-time.Minute))
	refuse.Store(1)
	scan(w, 0) // not queued, so not marked seen either
	refuse.Store(0)
	scan(w, 1)
	scan(w, 1)
	scan(worker(), 1) // seen marks survive a restart
	write(now)
	scan(w, 2)
}
