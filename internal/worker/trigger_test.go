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
	if reply := w.handle(ctx, alert); !strings.Contains(reply, "echo: cd / and look @ "+sub) || strings.Contains(reply, "`"+tilde(sub)+"` · session") {
		t.Fatalf("triggered reply: %q", reply)
	}
	triggered := f.last(t)
	if triggered.Resume || triggered.SessionID == human {
		t.Fatalf("triggered run resumed a session: %+v", triggered)
	}
	// The room now continues that session: a follow-up in the room resumes it.
	w.handle(ctx, proto.Task{ID: "t3", RoomID: "R1", UserID: "U1", Text: "go on"})
	if r := f.last(t); !r.Resume || r.SessionID != triggered.SessionID || r.Cwd != sub {
		t.Fatalf("follow-up: %+v", r)
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
		posts <- r.URL.Path + " " + m.Text
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
	f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Workdir: filepath.Join(f.root, "sub"), Command: []string{"cat", items}}}
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
	if p := next(); !strings.HasPrefix(p, "/room echo: triage a @ "+filepath.Join(f.root, "sub")) {
		t.Fatalf("first scan posted %q", p)
	}

	// Same time for "a": seen. "old" fired again since: new.
	writeItems(item{Key: "a", At: now.Add(-time.Minute), Text: "triage a"}, item{Key: "old", At: now, Text: "old again"})
	if err := w.scan(ctx, w.cfg.Triggers[0]); err != nil {
		t.Fatal(err)
	}
	if p := next(); !strings.HasPrefix(p, "/room echo: old again") {
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
