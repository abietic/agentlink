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

func TestStallStopsOnlyASilentRun(t *testing.T) {
	for _, tc := range []struct {
		prompt, want string
	}{
		{"stall", "**Stalled:** nothing happened in this run for 1s"},
		{"busy", "echo: busy"},   // longer than the window in all, never silent that long
		{"sleep", "echo: sleep"}, // a runner that prints no progress lines is left to timeout_sec
	} {
		t.Run(tc.prompt, func(t *testing.T) {
			t.Parallel()
			f := newFixtureParallel(t)
			f.cfg.StallSec = 1
			w, err := New(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			reply := w.handle(context.Background(), proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: tc.prompt})
			if !strings.Contains(reply, tc.want) {
				t.Fatalf("reply: %q", reply)
			}
			if tc.prompt == "stall" {
				if d := time.Since(start); d > 4*time.Second {
					t.Fatalf("stopped after %s", d)
				}
				if w.state["R1"].SessionID != f.last(t).SessionID {
					t.Fatal("the stalled run's session was not kept")
				}
			}
		})
	}
}

func TestRecoveryConfig(t *testing.T) {
	f := newFixture(t)
	f.cfg.Triggers = []Trigger{{Name: "alerts"}}
	for _, tc := range []struct {
		rec  map[string]Recovery
		want string
	}{
		{map[string]Recovery{"mention": {}}, "cannot be retried yet"},
		{map[string]Recovery{"alerts": {}}, `must be "trigger"`},
		{map[string]Recovery{"trigger:other": {}}, "no trigger has that name"},
		{map[string]Recovery{"trigger": {On: []string{"crash"}}}, `unknown "crash"`},
	} {
		c := f.cfg
		c.Recovery = tc.rec
		if err := c.checkRecovery(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err = %v, want %q", tc.rec, err, tc.want)
		}
	}
	c := f.cfg
	c.Triggers = []Trigger{{Name: "alerts"}, {Name: "builds"}}
	c.Recovery = map[string]Recovery{"trigger": {On: []string{"stall"}}, "trigger:builds": {MaxAttempts: 5, ResumePrompt: "go on"}}
	if err := c.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	if r := c.Recovery["trigger"]; r.MaxAttempts != 2 || r.RetryAfterSec != 300 || r.GiveUpAfterSec != 86400 || r.ResumePrompt != defaultResumePrompt || r.Quiet || r.Resume || r.NoticeAfterSec != 0 {
		t.Fatalf("defaults: %+v", r)
	}
	w, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if w.recovery("builds").MaxAttempts != 5 || w.recovery("alerts").MaxAttempts != 2 || w.recovery("") != nil {
		t.Fatal("recovery lookup: a trigger's own entry comes first, then \"trigger\"; mentions have none")
	}
	c.Recovery = nil
	if w, _ := New(c); w.recovery("alerts") != nil {
		t.Fatal("recovery without config")
	}
	c.StallSec = c.TimeoutSec
	if err := c.applyDefaults(); err == nil {
		t.Fatal("stall_sec at timeout_sec was accepted")
	}
}

// recoveryE2E is a server, a fake Tanka and a worker with one command trigger, "alerts", for
// room R1, whose items are the lines of the file write fills.
type recoveryE2E struct {
	f     *fixture
	w     *Worker
	posts chan string
	fails string // the runner stub's AGENTLINK_STUB_FAILS file
	start func() (stop func())
}

func newRecoveryE2E(t *testing.T, rec Recovery, items ...item) *recoveryE2E {
	t.Helper()
	recoveryTick = 50 * time.Millisecond
	t.Cleanup(func() { recoveryTick = 15 * time.Second })
	e := &recoveryE2E{posts: make(chan string, 16), fails: filepath.Join(t.TempDir(), "fails")}
	t.Setenv("AGENTLINK_STUB_FAILS", e.fails)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m tanka.Message
		json.NewDecoder(r.Body).Decode(&m)
		e.posts <- m.Text
		fmt.Fprint(w, `{"code":0}`)
	}))
	t.Cleanup(fake.Close)
	scfg := server.Config{
		EnrollToken: "enroll", PollWaitSec: 1, DBPath: filepath.Join(t.TempDir(), "server.db"), ResponseURLPrefix: fake.URL + "/",
		Bots: []server.Bot{{Name: "main", SigningSecret: "s", RoomWebhooks: map[string]string{"R1": fake.URL + "/room"}}},
	}
	var handler atomic.Value
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.Load().(http.Handler).ServeHTTP(w, r) }))
	t.Cleanup(hs.Close)
	srv, err := server.New(scfg)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(srv.Handler())

	e.f = newFixture(t)
	path := filepath.Join(t.TempDir(), "items.jsonl")
	var b strings.Builder
	for _, it := range items {
		line, _ := json.Marshal(it)
		b.Write(append(line, '\n'))
	}
	os.WriteFile(path, []byte(b.String()), 0o600)
	e.f.cfg.ServerURL, e.f.cfg.EnrollToken = hs.URL, "enroll"
	e.f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Workdir: filepath.Join(e.f.root, "sub"), Command: []string{"cat", path}}}
	e.f.cfg.Recovery = map[string]Recovery{"trigger": rec}
	if err := e.f.cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	if e.w, err = New(e.f.cfg); err != nil {
		t.Fatal(err)
	}
	if err := e.w.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	scfg.Bots[0].Rooms = map[string]string{"R1": e.w.DeviceID()}
	if srv, err = server.New(scfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	handler.Store(srv.Handler())
	e.start = func() func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.w.Run(ctx) }()
		return func() { cancel(); <-done }
	}
	return e
}

func (e *recoveryE2E) next(t *testing.T) string {
	t.Helper()
	select {
	case p := <-e.posts:
		return p
	case <-time.After(10 * time.Second):
		t.Fatal("nothing posted")
		return ""
	}
}

// settled stops the worker and fails if anything else was posted or the ledger is not empty.
func (e *recoveryE2E) settled(t *testing.T, stop func()) {
	t.Helper()
	time.Sleep(300 * time.Millisecond) // a few ledger checks
	stop()
	select {
	case p := <-e.posts:
		t.Fatalf("unexpected post %q", p)
	default:
	}
	if len(e.w.pending) != 0 {
		t.Fatalf("ledger not empty: %+v", e.w.pending)
	}
	if w2, err := New(e.f.cfg); err != nil || len(w2.pending) != 0 {
		t.Fatalf("stored ledger not empty: %v %+v", err, w2.pending)
	}
}

func (e *recoveryE2E) requests(t *testing.T) []runRequest {
	t.Helper()
	b, _ := os.ReadFile(e.f.log)
	var out []runRequest
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var r runRequest
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("runner log: %v", err)
		}
		out = append(out, r)
	}
	return out
}

var alertA = item{Key: "a", At: time.Now().Add(-time.Minute), Text: "triage a"}

func TestRecoveryResumesQuietly(t *testing.T) {
	e := newRecoveryE2E(t, Recovery{On: []string{"error"}, RetryAfterSec: 1, Resume: true, Quiet: true, ResumePrompt: "go on"}, alertA)
	os.WriteFile(e.fails, []byte("error 1"), 0o600)
	stop := e.start()
	// The failed first run posts nothing; the room sees only the second run's answer.
	if p := e.next(t); !strings.HasPrefix(p, "echo: go on @ ") || strings.Contains(p, "run 2") {
		t.Fatalf("posted %q", p)
	}
	e.settled(t, stop)
	rs := e.requests(t)
	if len(rs) != 2 || rs[0].Resume || rs[0].Prompt != "triage a" || !rs[1].Resume || rs[1].SessionID != rs[0].SessionID {
		t.Fatalf("runs: %+v", rs)
	}
}

func TestRecoveryAfterAStall(t *testing.T) {
	e := newRecoveryE2E(t, Recovery{On: []string{"stall"}, RetryAfterSec: 1, Resume: true, Quiet: true, ResumePrompt: "go on"}, alertA)
	e.w.runners["claude"] = SDK{Command: e.f.cfg.Runner, Timeout: 10 * time.Second, Stall: time.Second}
	os.WriteFile(e.fails, []byte("stall 1"), 0o600)
	stop := e.start()
	if p := e.next(t); !strings.HasPrefix(p, "echo: go on @ ") {
		t.Fatalf("posted %q", p)
	}
	e.settled(t, stop)
}

func TestTaskWithoutAKeyIsNotTracked(t *testing.T) {
	w, _, _ := ledgerWorker(t, Recovery{On: []string{"error"}, Quiet: true})
	fails := filepath.Join(t.TempDir(), "fails")
	t.Setenv("AGENTLINK_STUB_FAILS", fails)
	os.WriteFile(fails, []byte("error 1"), 0o600)
	task := proto.Task{ID: "t1", RoomID: "R1", Trigger: "alerts", Text: "triage"}
	if err := w.claimOrAdmit(task); err != nil {
		t.Fatal(err)
	}
	if reply := w.handle(context.Background(), task); !strings.Contains(reply, "**Run failed:** boom") || len(w.pending) != 0 {
		t.Fatalf("reply %q, ledger %+v", reply, w.pending)
	}
}

func TestRecoveryReportsOnceWhenRetriesRunOut(t *testing.T) {
	e := newRecoveryE2E(t, Recovery{On: []string{"error"}, RetryAfterSec: 1, Quiet: true}, alertA)
	os.WriteFile(e.fails, []byte("error 9"), 0o600)
	stop := e.start()
	if p := e.next(t); !strings.Contains(p, "**Run failed:** boom") || !strings.Contains(p, "This was run 2 of this task") {
		t.Fatalf("posted %q", p)
	}
	e.settled(t, stop)
	// Without resume each run starts over, in a session of its own.
	if rs := e.requests(t); len(rs) != 2 || rs[1].Resume || rs[1].Prompt != "triage a" || rs[1].SessionID == rs[0].SessionID {
		t.Fatalf("runs: %+v", rs)
	}
}

func TestRecoveryStartsOverWhenTheSessionCannotResume(t *testing.T) {
	e := newRecoveryE2E(t, Recovery{On: []string{"error"}, RetryAfterSec: 1, Resume: true, Quiet: true}, alertA)
	os.WriteFile(e.fails, []byte("error 1\nnoresume"), 0o600)
	stop := e.start()
	if p := e.next(t); !strings.HasPrefix(p, "echo: triage a @ ") {
		t.Fatalf("posted %q", p)
	}
	e.settled(t, stop)
	if rs := e.requests(t); len(rs) != 3 || !rs[1].Resume || rs[2].Resume {
		t.Fatalf("runs: %+v", rs)
	}
}

func TestRecoveryNotQuietSaysWhenItRunsAgain(t *testing.T) {
	e := newRecoveryE2E(t, Recovery{On: []string{"error"}, RetryAfterSec: 1}, alertA)
	os.WriteFile(e.fails, []byte("error 1"), 0o600)
	stop := e.start()
	if p := e.next(t); !strings.Contains(p, "**Run failed:** boom") || !strings.Contains(p, "↻ It runs again on its own at ") || !strings.Contains(p, "(run 2 of at most 2)") {
		t.Fatalf("first post %q", p)
	}
	if p := e.next(t); !strings.HasPrefix(p, "echo: triage a @ ") {
		t.Fatalf("second post %q", p)
	}
	e.settled(t, stop)
}

func TestRecoveryLeavesOtherEndsAlone(t *testing.T) {
	// "error" is not in on: the failure is the reply, as without any recovery.
	e := newRecoveryE2E(t, Recovery{On: []string{"stall"}, RetryAfterSec: 1, Quiet: true}, alertA)
	os.WriteFile(e.fails, []byte("error 9"), 0o600)
	stop := e.start()
	if p := e.next(t); !strings.Contains(p, "**Run failed:** boom") || strings.Contains(p, "This was run") {
		t.Fatalf("posted %q", p)
	}
	e.settled(t, stop)
	if rs := e.requests(t); len(rs) != 1 {
		t.Fatalf("runs: %+v", rs)
	}
}

func TestRecoveryAfterTheWorkerWasKilled(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attempts int // runs before the one that was killed
		want     string
	}{
		{"first run", 0, "echo: go on @ "},
		{"last run", 1, "**Gave up** on `alerts` task `a` after 2 run(s): the last one was cut off when the worker stopped."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRecoveryE2E(t, Recovery{On: []string{"shutdown"}, Resume: true, Quiet: true, ResumePrompt: "go on"})
			// What a worker killed mid-run leaves behind: the entry still says "running".
			e.w.pending["alerts/a"] = &pendingRun{Source: "trigger", Trigger: "alerts", Key: "a", RoomID: "R1", Text: "triage a",
				Session: "11111111-1111-4111-8111-111111111111", Harness: "claude", Attempts: tc.attempts, First: time.Now(), State: "running"}
			stop := e.start()
			if p := e.next(t); !strings.HasPrefix(p, tc.want) {
				t.Fatalf("posted %q", p)
			}
			e.settled(t, stop)
			if tc.attempts == 0 {
				if rs := e.requests(t); len(rs) != 1 || !rs[0].Resume || rs[0].SessionID != "11111111-1111-4111-8111-111111111111" {
					t.Fatalf("runs: %+v", rs)
				}
			} else if _, err := os.Stat(e.f.log); err == nil {
				t.Fatal("a task out of runs ran again")
			}
		})
	}
}

// ledgerWorker is a worker whose trigger requests go to a counter, with a clock the test moves.
func ledgerWorker(t *testing.T, rec Recovery) (w *Worker, asked *atomic.Int32, now *time.Time) {
	t.Helper()
	asked = new(atomic.Int32)
	hs := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		fmt.Fprint(rw, `{"id":"x"}`)
	}))
	t.Cleanup(hs.Close)
	f := newFixture(t)
	f.cfg.ServerURL = hs.URL
	f.cfg.Triggers = []Trigger{{Name: "alerts"}}
	f.cfg.Recovery = map[string]Recovery{"trigger": rec}
	if err := f.cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.setIdentity(Identity{DeviceID: "d", DeviceKey: seal.NewPrivateKey().Bytes(), ServerKey: seal.NewPrivateKey().PublicKey().Bytes()})
	clock := time.Now()
	w.now = func() time.Time { return clock }
	return w, asked, &clock
}

func TestLedgerNoticeResendAndGiveUp(t *testing.T) {
	w, asked, now := ledgerWorker(t, Recovery{On: []string{"stall"}, MaxAttempts: 3, RetryAfterSec: 600, NoticeAfterSec: 3600, GiveUpAfterSec: 7200})
	ctx := context.Background()
	task := proto.Task{ID: "t1", RoomID: "R1", Trigger: "alerts", Key: "a", Text: "triage a"}
	step := func(d time.Duration, want int32) {
		t.Helper()
		*now = now.Add(d)
		w.recoverOnce(ctx)
		if got := asked.Load(); got != want {
			t.Fatalf("asked the server %d times, want %d", got, want)
		}
	}
	w.pending["alerts/a"] = &pendingRun{Trigger: "alerts", Key: "a", RoomID: "R1", Text: "triage a", Attempts: 1, First: *now, Why: endStall, State: "waiting", Next: now.Add(10 * time.Minute)}

	step(9*time.Minute, 0) // not due yet
	step(time.Minute, 1)   // due: asks for the next run
	step(4*time.Minute, 1) // still within the time a task may take to come back
	step(2*time.Minute, 2) // it never came back: asks again
	if e := w.pending["alerts/a"]; e.State != "sent" || e.Want != "run" {
		t.Fatalf("entry: %+v", e)
	}
	// The run comes back and is admitted past the dedupe window; a second copy is refused.
	w.marks["ran/alerts/a"] = *now
	if _, retry, ok, err := w.claim(task); !ok || !retry || err != nil {
		t.Fatalf("claim of a retry: retry=%v ok=%v err=%v", retry, ok, err)
	}
	if _, _, ok, err := w.claim(task); ok || err == nil {
		t.Fatal("a second task for a key that is queued here was admitted")
	}
	// That run stalls too, with one more allowed: the notice goes out once, an hour after
	// the task first arrived, and says when the next run is.
	e := w.pending["alerts/a"]
	e.State, e.Attempts, e.Next = "waiting", 2, now.Add(50*time.Minute)
	step(30*time.Minute, 2)
	step(15*time.Minute, 3)
	reply, _, ok, err := w.claim(task)
	if ok || err != nil || !strings.HasPrefix(reply, "⏳ `alerts` task `a` has no result yet: a run stalled. Its next run is due ") {
		t.Fatalf("notice: %q ok=%v err=%v", reply, ok, err)
	}
	step(time.Minute, 3) // only once
	// Past give_up_after_sec the next run is not asked for; the final reply is.
	step(2*time.Hour, 4)
	if e.State != "sent" || e.Want != "final" {
		t.Fatalf("entry: %+v", e)
	}
	reply, _, ok, err = w.claim(task)
	if ok || err != nil || !strings.HasPrefix(reply, "**Gave up** on `alerts` task `a` after 2 run(s): the last one stalled.") {
		t.Fatalf("final: %q ok=%v err=%v", reply, ok, err)
	}
	if len(w.pending) != 0 {
		t.Fatalf("ledger: %+v", w.pending)
	}
	step(time.Hour, 4)
}

func TestLedgerNoticeOnlyAfterARunEndedEarly(t *testing.T) {
	w, asked, now := ledgerWorker(t, Recovery{On: []string{"stall"}, NoticeAfterSec: 60})
	w.pending["alerts/a"] = &pendingRun{Trigger: "alerts", Key: "a", RoomID: "R1", Text: "triage a", First: *now, State: "running"}
	*now = now.Add(time.Hour)
	w.recoverOnce(context.Background())
	if asked.Load() != 0 {
		t.Fatal("a first run that is still going got a notice")
	}
}

func TestLimitWaitsForTheResetWithoutUsingARun(t *testing.T) {
	limit := filepath.Join(t.TempDir(), "limit.json")
	t.Setenv("AGENTLINK_STUB_LIMIT", limit)
	// Quiet alone says nothing about the limit either.
	wq, _, _ := ledgerWorker(t, Recovery{On: []string{"limit"}, Quiet: true})
	os.WriteFile(limit, []byte(`{"type":"five_hour","resets_at":4102444800}`), 0o600)
	first := proto.Task{ID: "t0", RoomID: "R1", Trigger: "alerts", Key: "a", Text: "triage a"}
	if err := wq.claimOrAdmit(first); err != nil {
		t.Fatal(err)
	}
	if reply := wq.handle(context.Background(), first); reply != "" || wq.pending["alerts/a"] == nil {
		t.Fatalf("quiet reply at a limit: %q %+v", reply, wq.pending)
	}
	w, _, now := ledgerWorker(t, Recovery{On: []string{"limit"}, Quiet: true, AnnounceLimit: true})
	reset := now.Add(3 * time.Hour).Truncate(time.Second)
	os.WriteFile(limit, []byte(fmt.Sprintf(`{"type":"five_hour","resets_at":%d}`, reset.Unix())), 0o600)
	ctx := context.Background()
	run := func(id string) string {
		task := proto.Task{ID: id, RoomID: "R1", Trigger: "alerts", Key: "a", Text: "triage a"}
		if _, retry, ok, err := w.claim(task); !ok || err != nil {
			t.Fatalf("claim: %v", err)
		} else if err := w.admitTrigger(w.trigger("alerts"), task, retry); err != nil {
			t.Fatal(err)
		}
		return w.handle(ctx, task)
	}
	// The run that hits the limit counts; the room is told about the limit once, as before.
	if reply := run("t1"); !strings.Contains(reply, "hit its 5-hour limit") {
		t.Fatalf("first reply: %q", reply)
	}
	e := w.pending["alerts/a"]
	if e == nil || e.State != "waiting" || !e.Next.Equal(reset) || e.Attempts != 1 || e.Why != endLimit {
		t.Fatalf("entry: %+v", e)
	}
	// A task that arrives while the harness rests does not run, says nothing, and uses no run.
	if reply := run("t2"); reply != "" {
		t.Fatalf("reply while resting: %q", reply)
	}
	if e.State != "waiting" || !e.Next.Equal(reset) || e.Attempts != 1 {
		t.Fatalf("entry: %+v", e)
	}
	os.Remove(limit)
	*now = reset.Add(time.Second)
	if reply := run("t3"); !strings.HasPrefix(reply, "echo: triage a") || len(w.pending) != 0 {
		t.Fatalf("after the reset: %q %+v", reply, w.pending)
	}
}

func TestShutdownBeforeRunning(t *testing.T) {
	task := proto.Task{ID: "t1", RoomID: "R1", Trigger: "alerts", Key: "a", Text: "triage a"}
	w, _, _ := ledgerWorker(t, Recovery{On: []string{"shutdown"}})
	w.admitTrigger(w.trigger("alerts"), task, false)
	if r := w.unrun(task); r != "" || w.pending["alerts/a"].State != "queued" {
		t.Fatalf("a task that runs again after the restart: %q %+v", r, w.pending)
	}
	w.adopt()
	if e := w.pending["alerts/a"]; e.State != "waiting" || e.Attempts != 0 {
		t.Fatalf("adopted: %+v", e)
	}
	w, _, _ = ledgerWorker(t, Recovery{On: []string{"error"}})
	w.admitTrigger(w.trigger("alerts"), task, false)
	if r := w.unrun(task); !strings.Contains(r, "shut down before running") || len(w.pending) != 0 {
		t.Fatalf("shutdown not in on: %q %+v", r, w.pending)
	}
	if r := w.unrun(proto.Task{ID: "t2", RoomID: "R1", UserID: "U1", Text: "hi"}); !strings.Contains(r, "shut down before running") {
		t.Fatalf("a mention: %q", r)
	}
}
