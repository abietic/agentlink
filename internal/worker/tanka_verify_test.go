package worker

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

const testTankaSecret = "tanka-signing-secret"

// apiCapture stands in for the server on a worker's result and trigger calls, and records the
// results it is sent.
type apiCapture struct {
	*httptest.Server
	mu      sync.Mutex
	results []proto.Envelope
	// The keys the worker under test seals its results with, to open them again.
	serverKey *ecdh.PrivateKey
	deviceKey *ecdh.PublicKey
}

// spoken counts the results that carry text, which the server would post.
func (c *apiCapture) spoken(t *testing.T) int {
	n := 0
	for _, r := range c.opened(t) {
		if r.Text != "" {
			n++
		}
	}
	return n
}

// opened returns the results posted so far, opened.
func (c *apiCapture) opened(t *testing.T) []proto.Result {
	t.Helper()
	var out []proto.Result
	for _, env := range c.posted() {
		plain, err := seal.Open(c.serverKey, c.deviceKey, proto.ResultContext("dev-test", env.ID), env.Sealed)
		if err != nil {
			t.Fatalf("result %s does not open: %v", env.ID, err)
		}
		var res proto.Result
		json.Unmarshal(plain, &res)
		out = append(out, res)
	}
	return out
}

func newAPICapture(t *testing.T) *apiCapture {
	c := &apiCapture{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/result") {
			var env proto.Envelope
			json.NewDecoder(r.Body).Decode(&env)
			c.mu.Lock()
			c.results = append(c.results, env)
			c.mu.Unlock()
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *apiCapture) posted() []proto.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]proto.Envelope(nil), c.results...)
}

// signedEvent is one Tanka app_mention delivery with its body and signature headers.
type signedEvent struct {
	env  tanka.Envelope
	body []byte
	ts   string
	sig  string
}

func newSignedEvent(secret string, now time.Time, mod func(*tanka.Envelope)) signedEvent {
	ev := signedEvent{env: tanka.Envelope{
		Type:     "event_callback",
		EventID:  fmt.Sprintf("ev-%d", now.UnixNano()),
		APIAppID: "A1",
		Event: &tanka.MentionEvent{
			Type: "app_mention", UserID: "U1", UserName: "Someone",
			RoomID: "R1", RoomName: "Room", Text: "@bot hello",
		},
	}}
	if mod != nil {
		mod(&ev.env)
	}
	ev.body, _ = json.Marshal(ev.env)
	ev.ts = strconv.FormatInt(now.Unix(), 10)
	ev.sig = tanka.Sign([]byte(secret), ev.ts, ev.body)
	return ev
}

// task is what the server would hand the worker for this event, with fields it could have
// tampered with; the worker must use the event's instead.
func (ev signedEvent) task(id, room string) proto.Task {
	return proto.Task{ID: id, RoomID: room, UserID: "U1", Text: "hello", Raw: ev.body, RawTS: ev.ts, RawSig: ev.sig}
}

// newVerifiedWorker is a worker with the test signing secret stored, tanka_verify set to
// mode, and a fake server to post results to.
func newVerifiedWorker(t *testing.T, mode string, api *apiCapture) (*fixture, *Worker, time.Time) {
	t.Helper()
	f := newFixture(t)
	f.cfg.ServerURL = api.URL
	f.cfg.TankaVerify = mode
	f.cfg.TankaAppID = "A1"
	f.cfg.TankaMentions = []string{"Agent Link", "bot"}
	if err := SetSecret(f.cfg, "tanka", []byte(testTankaSecret)); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	dev := seal.NewPrivateKey()
	api.serverKey, api.deviceKey = seal.NewPrivateKey(), dev.PublicKey()
	w.setIdentity(Identity{DeviceID: "dev-test", DeviceKey: dev.Bytes(), ServerKey: api.serverKey.PublicKey().Bytes()})
	now := time.Now()
	w.now = func() time.Time { return now }
	return f, w, now
}

func (c *apiCapture) waitResults(t *testing.T, n int) []proto.Envelope {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got := c.posted(); len(got) >= n {
			return got
		}
	}
	t.Fatalf("got %d results, want %d", len(c.posted()), n)
	return nil
}

func runCount(t *testing.T, path string) int {
	t.Helper()
	b, _ := os.ReadFile(path)
	if len(bytes.TrimSpace(b)) == 0 {
		return 0
	}
	return len(bytes.Split(bytes.TrimSpace(b), []byte("\n")))
}

func TestVerifyTankaAcceptsSignedEvent(t *testing.T) {
	api := newAPICapture(t)
	_, w, now := newVerifiedWorker(t, "require", api)
	ev := newSignedEvent(testTankaSecret, now, func(env *tanka.Envelope) {
		env.Event.Text = "@Agent Link do the thing"
		env.Event.SenderType = "bot"
		env.Event.Attachments = []tanka.Attachment{{URL: "https://files.example/a.png", Name: "a.png"}}
		env.Event.Message = &tanka.MessageSnapshot{ReplyMessageID: "m-quoted"}
	})
	task := ev.task("t1", "R1")
	task.Text = "do the thing" // the server's hint, a tail of the signed text
	task.RouteSessionID = "sess-1"
	if err := w.verifyTanka(&task); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if task.RoomID != "R1" || task.UserID != "U1" || task.UserName != "Someone" || task.SenderType != "bot" || task.Text != "do the thing" {
		t.Fatalf("fields after verify: %+v", task)
	}
	if len(task.Attachments) != 1 || task.Attachments[0].Name != "a.png" {
		t.Fatalf("attachments after verify: %+v", task.Attachments)
	}
	if task.ReplyMessageID != "m-quoted" || task.RouteSessionID != "sess-1" {
		t.Fatalf("quoted message: %+v", task)
	}

	// Without a quote in the signed event, the server's session hint is not used.
	plain := newSignedEvent(testTankaSecret, now.Add(time.Second), nil).task("t2", "R1")
	plain.RouteSessionID = "sess-injected"
	if err := w.verifyTanka(&plain); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if plain.RouteSessionID != "" {
		t.Fatalf("a session hint without a quoted message was kept: %q", plain.RouteSessionID)
	}
}

func TestVerifyTankaRejects(t *testing.T) {
	api := newAPICapture(t)
	_, w, now := newVerifiedWorker(t, "require", api)
	ev := newSignedEvent(testTankaSecret, now, func(env *tanka.Envelope) {
		env.Event.Text = "@bot delete everything"
	})
	tampered := ev
	tampered.body = bytes.ReplaceAll(ev.body, []byte("delete everything"), []byte("delete nothing"))

	for _, tc := range []struct {
		name string
		task proto.Task
		want string
	}{
		{"tampered body", tampered.task("b", "R1"), "signature"},
		{"wrong room", ev.task("r", "R2"), "another room"},
		{"missing raw", proto.Task{ID: "n", RoomID: "R1", UserID: "U1", Text: "hello"}, "no Tanka event"},
	} {
		if err := w.verifyTanka(&tc.task); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}

	// An expired event, signed correctly with its own old timestamp.
	old := newSignedEvent(testTankaSecret, now.Add(-2*time.Hour), nil)
	expired := old.task("x", "R1")
	if err := w.verifyTanka(&expired); err == nil || !strings.Contains(err.Error(), "window") {
		t.Fatalf("expired event: err = %v", err)
	}

	// An event for another app id.
	w.cfg.TankaAppID = "A2"
	appTask := newSignedEvent(testTankaSecret, now.Add(2*time.Second), nil).task("a", "R1")
	if err := w.verifyTanka(&appTask); err == nil || !strings.Contains(err.Error(), "app id") {
		t.Fatalf("wrong app: err = %v", err)
	}

	// A repeat of an event already handled.
	w.cfg.TankaAppID = "A1"
	first := ev.task("d1", "R1")
	if err := w.verifyTanka(&first); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	second := ev.task("d2", "R1")
	if err := w.verifyTanka(&second); err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("a replay was accepted: %v", err)
	}

	// An event without an id could be replayed unnoticed, so it is refused.
	noID := newSignedEvent(testTankaSecret, now.Add(3*time.Second), func(env *tanka.Envelope) { env.EventID = "" }).task("e", "R1")
	if err := w.verifyTanka(&noID); err == nil || !strings.Contains(err.Error(), "event_id") {
		t.Fatalf("an event without event_id: err = %v", err)
	}
}

// TestPromptFromSignedTextOnly checks that the prompt is cut out of the signed text by the
// worker's own mention rule, so a server cannot drop words (say a leading "do not") by
// sending a shorter tail of the signed text as its stripped copy.
func TestPromptFromSignedTextOnly(t *testing.T) {
	api := newAPICapture(t)
	_, w, now := newVerifiedWorker(t, "require", api)
	for i, tc := range []struct {
		sender, raw, hint, want string
	}{
		{"", "@Agent Link do not delete the branch", "delete the branch", "do not delete the branch"},
		{"", "@bot do not deploy", "deploy", "do not deploy"},
		{"", "<@U99> do not deploy", "deploy", "do not deploy"},
		// A bot's words before the mention are for its own reader, as on the server.
		{"bot", "Over to you. @Agent Link run the tests", "", "run the tests"},
	} {
		task := newSignedEvent(testTankaSecret, now.Add(time.Duration(i)*time.Second), func(env *tanka.Envelope) {
			env.Event.Text, env.Event.SenderType = tc.raw, tc.sender
		}).task(fmt.Sprintf("p%d", i), "R1")
		task.Text = tc.hint
		if err := w.verifyTanka(&task); err != nil {
			t.Fatalf("%q: verify: %v", tc.raw, err)
		}
		if task.Text != tc.want {
			t.Errorf("%q with hint %q: prompt %q, want %q", tc.raw, tc.hint, task.Text, tc.want)
		}
	}
}

func TestTankaVerifyNeedsMentions(t *testing.T) {
	c := Config{TankaVerify: "observe", WorkdirRoots: []string{t.TempDir()}}
	if err := c.applyDefaults(); err == nil || !strings.Contains(err.Error(), "tanka_mentions") {
		t.Fatalf("tanka_verify without tanka_mentions: err = %v", err)
	}
}

// TestRequireDropsUnverified checks that in require mode a task that does not check out posts
// no reply and is counted, so a forged task never reaches the room.
func TestRequireDropsUnverified(t *testing.T) {
	api := newAPICapture(t)
	f, w, now := newVerifiedWorker(t, "require", api)
	ev := newSignedEvent(testTankaSecret, now, nil)
	tampered := ev
	tampered.body = bytes.ReplaceAll(ev.body, []byte("hello"), []byte("hellp"))

	for _, task := range []proto.Task{
		{ID: "no-raw", RoomID: "R1", UserID: "U1", Text: "hello"},
		tampered.task("tampered", "R1"),
	} {
		w.dispatch(context.Background(), task)
	}
	// Each gets an empty result, which the server posts nothing for, so it does not time out
	// and get announced as dropped either.
	api.waitResults(t, 2)
	for _, res := range api.opened(t) {
		if res.Text != "" || res.Confirm {
			t.Fatalf("a rejected task got result %+v", res)
		}
	}
	if runCount(t, f.log) != 0 {
		t.Fatal("a rejected task was run")
	}
	if w.tankaFailed != 2 || w.tankaRejected != 2 {
		t.Fatalf("failed = %d, rejected = %d, want 2 and 2", w.tankaFailed, w.tankaRejected)
	}
	if status := w.query("R1", "status"); !strings.Contains(status, "Tanka verification** `require` · 2 failed · 2 rejected") {
		t.Fatalf("status does not show the counts:\n%s", status)
	}
}

// TestObserveRunsAndCounts checks that in observe mode a task that does not check out is
// counted and logged but still run.
func TestObserveRunsAndCounts(t *testing.T) {
	api := newAPICapture(t)
	f, w, _ := newVerifiedWorker(t, "observe", api)
	w.dispatch(context.Background(), proto.Task{ID: "no-raw", RoomID: "R1", UserID: "U1", Text: "hello"})
	api.waitResults(t, 1)
	if runCount(t, f.log) != 1 {
		t.Fatal("an unverified task was not run in observe mode")
	}
	if w.tankaFailed != 1 || w.tankaRejected != 0 {
		t.Fatalf("failed = %d, rejected = %d, want 1 and 0", w.tankaFailed, w.tankaRejected)
	}
}

// TestTankaReplaySurvivesRestart checks that an event already handled is dropped after a
// worker restart, so a replay by the server does not run twice.
func TestTankaReplaySurvivesRestart(t *testing.T) {
	api := newAPICapture(t)
	_, w, now := newVerifiedWorker(t, "require", api)
	ev := newSignedEvent(testTankaSecret, now, nil)
	w.dispatch(context.Background(), ev.task("first", "R1"))
	api.waitResults(t, 1)

	w2, err := New(w.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w2.setIdentity(w.id)
	w2.now = func() time.Time { return now }
	w2.dispatch(context.Background(), ev.task("replay", "R1"))
	api.waitResults(t, 2)
	if n := api.spoken(t); n != 1 {
		t.Fatalf("a replay was answered after the restart: %d non-empty result(s)", n)
	}
	if w2.tankaRejected != 1 {
		t.Fatalf("rejected after restart = %d, want 1", w2.tankaRejected)
	}
}

// TestBotSenderComesFromRaw checks that allowed_bots is checked against the sender in the
// signed event, not the sender the server claims.
func TestBotSenderComesFromRaw(t *testing.T) {
	api := newAPICapture(t)
	f, w, now := newVerifiedWorker(t, "require", api)
	const bot = "6a7d7188238a191436b929db"
	w.cfg.AllowedBots = []string{bot}

	allowed := newSignedEvent(testTankaSecret, now, func(env *tanka.Envelope) {
		env.Event.UserID, env.Event.SenderType, env.Event.Text = bot, "bot", "@bot handed work"
	}).task("bot-ok", "R1")
	allowed.UserID = "U1" // the server claims a person sent it; the event says a bot
	w.dispatch(context.Background(), allowed)
	api.waitResults(t, 1)
	if runCount(t, f.log) != 1 {
		t.Fatal("a bot in allowed_bots was not run")
	}

	// A bot not in either list gets no run: its task, though the server names an allowed
	// person, is refused.
	stranger := newSignedEvent(testTankaSecret, now.Add(time.Second), func(env *tanka.Envelope) {
		env.Event.UserID, env.Event.SenderType, env.Event.Text = "6a7d7188238a191436b929dc", "bot", "@bot hi"
	}).task("bot-no", "R1")
	stranger.UserID = "U1"
	w.dispatch(context.Background(), stranger)
	api.waitResults(t, 2)
	if runCount(t, f.log) != 1 {
		t.Fatal("a bot outside allowed_bots was run")
	}
}

// TestTriggerLedger checks that only a trigger task this worker submitted is accepted back,
// that it is accepted once, and that it survives a restart.
func TestTriggerLedger(t *testing.T) {
	api := newAPICapture(t)
	f, w, _ := newVerifiedWorker(t, "require", api)
	f.cfg.Triggers = []Trigger{{Name: "alerts"}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w.cfg.Triggers = f.cfg.Triggers
	w.cfg.Digest = &Digest{RoomID: "R1", EverySec: 60}

	// A task the server made itself has no record: refused, no reply.
	w.dispatch(context.Background(), proto.Task{ID: "forged", RoomID: "R1", Trigger: "alerts", Key: "forged", Text: "rm -rf"})
	api.waitResults(t, 1)
	if n := api.spoken(t); n != 0 {
		t.Fatalf("a forged trigger task was answered: %d non-empty result(s)", n)
	}
	if runCount(t, f.log) != 0 || w.tankaRejected != 1 {
		t.Fatalf("forged trigger: runs %d, rejected %d", runCount(t, f.log), w.tankaRejected)
	}

	// A task this worker submitted is accepted, once.
	ctx := context.Background()
	if err := w.submit(ctx, "R1", "alerts", "k1", "do the thing"); err != nil {
		t.Fatal(err)
	}
	w.dispatch(ctx, proto.Task{ID: "legit", RoomID: "R1", Trigger: "alerts", Key: "k1", Text: "do the thing"})
	api.waitResults(t, 2)
	if runCount(t, f.log) != 1 {
		t.Fatal("a submitted trigger task was not run")
	}
	w.dispatch(ctx, proto.Task{ID: "replay", RoomID: "R1", Trigger: "alerts", Key: "k1", Text: "do the thing"})
	api.waitResults(t, 3)
	if n := api.spoken(t); n != 1 || runCount(t, f.log) != 1 {
		t.Fatalf("a trigger task was accepted twice: %d non-empty result(s)", n)
	}

	// The record outlives a restart, and is still good for one task only.
	if err := w.submit(ctx, "R1", "alerts", "k2", "after restart"); err != nil {
		t.Fatal(err)
	}
	w2, err := New(w.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w2.setIdentity(w.id)
	w2.dispatch(ctx, proto.Task{ID: "restarted", RoomID: "R1", Trigger: "alerts", Key: "k2", Text: "after restart"})
	api.waitResults(t, 4)
	if runCount(t, f.log) != 2 {
		t.Fatal("a submitted trigger did not survive the restart")
	}
	w2.dispatch(ctx, proto.Task{ID: "restarted-again", RoomID: "R1", Trigger: "alerts", Key: "k2", Text: "after restart"})
	api.waitResults(t, 5)
	if n := api.spoken(t); n != 2 || runCount(t, f.log) != 2 {
		t.Fatal("a trigger task was accepted twice after the restart")
	}

	// The digest's own submission is covered the same way: posted without a run.
	if err := w.submit(ctx, "R1", digestTrigger, "2026-11-27T00:00:00Z", "digest text"); err != nil {
		t.Fatal(err)
	}
	w.dispatch(ctx, proto.Task{ID: "digest", RoomID: "R1", Trigger: digestTrigger, Key: "2026-11-27T00:00:00Z", Text: "digest text"})
	api.waitResults(t, 6)
	if runCount(t, f.log) != 2 {
		t.Fatal("the digest was run as a prompt")
	}
}

// TestOffModeUnchanged checks the default: no secret, no verification, a task without a raw
// event still runs as before.
func TestOffModeUnchanged(t *testing.T) {
	api := newAPICapture(t)
	f := newFixture(t)
	f.cfg.ServerURL = api.URL
	w, err := New(f.cfg) // no secret, tanka_verify ""
	if err != nil {
		t.Fatal(err)
	}
	w.setIdentity(Identity{DeviceID: "dev-test", DeviceKey: seal.NewPrivateKey().Bytes(), ServerKey: seal.NewPrivateKey().PublicKey().Bytes()})
	w.dispatch(context.Background(), proto.Task{ID: "plain", RoomID: "R1", UserID: "U1", Text: "hello"})
	api.waitResults(t, 1)
	if runCount(t, f.log) != 1 || w.tankaFailed != 0 {
		t.Fatalf("off mode changed: runs %d, failed %d", runCount(t, f.log), w.tankaFailed)
	}
}

// TestSetSecretRoundTrip checks that the secret is stored encrypted, read back, removed by an
// empty value, required at startup, and never logged.
func TestSetSecretRoundTrip(t *testing.T) {
	f := newFixture(t)
	const value = "s3cr3t-value-should-not-leak"
	var logs bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldWriter)

	if err := SetSecret(f.cfg, "tanka", []byte(value)); err != nil {
		t.Fatal(err)
	}
	f.cfg.TankaVerify = "observe"
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(w.tankaSecret) != value {
		t.Fatalf("read back secret %q", w.tankaSecret)
	}

	// A task that fails verification logs its reason, never the secret.
	task := proto.Task{ID: "no-raw", RoomID: "R1", UserID: "U1", Text: "hello"}
	if !w.checkTanka(&task) {
		t.Fatal("observe mode dropped an unverified task")
	}

	if dbfile := filepath.Join(f.cfg.StateDir, "worker.db"); func() bool {
		b, _ := os.ReadFile(dbfile)
		return bytes.Contains(b, []byte(value))
	}() {
		t.Fatal("worker.db holds the secret in plain text")
	}
	if strings.Contains(logs.String(), value) {
		t.Fatalf("the secret reached the log:\n%s", logs.String())
	}

	// An empty value removes it; tanka_verify then refuses to start without one.
	if err := SetSecret(f.cfg, "tanka", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := New(f.cfg); err == nil || !strings.Contains(err.Error(), "set-secret tanka") {
		t.Fatalf("startup without a secret: err = %v", err)
	}
	if err := SetSecret(f.cfg, "other", []byte(value)); err == nil {
		t.Fatal("an unknown secret name was accepted")
	}
}

// TestConfirmCommand checks that a pair or unpair the server forwards is confirmed only for a
// verified event from a person in allowed_users, naming this device for pair, and is never run.
func TestConfirmCommand(t *testing.T) {
	api := newAPICapture(t)
	f, w, now := newVerifiedWorker(t, "require", api)
	cases := []struct {
		name, cmd, text, user, sender string
		tamper                        bool
		want                          bool
	}{
		{"pair this device", "pair", "@bot pair dev-test", "U1", "", false, true},
		{"pair another device", "pair", "@bot pair dev-other", "U1", "", false, false},
		{"unpair", "unpair", "@bot unpair", "U1", "", false, true},
		{"unpair by a stranger", "unpair", "@bot unpair", "U9", "", false, false},
		{"pair by a bot", "pair", "@bot pair dev-test", "U1", "bot", false, false},
		{"text says otherwise", "unpair", "@bot pair dev-test", "U1", "", false, false},
		{"forged event", "pair", "@bot pair dev-test", "U1", "", true, false},
	}
	for i, tc := range cases {
		ev := newSignedEvent(testTankaSecret, now.Add(time.Duration(i)*time.Second), func(env *tanka.Envelope) {
			env.Event.Text, env.Event.UserID, env.Event.SenderType = tc.text, tc.user, tc.sender
		})
		if tc.tamper {
			ev.sig = tanka.Sign([]byte("not-the-secret"), ev.ts, ev.body)
		}
		task := ev.task(fmt.Sprintf("c%d", i), "R1")
		task.UserID, task.Command = "U1", tc.cmd // the server's claims, which must not count
		w.dispatch(context.Background(), task)
		api.waitResults(t, i+1)
		res := api.opened(t)[i]
		if res.Confirm != tc.want || res.Text != "" {
			t.Errorf("%s: result %+v, want confirm %v", tc.name, res, tc.want)
		}
	}
	if runCount(t, f.log) != 0 {
		t.Fatal("a command was run as a prompt")
	}
}
