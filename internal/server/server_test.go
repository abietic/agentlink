package server

import (
	"bytes"
	"crypto/ecdh"
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
	"sync/atomic"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

const secret = "test-signing-secret"

type posted struct {
	path string
	text string
}

type fakeTanka struct {
	*httptest.Server
	msgs chan posted
}

func newFakeTanka(t *testing.T) *fakeTanka {
	f := &fakeTanka{msgs: make(chan posted, 16)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m tanka.Message
		json.NewDecoder(r.Body).Decode(&m)
		f.msgs <- posted{r.URL.Path, m.Text}
		io.WriteString(w, `{"code":0}`)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeTanka) next(t *testing.T) posted {
	t.Helper()
	select {
	case p := <-f.msgs:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was posted to Tanka")
		return posted{}
	}
}

func newTestServer(t *testing.T, mod func(*Config)) (*Server, string, *fakeTanka) {
	ft := newFakeTanka(t)
	cfg := Config{
		EnrollToken:       "enroll",
		DBPath:            filepath.Join(t.TempDir(), "server.db"),
		ResponseURLPrefix: ft.URL + "/hooks/response/",
		PollWaitSec:       1,
		Bots: []Bot{
			{Name: "main", SigningSecret: secret, Mentions: []string{"Agent Link"}},
			{Name: "second", SigningSecret: "second-secret", AppID: "A2"},
		},
	}
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(hs.Close)
	return s, hs.URL, ft
}

func signedPost(t *testing.T, url, signing string, body []byte, ts time.Time) *http.Response {
	t.Helper()
	tss := strconv.FormatInt(ts.Unix(), 10)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set(tanka.HeaderTimestamp, tss)
	req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte(signing), tss, body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

var eventSeq atomic.Int64

func mention(ft *fakeTanka, room, user, text string) tanka.Envelope {
	return tanka.Envelope{
		Type:    "event_callback",
		EventID: fmt.Sprintf("ev-%d", eventSeq.Add(1)),
		Event: &tanka.MentionEvent{
			Type: "app_mention", UserID: user, UserName: "someone", RoomID: room, RoomName: "Test room",
			Text: text, ResponseURL: ft.URL + "/hooks/response/tok",
		},
	}
}

func sendEvent(t *testing.T, base string, env tanka.Envelope) {
	t.Helper()
	body, _ := json.Marshal(env)
	if resp := signedPost(t, base+"/tanka/events", secret, body, time.Now()); resp.StatusCode != http.StatusOK {
		t.Fatalf("event: HTTP %d", resp.StatusCode)
	}
}

func api(t *testing.T, method, url, token string, in any, out any) int {
	t.Helper()
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode == http.StatusOK {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

// testDevice is a worker as the server sees it: registered with its own key pair.
type testDevice struct {
	proto.RegisterResponse
	key  *ecdh.PrivateKey
	base string
}

func register(t *testing.T, base, bot string) *testDevice {
	t.Helper()
	d := &testDevice{key: seal.NewPrivateKey(), base: base}
	req := proto.RegisterRequest{Name: "laptop", Bot: bot, PublicKey: d.key.PublicKey().Bytes()}
	if code := api(t, http.MethodPost, base+"/api/v1/register", "enroll", req, &d.RegisterResponse); code != http.StatusOK {
		t.Fatalf("register: HTTP %d", code)
	}
	return d
}

// poll returns the next task (opened with the device key), or nil on 204.
func (d *testDevice) poll(t *testing.T) *proto.Task {
	t.Helper()
	var env proto.Envelope
	code := api(t, http.MethodGet, d.base+"/api/v1/poll", d.DeviceToken, nil, &env)
	if code == http.StatusNoContent {
		return nil
	}
	if code != http.StatusOK {
		t.Fatalf("poll: HTTP %d", code)
	}
	serverKey, _ := seal.ParsePublicKey(d.ServerKey)
	plain, err := seal.Open(d.key, serverKey, proto.TaskContext(d.DeviceID, env.ID), env.Sealed)
	if err != nil {
		t.Fatalf("task %s does not open: %v", env.ID, err)
	}
	var task proto.Task
	json.Unmarshal(plain, &task)
	return &task
}

// result seals text with sender (normally the device key) and posts it.
func (d *testDevice) result(t *testing.T, sender *ecdh.PrivateKey, taskID, text string) int {
	t.Helper()
	serverKey, _ := seal.ParsePublicKey(d.ServerKey)
	plain, _ := json.Marshal(proto.Result{Text: text})
	sealed, err := seal.Seal(sender, serverKey, proto.ResultContext(d.DeviceID, taskID), plain)
	if err != nil {
		t.Fatal(err)
	}
	return api(t, http.MethodPost, d.base+"/api/v1/tasks/"+taskID+"/result", d.DeviceToken, proto.Envelope{ID: taskID, Sealed: sealed}, nil)
}

func TestURLVerificationAndSignatures(t *testing.T) {
	_, base, _ := newTestServer(t, nil)
	body := []byte(`{"type":"url_verification","challenge":"abc123"}`)

	tss := strconv.FormatInt(time.Now().Unix(), 10)
	req, _ := http.NewRequest(http.MethodPost, base+"/tanka/events/second", bytes.NewReader(body))
	req.Header.Set(tanka.HeaderTimestamp, tss)
	req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte("second-secret"), tss, body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if got["challenge"] != "abc123" {
		t.Fatalf("challenge = %q", got["challenge"])
	}

	for name, c := range map[string]struct {
		path, secret string
		ts           time.Time
		want         int
	}{
		"stale timestamp":       {"/tanka/events", secret, time.Now().Add(-10 * time.Minute), http.StatusUnauthorized},
		"wrong secret":          {"/tanka/events", "wrong", time.Now(), http.StatusUnauthorized},
		"another bot's secret":  {"/tanka/events/second", secret, time.Now(), http.StatusUnauthorized},
		"unknown bot":           {"/tanka/events/nope", secret, time.Now(), http.StatusNotFound},
		"default bot, explicit": {"/tanka/events/main", secret, time.Now(), http.StatusOK},
	} {
		if resp := signedPost(t, base+c.path, c.secret, body, c.ts); resp.StatusCode != c.want {
			t.Errorf("%s: HTTP %d, want %d", name, resp.StatusCode, c.want)
		}
	}
}

func TestMentionRoutingAndResult(t *testing.T) {
	_, base, ft := newTestServer(t, nil)

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link hello"))
	if p := ft.next(t); !strings.Contains(p.text, "not paired") {
		t.Fatalf("unpaired room reply: %q", p.text)
	}

	dev := register(t, base, "")
	if dev.Bot != "main" || len(dev.ServerKey) != 32 {
		t.Fatalf("registration: %+v", dev.RegisterResponse)
	}
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	if p := ft.next(t); !strings.Contains(p.text, "Paired this room with device `"+dev.DeviceID+"`") {
		t.Fatalf("pair reply: %q", p.text)
	}

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link hi"))
	if p := ft.next(t); !strings.Contains(p.text, "offline") {
		t.Fatalf("offline device reply: %q", p.text)
	}

	if task := dev.poll(t); task != nil {
		t.Fatalf("empty poll returned %+v", task)
	}
	if code := api(t, http.MethodGet, base+"/api/v1/poll", "bogus", nil, nil); code != http.StatusUnauthorized {
		t.Fatalf("bad token poll: HTTP %d", code)
	}

	ev := mention(ft, "R1", "U1", "<@BOT1> do the thing")
	sendEvent(t, base, ev)
	sendEvent(t, base, ev) // Tanka retry with the same event_id
	task := dev.poll(t)
	if task == nil || task.Text != "do the thing" || task.RoomID != "R1" || task.UserID != "U1" {
		t.Fatalf("task = %+v", task)
	}
	if again := dev.poll(t); again != nil {
		t.Fatalf("retried event was queued twice: %+v", again)
	}

	if code := dev.result(t, seal.NewPrivateKey(), task.ID, "forged"); code != http.StatusBadRequest {
		t.Fatalf("result sealed with another key: HTTP %d, want 400", code)
	}
	if code := dev.result(t, dev.key, task.ID, "done"); code != http.StatusOK {
		t.Fatalf("result: HTTP %d", code)
	}
	if p := ft.next(t); p.text != "done" || p.path != "/hooks/response/tok" {
		t.Fatalf("result reply: %+v", p)
	}
	if code := dev.result(t, dev.key, task.ID, "again"); code != http.StatusNotFound {
		t.Fatalf("second result: HTTP %d, want 404", code)
	}

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link unpair"))
	if p := ft.next(t); !strings.Contains(p.text, "Unpaired") {
		t.Fatalf("unpair reply: %q", p.text)
	}
}

// TestDevicesQueueIndependently checks two devices on two bots: each pairs only through
// its own bot and only ever receives its own rooms' tasks.
func TestDevicesQueueIndependently(t *testing.T) {
	_, base, ft := newTestServer(t, nil)
	a, b := register(t, base, ""), register(t, base, "second")
	if b.Bot != "second" {
		t.Fatalf("bot = %q", b.Bot)
	}
	second := func(room, text string) int {
		env := mention(ft, room, "U1", "@x "+text)
		env.APIAppID = "A2"
		body, _ := json.Marshal(env)
		return signedPost(t, base+"/tanka/events/second", "second-secret", body, time.Now()).StatusCode
	}

	sendEvent(t, base, mention(ft, "R2", "U1", "@Agent Link pair "+b.DeviceID))
	if p := ft.next(t); !strings.Contains(p.text, "set up for bot `second`") {
		t.Fatalf("pairing through the wrong bot: %q", p.text)
	}
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+a.DeviceID))
	ft.next(t)
	if code := second("R2", "pair "+b.DeviceID); code != http.StatusOK {
		t.Fatalf("second bot event: HTTP %d", code)
	}
	if p := ft.next(t); !strings.Contains(p.text, "Paired") {
		t.Fatalf("pair through second bot: %q", p.text)
	}

	a.poll(t)
	b.poll(t) // both online now
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link for a"))
	second("R2", "for b, one")
	second("R2", "for b, two")
	if got := a.poll(t); got == nil || got.Text != "for a" {
		t.Fatalf("device a got %+v", got)
	}
	if got := a.poll(t); got != nil {
		t.Fatalf("device a got b's task %+v", got)
	}
	for _, want := range []string{"for b, one", "for b, two"} {
		if got := b.poll(t); got == nil || got.Text != want {
			t.Fatalf("device b got %+v, want %q", got, want)
		}
	}

	env := mention(ft, "R2", "U1", "@x hi")
	env.APIAppID = "A9"
	body, _ := json.Marshal(env)
	if code := signedPost(t, base+"/tanka/events/second", "second-secret", body, time.Now()).StatusCode; code != http.StatusForbidden {
		t.Fatalf("event for another app id: HTTP %d, want 403", code)
	}
}

func TestStateSurvivesRestartEncrypted(t *testing.T) {
	s, base, ft := newTestServer(t, nil)
	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)
	dev.poll(t)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link kiwi-canary"))
	s.Close()

	s2, err := New(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	hs := httptest.NewServer(s2.Handler())
	defer hs.Close()
	dev.base = hs.URL
	if task := dev.poll(t); task == nil || task.Text != "kiwi-canary" {
		t.Fatalf("queued task after restart: %+v", task)
	}
	for _, p := range []string{s.cfg.DBPath, s.cfg.DBPath + "-wal"} {
		b, _ := os.ReadFile(p)
		for _, plain := range []string{"kiwi-canary", dev.DeviceToken, "/hooks/response/tok"} {
			if bytes.Contains(b, []byte(plain)) {
				t.Errorf("%s holds %q in plain text", filepath.Base(p), plain)
			}
		}
	}
}

func TestAllowedUsers(t *testing.T) {
	_, base, ft := newTestServer(t, func(c *Config) { c.AllowedUsers = []string{"U1"} })
	sendEvent(t, base, mention(ft, "R1", "U2", "@Agent Link help"))
	if p := ft.next(t); !strings.Contains(p.text, "`U2` is not allowed") {
		t.Fatalf("reply: %q", p.text)
	}
}

func TestExpiryAndWebhookFallback(t *testing.T) {
	s, base, ft := newTestServer(t, func(c *Config) {
		c.Bots[0].RoomWebhooks = map[string]string{"R1": strings.TrimSuffix(c.ResponseURLPrefix, "/hooks/response/") + "/hooks/webhook"}
	})

	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)
	dev.poll(t)

	ev := mention(ft, "R1", "U1", "@Agent Link long job")
	ev.Event.ResponseURL = "https://elsewhere.example/hooks/response/tok" // outside response_url_prefix
	sendEvent(t, base, ev)
	task := dev.poll(t)
	if task == nil {
		t.Fatal("no task")
	}
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link never picked up"))

	s.reapExpired(time.Now().Add(time.Hour))
	got := []posted{ft.next(t), ft.next(t)}
	leased, queued := false, false
	for _, p := range got {
		leased = leased || (p.path == "/hooks/webhook" && strings.Contains(p.text, "sent no result"))
		queued = queued || (p.path == "/hooks/response/tok" && strings.Contains(p.text, "did not pick this task up"))
	}
	if !leased || !queued {
		t.Fatalf("expiry replies: %+v", got)
	}
	if code := dev.result(t, dev.key, task.ID, "late"); code != http.StatusNotFound {
		t.Fatalf("late result: HTTP %d, want 404", code)
	}
}

func TestStripMention(t *testing.T) {
	names := []string{"Agent Link"}
	for in, want := range map[string]string{
		"@Agent Link status":  "status",
		"<@B123> cd ~/Code/x": "cd ~/Code/x",
		"@bot fix the test":   "fix the test",
		"  @Agent Link  ":     "",
		"no mention here":     "no mention here",
		"@Agent Link @Lee 看看": "@Lee 看看",
	} {
		if got := stripMention(in, names); got != want {
			t.Errorf("stripMention(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestShape(t *testing.T) {
	var v any
	if err := json.Unmarshal([]byte(`{"type":"event_callback","event":{"text":"secret words","refs":[{"url":"https://x/y","w":240}],"n":null,"ok":true,"tags":[]}}`), &v); err != nil {
		t.Fatal(err)
	}
	want := "{event:{n:null,ok:bool,refs:[{url:string,w:number}]x1,tags:[],text:string},type:string}"
	if got := shape(v); got != want {
		t.Fatalf("shape = %s\nwant    %s", got, want)
	}
}

// TestImageOnlyMention: an @ with an image and no text is a prompt, not a request for help,
// and the attachment links reach the device inside the sealed task.
func TestImageOnlyMention(t *testing.T) {
	_, base, ft := newTestServer(t, nil)
	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)
	dev.poll(t) // online

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link"))
	if p := ft.next(t); !strings.Contains(p.text, "agentlink") {
		t.Fatalf("a bare mention should get help: %q", p.text)
	}

	// As Tanka sends it, with the fields this project does not read.
	body := []byte(`{"type":"event_callback","event_id":"ev-img","event":{"type":"app_mention","user_id":"U1","room_id":"R1",
		"text":"@Agent Link ","response_url":"` + ft.URL + `/hooks/response/tok","attachments":[{"file_id":"f1","file_type":"png",
		"height":10,"width":20,"message_id":"m1","msg_type":2,"name":"shot.png","size":123,"url":"https://gw-tk.tanka.ai/npc/v2/file/abc"}]}}`)
	if resp := signedPost(t, base+"/tanka/events", secret, body, time.Now()); resp.StatusCode != http.StatusOK {
		t.Fatalf("event: HTTP %d", resp.StatusCode)
	}
	task := dev.poll(t)
	want := []proto.Attachment{{URL: "https://gw-tk.tanka.ai/npc/v2/file/abc", Name: "shot.png"}}
	if task == nil || task.Text != "" || !slices.Equal(task.Attachments, want) {
		t.Fatalf("task = %+v", task)
	}
}
