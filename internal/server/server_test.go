package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agentlink/internal/proto"
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
		SigningSecret:     secret,
		EnrollToken:       "enroll",
		StatePath:         filepath.Join(t.TempDir(), "state.json"),
		ResponseURLPrefix: ft.URL + "/hooks/response/",
		PollWaitSec:       1,
		BotNames:          []string{"Agent Link"},
	}
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(hs.Close)
	return s, hs.URL, ft
}

func signedPost(t *testing.T, base string, body []byte, ts time.Time) *http.Response {
	t.Helper()
	tss := strconv.FormatInt(ts.Unix(), 10)
	req, _ := http.NewRequest(http.MethodPost, base+"/tanka/events", bytes.NewReader(body))
	req.Header.Set(tanka.HeaderTimestamp, tss)
	req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte(secret), tss, body))
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
	if resp := signedPost(t, base, body, time.Now()); resp.StatusCode != http.StatusOK {
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

func register(t *testing.T, base string) proto.RegisterResponse {
	t.Helper()
	var reg proto.RegisterResponse
	if code := api(t, http.MethodPost, base+"/api/v1/register", "enroll", proto.RegisterRequest{Name: "laptop"}, &reg); code != http.StatusOK {
		t.Fatalf("register: HTTP %d", code)
	}
	return reg
}

func TestURLVerificationAndSignatures(t *testing.T) {
	_, base, _ := newTestServer(t, nil)
	body := []byte(`{"type":"url_verification","challenge":"abc123"}`)

	req, _ := http.NewRequest(http.MethodPost, base+"/tanka/events", bytes.NewReader(body))
	tss := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(tanka.HeaderTimestamp, tss)
	req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte(secret), tss, body))
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

	if resp := signedPost(t, base, body, time.Now().Add(-10*time.Minute)); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("stale timestamp: HTTP %d, want 401", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPost, base+"/tanka/events", bytes.NewReader(body))
	req.Header.Set(tanka.HeaderTimestamp, tss)
	req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte("wrong"), tss, body))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong secret: HTTP %d, want 401", resp.StatusCode)
	}
}

func TestMentionRoutingAndResult(t *testing.T) {
	_, base, ft := newTestServer(t, nil)

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link hello"))
	if p := ft.next(t); !strings.Contains(p.text, "not paired") {
		t.Fatalf("unpaired room reply: %q", p.text)
	}

	reg := register(t, base)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+reg.DeviceID))
	if p := ft.next(t); !strings.Contains(p.text, "Paired this room with device `"+reg.DeviceID+"`") {
		t.Fatalf("pair reply: %q", p.text)
	}

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link hi"))
	if p := ft.next(t); !strings.Contains(p.text, "offline") {
		t.Fatalf("offline device reply: %q", p.text)
	}

	if code := api(t, http.MethodGet, base+"/api/v1/poll", reg.DeviceToken, nil, nil); code != http.StatusNoContent {
		t.Fatalf("empty poll: HTTP %d", code)
	}
	if code := api(t, http.MethodGet, base+"/api/v1/poll", "bogus", nil, nil); code != http.StatusUnauthorized {
		t.Fatalf("bad token poll: HTTP %d", code)
	}

	ev := mention(ft, "R1", "U1", "<@BOT1> do the thing")
	sendEvent(t, base, ev)
	sendEvent(t, base, ev) // Tanka retry with the same event_id
	var task proto.Task
	if code := api(t, http.MethodGet, base+"/api/v1/poll", reg.DeviceToken, nil, &task); code != http.StatusOK {
		t.Fatalf("poll: HTTP %d", code)
	}
	if task.Text != "do the thing" || task.RoomID != "R1" || task.UserID != "U1" {
		t.Fatalf("task = %+v", task)
	}
	if code := api(t, http.MethodGet, base+"/api/v1/poll", reg.DeviceToken, nil, nil); code != http.StatusNoContent {
		t.Fatalf("retried event was queued twice: HTTP %d", code)
	}

	resultURL := base + "/api/v1/tasks/" + task.ID + "/result"
	if code := api(t, http.MethodPost, resultURL, reg.DeviceToken, proto.Result{Text: "done"}, nil); code != http.StatusOK {
		t.Fatalf("result: HTTP %d", code)
	}
	if p := ft.next(t); p.text != "done" || p.path != "/hooks/response/tok" {
		t.Fatalf("result reply: %+v", p)
	}
	if code := api(t, http.MethodPost, resultURL, reg.DeviceToken, proto.Result{Text: "again"}, nil); code != http.StatusNotFound {
		t.Fatalf("second result: HTTP %d, want 404", code)
	}

	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link unpair"))
	if p := ft.next(t); !strings.Contains(p.text, "Unpaired") {
		t.Fatalf("unpair reply: %q", p.text)
	}
}

func TestBindingsSurviveRestart(t *testing.T) {
	s, base, ft := newTestServer(t, nil)
	reg := register(t, base)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+reg.DeviceID))
	ft.next(t)

	s2, err := New(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if b := s2.st.Bindings["R1"]; b == nil || b.DeviceID != reg.DeviceID {
		t.Fatalf("binding after restart: %+v", b)
	}
	if strings.Contains(fmt.Sprint(s2.st.Devices[reg.DeviceID]), reg.DeviceToken) {
		t.Fatal("device token stored in plain text")
	}
}

func TestAllowedUsers(t *testing.T) {
	_, base, ft := newTestServer(t, func(c *Config) { c.AllowedUsers = []string{"U1"} })
	sendEvent(t, base, mention(ft, "R1", "U2", "@Agent Link help"))
	if p := ft.next(t); !strings.Contains(p.text, "`U2` is not allowed") {
		t.Fatalf("reply: %q", p.text)
	}
}

func TestLeaseExpiryAndWebhookFallback(t *testing.T) {
	s, base, ft := newTestServer(t, func(c *Config) {
		c.RoomWebhooks = map[string]string{"R1": strings.TrimSuffix(c.ResponseURLPrefix, "/hooks/response/") + "/hooks/webhook"}
	})

	reg := register(t, base)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+reg.DeviceID))
	ft.next(t)
	api(t, http.MethodGet, base+"/api/v1/poll", reg.DeviceToken, nil, nil)

	ev := mention(ft, "R1", "U1", "@Agent Link long job")
	ev.Event.ResponseURL = "https://elsewhere.example/hooks/response/tok" // outside response_url_prefix
	sendEvent(t, base, ev)
	var task proto.Task
	if code := api(t, http.MethodGet, base+"/api/v1/poll", reg.DeviceToken, nil, &task); code != http.StatusOK {
		t.Fatalf("poll: HTTP %d", code)
	}

	s.reapExpired(time.Now().Add(time.Hour))
	if p := ft.next(t); p.path != "/hooks/webhook" || !strings.Contains(p.text, "sent no result") {
		t.Fatalf("expiry reply: %+v", p)
	}
	if code := api(t, http.MethodPost, base+"/api/v1/tasks/"+task.ID+"/result", reg.DeviceToken, proto.Result{Text: "late"}, nil); code != http.StatusNotFound {
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
