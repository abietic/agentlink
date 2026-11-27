package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

// newWorkerVerifyServer is a test server whose bot "main" has verify "worker" and no secret.
func newWorkerVerifyServer(t *testing.T) (*Server, string, *fakeTanka) {
	return newTestServer(t, func(c *Config) {
		c.Bots[0].SigningSecret, c.Bots[0].Verify = "", "worker"
	})
}

// unsigned posts body to the events endpoint with no signature, as anyone on the internet can.
func unsigned(t *testing.T, base string, body []byte) (int, string) {
	t.Helper()
	resp, err := http.Post(base+"/tanka/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func sendUnsigned(t *testing.T, base string, env tanka.Envelope) int {
	t.Helper()
	body, _ := json.Marshal(env)
	code, _ := unsigned(t, base, body)
	return code
}

// confirm answers a forwarded pair or unpair.
func (d *testDevice) confirm(t *testing.T, taskID string, ok bool) int {
	t.Helper()
	serverKey, _ := seal.ParsePublicKey(d.ServerKey)
	plain, _ := json.Marshal(proto.Result{Confirm: ok, Text: "text a device sends is not posted"})
	sealed, err := seal.Seal(d.key, serverKey, proto.ResultContext(d.DeviceID, taskID), plain)
	if err != nil {
		t.Fatal(err)
	}
	return api(t, http.MethodPost, d.base+"/api/v1/tasks/"+taskID+"/result", d.DeviceToken, proto.Envelope{ID: taskID, Sealed: sealed, Reply: d.replies[taskID]}, nil)
}

func TestWorkerVerifyConfig(t *testing.T) {
	for _, tc := range []struct {
		secret, verify, want string
	}{
		{"s", "worker", "remove signing_secret"},
		{"", "server", "signing_secret"},
		{"", "", "signing_secret"},
		{"s", "both", "verify must be"},
		{"", "worker", ""},
	} {
		c := Config{EnrollToken: "e", Bots: []Bot{{Name: "main", SigningSecret: tc.secret, Verify: tc.verify}}}
		err := c.check()
		if (tc.want == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("secret %q verify %q: err = %v, want %q", tc.secret, tc.verify, err, tc.want)
		}
	}
}

// TestWorkerVerifyMode walks a bot with verify "worker" through what an unsigned event can
// and cannot do: answer url_verification, post nothing for an unpaired room, and pair or
// unpair only with the device's confirmation.
func TestWorkerVerifyMode(t *testing.T) {
	_, base, ft := newWorkerVerifyServer(t)

	code, body := unsigned(t, base, []byte(`{"type":"url_verification","challenge":"c-123"}`))
	if code != http.StatusOK || !strings.Contains(body, `"c-123"`) {
		t.Fatalf("url_verification: HTTP %d %s", code, body)
	}

	// An unpaired room hears nothing, whatever is sent.
	for _, text := range []string{"@Agent Link help", "@Agent Link hello", "@Agent Link unpair", "@Agent Link pair dev-nobody"} {
		if code := sendUnsigned(t, base, mention(ft, "R1", "U1", text)); code != http.StatusOK {
			t.Fatalf("%q: HTTP %d", text, code)
		}
	}
	ft.quiet(t)

	// A device that does not verify every task is sent nothing: it would run a forged event.
	lax := register(t, base, "main")
	lax.status = `{"version":"test","tanka_verify":"observe"}`
	lax.poll(t)
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+lax.DeviceID))
	if task := lax.poll(t); task != nil {
		t.Fatalf("a device at tanka_verify observe was sent %+v", task)
	}

	dev := register(t, base, "main")
	dev.status = `{"version":"test","tanka_verify":"require"}`
	if task := dev.poll(t); task != nil { // marks the device online
		t.Fatalf("unexpected task %+v", task)
	}

	// pair goes to the device; unconfirmed, nothing changes and nothing is posted.
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	task := dev.poll(t)
	if task == nil || task.Command != "pair" || len(task.Raw) == 0 || task.RoomID != "R1" {
		t.Fatalf("forwarded pair = %+v", task)
	}
	if code := dev.confirm(t, task.ID, false); code != http.StatusOK {
		t.Fatalf("refusal: HTTP %d", code)
	}
	ft.quiet(t)
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link hello"))
	if task := dev.poll(t); task != nil {
		t.Fatalf("an unconfirmed pair bound the room: got %+v", task)
	}
	ft.quiet(t)

	// Confirmed, the room is paired and the server says so (not the device's text).
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	task = dev.poll(t)
	dev.confirm(t, task.ID, true)
	if p := ft.next(t); !strings.Contains(p.text, "Paired this room with device `"+dev.DeviceID+"`") {
		t.Fatalf("pair reply: %q", p.text)
	}
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link hello"))
	if task := dev.poll(t); task == nil || task.Text != "hello" || task.Command != "" {
		t.Fatalf("prompt in the paired room: %+v", task)
	}
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link help"))
	if p := ft.next(t); !strings.Contains(p.text, "pair <device-id>") {
		t.Fatalf("help in a paired room: %q", p.text)
	}

	// unpair goes to the paired device, and needs its confirmation too.
	sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link unpair"))
	task = dev.poll(t)
	if task == nil || task.Command != "unpair" {
		t.Fatalf("forwarded unpair = %+v", task)
	}
	dev.confirm(t, task.ID, true)
	if p := ft.next(t); !strings.Contains(p.text, "Unpaired this room from device `"+dev.DeviceID+"`") {
		t.Fatalf("unpair reply: %q", p.text)
	}
}

func TestWorkerVerifyQuietForStrangers(t *testing.T) {
	_, base, ft := newTestServer(t, func(c *Config) {
		c.Bots[0].SigningSecret, c.Bots[0].Verify = "", "worker"
		c.AllowedUsers = []string{"U1"}
	})
	sendUnsigned(t, base, mention(ft, "R1", "U9", "@Agent Link hello"))
	ft.quiet(t)
}

func TestWorkerVerifyRateLimit(t *testing.T) {
	_, base, ft := newWorkerVerifyServer(t)
	for i := 1; i <= eventsPerRoom+1; i++ {
		code := sendUnsigned(t, base, mention(ft, "R1", "U1", "@Agent Link hello"))
		if want := http.StatusOK; i > eventsPerRoom {
			want = http.StatusTooManyRequests
			if code != want {
				t.Fatalf("event %d: HTTP %d, want %d", i, code, want)
			}
		} else if code != want {
			t.Fatalf("event %d: HTTP %d, want %d", i, code, want)
		}
	}
	// Another room has its own count.
	if code := sendUnsigned(t, base, mention(ft, "R2", "U1", "@Agent Link hello")); code != http.StatusOK {
		t.Fatalf("another room: HTTP %d", code)
	}
}

func TestClientIP(t *testing.T) {
	for _, tc := range []struct{ remote, cf, want string }{
		{"127.0.0.1:5000", "203.0.113.9", "203.0.113.9"}, // through the local tunnel
		{"[::1]:5000", "203.0.113.9", "203.0.113.9"},
		{"198.51.100.7:5000", "203.0.113.9", "198.51.100.7"}, // direct: the header is not trusted
		{"127.0.0.1:5000", "", "127.0.0.1"},
	} {
		r := httptest.NewRequest(http.MethodPost, "/tanka/events", nil)
		r.RemoteAddr = tc.remote
		if tc.cf != "" {
			r.Header.Set("CF-Connecting-IP", tc.cf)
		}
		if got := clientIP(r); got != tc.want {
			t.Errorf("clientIP(%s, %q) = %s, want %s", tc.remote, tc.cf, got, tc.want)
		}
	}
}
