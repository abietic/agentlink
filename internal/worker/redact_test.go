package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agentlink/internal/server"
	"agentlink/internal/tanka"
)

func TestRedactSecrets(t *testing.T) {
	const (
		ghToken   = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
		ghPat     = "github_pat_11ABCDEFG0123456789_abcdefghij"
		gitlabTok = "glpat-abcdefghij0123456789"
		skToken   = "sk-abcdefghijklmnopqrstuvwxyz012345"
		slackTok  = "xoxb-1234567890-abcdefghij"
		sentryTok = "sntrys_abcdefghijklmnopqrstuvwxyz0123456789"
		googleKey = "AIzaSyA1234567890abcdefghijklmnopqrstuv"
		jwt       = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
		awsKey    = "AKIAIOSFODNN7EXAMPLE"
	)
	pemBlock := "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\n-----END RSA PRIVATE KEY-----"
	positives := []struct {
		name  string
		in    string
		want  int
		exact string // whole output, when it matters
	}{
		{name: "aws key id", in: "id " + awsKey + " end"},
		{name: "aws temporary key", in: "ASIAY34FZKBOKMUTVV7A"},
		{name: "pem block", in: pemBlock, exact: redacted},
		{name: "unterminated pem", in: "before\n-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXk\n", exact: "before\n" + redacted},
		{name: "github token", in: ghToken},
		{name: "github fine-grained pat", in: ghPat},
		{name: "gitlab token", in: gitlabTok},
		{name: "sk key", in: skToken},
		{name: "slack token", in: slackTok},
		{name: "sentry token", in: sentryTok},
		{name: "google api key", in: googleKey},
		{name: "jwt", in: jwt},
		{name: "password=value", in: "password=hunter2hunter2"},
		{name: "Password: value", in: "Password: hunter2hunter2"},
		{name: "secret=value", in: "secret=hunter2hunter2"},
		{name: "client_secret", in: "client_secret=hunter2hunter2"},
		{name: "token", in: "token=hunter2hunter2"},
		{name: "access_token", in: "access_token=hunter2hunter2"},
		{name: "refresh_token", in: "refresh_token=hunter2hunter2"},
		{name: "api_key", in: "api_key=hunter2hunter2"},
		{name: "apikey", in: "apikey=hunter2hunter2"},
		{name: "api-key", in: "api-key: hunter2hunter2"},
		{name: "access_key", in: "access_key=hunter2hunter2"},
		{name: "secret_access_key", in: "secret_access_key=hunter2hunter2"},
		{name: "aws_secret_access_key", in: "aws_secret_access_key = hunter2hunter2"},
		{name: "private_key", in: "private_key=hunter2hunter2"},
		{name: "authorization", in: "authorization=hunter2hunter2"},
		{name: "quoted key and value", in: `"password": "hunter2hunter2"`},
		{name: "bearer token", in: "Authorization: Bearer abcdefghijklmnopqrstuvwxyz", want: 1},
		{name: "https userinfo", in: "https://user:s3cr3tpass@example.com/path"},
		{name: "postgres userinfo", in: "postgres://user:password123@db.internal:5432/app", exact: "postgres://user:" + redacted + "@db.internal:5432/app"},
		{name: "mongodb srv userinfo", in: "mongodb+srv://admin:passw0rd123@cluster.example"},
		{name: "url token param", in: "https://example.com/cb?token=abcdefgh12345"},
		{name: "url api_key param", in: "https://example.com/x?api_key=abcdefgh12345"},
		{name: "url sig param", in: "https://example.com/x?a=1&sig=abcdefgh12345"},
		{name: "url signature param", in: "https://example.com/x?signature=abcdefgh12345"},
		{name: "url secret param", in: "https://example.com/x?secret=abcdefgh12345"},
		{name: "url password param", in: "https://example.com/x?password=abcdefgh12345"},
		{name: "url key param", in: "https://example.com/x?key=abcdefgh12345"},
		{name: "url access_token param", in: "https://example.com/x?access_token=abcdefgh12345"},
	}
	for _, tc := range positives {
		t.Run(tc.name, func(t *testing.T) {
			want := max(tc.want, 1)
			got, n := redactSecrets(tc.in)
			if n != want {
				t.Fatalf("redactSecrets(%q) masked %d, want %d: %q", tc.in, n, want, got)
			}
			if tc.exact != "" {
				if got != tc.exact {
					t.Fatalf("redactSecrets(%q) = %q, want %q", tc.in, got, tc.exact)
				}
				return
			}
			if !strings.Contains(got, redacted) {
				t.Fatalf("redactSecrets(%q) = %q: no %s", tc.in, got, redacted)
			}
		})
	}
}

func TestRedactSecretsLeavesOrdinaryText(t *testing.T) {
	for _, in := range []string{
		"da39a3ee5e6b4b0d3255bfef95601890afd80709", // 40-char git SHA
		"550e8400-e29b-41d4-a716-446655440000",     // UUID
		"6a7d7188238a191436b929da",                 // Tanka member id
		"the token budget is tight",
		"pwd: /Users/admin/Code/theta-smart",
		"see the password reset page",
		"**password**: see the docs",
		"key: value",
		"secret: short",
		"token: 12345678", // a number, not a credential
		"secret: 1234567890",
		"?key=12345678", // numeric query value
		"sk-learn is a library",
		"**bold** and `code` and [a link](https://example.com)",
		"the build ran 12345678 times",
	} {
		got, n := redactSecrets(in)
		if n != 0 || got != in {
			t.Fatalf("redactSecrets(%q) = %q, %d matches; want it untouched", in, got, n)
		}
	}
}

func TestRedactSecretsKeepsNamesAndCounts(t *testing.T) {
	in := "password=hunter2hunter2 token: abcdefghijk AKIAIOSFODNN7EXAMPLE"
	got, n := redactSecrets(in)
	if n != 3 {
		t.Fatalf("masked %d, want 3: %q", n, got)
	}
	want := "password=" + redacted + " token: " + redacted + " " + redacted
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRedactSecretsURLCredentials(t *testing.T) {
	got, n := redactSecrets("postgres://user:password123@db:5432/x")
	if n != 1 || got != "postgres://user:"+redacted+"@db:5432/x" {
		t.Fatalf("got %q, %d matches", got, n)
	}
}

func TestDigestTextRedacts(t *testing.T) {
	at := time.Date(2026, 11, 27, 9, 5, 0, 0, time.Local)
	items := []suppressed{
		{Trigger: "sentry", Key: "k1", Why: whySilent, Summary: "request failed with AKIAIOSFODNN7EXAMPLE", At: at},
	}
	got := digestText(items, 0, time.Hour, 0)
	if strings.Contains(got, "AKIAIOSFODNN7EXAMPLE") || !strings.Contains(got, redacted) || !strings.Contains(got, redactionNote(1)) {
		t.Fatalf("digest not redacted:\n%s", got)
	}
}

// TestResultPathRedactsSecrets proves the text a fake server hands to the room has been
// redacted on the way out, not only that redactSecrets works.
func TestResultPathRedactsSecrets(t *testing.T) {
	posts := make(chan string, 16)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m tanka.Message
		json.NewDecoder(r.Body).Decode(&m)
		posts <- m.Text
		fmt.Fprint(w, `{"code":0}`)
	}))
	defer fake.Close()

	scfg := server.Config{
		EnrollToken:       "enroll",
		PollWaitSec:       1,
		DBPath:            filepath.Join(t.TempDir(), "server.db"),
		ResponseURLPrefix: fake.URL + "/",
		Bots:              []server.Bot{{Name: "main", SigningSecret: "s", RoomWebhooks: map[string]string{"R1": fake.URL + "/room"}}},
	}
	var handler atomic.Value
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Load().(http.Handler).ServeHTTP(w, r)
	}))
	defer hs.Close()
	srv, err := server.New(scfg)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(srv.Handler())

	f := newFixture(t)
	f.cfg.ServerURL, f.cfg.EnrollToken = hs.URL, "enroll"
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
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()

	const secretPrompt = "password=hunter2hunter2 and AKIAIOSFODNN7EXAMPLE"
	var got string
	deadline := time.Now().Add(10 * time.Second)
	for n := 1; ; n++ {
		body, _ := json.Marshal(tanka.Envelope{Type: "event_callback", EventID: fmt.Sprintf("e-secret-%d", n), Event: &tanka.MentionEvent{
			Type: "app_mention", UserID: "U1", RoomID: "R1",
			Text:        "@bot " + secretPrompt,
			ResponseURL: fmt.Sprintf("%s/hooks/response/%d", fake.URL, n),
		}})
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		req, _ := http.NewRequest(http.MethodPost, hs.URL+"/tanka/events", bytes.NewReader(body))
		req.Header.Set(tanka.HeaderTimestamp, ts)
		req.Header.Set(tanka.HeaderSignature, tanka.Sign([]byte("s"), ts, body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		select {
		case got = <-posts:
		case <-time.After(10 * time.Second):
			t.Fatal("no reply posted to the room")
		}
		if !strings.Contains(got, "offline") || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(got, redacted) {
		t.Fatalf("reply has no %s: %q", redacted, got)
	}
	if strings.Contains(got, "hunter2hunter2") || strings.Contains(got, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("reply leaks a secret: %q", got)
	}
	if !strings.Contains(got, redactionNote(2)) {
		t.Fatalf("reply does not say 2 were masked: %q", got)
	}
}
