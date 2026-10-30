package tanka

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	secret := []byte("s3cret")
	body := []byte(`{"type":"event_callback"}`)
	now := time.Unix(1_700_000_000, 0)
	ts := strconv.FormatInt(now.Unix(), 10)
	sig := Sign(secret, ts, body)
	if !strings.HasPrefix(sig, "v0=") {
		t.Fatalf("signature %q lacks v0= prefix", sig)
	}
	cases := []struct {
		name    string
		secret  []byte
		ts, sig string
		body    []byte
		ok      bool
	}{
		{"valid", secret, ts, sig, body, true},
		{"wrong secret", []byte("other"), ts, sig, body, false},
		{"tampered body", secret, ts, sig, []byte(`{"type":"x"}`), false},
		{"missing timestamp", secret, "", sig, body, false},
		{"stale timestamp", secret, strconv.FormatInt(now.Add(-6*time.Minute).Unix(), 10), Sign(secret, strconv.FormatInt(now.Add(-6*time.Minute).Unix(), 10), body), body, false},
	}
	for _, c := range cases {
		err := Verify(c.secret, c.ts, c.sig, c.body, now, 5*time.Minute)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrBadSignature) {
			t.Errorf("%s: err %v is not ErrBadSignature", c.name, err)
		}
	}
}

func TestPostErrorsNeverContainTheURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":404,"msg":"response_url_expired"}`))
	}))
	defer srv.Close()
	secretURL := srv.URL + "/hooks/response/SECRET-TOKEN"
	err := Post(context.Background(), srv.Client(), secretURL, Message{Text: "hi"})
	if err == nil || !strings.Contains(err.Error(), "response_url_expired") {
		t.Fatalf("err = %v, want response_url_expired", err)
	}
	srv.Close()
	err = Post(context.Background(), srv.Client(), secretURL, Message{Text: "hi"})
	if err == nil || strings.Contains(err.Error(), "SECRET-TOKEN") {
		t.Fatalf("transport error leaks the URL or is nil: %v", err)
	}
}

func TestTruncate(t *testing.T) {
	if got := Truncate("短文本", 10); got != "短文本" {
		t.Errorf("short text changed: %q", got)
	}
	got := Truncate(strings.Repeat("长", 100), 30)
	if n := len([]rune(got)); n != 30 || !strings.HasSuffix(got, "(truncated)") {
		t.Errorf("Truncate gave %d runes: %q", n, got)
	}
}
