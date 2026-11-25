package tanka

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   bool
	}{
		{http.StatusBadGateway, ``, true},
		{http.StatusServiceUnavailable, ``, true},
		{http.StatusTooManyRequests, ``, true},
		{http.StatusBadRequest, `{"code":400,"msg":"bad"}`, false},
		{http.StatusNotFound, ``, false},
		{http.StatusOK, `{"code":1001,"msg":"expired"}`, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			w.Write([]byte(tc.body))
		}))
		err := Post(context.Background(), srv.Client(), srv.URL, Message{Text: "x"})
		srv.Close()
		if err == nil || Retryable(err) != tc.want {
			t.Errorf("HTTP %d %s: err=%v retryable=%v, want %v", tc.status, tc.body, err, Retryable(err), tc.want)
		}
	}
	if Retryable(nil) {
		t.Error("nil error is retryable")
	}
	if Retryable(Post(context.Background(), http.DefaultClient, "://bad", Message{})) {
		t.Error("a bad URL is retryable")
	}
	if !Retryable(errors.New("tanka: post: connection reset")) {
		t.Error("a post with no answer is not retryable")
	}
}
