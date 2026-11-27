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

func TestDigestConfig(t *testing.T) {
	f := newFixture(t)
	c := f.cfg
	c.Triggers = []Trigger{{Name: digestTrigger}}
	if err := c.checkTriggers(); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("a trigger took the digest's name: %v", err)
	}
	c.Digest = &Digest{}
	if err := c.checkDigest(); err == nil {
		t.Fatal("a digest without a room was accepted")
	}
	c.Digest = &Digest{RoomID: "R2"}
	if err := c.checkDigest(); err != nil || c.Digest.EverySec != 3600 {
		t.Fatalf("defaults: %v %+v", err, c.Digest)
	}
	c.Digest = &Digest{RoomID: "R2", EverySec: 5}
	if c.checkDigest(); c.Digest.EverySec != 60 {
		t.Fatalf("every_sec below a minute: %+v", c.Digest)
	}
}

func TestDigestText(t *testing.T) {
	at := time.Date(2026, 11, 27, 9, 5, 0, 0, time.Local)
	items := []suppressed{
		{Trigger: "sentry", Key: "k1", Why: whySilent, Summary: "issue A.", At: at},
		{Trigger: "sentry", Key: "k2", Why: whyStale, Summary: "issue B.", At: at},
		{Trigger: "sentry", Key: "k3", Why: whySilent, Summary: "issue C.", At: at},
		{Trigger: "perf", Key: "k4", Why: whyBurst, At: at},
	}
	got := digestText(items, 2, time.Hour, 0)
	for _, want := range []string{"last 1h0m0s: 6 trigger item(s)", "**sentry** · " + whySilent + " · 2", "- 09:05 issue A.\n- 09:05 issue C.", "**perf** · " + whyBurst + " · 1\n- 09:05 `k4`", "…and 2 more"} {
		if !strings.Contains(got, want) {
			t.Fatalf("digest lacks %q:\n%s", want, got)
		}
	}
	short := digestText(items, 0, time.Hour, 260)
	if len(short) > 260 || !strings.Contains(short, "more not listed") {
		t.Fatalf("limit not kept (%d):\n%s", len(short), short)
	}
	if s := summarize("Sentry alert fired for issue X (id 1).  Use the skill to triage it."); s != "Sentry alert fired for issue X (id 1)." {
		t.Fatalf("summary %q", s)
	}
}

// TestDigestRecords checks each way an item goes untriaged is recorded, and that nothing is
// recorded without a digest.
func TestDigestRecords(t *testing.T) {
	var refuse atomic.Bool
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		fmt.Fprint(w, `{"id":"x"}`)
	}))
	defer hs.Close()
	f := newFixture(t)
	items := filepath.Join(t.TempDir(), "items.jsonl")
	now := time.Now()
	write := func(its ...item) {
		var b []byte
		for _, it := range its {
			l, _ := json.Marshal(it)
			b = append(append(b, l...), '\n')
		}
		os.WriteFile(items, b, 0o600)
	}
	f.cfg.ServerURL = hs.URL
	f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Command: []string{"cat", items}, MaxPending: 1, MaxPerMin: -1}}
	f.cfg.Digest = &Digest{RoomID: "R2"}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	if err := f.cfg.checkDigest(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.setIdentity(Identity{DeviceID: "d", DeviceKey: seal.NewPrivateKey().Bytes(), ServerKey: seal.NewPrivateKey().PublicKey().Bytes()})
	ctx := context.Background()
	whys := func() []string {
		w.mu.Lock()
		defer w.mu.Unlock()
		var out []string
		for _, it := range w.supp.Items {
			out = append(out, it.Key+": "+it.Why)
		}
		return out
	}

	write(item{Key: "old", At: now.Add(-2 * time.Hour), Text: "old"},
		item{Key: "res", At: now.Add(-time.Minute), Text: "resolved one", Skip: "resolved"},
		item{Key: "a", At: now.Add(-time.Minute), Text: "a"},
		item{Key: "b", At: now.Add(-time.Minute), Text: "b"})
	if err := w.scan(ctx, w.cfg.Triggers[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := w.admit(proto.Task{Trigger: "alerts", Key: "x", Text: "pushed"}); err == nil {
		t.Fatal("the burst gate let a second task in")
	}
	write(item{Key: "a", At: now.Add(-time.Minute), Text: "a"}) // b leaves the list while held
	if err := w.scan(ctx, w.cfg.Triggers[0]); err != nil {
		t.Fatal(err)
	}
	if w.format(ctx, proto.Task{Trigger: "alerts", Key: "a", Text: "a"}, Run{}, RunResult{Text: "[[silent]]"}, nil, nil) != "" {
		t.Fatal("silent reply was posted")
	}
	want := []string{"old: " + whyStale, "res: " + fmt.Sprintf(whySkip, "resolved"), "x: " + whyBurst, "b: " + fmt.Sprintf(whyHeld, "left the list"), "a: " + whySilent}
	if got := whys(); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("recorded %q\nwant     %q", got, want)
	}
	// A skipped item is seen: the next poll does not record it again.
	write(item{Key: "res", At: now.Add(-time.Minute), Text: "resolved one", Skip: "resolved"})
	if err := w.scan(ctx, w.cfg.Triggers[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(whys()); n != len(want) {
		t.Fatalf("a seen skip was recorded again: %d", n)
	}

	// The record survives a restart and is kept while the server cannot take the digest.
	w2, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w2.setIdentity(w.id)
	refuse.Store(true)
	w2.sendDigest(ctx)
	if n := len(w2.supp.Items); n != len(want) {
		t.Fatalf("after a failed digest %d item(s) are kept, want %d", n, len(want))
	}
	refuse.Store(false)
	w2.sendDigest(ctx)
	if n := len(w2.supp.Items); n != 0 {
		t.Fatalf("after a sent digest %d item(s) are kept", n)
	}

	// Without a digest, nothing is recorded.
	f.cfg.Digest = nil
	w3, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w3.suppress("alerts", "k", whyStale, "t")
	if len(w3.supp.Items) != 0 {
		t.Fatal("recorded without a digest")
	}
}

// TestDigestEndToEnd checks the digest goes through the server and is posted, as it is,
// to the digest's room, with no run.
func TestDigestEndToEnd(t *testing.T) {
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
	scfg := server.Config{
		EnrollToken: "enroll", PollWaitSec: 1, DBPath: filepath.Join(t.TempDir(), "server.db"), ResponseURLPrefix: fake.URL + "/",
		Bots: []server.Bot{{Name: "main", SigningSecret: "s", RoomWebhooks: map[string]string{"R1": fake.URL + "/room", "R2": fake.URL + "/digest"}}},
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
	old, _ := json.Marshal(item{Key: "old", At: time.Now().Add(-2 * time.Hour), Text: "Alert for issue OLD-1 (issue id 7). Triage it."})
	os.WriteFile(items, append(old, '\n'), 0o600)
	f.cfg.ServerURL, f.cfg.EnrollToken = hs.URL, "enroll"
	f.cfg.Triggers = []Trigger{{Name: "alerts", RoomID: "R1", Command: []string{"cat", items}}}
	f.cfg.Digest = &Digest{RoomID: "R2"}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	if err := f.cfg.checkDigest(); err != nil {
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
	scfg.Bots[0].Rooms = map[string]string{"R1": w.DeviceID(), "R2": w.DeviceID()}
	if srv, err = server.New(scfg); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	handler.Store(srv.Handler())

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.mu.Lock()
		n := len(w.supp.Items)
		w.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the stale item was not recorded")
		}
		time.Sleep(20 * time.Millisecond)
	}
	w.sendDigest(ctx)
	p := next()
	if !strings.HasPrefix(p, "/digest 🧾 Not triaged") || !strings.Contains(p, "**alerts** · "+whyStale) || !strings.Contains(p, "Alert for issue OLD-1 (issue id 7).") || strings.Contains(p, "Triage it") {
		t.Fatalf("digest posted %q", p)
	}
	cancel()
	<-done
	select {
	case p := <-posts:
		t.Fatalf("unexpected post %q", p)
	default:
	}
}
