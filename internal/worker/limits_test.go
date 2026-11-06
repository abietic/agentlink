package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"agentlink/internal/proto"
)

const stubKimiSession = "5e550000-0000-4000-8000-000000000001"

// stubKimi stands in for the kimi CLI. It logs its args to logPath. `web` serves a usage
// endpoint whose usages are the JSON in the file named by AGENTLINK_STUB_KIMI_QUOTA (none
// used up without it) until SIGTERM. Otherwise it answers the -p prompt in stream-json; the
// prompt "fail" exits 1 as an exhausted quota does.
func stubKimi(logPath string, args []string) {
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	json.NewEncoder(f).Encode(args)
	f.Close()
	if args[0] == "web" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		usages := json.RawMessage(`{"limit5h":{"usedRatio":0.2,"resetAt":"2026-11-06T15:00:00Z"}}`)
		if b, err := os.ReadFile(os.Getenv("AGENTLINK_STUB_KIMI_QUOTA")); err == nil {
			usages = b
		}
		go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/oauth/usage" || r.Header.Get("Authorization") != "Bearer stub-token" {
				http.Error(w, "no", http.StatusUnauthorized)
				return
			}
			fmt.Fprintf(w, `{"code":0,"data":{"kind":"ok","quota":{"usages":%s,"extraUsage":null}}}`, usages)
		}))
		fmt.Printf("\n  Kimi server ready\n\n  Local:    http://%s/#token=stub-token\n  Token:    stub-token\n", ln.Addr())
		<-ctx.Done()
		return
	}
	at := func(flag string) string {
		if i := slices.Index(args, flag); i >= 0 && i+1 < len(args) {
			return args[i+1]
		}
		return ""
	}
	prompt, session := at("-p"), cmpOr(at("-S"), "session_"+stubKimiSession)
	if prompt == "fail" {
		fmt.Fprintln(os.Stderr, "Error: 429 quota exhausted")
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.Encode(map[string]any{"role": "meta", "type": "system.version", "version": "2.5.0"})
	enc.Encode(map[string]any{"role": "assistant", "content": "Let me look."})
	enc.Encode(map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"type": "function", "id": "t1"}}})
	enc.Encode(map[string]any{"role": "tool", "tool_call_id": "t1", "content": "done"})
	enc.Encode(map[string]any{"role": "assistant", "content": "kimi: " + prompt})
	enc.Encode(map[string]any{"role": "meta", "type": "session.resume_hint", "session_id": session})
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func lines(t *testing.T, path string) int {
	t.Helper()
	b, _ := os.ReadFile(path)
	return strings.Count(string(b), "\n")
}

func TestKimiRunner(t *testing.T) {
	dir := t.TempDir()
	klog := filepath.Join(dir, "kimi.log")
	t.Setenv("AGENTLINK_STUB_KIMI", klog)
	k := Kimi{Command: []string{os.Args[0]}, Model: "kimi-code/kimi-for-coding", Timeout: 10 * time.Second}
	ctx := context.Background()

	res, err := k.Run(ctx, Run{Workdir: dir, SessionID: "chosen-by-worker", Prompt: "hi", Images: []Image{{MediaType: "image/png", Data: []byte("png")}}})
	args := stubCalls(t, klog)[0]
	if err != nil || res.SessionID != stubKimiSession || !strings.HasPrefix(res.Text, "kimi: The message came with 1 image(s)") ||
		!strings.HasSuffix(res.Text, "\n\nhi") || res.Turns != 1 || res.Limit != nil {
		t.Fatalf("new session: %v %+v", err, res)
	}
	if slices.Contains(args, "-S") || !slices.Contains(args, "--add-dir") || args[slices.Index(args, "-m")+1] != "kimi-code/kimi-for-coding" {
		t.Fatalf("new session args: %q", args)
	}
	if _, err := os.Stat(args[slices.Index(args, "--add-dir")+1]); !os.IsNotExist(err) {
		t.Fatalf("image dir left behind: %v", err)
	}

	res, err = k.Run(ctx, Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "again"})
	if args := stubCalls(t, klog)[1]; err != nil || res.SessionID != "abc" || res.Text != "kimi: again" || args[slices.Index(args, "-S")+1] != "session_abc" {
		t.Fatalf("resume: %v %+v %q", err, res, args)
	}

	// A failure with quota left is only a failure.
	res, err = k.Run(ctx, Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "fail"})
	if err == nil || !strings.Contains(err.Error(), "429") || res.Limit != nil {
		t.Fatalf("failure: %v %+v", err, res)
	}
	// With windows used up, it reports the one that lifts last.
	quota := filepath.Join(dir, "quota.json")
	os.WriteFile(quota, []byte(`{"limit5h":{"usedRatio":1,"resetAt":"2026-11-06T15:00:00Z"},"limit7d":{"usedRatio":1.0,"resetAt":"2026-11-09T00:00:00Z"},"monthTotal":{"usedRatio":0.5}}`), 0o600)
	t.Setenv("AGENTLINK_STUB_KIMI_QUOTA", quota)
	res, err = k.Run(ctx, Run{Workdir: dir, SessionID: "abc", Resume: true, Prompt: "fail"})
	if want := (Limit{Name: "limit7d", ResetsAt: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)}); err == nil || res.Limit == nil || res.Limit.Name != want.Name || !res.Limit.ResetsAt.Equal(want.ResetsAt) {
		t.Fatalf("used up: %v %+v", err, res.Limit)
	}
}

// TestLimitNoticeOnceThenFallback: the first run to hit a limit says so, with the limit and
// when it lifts; later prompts go straight to the fallback without a word, across restarts,
// until the limit lifts.
func TestLimitNoticeOnceThenFallback(t *testing.T) {
	f := newFixture(t)
	klog := filepath.Join(t.TempDir(), "kimi.log")
	t.Setenv("AGENTLINK_STUB_KIMI", klog)
	limit := filepath.Join(t.TempDir(), "limit.json")
	t.Setenv("AGENTLINK_STUB_LIMIT", limit)
	f.cfg.Harnesses = map[string]Harness{
		"claude": {Type: "sdk", Fallback: []string{"kimi"}},
		"kimi":   {Type: "kimi", Command: []string{os.Args[0]}},
	}
	if err := f.cfg.checkHarnesses(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	task := func(w *Worker, room, text string) string {
		return w.handle(context.Background(), proto.Task{ID: "t", RoomID: room, UserID: "U1", Text: text})
	}

	if r := task(w, "R1", "first"); !strings.HasPrefix(r, "echo: first") {
		t.Fatalf("before the limit: %q", r)
	}
	claudeSession := w.state["R1"].SessionID

	reset := time.Now().Add(time.Hour).Truncate(time.Second)
	os.WriteFile(limit, fmt.Appendf(nil, `{"type":"seven_day","resets_at":%d}`, reset.Unix()), 0o600)
	r := task(w, "R1", "second")
	for _, want := range []string{
		"⚠️ `claude` hit its weekly limit (`seven_day`); it resets " + reset.Local().Format("Mon 01-02 15:04 MST"),
		"`kimi` took this over in a new session, without session `" + short(claudeSession) + "`'s history.",
		"kimi: second",
		"kimi · session `5e550000`",
	} {
		if !strings.Contains(r, want) {
			t.Fatalf("first hit: %q lacks %q", r, want)
		}
	}
	if w.state["R1"].SessionID != stubKimiSession || lines(t, f.log) != 2 {
		t.Fatalf("after the first hit: session %q, %d sdk runs", w.state["R1"].SessionID, lines(t, f.log))
	}

	// The room's session is now kimi's and continues there; claude is not tried.
	if r := task(w, "R1", "third"); strings.Contains(r, "⚠️") || !strings.HasPrefix(r, "kimi: third") || lines(t, f.log) != 2 {
		t.Fatalf("while limited: %q", r)
	}
	if args := stubCalls(t, klog); !slices.Contains(args[len(args)-1], "session_"+stubKimiSession) {
		t.Fatalf("kimi session not continued: %q", args[len(args)-1])
	}
	if r := w.query("R1", "status"); !strings.Contains(r, "**At a limit** `claude`: weekly limit (`seven_day`), until "+reset.Local().Format("Mon 01-02 15:04 MST")) {
		t.Fatalf("status: %q", r)
	}

	// A restart keeps the limit: a new session in another room goes to kimi, silently.
	w2, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r := task(w2, "R2", "hello"); strings.Contains(r, "⚠️") || strings.Contains(r, "took this over") || !strings.HasPrefix(r, "kimi: hello") || lines(t, f.log) != 2 {
		t.Fatalf("after a restart: %q", r)
	}

	// Once it lifts, new sessions are claude's again and the limit is forgotten.
	os.Remove(limit)
	w2.now = func() time.Time { return reset.Add(time.Minute) }
	if r := task(w2, "R1", "new fourth"); !strings.HasPrefix(r, "echo: fourth") {
		t.Fatalf("after the reset: %q", r)
	}
	if len(w2.limits) != 0 || strings.Contains(w2.query("R1", "status"), "At a limit") {
		t.Fatalf("limit kept: %+v", w2.limits)
	}
}

// TestLimitWithoutFallback: with nowhere else to go, the first triggered task says why it did
// not run, later ones post nothing, and a person who asks is told.
func TestLimitWithoutFallback(t *testing.T) {
	f := newFixture(t)
	limit := filepath.Join(t.TempDir(), "limit.json")
	t.Setenv("AGENTLINK_STUB_LIMIT", limit)
	os.WriteFile(limit, []byte(`{"type":""}`), 0o600) // says neither which limit nor when it lifts
	f.cfg.Triggers = []Trigger{{Name: "alerts"}}
	if err := f.cfg.checkTriggers(); err != nil {
		t.Fatal(err)
	}
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	w.now = func() time.Time { return now }
	alert := func(id string) string {
		return w.handle(context.Background(), proto.Task{ID: id, Trigger: "alerts", RoomID: "R9", Text: "alert " + id})
	}
	retry := now.Add(limitRetry).Local().Format("Mon 01-02 15:04 MST")
	if r := alert("a1"); r != "⚠️ `claude` hit a usage limit and did not say when it resets; this device tries it again after "+retry+", and says so only this once.\n\nThis `alerts` task was not run." {
		t.Fatalf("first alert: %q", r)
	}
	if r := alert("a2"); r != "" || lines(t, f.log) != 1 {
		t.Fatalf("second alert: %q, %d runs", r, lines(t, f.log))
	}
	if r := w.handle(context.Background(), proto.Task{ID: "m", RoomID: "R1", UserID: "U1", Text: "hi"}); r != "Nothing ran: `claude` is at a usage limit until "+retry+"." {
		t.Fatalf("mention: %q", r)
	}
	// Still at it once retried: no second notice.
	now = now.Add(limitRetry + time.Minute)
	if r := alert("a3"); r != "" || lines(t, f.log) != 2 {
		t.Fatalf("retry: %q, %d runs", r, lines(t, f.log))
	}
}
