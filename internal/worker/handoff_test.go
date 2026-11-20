package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agentlink/internal/proto"
)

func TestWithHandoffRule(t *testing.T) {
	const tanka = "You are answering a message from a Tanka group chat."
	for _, tc := range []struct {
		name, in string
		want     any // the systemPrompt it should end up with
	}{
		{"none", ``, map[string]any{"type": "preset", "preset": "claude_code", "append": handoffRule}},
		{"no systemPrompt", `{"permissionMode":"auto","maxTurns":50}`, map[string]any{"type": "preset", "preset": "claude_code", "append": handoffRule}},
		{"preset with append", `{"systemPrompt":{"type":"preset","preset":"claude_code","append":"` + tanka + `"}}`,
			map[string]any{"type": "preset", "preset": "claude_code", "append": tanka + "\n\n" + handoffRule}},
		{"preset alone", `{"systemPrompt":{"type":"preset","preset":"claude_code"}}`, map[string]any{"type": "preset", "preset": "claude_code", "append": handoffRule}},
		{"custom prompt", `{"systemPrompt":"Be brief."}`, "Be brief.\n\n" + handoffRule},
	} {
		out, err := withHandoffRule(json.RawMessage(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var got map[string]any
		json.Unmarshal(out, &got)
		if g, _ := json.Marshal(got["systemPrompt"]); string(g) != mustJSON(tc.want) {
			t.Errorf("%s: systemPrompt = %s", tc.name, g)
		}
		if tc.name == "no systemPrompt" && (got["permissionMode"] != "auto" || !strings.Contains(string(out), `"maxTurns":50`)) {
			t.Errorf("%s: other options changed: %s", tc.name, out)
		}
		// Checking a config twice adds the rule once.
		again, _ := withHandoffRule(out)
		if strings.Count(string(again), "[[handoff") != 1 {
			t.Errorf("%s: applied twice: %s", tc.name, again)
		}
	}
	for _, bad := range []string{`{"systemPrompt":3}`, `{nope`} {
		if _, err := withHandoffRule(json.RawMessage(bad)); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestParseHandoff(t *testing.T) {
	for in, want := range map[string]string{
		"[[handoff 83799f9e]]":           "83799f9e",
		"\n  [[handoff `83799F9E`]]  \n": "83799f9e",
		"[[handoff]]":                    "",
		"[[handoff <id>]]":               "id",
	} {
		if got, ok := parseHandoff(in); !ok || got != want {
			t.Errorf("parseHandoff(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"Sure.\n[[handoff 83799f9e]]", "[[handoff 83799f9e]] because", "handoff 83799f9e", ""} {
		if _, ok := parseHandoff(in); ok {
			t.Errorf("parseHandoff(%q) is a hand-off", in)
		}
	}
}

func TestClaudeSummary(t *testing.T) {
	dir := t.TempDir()
	const workdir, id = "/Users/someone/Code/my.app dir_x", "016be2cf-4c44-4ece-9c65-33a472b5715d"
	project := filepath.Join(dir, "projects", "-Users-someone-Code-my-app-dir-x")
	os.MkdirAll(project, 0o700)
	os.WriteFile(filepath.Join(project, id+".jsonl"), []byte(`{"type":"user","message":{"content":"mentions \"ai-title\" in passing"}}
{"type":"ai-title","aiTitle":"First title","sessionId":"`+id+`"}
{"type":"assistant","message":{"content":[{"type":"text","text":"answer"}]}}
{"type":"user","message":{"content":[{"type":"tool_result","content":"tool output"}]}}
{"type":"user","isMeta":true,"message":{"content":"meta"}}
{"type":"user","message":{"content":"现在在接下来的对话里\n你要扮演 glados"}}
{"type":"ai-title","aiTitle":"Sentry alert THETA-1T6","sessionId":"`+id+`"}
{"type":"ai-title","aiTitle":"  ","sessionId":"`+id+`"}
{"type":"user","message":{"content":[{"type":"text","text":"[AgentLink] Other sessions in this room (id · last used · title):\n- abc · x\n`+othersFooter+`\n\n这里我说了什么"}]}}
{"type":"user","message":{"content":[{"type":"text","text":"GLaDOS 看我请来了谁"}]}}
{"type":"user","message":{"content":"<task-notification> <task-id>b1</task-id> done"}}
{"type":"user","message":{"content":"`+defaultResumePrompt+`"}}
{"type":"user","message":{"content":"configured resume"}}
{"type":"assistant"}`), 0o600)
	w := &Worker{cfg: Config{Recovery: map[string]Recovery{"sentry": {ResumePrompt: "configured\n resume"}}}}
	title, recent := claudeSummary(dir, workdir, id, w.notTyped)
	if title != "Sentry alert THETA-1T6" {
		t.Errorf("title = %q", title)
	}
	if want := []string{"现在在接下来的对话里 你要扮演 glados", "这里我说了什么", "GLaDOS 看我请来了谁"}; strings.Join(recent, "|") != strings.Join(want, "|") {
		t.Errorf("recent = %q", recent)
	}
	for _, c := range [][2]string{{workdir, "00000000-0000-0000-0000-000000000000"}, {"/elsewhere", id}, {workdir, "../" + id}} {
		if title, recent := claudeSummary(dir, c[0], c[1], nil); title != "" || recent != nil {
			t.Errorf("claudeSummary(%q, %q) = %q, %q", c[0], c[1], title, recent)
		}
	}
}

func TestNotTyped(t *testing.T) {
	w := &Worker{}
	for _, p := range []string{"<task-notification> <task-id>x</task-id>", "<system-reminder>hi", "<scheduled-task name=\"x\">", "<local-command-stdout></local-command-stdout>", "<br/>", defaultResumePrompt} {
		if !w.notTyped(p) {
			t.Errorf("%q counts as typed", p)
		}
	}
	for _, p := range []string{"GLaDOS 看我请来了谁", "<[at]Mirasim Link> 算出 17+25", "< 3 items", "<3", "ok <tag>"} {
		if w.notTyped(p) {
			t.Errorf("%q counts as injected", p)
		}
	}
}

func TestHandsOffOnlyOnSDK(t *testing.T) {
	w := &Worker{cfg: Config{Harnesses: map[string]Harness{"claude": {Type: "sdk"}, "mirasim": {Type: "mirasim"}, "kimi": {Type: "kimi"}}}}
	if !w.handsOff("claude") || w.handsOff("mirasim") || w.handsOff("kimi") {
		t.Fatal("only sdk harnesses carry the hand-off rule")
	}
}

// TestQuotedReplyRouting: a prompt goes to a session id it starts with, else to the session
// the server matched its quote to, else to the room's current session, which is shown the
// room's other sessions and may hand the prompt on once. Nothing is held back any more.
func TestQuotedReplyRouting(t *testing.T) {
	f := newFixture(t)
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.claudeDir = t.TempDir()
	ctx := context.Background()
	task := func(text, quoted, route string) proto.Task {
		return proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Text: text, ReplyMessageID: quoted, RouteSessionID: route}
	}
	runs := func() int {
		b, _ := os.ReadFile(f.log)
		return strings.Count(string(b), "\n")
	}

	// A quoted prompt in a room with no other session runs as any prompt does.
	w.handle(ctx, task("alert triage", "m0", ""))
	first := f.last(t).SessionID
	if got := f.last(t); got.Prompt != "alert triage" {
		t.Fatalf("quoted prompt in an empty room: %+v", got)
	}
	w.handle(ctx, task("new redis timeout", "", ""))
	second := f.last(t).SessionID
	if first == second || w.state["R1"].SessionID != second {
		t.Fatalf("setup: first %s, second %s, current %s", first, second, w.state["R1"].SessionID)
	}
	project := filepath.Join(w.claudeDir, "projects", notAlnum.ReplaceAllString(f.root, "-"))
	os.MkdirAll(project, 0o700)
	os.WriteFile(filepath.Join(project, first+".jsonl"), []byte(`{"type":"ai-title","aiTitle":"Sentry alert THETA-1T6"}`+"\n"+
		`{"type":"user","message":{"content":"现在扮演 glados"}}`+"\n"), 0o600)

	// Layer 2: the session the server matched, as if the text started with its id.
	r := w.handle(ctx, task("这个告警要处理吗", "m1", first))
	if got := f.last(t); !got.Resume || got.SessionID != first || got.Prompt != "这个告警要处理吗" {
		t.Fatalf("routed by the server: %+v", got)
	}
	if !strings.Contains(r, "session `"+first[:8]+"` · 按引用的消息接到会话 `"+first[:8]+"`") || w.state["R1"].SessionID != second {
		t.Fatalf("routed reply %q, current %s", r, w.state["R1"].SessionID)
	}
	if w.sessionOf["t"] != first {
		t.Fatalf("the result would not carry session %s: %v", first, w.sessionOf)
	}
	// Layer 1: an id at the start wins over the server's match, and needs no note.
	r = w.handle(ctx, task(second[:8]+" 这个告警", "m1", first))
	if got := f.last(t); got.SessionID != second || got.Prompt != "这个告警" || strings.Contains(r, "按引用") {
		t.Fatalf("prefix over the server's match: %+v %q", got, r)
	}
	// A command is a command, quoted and matched or not.
	w.handle(ctx, task("new 新问题", "m1", first))
	third := f.last(t).SessionID
	if got := f.last(t); got.Resume || third == first || third == second || got.Prompt != "新问题" {
		t.Fatalf("quoted new: %+v", got)
	}

	// Layer 4: no match (or a session this room no longer has) runs in the current session,
	// shown the room's other sessions, newest first, titled by Claude Code where it can.
	for _, route := range []string{"", "0badc0de-0000-4000-8000-000000000000"} {
		w.handle(ctx, task("这个要处理吗", "m2", route))
		got := f.last(t)
		want := othersHeader + "\n- " + second[:8] + " · "
		if !got.Resume || got.SessionID != third || !strings.HasPrefix(got.Prompt, want) || !strings.HasSuffix(got.Prompt, othersFooter+"\n\n这个要处理吗") ||
			!strings.Contains(got.Prompt, "- "+first[:8]+" · ") || !strings.Contains(got.Prompt, "Sentry alert THETA-1T6 · 「现在扮演 glados」") || !strings.Contains(got.Prompt, "redis timeout") ||
			strings.Contains(got.Prompt, third[:8]) {
			t.Fatalf("route %q: default session's prompt:\n%s", route, got.Prompt)
		}
	}
	// The default session hands the prompt on: it runs again, without the list, in that
	// session, which does not become current, and the footer says so. One hop only.
	before := runs()
	r = w.handle(ctx, task("handoff-to "+first[:8], "m3", ""))
	if got := f.last(t); runs() != before+2 || !got.Resume || got.SessionID != first || got.Prompt != "handoff-to "+first[:8] {
		t.Fatalf("hand-off: %d runs, last %+v", runs()-before, got)
	}
	if !strings.Contains(r, "echo: handoff-to "+first[:8]) || !strings.Contains(r, "由会话 `"+third[:8]+"` 转到 `"+first[:8]+"`") || strings.Contains(r, "[[handoff") {
		t.Fatalf("hand-off reply: %q", r)
	}
	if w.state["R1"].SessionID != third || w.sessionOf["t"] != first {
		t.Fatalf("after a hand-off: current %s, result session %s", w.state["R1"].SessionID, w.sessionOf["t"])
	}
	// An id that is no other session of the room hands nothing on, and says so in words.
	for _, bad := range []string{"zzz", third[:8], "deadbeef"} {
		before = runs()
		r = w.handle(ctx, task("handoff-to "+bad, "m3", ""))
		if runs() != before+1 || strings.Contains(r, "[[handoff") || !strings.Contains(r, "对不上") || !strings.Contains(r, "`"+third[:8]+"`") {
			t.Fatalf("hand-off to %q: %d runs, reply %q", bad, runs()-before, r)
		}
	}
	// An unquoted prompt is shown the list too: a room reuses sessions, and people rarely quote.
	w.handle(ctx, task("plain follow-up", "", ""))
	if got := f.last(t); got.SessionID != third || !strings.HasPrefix(got.Prompt, othersHeader) || !strings.HasSuffix(got.Prompt, "\n\nplain follow-up") {
		t.Fatalf("unquoted prompt: %+v", got)
	}
	// A bot's mention is not: bots hand work to each other on purpose.
	bot := task("bot follow-up", "", "")
	bot.SenderType = "bot"
	w.handle(ctx, bot)
	if got := f.last(t); got.SessionID != third || got.Prompt != "bot follow-up" {
		t.Fatalf("bot prompt: %+v", got)
	}
	// The rule rides on every SDK run's system prompt, after the config's own append.
	var opts struct{ SystemPrompt struct{ Append string } }
	if json.Unmarshal(f.last(t).Options, &opts); !strings.HasSuffix(opts.SystemPrompt.Append, handoffRule) {
		t.Fatalf("options lack the hand-off rule: %s", f.last(t).Options)
	}
}
