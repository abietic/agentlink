package worker

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"agentlink/internal/proto"
)

// A person's prompt that names no session, and quotes no message the server could match to
// one, runs in the room's current session, as any prompt does. On a harness that has handoffRule in its system prompt
// it goes with a list of the room's other sessions first, and the session may answer only
// [[handoff <id>]]: the prompt then runs again, without the list, in that session, whose
// reply is the one posted. A prompt is handed on once at most.

// handoffRule is appended to every sdk harness's system prompt. It never changes, so it does
// not break the prompt cache, and it only acts on a prompt that starts with othersHeader.
const handoffRule = `This session is driven from a Tanka group chat through AgentLink. A message may begin with a block headed "[AgentLink] Other sessions in this room". When it does, and the message is clearly unrelated to this session but clearly about one of the sessions in that block, reply with exactly one line, [[handoff <id>]], with that session's id from the block, and do nothing else. If you are not sure, answer the message normally and ignore the block.`

const (
	othersHeader = "[AgentLink] Other sessions in this room (id · last used · title · recent prompts, newest last):"
	othersFooter = "[End of list. The message from the room follows.]"
)

// withHandoffRule adds handoffRule to sdk_options' systemPrompt: after the append of a preset
// prompt, or after a custom prompt string. Without a systemPrompt it sets Claude Code's preset.
func withHandoffRule(raw json.RawMessage) (json.RawMessage, error) {
	opts := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 && string(bytes.TrimSpace(raw)) != "null" {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber() // keeps numbers as written
		if err := d.Decode(&opts); err != nil {
			return nil, fmt.Errorf("sdk_options: %w", err)
		}
	}
	switch sp := opts["systemPrompt"].(type) {
	case nil:
		opts["systemPrompt"] = map[string]any{"type": "preset", "preset": "claude_code", "append": handoffRule}
	case string:
		opts["systemPrompt"] = joinPrompt(sp, handoffRule)
	case map[string]any:
		a, _ := sp["append"].(string)
		sp["append"] = joinPrompt(a, handoffRule)
	default:
		return nil, errors.New("sdk_options: systemPrompt must be a string or an object")
	}
	return json.Marshal(opts)
}

func joinPrompt(a, b string) string {
	if strings.Contains(a, b) { // the config was checked before
		return a
	}
	if strings.TrimSpace(a) == "" {
		return b
	}
	return strings.TrimRight(a, "\n") + "\n\n" + b
}

// handsOff reports whether harness h has handoffRule. mirasim and kimi cannot add to the
// system prompt of a session they continue, so their sessions are never offered the list.
func (w *Worker) handsOff(h string) bool { return w.cfg.Harnesses[h].Type == "sdk" }

// handoffLine is a reply that only hands the prompt on. The id is checked by promptOrHandOff,
// so a malformed one is still caught there rather than posted as it is.
var handoffLine = regexp.MustCompile(`^\[\[handoff\s*([^\]]*)\]\]$`)

// parseHandoff returns the session id in a reply that is only a hand-off line.
func parseHandoff(text string) (string, bool) {
	m := handoffLine.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return "", false
	}
	return strings.ToLower(strings.Trim(m[1], " `#<>")), true
}

// recentPrompts is how many of a session's latest prompts its line in the list shows: a
// session's title is set early and goes stale when the room keeps reusing it.
const recentPrompts = 3

// promptOrHandOff runs a prompt in the room's current session, offering it the room's
// other sessions to hand it on to.
func (w *Worker) promptOrHandOff(ctx context.Context, t proto.Task, prompt string) string {
	others := w.othersBlock(t.RoomID)
	if others == "" {
		return w.prompt(ctx, t, Run{Prompt: prompt}, true)
	}
	reply, out := w.run(ctx, t, Run{Prompt: prompt, Others: others}, true)
	if !out.handedOn {
		return reply
	}
	from := out.session
	w.mu.Lock()
	to, ok := Session{}, false
	if len(out.handoff) >= 6 && strings.Trim(out.handoff, "0123456789abcdef-") == "" {
		to, ok = sessionByPrefix(w.roomLocked(t.RoomID).Sessions, out.handoff)
	}
	w.mu.Unlock()
	if !ok || to.ID == from {
		log.Printf("task %s: session %s handed off to an id that is no other session of the room", t.ID, short(from))
		return fmt.Sprintf("会话 `%s` 认为这条消息属于本群另一个会话，但它给出的会话号 `%s` 对不上本群其他会话中的某一个，所以没有转交，这条消息也还没有回答。请以会话号开头重发（`sessions` 列出本群的会话）。",
			short(from), preview(out.handoff, 40))
	}
	if _, err := resolveWorkdir(w.cfg.WorkdirRoots, "/", to.Workdir); err != nil {
		return fmt.Sprintf("会话 `%s` 把这条消息转给会话 `%s`，但转不过去：%v", short(from), short(to.ID), err)
	}
	log.Printf("task %s: session %s handed the prompt to %s", t.ID, short(from), short(to.ID))
	return w.prompt(ctx, t, Run{Harness: to.harness(), Workdir: to.Workdir, SessionID: to.ID, Resume: true, Prompt: prompt,
		Route: fmt.Sprintf("由会话 `%s` 转到 `%s`", short(from), short(to.ID))}, false)
}

// othersBlock lists the room's most recent sessions other than its current one, to go before a
// prompt, or returns "" when the room has no current session or no other.
func (w *Worker) othersBlock(roomID string) string {
	w.mu.Lock()
	rs := w.roomLocked(roomID)
	var others []Session
	for _, s := range rs.Sessions {
		if rs.SessionID != "" && s.ID != rs.SessionID && len(others) < otherSessions {
			others = append(others, s)
		}
	}
	w.mu.Unlock()
	if len(others) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(othersHeader + "\n")
	for _, s := range others {
		title, recent := claudeSummary(w.claudeDir, s.Workdir, s.ID)
		fmt.Fprintf(&b, "- %s · %s · %s", short(s.ID), s.LastUsed.Local().Format("01-02 15:04"), preview(cmp.Or(title, s.Title), 60))
		for _, p := range recent {
			fmt.Fprintf(&b, " · 「%s」", preview(p, 40))
		}
		b.WriteString("\n")
	}
	b.WriteString(othersFooter + "\n\n")
	return b.String()
}

// claudeConfigDir is where Claude Code keeps its sessions: $CLAUDE_CONFIG_DIR, or ~/.claude.
func claudeConfigDir() string {
	if d := os.Getenv("CLAUDE_CONFIG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude")
}

var notAlnum = regexp.MustCompile(`[^A-Za-z0-9]`)

// claudeSummary is the last title Claude Code gave session id, from the "ai-title" lines of
// its transcript, and the session's last recentPrompts prompts, oldest first, without any list
// of other sessions put before them. The transcript is projects/<workdir with every character
// but letters and digits as "-">/<id>.jsonl under dir. Both are empty if there is no such
// transcript (another harness's session).
func claudeSummary(dir, workdir, id string) (title string, recent []string) {
	if dir == "" || workdir == "" || id == "" || filepath.Base(id) != id {
		return "", nil
	}
	f, err := os.Open(filepath.Join(dir, "projects", notAlnum.ReplaceAllString(workdir, "-"), id+".jsonl"))
	if err != nil {
		return "", nil
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<16)
	for {
		line, err := r.ReadBytes('\n')
		switch {
		case bytes.Contains(line, []byte(`"ai-title"`)):
			var rec struct {
				Type    string `json:"type"`
				AITitle string `json:"aiTitle"`
			}
			if json.Unmarshal(line, &rec) == nil && rec.Type == "ai-title" && strings.TrimSpace(rec.AITitle) != "" {
				title = rec.AITitle
			}
		case bytes.Contains(line, []byte(`"type":"user"`)):
			if p := userPrompt(line); p != "" {
				recent = append(recent, p)
				if len(recent) > recentPrompts {
					recent = recent[1:]
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("session %s: reading its transcript: %v", short(id), err)
			}
			return title, recent
		}
	}
}

// userPrompt is the text a person typed in a transcript line, or "" for any other line: a
// tool result, an injected meta message, or one without text. A list of other sessions put
// before the prompt is cut off.
func userPrompt(line []byte) string {
	var rec struct {
		Type    string `json:"type"`
		IsMeta  bool   `json:"isMeta"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(line, &rec) != nil || rec.Type != "user" || rec.IsMeta {
		return ""
	}
	var text string
	if json.Unmarshal(rec.Message.Content, &text) != nil {
		var blocks []struct{ Type, Text string }
		if json.Unmarshal(rec.Message.Content, &blocks) != nil {
			return ""
		}
		var parts []string
		for _, b := range blocks {
			if b.Type == "tool_result" {
				return ""
			}
			if b.Type == "text" {
				parts = append(parts, b.Text)
			}
		}
		text = strings.Join(parts, " ")
	}
	if strings.HasPrefix(text, othersHeader[:len("[AgentLink] Other sessions in this room")]) {
		if _, rest, ok := strings.Cut(text, othersFooter); ok {
			text = rest
		}
	}
	return strings.Join(strings.Fields(text), " ")
}
