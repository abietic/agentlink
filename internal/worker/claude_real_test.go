package worker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRealClaude runs the installed CLI twice (a new session, then --resume) to prove the
// flags and stdin prompt work end to end. It spends real tokens, so it only runs when
// AGENTLINK_REAL_CLAUDE=1.
func TestRealClaude(t *testing.T) {
	if os.Getenv("AGENTLINK_REAL_CLAUDE") != "1" {
		t.Skip("set AGENTLINK_REAL_CLAUDE=1 to run against the real claude CLI")
	}
	c := Claude{Bin: "claude", Args: []string{"--permission-prompts", "none", "--model", "haiku"}, Timeout: 2 * time.Minute}
	dir := t.TempDir()
	sid := newUUID()
	ctx := context.Background()

	first, err := c.Run(ctx, Run{Workdir: dir, SessionID: sid, Prompt: "Remember the code word PAPAYA. Reply with just OK."})
	if err != nil || first.IsError || first.SessionID != sid {
		t.Fatalf("first run: %+v, %v", first, err)
	}
	second, err := c.Run(ctx, Run{Workdir: dir, SessionID: sid, Resume: true, Prompt: "What was the code word? Reply with just the word."})
	if err != nil || second.SessionID != sid || !strings.Contains(strings.ToUpper(second.Text), "PAPAYA") {
		t.Fatalf("resumed run: %+v, %v", second, err)
	}
	t.Logf("cost $%.4f + $%.4f", first.CostUSD, second.CostUSD)
}
