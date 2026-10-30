package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRealSDK runs sdk/runner.mjs twice through the real Agent SDK (a new session under a
// chosen id, then a resume of it) to prove the session stays the same and keeps context.
// It spends real tokens, so it only runs when AGENTLINK_REAL_CLAUDE=1.
func TestRealSDK(t *testing.T) {
	if os.Getenv("AGENTLINK_REAL_CLAUDE") != "1" {
		t.Skip("set AGENTLINK_REAL_CLAUDE=1 to run against the real Agent SDK")
	}
	runner, _ := filepath.Abs("../../sdk/runner.mjs")
	c := SDK{Command: []string{"node", runner}, Options: json.RawMessage(`{"permissionMode":"dontAsk","model":"haiku"}`), Timeout: 2 * time.Minute}
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
