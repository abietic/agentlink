package worker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRealKimi runs the real kimi CLI twice (a new session, then a resume of it) and asks
// its local server for the plan quota. It spends real quota, so it only runs when
// AGENTLINK_REAL_KIMI=1.
func TestRealKimi(t *testing.T) {
	if os.Getenv("AGENTLINK_REAL_KIMI") != "1" {
		t.Skip("set AGENTLINK_REAL_KIMI=1 to run against the real kimi CLI")
	}
	k := Kimi{Command: []string{"kimi"}, Timeout: 3 * time.Minute}
	dir := t.TempDir()
	ctx := context.Background()
	res, err := k.Run(ctx, Run{Workdir: dir, SessionID: newUUID(), Prompt: "Reply with exactly the word: pelican"})
	if err != nil || !isUUID(res.SessionID) || !strings.Contains(strings.ToLower(res.Text), "pelican") {
		t.Fatalf("new session: %v %+v", err, res)
	}
	again, err := k.Run(ctx, Run{Workdir: dir, SessionID: res.SessionID, Resume: true, Prompt: "Which word did you reply with? Answer with just that word."})
	if err != nil || again.SessionID != res.SessionID || !strings.Contains(strings.ToLower(again.Text), "pelican") {
		t.Fatalf("resume: %v %+v", err, again)
	}
	l, err := k.usedUp(ctx)
	if err != nil {
		t.Fatalf("quota: %v", err)
	}
	t.Logf("used-up window: %+v", l)
}
