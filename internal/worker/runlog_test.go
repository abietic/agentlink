package worker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPruneRunLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	old := `{"at":"2026-08-01T00:00:00Z","event":"run","task":"t-old"}` + "\n"
	recent := `{"at":"2026-11-01T00:00:00Z","event":"result","task":"t-new"}` + "\n"
	os.WriteFile(path, []byte(old+recent+"not json\n"), 0o600)
	if err := pruneRunLog(path, now); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != recent+"not json\n" {
		t.Fatalf("after pruning: %q", b)
	}
	if err := pruneRunLog(filepath.Join(t.TempDir(), "missing"), now); err != nil {
		t.Fatalf("missing log: %v", err)
	}
}
