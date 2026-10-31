package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/draw"
	"image/png"
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

// TestRealSDKImage sends an image with no text through runner.mjs and checks Claude saw it.
func TestRealSDKImage(t *testing.T) {
	if os.Getenv("AGENTLINK_REAL_CLAUDE") != "1" {
		t.Skip("set AGENTLINK_REAL_CLAUDE=1 to run against the real Agent SDK")
	}
	runner, _ := filepath.Abs("../../sdk/runner.mjs")
	c := SDK{Command: []string{"node", runner}, Options: json.RawMessage(`{"permissionMode":"dontAsk","model":"haiku","systemPrompt":"Name the single colour that fills the image in one lowercase word."}`), Timeout: 2 * time.Minute}
	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	draw.Draw(img, img.Bounds(), image.NewUniform(color.RGBA{255, 0, 0, 255}), image.Point{}, draw.Src)
	var b bytes.Buffer
	png.Encode(&b, img)

	res, err := c.Run(context.Background(), Run{Workdir: t.TempDir(), SessionID: newUUID(), Images: []Image{{MediaType: "image/png", Data: b.Bytes()}}})
	if err != nil || res.IsError || !strings.Contains(strings.ToLower(res.Text), "red") {
		t.Fatalf("run: %+v, %v", res, err)
	}
	t.Logf("%q, cost $%.4f", res.Text, res.CostUSD)
}
