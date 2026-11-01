package worker

import (
	"bytes"
	"context"
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

// TestRealMirasim runs a new session, a resume of it, and an image-only prompt through the
// mirasim desktop app (it must be running). The sessions land in mirasim's sidebar under
// this repo. It spends real tokens, so it only runs when AGENTLINK_REAL_MIRASIM=1.
func TestRealMirasim(t *testing.T) {
	if os.Getenv("AGENTLINK_REAL_MIRASIM") != "1" {
		t.Skip("set AGENTLINK_REAL_MIRASIM=1 to run against the mirasim desktop app")
	}
	home, _ := os.UserHomeDir()
	m := Mirasim{Command: []string{filepath.Join(home, ".mirasim/bin/mirasim")}, Port: 4970, Agent: "claude", Model: os.Getenv("AGENTLINK_REAL_MIRASIM_MODEL"), Route: "own", Timeout: 3 * time.Minute}
	dir, _ := filepath.Abs("../..")
	ctx := context.Background()

	first, err := m.Run(ctx, Run{Workdir: dir, SessionID: newUUID(), Prompt: "Remember the code word PAPAYA. Reply with just OK."})
	if err != nil || first.IsError || !isUUID(first.SessionID) {
		t.Fatalf("first run: %+v, %v", first, err)
	}
	second, err := m.Run(ctx, Run{Workdir: dir, SessionID: first.SessionID, Resume: true, Prompt: "What was the code word? Reply with just the word."})
	if err != nil || second.SessionID != first.SessionID || !strings.Contains(strings.ToUpper(second.Text), "PAPAYA") {
		t.Fatalf("resumed run: %+v, %v", second, err)
	}

	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	draw.Draw(img, img.Bounds(), image.NewUniform(color.RGBA{0, 0, 255, 255}), image.Point{}, draw.Src)
	var b bytes.Buffer
	png.Encode(&b, img)
	third, err := m.Run(ctx, Run{Workdir: dir, SessionID: newUUID(), Prompt: "Name the single colour that fills the attached image in one lowercase word.", Images: []Image{{MediaType: "image/png", Data: b.Bytes()}}})
	if err != nil || third.IsError || !strings.Contains(strings.ToLower(third.Text), "blue") {
		t.Fatalf("image run: %+v, %v", third, err)
	}
	t.Logf("sessions %s, %s; %s + %s + %s", short(first.SessionID), short(third.SessionID), first.Duration.Round(time.Second), second.Duration.Round(time.Second), third.Duration.Round(time.Second))
}
