package worker

import (
	"bytes"
	"context"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"agentlink/internal/proto"
)

func pngBytes(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// fileServer serves Tanka-like file links: /png, /text, /big, /missing, and /hop, which
// redirects to plain http.
func fileServer(t *testing.T) *httptest.Server {
	pic := pngBytes(t)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/png":
			w.Header().Set("Content-Type", "text/plain") // the bytes decide, not this
			w.Write(pic)
		case "/text":
			w.Write([]byte("just text"))
		case "/big":
			w.Write(append(pic, make([]byte, maxImageBytes)...))
		case "/hop":
			http.Redirect(w, r, "http://example.com/x", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestFetchImages(t *testing.T) {
	ts := fileServer(t)
	c := ts.Client()
	c.CheckRedirect = attachmentClient.CheckRedirect
	down := httptest.NewTLSServer(http.NotFoundHandler())
	down.Close() // a link whose download fails before any response
	allowed := func(u *url.URL) bool {
		return u.Scheme == "https" && (ts.URL == "https://"+u.Host || down.URL == "https://"+u.Host)
	}

	atts := []proto.Attachment{
		{URL: ts.URL + "/png?token=SECRET", Name: "shot.png"},
		{URL: ts.URL + "/text?token=SECRET", Name: "notes.txt"},
		{URL: ts.URL + "/big?token=SECRET", Name: "huge.png"},
		{URL: ts.URL + "/missing?token=SECRET"},
		{URL: ts.URL + "/hop?token=SECRET", Name: "hop.png"},
		{URL: down.URL + "/png?token=SECRET", Name: "down.png"},
		{URL: "https://evil.example/png?token=SECRET", Name: "elsewhere.png"},
	}
	imgs, skipped := fetchImages(context.Background(), c, allowed, atts)
	if len(imgs) != 1 || imgs[0].MediaType != "image/png" || !bytes.Equal(imgs[0].Data, pngBytes(t)) {
		t.Fatalf("images = %+v", imgs)
	}
	want := []string{"`notes.txt` (not a PNG", "`huge.png` (larger than", "`attachment 4` (download failed: HTTP 404)",
		"`hop.png` (download failed: redirected off https", "`down.png` (download failed: ", "`elsewhere.png` (not a Tanka file link: https://evil.example)"}
	if len(skipped) != len(want) {
		t.Fatalf("skipped = %q", skipped)
	}
	for i, w := range want {
		if !strings.HasPrefix(skipped[i], w) {
			t.Errorf("skipped[%d] = %q, want prefix %q", i, skipped[i], w)
		}
		if strings.Contains(skipped[i], "SECRET") {
			t.Errorf("skipped[%d] leaks the link: %q", i, skipped[i])
		}
	}

	many := make([]proto.Attachment, maxImages+1)
	for i := range many {
		many[i] = atts[0]
	}
	if imgs, skipped := fetchImages(context.Background(), c, allowed, many); len(imgs) != maxImages || len(skipped) != 1 {
		t.Fatalf("over the limit: %d images, skipped %q", len(imgs), skipped)
	}
}

func TestTankaFile(t *testing.T) {
	for link, ok := range map[string]bool{
		"https://gw-tk.tanka.ai/npc/v2/file/abc": true,
		"https://tanka.ai/f":                     true,
		"http://gw-tk.tanka.ai/f":                false,
		"https://tanka.ai.evil.example/f":        false,
		"https://eviltanka.ai/f":                 false,
		"https://localhost/f":                    false,
	} {
		u, _ := url.Parse(link)
		if tankaFile(u) != ok {
			t.Errorf("tankaFile(%s) = %v", link, !ok)
		}
	}
}

// TestImagePrompt: the images of a mention reach the runner, an image alone is a prompt,
// and what was left out is said in the reply.
func TestImagePrompt(t *testing.T) {
	ts := fileServer(t)
	f := newFixture(t)
	w, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.files = ts.Client()
	w.fileOK = func(*url.URL) bool { return true }
	task := proto.Task{ID: "t", RoomID: "R1", UserID: "U1", Attachments: []proto.Attachment{
		{URL: ts.URL + "/png", Name: "shot.png"}, {URL: ts.URL + "/text", Name: "notes.txt"},
	}}

	reply := w.handle(context.Background(), task)
	got := f.last(t)
	if len(got.Images) != 1 || got.Images[0].MediaType != "image/png" || got.Prompt != "" {
		t.Fatalf("runner got %+v", got)
	}
	if !strings.Contains(reply, "⚠️ Not passed on to claude: `notes.txt` (not a PNG") {
		t.Fatalf("reply: %q", reply)
	}
	if title := w.state["R1"].Sessions[0].Title; title != "(image)" {
		t.Fatalf("session title %q", title)
	}

	task.Attachments = task.Attachments[1:]
	if reply := w.handle(context.Background(), task); !strings.HasPrefix(reply, "Nothing to run") {
		t.Fatalf("no text and no image: %q", reply)
	}
}
