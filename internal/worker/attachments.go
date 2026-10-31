package worker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"agentlink/internal/proto"
)

const (
	maxImages = 5
	// The Claude API refuses an image whose base64 form is over 5 MB, and the whole request
	// over 32 MB. Larger images are left out rather than scaled down.
	maxImageBytes = 5 << 20 / 4 * 3
)

// Image is one image for the run's first message; Data travels to runner.mjs as base64.
type Image struct {
	MediaType string `json:"media_type"`
	Data      []byte `json:"data"`
}

var imageTypes = []string{"image/png", "image/jpeg", "image/gif", "image/webp"}

var attachmentClient = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(r *http.Request, via []*http.Request) error {
		// Tanka's file links redirect to signed object-storage URLs.
		if r.URL.Scheme != "https" || len(via) > 3 {
			return errors.New("redirected off https or too often")
		}
		return nil
	},
}

// tankaFile accepts the links Tanka puts in attachments; anything else is not fetched.
func tankaFile(u *url.URL) bool {
	h := u.Hostname()
	return u.Scheme == "https" && (h == "tanka.ai" || strings.HasSuffix(h, ".tanka.ai"))
}

// fetchImages downloads the images among atts. It returns why each attachment it left
// out was left out, naming it but never its link, which anyone could download with.
func fetchImages(ctx context.Context, c *http.Client, allowed func(*url.URL) bool, atts []proto.Attachment) (imgs []Image, skipped []string) {
	for i, a := range atts {
		name := cmp.Or(a.Name, fmt.Sprintf("attachment %d", i+1))
		if len(imgs) == maxImages {
			skipped = append(skipped, fmt.Sprintf("`%s` (only %d images are passed on per message)", name, maxImages))
			continue
		}
		img, err := fetchImage(ctx, c, allowed, a.URL)
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("`%s` (%v)", name, err))
			continue
		}
		imgs = append(imgs, img)
	}
	return imgs, skipped
}

func fetchImage(ctx context.Context, c *http.Client, allowed func(*url.URL) bool, link string) (Image, error) {
	u, err := url.Parse(link)
	if err != nil || !allowed(u) {
		host := "?"
		if err == nil {
			host = u.Scheme + "://" + u.Host
		}
		return Image{}, fmt.Errorf("not a Tanka file link: %s", host)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return Image{}, errors.New("bad link")
	}
	resp, err := c.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) { // its message holds the link
			err = ue.Err
		}
		return Image{}, fmt.Errorf("download failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Image{}, fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxImageBytes+1))
	if err != nil {
		return Image{}, errors.New("download failed")
	}
	if len(data) > maxImageBytes {
		return Image{}, fmt.Errorf("larger than %.2f MB", float64(maxImageBytes)/(1<<20))
	}
	mt := http.DetectContentType(data) // the bytes decide, not the name or the header
	if !slices.Contains(imageTypes, mt) {
		return Image{}, errors.New("not a PNG, JPEG, GIF or WebP image")
	}
	return Image{MediaType: mt, Data: data}, nil
}
