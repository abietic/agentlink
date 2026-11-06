package worker

import (
	"cmp"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"time"
)

// limitRetry is how long a harness rests after a limit that did not say when it lifts.
const limitRetry = 30 * time.Minute

// Limit is a usage limit a harness ran into.
type Limit struct {
	Name     string    `json:"name,omitempty"`      // the harness's own name for it, e.g. "seven_day"
	ResetsAt time.Time `json:"resets_at,omitempty"` // zero if the harness did not say
}

// limitNames are readable names for the limits harnesses report.
var limitNames = map[string]string{
	"five_hour":                  "5-hour limit",
	"seven_day":                  "weekly limit",
	"seven_day_opus":             "weekly Opus limit",
	"seven_day_sonnet":           "weekly Sonnet limit",
	"seven_day_overage_included": "weekly limit",
	"overage":                    "extra-usage limit",
	"limit5h":                    "5-hour limit",
	"limit7d":                    "weekly limit",
	"monthTotal":                 "monthly limit",
	"monthCode":                  "monthly coding limit",
}

func (l Limit) String() string {
	if l.Name == "" {
		return "a usage limit"
	}
	if n, ok := limitNames[l.Name]; ok {
		return fmt.Sprintf("its %s (`%s`)", n, l.Name)
	}
	return fmt.Sprintf("its `%s` limit", l.Name)
}

// until says when l lifts, in this device's time zone.
func (l Limit) until() string {
	return l.ResetsAt.Local().Format("Mon 01-02 15:04 MST")
}

// limitedLocked reports the limit h is still resting at.
func (w *Worker) limitedLocked(h string) (Limit, bool) {
	l, ok := w.limits[h]
	return l, ok && w.now().Before(l.ResetsAt)
}

// hitLimit records that h stopped at l and returns the notice to post, which is empty unless
// this is the first time since h last finished a run without one.
func (w *Worker) hitLimit(h string, l Limit) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, again := w.limits[h]
	known := !l.ResetsAt.IsZero()
	if !known {
		l.ResetsAt = w.now().Add(limitRetry)
	}
	w.limits[h] = l
	w.saveLimitsLocked()
	log.Printf("harness %s: at %s until %s (first=%v)", h, cmp.Or(l.Name, "an unnamed limit"), l.ResetsAt.Format(time.RFC3339), !again)
	if again {
		return ""
	}
	if known {
		return fmt.Sprintf("⚠️ `%s` hit %s; it resets %s. Until then this device does not run prompts on it, and says so only this once.", h, l, l.until())
	}
	return fmt.Sprintf("⚠️ `%s` hit %s and did not say when it resets; this device tries it again after %s, and says so only this once.", h, l, l.until())
}

// clearLimit forgets h's limit once a run on it finished without one.
func (w *Worker) clearLimit(h string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.limits[h]; ok {
		delete(w.limits, h)
		w.saveLimitsLocked()
		log.Printf("harness %s: no longer at a limit", h)
	}
}

func (w *Worker) saveLimitsLocked() {
	if err := w.store.put("limits", w.limits); err != nil {
		log.Printf("save limits: %v", err)
	}
}

// limitsLocked describes the harnesses resting at a limit, for `status`.
func (w *Worker) limitsLocked() string {
	var b strings.Builder
	for _, h := range slices.Sorted(maps.Keys(w.cfg.Harnesses)) {
		if l, ok := w.limitedLocked(h); ok {
			fmt.Fprintf(&b, "- **At a limit** `%s`: %s, until %s\n", h, strings.TrimPrefix(l.String(), "its "), l.until())
		}
	}
	return b.String()
}
