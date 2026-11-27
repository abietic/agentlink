package worker

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// digestTrigger is the trigger name the suppression digest travels under. The worker submits
// it like a poll item; when the server hands the task back, the worker posts its text as it
// is, with no run. No configured trigger may take this name.
const digestTrigger = "digest"

// Digest makes the items the worker did not triage visible, so a bound or a dedupe rule that
// drops the wrong thing is noticed: every EverySec, when there is anything to report, it posts
// to RoomID which trigger items were not triaged and why. Without it nothing is recorded.
type Digest struct {
	RoomID   string `json:"room_id"`
	EverySec int    `json:"every_sec"` // default 3600, at least 60
}

// Why an item was not triaged, as the digest says it.
const (
	whyStale     = "older than max_age_sec when first seen"
	whyHeld      = "held at the burst bounds until it %s"
	whyBurst     = "refused at the burst bounds"
	whySilent    = "the run found nothing new ([[silent]])"
	whySkip      = "skipped by the poll: %s"
	whySignature = "signature" // a mention whose Tanka event did not check out
)

// suppressedKeep bounds the record between digests; past it only a count is kept.
const suppressedKeep = 500

type suppressed struct {
	Trigger string    `json:"trigger"`
	Key     string    `json:"key"`
	Why     string    `json:"why"`
	Summary string    `json:"summary"`
	At      time.Time `json:"at"`
}

type suppressedLog struct {
	Items []suppressed `json:"items"`
	Over  int          `json:"over"` // items past suppressedKeep, counted only
}

func (c *Config) checkDigest() error {
	d := c.Digest
	if d == nil {
		return nil
	}
	if d.RoomID == "" {
		return fmt.Errorf("digest: room_id is required")
	}
	d.EverySec = max(defaultInt(d.EverySec, 3600), 60)
	return nil
}

// suppressLocked records that an item of trigger was not triaged. Caller holds w.mu.
func (w *Worker) suppressLocked(trigger, key, why, text string) {
	if w.cfg.Digest == nil || trigger == digestTrigger {
		return
	}
	log.Printf("trigger %s: %s not triaged: %s", trigger, key, why)
	if len(w.supp.Items) >= suppressedKeep {
		w.supp.Over++
	} else {
		w.supp.Items = append(w.supp.Items, suppressed{Trigger: trigger, Key: key, Why: why, Summary: summarize(text), At: w.now()})
	}
	if err := w.store.put("suppressed", w.supp); err != nil {
		log.Printf("save suppressed: %v", err)
	}
}

func (w *Worker) suppress(trigger, key, why, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.suppressLocked(trigger, key, why, text)
}

// summarize keeps an item's first sentence or line, short enough to list.
func summarize(text string) string {
	s := strings.Join(strings.Fields(text), " ")
	if i := strings.Index(s, ". "); i > 0 {
		s = s[:i+1]
	}
	if utf8.RuneCountInString(s) > 160 {
		r := []rune(s)
		s = string(r[:159]) + "…"
	}
	return s
}

func (w *Worker) digestLoop(ctx context.Context) {
	defer w.wg.Done()
	tick := time.NewTicker(time.Duration(w.cfg.Digest.EverySec) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.sendDigest(ctx)
		}
	}
}

// sendDigest submits what was recorded since the last digest and forgets it once the server
// has it. Anything recorded meanwhile stays for the next one.
func (w *Worker) sendDigest(ctx context.Context) {
	w.mu.Lock()
	items, over := slices.Clone(w.supp.Items), w.supp.Over
	w.mu.Unlock()
	if len(items) == 0 && over == 0 {
		return
	}
	text := digestText(items, over, time.Duration(w.cfg.Digest.EverySec)*time.Second, w.cfg.MaxOutputChars)
	if err := w.submit(ctx, w.cfg.Digest.RoomID, digestTrigger, w.now().UTC().Format(time.RFC3339), text); err != nil {
		log.Printf("digest: %v (kept for the next one)", err)
		return
	}
	w.mu.Lock()
	w.supp.Items = w.supp.Items[min(len(items), len(w.supp.Items)):]
	w.supp.Over -= over
	if err := w.store.put("suppressed", w.supp); err != nil {
		log.Printf("save suppressed: %v", err)
	}
	w.mu.Unlock()
	log.Printf("digest: %d item(s) sent", len(items)+over)
}

// digestText lists items by trigger and reason, oldest first, within limit characters.
func digestText(items []suppressed, over int, every time.Duration, limit int) string {
	type group struct {
		trigger, why string
		items        []suppressed
	}
	var groups []*group
	for _, it := range items {
		i := slices.IndexFunc(groups, func(g *group) bool { return g.trigger == it.Trigger && g.why == it.Why })
		if i < 0 {
			groups = append(groups, &group{trigger: it.Trigger, why: it.Why})
			i = len(groups) - 1
		}
		groups[i].items = append(groups[i].items, it)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "🧾 Not triaged in the last %s: %d trigger item(s). Check that none of them should have run.", every.Round(time.Minute), len(items)+over)
	cut, nRedacted := 0, 0
	for _, g := range groups {
		head := fmt.Sprintf("\n\n**%s** · %s · %d", g.trigger, g.why, len(g.items))
		if limit > 0 && b.Len()+len(head) > limit-80 {
			cut += len(g.items)
			continue
		}
		b.WriteString(head)
		for _, it := range g.items {
			// Mask before the limit check, so a secret is never cut in half and left as a
			// fragment no pattern matches.
			body, n := redactSecrets(it.Summary)
			nRedacted += n
			line := fmt.Sprintf("\n- %s %s", it.At.Local().Format("15:04"), body)
			if it.Summary == "" {
				key, n := redactSecrets(it.Key)
				nRedacted += n
				line = fmt.Sprintf("\n- %s `%s`", it.At.Local().Format("15:04"), key)
			}
			if limit > 0 && b.Len()+len(line) > limit-80 {
				cut++
				continue
			}
			b.WriteString(line)
		}
	}
	if cut+over > 0 {
		fmt.Fprintf(&b, "\n\n…and %d more not listed; the worker log has each one.", cut+over)
	}
	if nRedacted > 0 {
		log.Printf("task %s: redacted %d secret-like strings", digestTrigger, nRedacted)
		b.WriteString("\n\n")
		b.WriteString(redactionNote(nRedacted))
	}
	return b.String()
}
