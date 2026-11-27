package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/tanka"
)

// This file is the worker's own check of what drives it. The server still receives every
// Tanka event and routes it, but with tanka_verify set the worker re-checks the signed event
// carried inside the sealed task, uses that event's fields instead of the server's, refuses a
// replayed one, and only takes back trigger tasks it submitted itself. With the setting off,
// none of it runs and the worker behaves as before.

// checkTanka reports whether a task may run. A non-trigger task is verified from the Tanka
// event it carries; a triggered task must match a record of a trigger this worker submitted.
// A task that does not check out is dropped when tanka_verify is "require", and only counted
// and logged when it is "observe"; off is handled by the caller not calling this at all.
func (w *Worker) checkTanka(t *proto.Task) bool {
	if w.cfg.TankaVerify == "" {
		return true
	}
	if t.Trigger == "" {
		if err := w.verifyTanka(t); err != nil {
			w.suppress("mention", t.ID, whySignature, "")
			return !w.badTanka(*t, err)
		}
		return true
	}
	if err := w.consumeTrigger(*t); err != nil {
		return !w.badTanka(*t, err)
	}
	return true
}

// verifyTanka checks t against the Tanka event it carries and replaces t's routing fields
// with the event's own, so a compromised server cannot put words in a sender's mouth. It
// returns why the event does not check out, if it does not; where the event could be parsed,
// t is filled from it even on a failure, so "observe" runs the authentic text.
func (w *Worker) verifyTanka(t *proto.Task) error {
	if len(t.Raw) == 0 {
		return errors.New("the task carries no Tanka event")
	}
	if err := tanka.Verify(w.tankaSecret, t.RawTS, t.RawSig, t.Raw, w.now(), w.cfg.tankaMaxAge()); err != nil {
		return err
	}
	var env tanka.Envelope
	if err := json.Unmarshal(t.Raw, &env); err != nil {
		return errors.New("the Tanka event does not parse")
	}
	ev := env.Event
	if env.Type != "event_callback" || ev == nil || ev.Type != "app_mention" {
		return errors.New("the Tanka event is not an app_mention")
	}
	replay := env.EventID != "" && w.tankaEventSeen(env.EventID)
	room := t.RoomID
	// From here on the event's own fields are the truth; the server's are only hints.
	t.RoomID, t.RoomName = ev.RoomID, ev.RoomName
	t.UserID, t.UserName = ev.UserID, ev.UserName
	t.SenderType = ev.SenderType
	t.Text = textAfterMention(ev.Text, t.Text)
	t.Attachments = attachmentsFromRaw(ev.Attachments)
	if reply := ev.ReplyMessageID(); reply != "" {
		t.ReplyMessageID = reply
	} else {
		// No quote in the signed event: the server's routing hint is not used either.
		t.ReplyMessageID, t.RouteSessionID = "", ""
	}
	if ev.RoomID != room {
		return errors.New("the event is for another room than the task")
	}
	if w.cfg.TankaAppID != "" && env.APIAppID != w.cfg.TankaAppID {
		return errors.New("the event's app id is not tanka_app_id")
	}
	if replay {
		return errors.New("a replay of a Tanka event already handled")
	}
	if env.EventID != "" {
		w.markTankaEvent(env.EventID)
	}
	return nil
}

// textAfterMention is the signed event's text with the bot mention removed. The server's
// already-stripped text is used as a hint when it is a tail of the event's own text, so a
// display name with spaces is cut where the server cut it; otherwise the first mention is
// dropped here. Either way only words from the signed text survive.
func textAfterMention(raw, hint string) string {
	raw = strings.TrimSpace(raw)
	if hint = strings.TrimSpace(hint); hint != "" && strings.HasSuffix(raw, hint) {
		return hint
	}
	if strings.HasPrefix(raw, "<@") {
		if i := strings.IndexByte(raw, '>'); i > 0 {
			return strings.TrimSpace(raw[i+1:])
		}
	}
	if strings.HasPrefix(raw, "@") {
		_, rest := proto.CutWord(raw)
		return rest
	}
	return raw
}

// attachmentsFromRaw copies the attachments of the signed event; the server's list is unused.
func attachmentsFromRaw(as []tanka.Attachment) []proto.Attachment {
	var out []proto.Attachment
	for _, a := range as {
		out = append(out, proto.Attachment{URL: a.URL, Name: a.Name})
	}
	return out
}

// tankaEventSeen reports whether id is a Tanka event already handled, within its window.
func (w *Worker) tankaEventSeen(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	until, ok := w.tankaSeen[id]
	return ok && w.now().Before(until)
}

// markTankaEvent remembers a Tanka event id for maxAge+1h, so a replay is refused. It is
// kept in worker.db, so a worker restart does not forget it either.
func (w *Worker) markTankaEvent(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	w.tankaSeen[id] = now.Add(w.cfg.tankaMaxAge() + time.Hour)
	for k, until := range w.tankaSeen {
		if !now.Before(until) {
			delete(w.tankaSeen, k)
		}
	}
	if err := w.store.put("tanka_seen", w.tankaSeen); err != nil {
		log.Printf("save tanka events: %v", err)
	}
}

// badTanka counts and logs a task that did not check out, and reports whether it is dropped:
// in "require" the task gets no reply at all, so the room does not hear from a forged one.
func (w *Worker) badTanka(t proto.Task, err error) bool {
	w.mu.Lock()
	w.tankaFailed++
	drop := w.cfg.TankaVerify == "require"
	if drop {
		w.tankaRejected++
	}
	w.mu.Unlock()
	if drop {
		log.Printf("task %s: rejected: %v", t.ID, err)
		return true
	}
	log.Printf("task %s: verification failed (tanka_verify: observe), running anyway: %v", t.ID, err)
	return false
}

// triggerKey names a submitted trigger task by everything that makes it the one this worker
// asked for: its trigger, its key and the exact text it submitted.
func triggerKey(trigger, key, text string) string {
	sum := sha256.Sum256([]byte(text))
	return trigger + "\x00" + key + "\x00" + hex.EncodeToString(sum[:])
}

// recordTrigger remembers a trigger task this worker asked the server for, for its trigger's
// max_age_sec plus an hour, so only that task is accepted back.
func (w *Worker) recordTrigger(trigger, key, text string) {
	keep := 3600 * time.Second
	if tr := w.trigger(trigger); tr != nil {
		keep = time.Duration(tr.MaxAgeSec) * time.Second
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	w.triggerLedger[triggerKey(trigger, key, text)] = now.Add(keep + time.Hour)
	for k, until := range w.triggerLedger {
		if !now.Before(until) {
			delete(w.triggerLedger, k)
		}
	}
	if err := w.store.put("trigger_ledger", w.triggerLedger); err != nil {
		log.Printf("save trigger ledger: %v", err)
	}
}

// consumeTrigger requires t to match an unused record of a trigger this worker submitted, and
// consumes it so the task is accepted once. A task the server made itself (its Sentry hook)
// has no record.
func (w *Worker) consumeTrigger(t proto.Task) error {
	k := triggerKey(t.Trigger, t.Key, t.Text)
	w.mu.Lock()
	defer w.mu.Unlock()
	until, ok := w.triggerLedger[k]
	if !ok || !w.now().Before(until) {
		delete(w.triggerLedger, k)
		return fmt.Errorf("no matching trigger submission for %q", t.Trigger)
	}
	delete(w.triggerLedger, k)
	if err := w.store.put("trigger_ledger", w.triggerLedger); err != nil {
		log.Printf("save trigger ledger: %v", err)
	}
	return nil
}

// forgetTrigger drops a record whose submission the server refused, so it cannot later admit
// a task this worker never got to ask for.
func (w *Worker) forgetTrigger(trigger, key, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.triggerLedger, triggerKey(trigger, key, text))
	if err := w.store.put("trigger_ledger", w.triggerLedger); err != nil {
		log.Printf("save trigger ledger: %v", err)
	}
}
