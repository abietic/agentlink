package worker

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"strings"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

// Trigger lets tasks start without anyone typing. Tasks named after it come from the server
// (e.g. its Sentry hook) or from Command, which the worker runs every EverySec: each stdout
// line is a JSON item {"key", "at", "text"}, and an item whose key has not been seen at that
// time or later is queued for RoomID. A triggered task skips allowed_users and the room
// commands and starts a new session in Workdir. That session joins the room's history but
// does not become its current one: people follow up by starting a message with its id, so
// each alert keeps its own thread and they run side by side.
type Trigger struct {
	Name      string   `json:"name"`
	RoomID    string   `json:"room_id"` // required with command: the room its tasks run for and post to
	Workdir   string   `json:"workdir"` // default default_workdir
	Command   []string `json:"command"`
	EverySec  int      `json:"every_sec"`   // default 60
	DedupeSec int      `json:"dedupe_sec"`  // a key that ran within this long is skipped; default 1800
	MaxAgeSec int      `json:"max_age_sec"` // items older than this at first sight are skipped; default 3600
	// MaxPending bounds how many of this trigger's tasks are admitted and not yet answered;
	// MaxPerMin bounds how many it admits in any minute. Together they keep a burst (a rollout,
	// a flapping monitor) from taking every lane the device has and burying people's prompts.
	// scan holds items over the bound until there is room; those that leave its list or go
	// stale first are counted and reported on the trigger's next reply and in the digest.
	// 0 takes the default; -1 removes that bound.
	MaxPending int `json:"max_pending"` // default 2
	MaxPerMin  int `json:"max_per_min"` // default 6
	// Themes colors a reply by its content: the first line that starts with one of these
	// labels (ignoring leading markdown emphasis) gets that Tanka theme. Longest label wins.
	Themes map[string]string `json:"themes"`
}

// item is one line of a trigger command's output.
type item struct {
	Key  string    `json:"key"`
	At   time.Time `json:"at"`
	Text string    `json:"text"`
	// Skip, when set, says why the command chose not to raise this item (it is resolved, say).
	// It is not run; it is marked seen and, with a digest, listed there with this reason.
	Skip string `json:"skip,omitempty"`
}

// marksKeep bounds how long dedupe marks are kept.
const marksKeep = 7 * 24 * time.Hour

// Defaults bound a trigger's burst, so a flood of alerts cannot take the device's lanes.
const (
	defaultMaxPending = 2 // tasks admitted and unanswered
	defaultMaxPerMin  = 6 // tasks admitted per minute
)

func (c *Config) checkTriggers() error {
	seen := map[string]bool{}
	for i := range c.Triggers {
		tr := &c.Triggers[i]
		if tr.Name == "" || seen[tr.Name] {
			return fmt.Errorf("triggers[%d]: name must be set and unique", i)
		}
		if tr.Name == digestTrigger {
			return fmt.Errorf("triggers[%d]: %q is the suppression digest's name", i, digestTrigger)
		}
		seen[tr.Name] = true
		if len(tr.Command) > 0 && tr.RoomID == "" {
			return fmt.Errorf("trigger %s: room_id is required with command", tr.Name)
		}
		for j, a := range tr.Command {
			tr.Command[j] = expandHome(a)
		}
		dir, err := resolveWorkdir(c.WorkdirRoots, "/", cmp.Or(tr.Workdir, c.DefaultWorkdir))
		if err != nil {
			return fmt.Errorf("trigger %s: workdir: %w", tr.Name, err)
		}
		tr.Workdir = dir
		tr.EverySec = defaultInt(tr.EverySec, 60)
		tr.DedupeSec = defaultInt(tr.DedupeSec, 1800)
		tr.MaxAgeSec = defaultInt(tr.MaxAgeSec, 3600)
		if tr.MaxPending < -1 || tr.MaxPerMin < -1 {
			return fmt.Errorf("trigger %s: max_pending and max_per_min are 0 (default) or -1 (no bound)", tr.Name)
		}
		if tr.MaxPending == 0 {
			tr.MaxPending = defaultMaxPending
		}
		if tr.MaxPerMin == 0 {
			tr.MaxPerMin = defaultMaxPerMin
		}
		for label, theme := range tr.Themes {
			if label == "" || !slices.Contains(tanka.Themes, theme) {
				return fmt.Errorf("trigger %s: themes[%q] = %q: label must be set and theme one of %v", tr.Name, label, theme, tanka.Themes)
			}
		}
	}
	return nil
}

func defaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// theme is the Tanka theme for a reply: the first line that starts with a label in
// themes picks it, the longest matching label winning; no match means no theme.
func theme(themes map[string]string, text string) string {
	for line := range strings.Lines(text) {
		line = strings.TrimLeft(line, "*_ \t")
		best := ""
		for label := range themes {
			if strings.HasPrefix(line, label) && len(label) > len(best) {
				best = label
			}
		}
		if best != "" {
			return themes[best]
		}
	}
	return ""
}

func (w *Worker) trigger(name string) *Trigger {
	for i := range w.cfg.Triggers {
		if w.cfg.Triggers[i].Name == name {
			return &w.cfg.Triggers[i]
		}
	}
	return nil
}

// admit decides whether a triggered task runs: its trigger must be configured here and its
// key must not have run within the trigger's dedupe window. Admitting records the run.
func (w *Worker) admit(t proto.Task) error {
	tr := w.trigger(t.Trigger)
	if tr == nil {
		return errors.New("no trigger named " + t.Trigger + " in this worker's config")
	}
	return w.admitTrigger(tr, t, false)
}

// admitTrigger is admit for a configured trigger; with retry the dedupe window is skipped.
func (w *Worker) admitTrigger(tr *Trigger, t proto.Task, retry bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	mark := "ran/" + tr.Name + "/" + t.Key
	if last, ok := w.marks[mark]; ok && !retry && t.Key != "" && w.now().Sub(last) < time.Duration(tr.DedupeSec)*time.Second {
		return fmt.Errorf("key %s already ran at %s", t.Key, last.Local().Format("15:04"))
	}
	// The burst gate comes before the mark: an item it refuses is still marked, so the poll
	// does not hand it back every minute, and counted, so the trigger's next reply says how
	// many were dropped. Dropping is the point — a burst must not queue. A recovery retry is
	// work already admitted before an interruption, so it is counted but never refused.
	if err := w.burstLocked(tr); err != nil && !retry {
		w.setMarkLocked(mark, w.now())
		w.tgDrop[tr.Name]++
		w.suppressLocked(tr.Name, t.Key, whyBurst, t.Text)
		return err
	}
	w.setMarkLocked(mark, w.now())
	w.tgLoad[tr.Name]++
	if w.recovery(tr.Name) != nil {
		w.trackLocked(t)
	}
	return nil
}

// burstLocked admits one task against tr's bounds, or says why not. Caller holds w.mu.
func (w *Worker) burstLocked(tr *Trigger) error {
	if tr.MaxPending > 0 && w.tgLoad[tr.Name] >= tr.MaxPending {
		return fmt.Errorf("%d of its tasks are still unanswered", w.tgLoad[tr.Name])
	}
	if tr.MaxPerMin > 0 {
		cut := w.now().Add(-time.Minute)
		runs := w.tgRuns[tr.Name]
		i := 0
		for i < len(runs) && runs[i].Before(cut) {
			i++
		}
		runs = append(runs[:0], runs[i:]...)
		if len(runs) >= tr.MaxPerMin {
			w.tgRuns[tr.Name] = runs
			return fmt.Errorf("%d ran in the last minute", len(runs))
		}
		w.tgRuns[tr.Name] = append(runs, w.now())
	}
	return nil
}

// burstRoom says how many more tasks tr may start right now without admitting any: -1 when
// nothing bounds it. scan spends it item by item, because the tasks it submits are admitted
// only after a server round trip and would all see the same free slot.
func (w *Worker) burstRoom(tr *Trigger) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	room := -1
	if tr.MaxPending > 0 {
		room = max(0, tr.MaxPending-w.tgLoad[tr.Name])
	}
	if tr.MaxPerMin > 0 {
		cut := w.now().Add(-time.Minute)
		n := 0
		for _, at := range w.tgRuns[tr.Name] {
			if !at.Before(cut) {
				n++
			}
		}
		if r := max(0, tr.MaxPerMin-n); room < 0 || r < room {
			room = r
		}
	}
	return room
}

// triggerFinished gives back the slot a trigger task held. When the task's reply will be
// posted (posts), it adds a note about items the trigger did not run; a silent reply ("")
// is broken by that note, because people must hear that alerts went unexamined. Caller does
// not hold w.mu.
func (w *Worker) triggerFinished(trigger, reply string, posts bool) string {
	w.mu.Lock()
	if w.tgLoad[trigger] > 0 {
		w.tgLoad[trigger]--
	}
	dropped := w.tgDrop[trigger]
	if posts {
		w.tgDrop[trigger] = 0
	}
	w.mu.Unlock()
	if dropped == 0 || !posts {
		return reply
	}
	bounds := "the trigger's bounds"
	if tr := w.trigger(trigger); tr != nil {
		bounds = fmt.Sprintf("`max_pending` %d and `max_per_min` %d in worker.json", tr.MaxPending, tr.MaxPerMin)
	}
	note := fmt.Sprintf("⚠️ %d `%s` item(s) were not run: the trigger was at its bounds (%s) until they left its list or went stale. Raise those bounds if this was not a burst.",
		dropped, trigger, bounds)
	if reply == "" {
		return note
	}
	return reply + "\n\n" + note
}

func (w *Worker) setMarkLocked(k string, at time.Time) {
	w.marks[k] = at
	for k, v := range w.marks {
		if w.now().Sub(v) > marksKeep {
			delete(w.marks, k)
		}
	}
	if err := w.store.put("marks", w.marks); err != nil {
		log.Printf("save marks: %v", err)
	}
}

// watch runs tr's command every EverySec until ctx ends.
func (w *Worker) watch(ctx context.Context, tr Trigger) {
	defer w.wg.Done()
	tick := time.NewTicker(time.Duration(tr.EverySec) * time.Second)
	defer tick.Stop()
	for {
		if err := w.scan(ctx, tr); err != nil && ctx.Err() == nil {
			log.Printf("trigger %s: %v", tr.Name, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// scan runs tr's command once and queues the items it has not seen yet. It submits no more
// than the trigger's bounds have room for; the rest wait unmarked, so a later poll that still
// lists them queues them once there is room. One that leaves the list or goes stale first is
// counted as dropped and reported on the trigger's next reply.
func (w *Worker) scan(ctx context.Context, tr Trigger) error {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, tr.Command[0], tr.Command[1:]...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("command: %v: %s", err, bytes.TrimSpace(stderr.Bytes()[:min(stderr.Len(), 300)]))
	}
	room := w.burstRoom(&tr)
	listed := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var it item
		if err := json.Unmarshal(sc.Bytes(), &it); err != nil || it.Key == "" || it.Text == "" || it.At.IsZero() {
			log.Printf("trigger %s: skipping a line that is not {key, at, text}", tr.Name)
			continue
		}
		listed[it.Key] = true
		mark := "seen/" + tr.Name + "/" + it.Key
		w.mu.Lock()
		last, seen := w.marks[mark]
		stale := !seen && w.now().Sub(it.At) > time.Duration(tr.MaxAgeSec)*time.Second
		if stale {
			w.setMarkLocked(mark, it.At)
			if !w.unwaitLocked(tr.Name, it.Key, "went stale") && it.Skip == "" {
				w.suppressLocked(tr.Name, it.Key, whyStale, it.Text)
			}
		}
		fresh := !stale && (!seen || it.At.After(last))
		if fresh && it.Skip != "" {
			w.setMarkLocked(mark, it.At)
			w.unwaitLocked(tr.Name, it.Key, "")
			w.suppressLocked(tr.Name, it.Key, fmt.Sprintf(whySkip, it.Skip), it.Text)
			fresh = false
		}
		w.mu.Unlock()
		if !fresh {
			continue
		}
		if room == 0 {
			w.mu.Lock()
			waits := w.tgWait[tr.Name]
			if waits == nil {
				waits = map[string]string{}
				w.tgWait[tr.Name] = waits
			}
			_, held := waits[it.Key]
			first := !held
			waits[it.Key] = it.Text
			w.mu.Unlock()
			if first {
				log.Printf("trigger %s: holding %s: the trigger is at its bounds", tr.Name, it.Key)
			}
			continue
		}
		if err := w.submit(ctx, tr.RoomID, tr.Name, it.Key, it.Text); err != nil {
			return fmt.Errorf("queue %s: %w", it.Key, err)
		}
		if room > 0 {
			room--
		}
		log.Printf("trigger %s: queued %s", tr.Name, it.Key)
		w.mu.Lock()
		w.setMarkLocked(mark, it.At)
		delete(w.tgWait[tr.Name], it.Key)
		w.mu.Unlock()
	}
	if err := sc.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	for k := range w.tgWait[tr.Name] {
		if !listed[k] {
			w.unwaitLocked(tr.Name, k, "left the list")
		}
	}
	w.mu.Unlock()
	return nil
}

// unwaitLocked gives up on a held item, saying why: it is counted as dropped and recorded for
// the digest. An empty why only forgets it. Reports whether the item was held. Caller holds w.mu.
func (w *Worker) unwaitLocked(trigger, key, why string) bool {
	text, held := w.tgWait[trigger][key]
	if !held {
		return false
	}
	delete(w.tgWait[trigger], key)
	if why == "" {
		return true
	}
	w.tgDrop[trigger]++
	w.suppressLocked(trigger, key, fmt.Sprintf(whyHeld, why), text)
	log.Printf("trigger %s: dropped %s: it %s before there was room", trigger, key, why)
	return true
}

// submit asks the server to queue a triggered task for roomID.
func (w *Worker) submit(ctx context.Context, roomID, trigger, key, text string) error {
	plain, _ := json.Marshal(proto.TriggerBody{Trigger: trigger, Key: key, Text: text})
	sealed, err := seal.Seal(w.key, w.serverKey, proto.TriggerContext(w.id.DeviceID, roomID), plain)
	if err != nil {
		return err
	}
	return w.client.Trigger(ctx, proto.TriggerRequest{RoomID: roomID, Sealed: sealed})
}
