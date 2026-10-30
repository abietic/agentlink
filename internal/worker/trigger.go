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
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
)

// Trigger lets tasks start without anyone typing. Tasks named after it come from the server
// (e.g. its Sentry hook) or from Command, which the worker runs every EverySec: each stdout
// line is a JSON item {"key", "at", "text"}, and an item whose key has not been seen at that
// time or later is queued for RoomID. A triggered task skips allowed_users and the room
// commands, starts a new session in Workdir, and that session becomes the room's current
// one, so people can follow up in the room.
type Trigger struct {
	Name      string   `json:"name"`
	RoomID    string   `json:"room_id"` // required with command: the room its tasks run for and post to
	Workdir   string   `json:"workdir"` // default default_workdir
	Command   []string `json:"command"`
	EverySec  int      `json:"every_sec"`   // default 60
	DedupeSec int      `json:"dedupe_sec"`  // a key that ran within this long is skipped; default 1800
	MaxAgeSec int      `json:"max_age_sec"` // items older than this at first sight are skipped; default 3600
}

// item is one line of a trigger command's output.
type item struct {
	Key  string    `json:"key"`
	At   time.Time `json:"at"`
	Text string    `json:"text"`
}

// marksKeep bounds how long dedupe marks are kept.
const marksKeep = 7 * 24 * time.Hour

func (c *Config) checkTriggers() error {
	seen := map[string]bool{}
	for i := range c.Triggers {
		tr := &c.Triggers[i]
		if tr.Name == "" || seen[tr.Name] {
			return fmt.Errorf("triggers[%d]: name must be set and unique", i)
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
	}
	return nil
}

func defaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
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
	w.mu.Lock()
	defer w.mu.Unlock()
	mark := "ran/" + tr.Name + "/" + t.Key
	if last, ok := w.marks[mark]; ok && t.Key != "" && w.now().Sub(last) < time.Duration(tr.DedupeSec)*time.Second {
		return fmt.Errorf("key %s already ran at %s", t.Key, last.Local().Format("15:04"))
	}
	w.setMarkLocked(mark, w.now())
	return nil
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

// scan runs tr's command once and queues the items it has not seen yet.
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
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var it item
		if err := json.Unmarshal(sc.Bytes(), &it); err != nil || it.Key == "" || it.Text == "" || it.At.IsZero() {
			log.Printf("trigger %s: skipping a line that is not {key, at, text}", tr.Name)
			continue
		}
		mark := "seen/" + tr.Name + "/" + it.Key
		w.mu.Lock()
		last, seen := w.marks[mark]
		stale := !seen && w.now().Sub(it.At) > time.Duration(tr.MaxAgeSec)*time.Second
		if stale {
			w.setMarkLocked(mark, it.At)
		}
		w.mu.Unlock()
		if stale || (seen && !it.At.After(last)) {
			continue
		}
		if err := w.submit(ctx, tr, it); err != nil {
			return fmt.Errorf("queue %s: %w", it.Key, err)
		}
		log.Printf("trigger %s: queued %s", tr.Name, it.Key)
		w.mu.Lock()
		w.setMarkLocked(mark, it.At)
		w.mu.Unlock()
	}
	return sc.Err()
}

func (w *Worker) submit(ctx context.Context, tr Trigger, it item) error {
	plain, _ := json.Marshal(proto.TriggerBody{Trigger: tr.Name, Key: it.Key, Text: it.Text})
	sealed, err := seal.Seal(w.key, w.serverKey, proto.TriggerContext(w.id.DeviceID, tr.RoomID), plain)
	if err != nil {
		return err
	}
	return w.client.Trigger(ctx, proto.TriggerRequest{RoomID: tr.RoomID, Sealed: sealed})
}
