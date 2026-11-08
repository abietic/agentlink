package server

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"time"

	"agentlink/internal/proto"
)

// The task log keeps what became of each task for logRetention: who asked, which room and
// device, when it was queued, taken and answered, how big it was, and its outcome. It never
// holds the task's text, attachments or links, or where its reply goes. Writing it never
// holds up a task: a failed write is only logged.
const logRetention = 90 * 24 * time.Hour

// Task outcomes; a task waiting or running has none.
const (
	outPosted      = "posted"       // the result was posted to the room
	outEmpty       = "empty"        // the worker had nothing to post
	outReplyFailed = "reply_failed" // Tanka did not take the result
	outNotTaken    = "not_taken"    // the device did not take it within queue_ttl_sec
	outNoResult    = "no_result"    // the device took it and sent no result within lease_sec
	outLost        = "lost"         // the server restarted before the device took it
)

func (s *Server) logExec(what, query string, args ...any) {
	if _, err := s.db.Exec(query, args...); err != nil {
		log.Printf("task log: %s: %v", what, err)
	}
}

// logQueued records a new task, and open as the most its device has waiting or running if
// that is a new high.
func (s *Server) logQueued(id, bot, dev string, t proto.Task, sender string, size, open int) {
	now := s.now().UnixMilli()
	s.logExec("queued", `INSERT INTO task_log (id, bot, room_id, device_id, source, sender, attachments, task_bytes, queued_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, bot, t.RoomID, dev, cmp.Or(t.Trigger, "mention"), sender, len(t.Attachments), size, now)
	s.logExec("peak", `INSERT INTO device_stats (device_id, peak_queue, peak_at) VALUES (?, ?, ?)
		ON CONFLICT (device_id) DO UPDATE SET peak_queue = excluded.peak_queue, peak_at = excluded.peak_at
		WHERE excluded.peak_queue > device_stats.peak_queue`, dev, open, now)
}

func (s *Server) logLeased(id string, until time.Time) {
	s.logExec("leased", `UPDATE task_log SET leased_at = ?, lease_until = ? WHERE id = ?`, s.now().UnixMilli(), until.UnixMilli(), id)
}

// logDone sets a task's outcome unless it already has one.
func (s *Server) logDone(id, outcome string, resultBytes int) {
	s.logExec("done", `UPDATE task_log SET outcome = ?, done_at = ?, result_bytes = ? WHERE id = ? AND outcome IS NULL`,
		outcome, s.now().UnixMilli(), resultBytes, id)
}

// sweepLog closes tasks taken before a restart whose lease has ended (the ones taken since
// are closed by reapExpired), and forgets tasks older than logRetention.
func (s *Server) sweepLog(now time.Time) {
	s.logExec("sweep", `UPDATE task_log SET outcome = ?, done_at = lease_until WHERE outcome IS NULL AND lease_until < ?`, outNoResult, now.UnixMilli())
	s.logExec("retention", `DELETE FROM task_log WHERE queued_at < ?`, now.Add(-logRetention).UnixMilli())
}

// markLost closes the tasks that were still waiting when the server last stopped: they lived
// only in memory. Each room with a webhook hears how many it lost, never what they were.
func (s *Server) markLost() {
	rows, err := s.db.Query(`UPDATE task_log SET outcome = ?, done_at = ? WHERE outcome IS NULL AND leased_at IS NULL
		RETURNING bot, room_id, device_id, source`, outLost, s.now().UnixMilli())
	if err != nil {
		log.Printf("task log: lost: %v", err)
		return
	}
	type room struct{ bot, id, dev string }
	lost := map[room]map[string]int{} // by source
	for rows.Next() {
		var r room
		var source string
		if err := rows.Scan(&r.bot, &r.id, &r.dev, &source); err != nil {
			log.Printf("task log: lost: %v", err)
			break
		}
		if lost[r] == nil {
			lost[r] = map[string]int{}
		}
		lost[r][source]++
	}
	rows.Close()
	for r, bySource := range lost {
		n, parts := 0, []string{}
		for _, src := range slices.Sorted(maps.Keys(bySource)) {
			n += bySource[src]
			name := "`" + src + "`"
			if src == "mention" {
				name = "mention"
			}
			parts = append(parts, fmt.Sprintf("%s ×%d", name, bySource[src]))
		}
		log.Printf("task log: %d task(s) for bot %s room %s were lost in a restart", n, r.bot, r.id)
		bot := s.bots[r.bot]
		if bot == nil || bot.RoomWebhooks[r.id] == "" {
			continue
		}
		s.replyAsync(bot, r.id, "", fmt.Sprintf("agentlink-server restarted before device `%s` took %d task(s) for this room (%s), so they did not run. Mention me again for any that still matter.",
			r.dev, n, strings.Join(parts, ", ")))
	}
}

// noteStatus keeps what a worker reported about itself with a poll.
func (s *Server) noteStatus(dev, header string) {
	var st proto.WorkerStatus
	if header == "" || len(header) > 4096 || json.Unmarshal([]byte(header), &st) != nil {
		return
	}
	b, _ := json.Marshal(st) // only the fields this server knows
	s.logExec("status", `INSERT INTO device_stats (device_id, status, status_at) VALUES (?, ?, ?)
		ON CONFLICT (device_id) DO UPDATE SET status = excluded.status, status_at = excluded.status_at`, dev, string(b), s.now().UnixMilli())
}
