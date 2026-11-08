package server

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/sqlite"
)

// Stats prints what the database at dbPath says about devices, rooms and the tasks queued
// in the last `days` days; with task set, everything logged about that one task. It only
// reads, so it runs beside the server: `agentlink-server stats`.
func Stats(w io.Writer, dbPath string, days int, task string, now time.Time) error {
	db, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if task != "" {
		return taskDetail(w, db, task)
	}
	since := now.AddDate(0, 0, -days).UnixMilli()
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "DEVICE\tBOT\tNAME\tLAST POLL\tVERSION\tSERVER WAITING/TAKEN\tWORKER RUNNING/QUEUED\tPEAK\tAT A LIMIT")
	rows, err := db.Query(`SELECT d.id, d.bot, d.name, d.last_seen, COALESCE(st.status, ''), COALESCE(st.peak_queue, 0), COALESCE(st.peak_at, 0),
		(SELECT COUNT(*) FROM task_log WHERE device_id = d.id AND outcome IS NULL AND leased_at IS NULL),
		(SELECT COUNT(*) FROM task_log WHERE device_id = d.id AND outcome IS NULL AND leased_at IS NOT NULL)
		FROM devices d LEFT JOIN device_stats st ON st.device_id = d.id ORDER BY d.bot, d.id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, bot, name, status string
		var seen, peak, peakAt int64
		var waiting, taken int
		if err := rows.Scan(&id, &bot, &name, &seen, &status, &peak, &peakAt, &waiting, &taken); err != nil {
			rows.Close()
			return err
		}
		var st proto.WorkerStatus
		worker, version := "-", "-"
		if json.Unmarshal([]byte(status), &st) == nil {
			worker, version = fmt.Sprintf("%d/%d", st.Running, st.Queued), st.Version
		}
		var limits []string
		for _, l := range st.Limits {
			limits = append(limits, fmt.Sprintf("%s %s until %s", l.Harness, cmp.Or(l.Limit, "limit"), l.Until.Local().Format("01-02 15:04 MST")))
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\t%s\n", id, bot, name, ago(now, seen), version, waiting, taken, worker,
			peakText(peak, peakAt), cmp.Or(strings.Join(limits, "; "), "-"))
	}
	rows.Close()

	fmt.Fprintf(tw, "\nTASKS SINCE %s\n", time.UnixMilli(since).Local().Format("2006-01-02 15:04 MST"))
	for _, g := range []struct{ title, cols string }{
		{"BOT\tROOM\tNAME", "bot, room_id, COALESCE((SELECT room_name FROM bindings b WHERE b.bot = t.bot AND b.room_id = t.room_id), '')"},
		{"DEVICE", "device_id"},
		{"SOURCE\tSENDER", "source, sender"},
	} {
		if err := outcomeTable(tw, db, g.title, g.cols, since); err != nil {
			return err
		}
	}

	var waits, runs []int64
	rows, err = db.Query(`SELECT leased_at - queued_at, COALESCE(done_at - leased_at, -1), COALESCE(outcome, '') FROM task_log
		WHERE queued_at >= ? AND leased_at IS NOT NULL`, since)
	if err != nil {
		return err
	}
	for rows.Next() {
		var wait, run int64
		var outcome string
		if err := rows.Scan(&wait, &run, &outcome); err != nil {
			rows.Close()
			return err
		}
		waits = append(waits, wait)
		if outcome == outPosted || outcome == outEmpty || outcome == outReplyFailed {
			runs = append(runs, run)
		}
	}
	rows.Close()
	fmt.Fprintf(tw, "\nWAIT (queued → taken)\t%s\nRUN (taken → answered)\t%s\n", spread(waits), spread(runs))

	fmt.Fprintln(tw, "\nRECENT\tQUEUED\tBOT\tROOM\tDEVICE\tSOURCE\tSENDER\tFILES\tBYTES IN/OUT\tWAIT\tRUN\tOUTCOME")
	rows, err = db.Query(`SELECT id, queued_at, bot, room_id, device_id, source, sender, attachments, task_bytes, result_bytes,
		COALESCE(leased_at - queued_at, -1), COALESCE(done_at - leased_at, -1), COALESCE(outcome, CASE WHEN leased_at IS NULL THEN 'waiting' ELSE 'running' END)
		FROM task_log ORDER BY queued_at DESC LIMIT 20`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, bot, room, dev, source, sender, outcome string
		var queued, wait, run int64
		var files, in, out int
		if err := rows.Scan(&id, &queued, &bot, &room, &dev, &source, &sender, &files, &in, &out, &wait, &run, &outcome); err != nil {
			return err
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d/%d\t%s\t%s\t%s\n", id, time.UnixMilli(queued).Local().Format("01-02 15:04:05"),
			bot, room, dev, source, cmp.Or(sender, "-"), files, in, out, msText(wait), msText(run), outcome)
	}
	return rows.Err()
}

// outcomeTable prints how the tasks since `since` ended, grouped by cols, one column of
// title per column of cols.
func outcomeTable(tw io.Writer, db *sql.DB, title, cols string, since int64) error {
	n := strings.Count(title, "\t") + 1
	nums := make([]string, n+1) // the group's columns, then the outcome
	for i := range nums {
		nums[i] = fmt.Sprint(i + 1)
	}
	rows, err := db.Query(`SELECT `+cols+`, COALESCE(outcome, 'open'), COUNT(*) FROM task_log t WHERE queued_at >= ?
		GROUP BY `+strings.Join(nums, ", ")+` ORDER BY `+strings.Join(nums, ", "), since)
	if err != nil {
		return err
	}
	defer rows.Close()
	type group struct {
		key    string
		counts map[string]int
	}
	var groups []*group
	for rows.Next() {
		keys := make([]string, n)
		dest := make([]any, 0, n+2)
		for i := range keys {
			dest = append(dest, &keys[i])
		}
		var outcome string
		var count int
		if err := rows.Scan(append(dest, &outcome, &count)...); err != nil {
			return err
		}
		for i := range keys {
			keys[i] = cmp.Or(keys[i], "-")
		}
		key := strings.Join(keys, "\t")
		if len(groups) == 0 || groups[len(groups)-1].key != key {
			groups = append(groups, &group{key: key, counts: map[string]int{}})
		}
		groups[len(groups)-1].counts[outcome] += count
	}
	fmt.Fprintf(tw, "\n%s\tTASKS\tOUTCOMES\n", title)
	for _, g := range groups {
		total, parts := 0, []string{}
		for _, o := range slices.Sorted(maps.Keys(g.counts)) {
			total += g.counts[o]
			parts = append(parts, fmt.Sprintf("%s %d", o, g.counts[o]))
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\n", g.key, total, strings.Join(parts, ", "))
	}
	return rows.Err()
}

func taskDetail(w io.Writer, db *sql.DB, id string) error {
	var bot, room, dev, source, sender string
	var files, in, out int
	var queued int64
	var leased, until, done sql.NullInt64
	var outcome sql.NullString
	err := db.QueryRow(`SELECT bot, room_id, device_id, source, sender, attachments, task_bytes, result_bytes, queued_at, leased_at, lease_until, done_at, outcome
		FROM task_log WHERE id = ?`, id).Scan(&bot, &room, &dev, &source, &sender, &files, &in, &out, &queued, &leased, &until, &done, &outcome)
	if err == sql.ErrNoRows {
		return fmt.Errorf("task %s is not in the log (it keeps %s)", id, logRetention)
	}
	if err != nil {
		return err
	}
	at := func(v sql.NullInt64) string {
		if !v.Valid {
			return "-"
		}
		return time.UnixMilli(v.Int64).Local().Format("2006-01-02 15:04:05 MST")
	}
	state := outcome.String
	if !outcome.Valid {
		state = map[bool]string{false: "waiting", true: "running"}[leased.Valid]
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "task\t%s\nbot\t%s\nroom\t%s\ndevice\t%s\nsource\t%s\nsender\t%s\nattachments\t%d\nbytes in/out\t%d/%d\n", id, bot, room, dev, source, cmp.Or(sender, "-"), files, in, out)
	fmt.Fprintf(tw, "queued\t%s\ntaken\t%s\nlease until\t%s\ndone\t%s\noutcome\t%s\n", at(sql.NullInt64{Int64: queued, Valid: true}), at(leased), at(until), at(done), state)
	return tw.Flush()
}

func ago(now time.Time, ms int64) string {
	if ms == 0 {
		return "never"
	}
	return now.Sub(time.UnixMilli(ms)).Round(time.Second).String() + " ago"
}

func peakText(peak, at int64) string {
	if peak == 0 {
		return "-"
	}
	return fmt.Sprintf("%d @ %s", peak, time.UnixMilli(at).Local().Format("01-02 15:04"))
}

func msText(ms int64) string {
	if ms < 0 {
		return "-"
	}
	return (time.Duration(ms) * time.Millisecond).Round(time.Second).String()
}

// spread is the median, 95th percentile and maximum of durations in ms.
func spread(ms []int64) string {
	if len(ms) == 0 {
		return "-"
	}
	slices.Sort(ms)
	at := func(q float64) string { return msText(ms[int(q*float64(len(ms)-1))]) }
	return fmt.Sprintf("p50 %s · p95 %s · max %s (%d tasks)", at(0.5), at(0.95), at(1), len(ms))
}
