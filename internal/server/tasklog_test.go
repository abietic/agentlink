package server

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/tanka"
)

type logRow struct {
	source, sender, outcome string
	files, in, out          int
	leased                  bool
}

func taskRow(t *testing.T, s *Server, id string) logRow {
	t.Helper()
	var r logRow
	var leased sql.NullInt64
	var outcome sql.NullString
	err := s.db.QueryRow(`SELECT source, sender, attachments, task_bytes, result_bytes, leased_at, outcome FROM task_log WHERE id = ?`, id).
		Scan(&r.source, &r.sender, &r.files, &r.in, &r.out, &leased, &outcome)
	if err != nil {
		t.Fatalf("task %s: %v", id, err)
	}
	r.leased, r.outcome = leased.Valid, outcome.String
	return r
}

// TestTaskLog follows tasks through the log, with what workers report, and checks what
// `stats` makes of it and that the log holds no content.
func TestTaskLog(t *testing.T) {
	s, base, ft := newTestServer(t, nil)
	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)

	// A poll carries the worker's status.
	until := time.Date(2026, 11, 9, 3, 0, 0, 0, time.UTC)
	req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/poll", nil)
	req.Header.Set("Authorization", "Bearer "+dev.DeviceToken)
	req.Header.Set(proto.HeaderWorkerStatus, `{"version":"v9.9.9","running":1,"queued":2,"limits":[{"harness":"claude","limit":"seven_day","until":"2026-11-09T03:00:00Z"}],"extra":"dropped"}`)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("poll with status: %v %v", resp, err)
	}

	ev := mention(ft, "R1", "U1", "@Agent Link look at this")
	ev.Event.Attachments = []tanka.Attachment{{URL: "https://files.example/secret-file-77", Name: "shot.png"}}
	sendEvent(t, base, ev)
	posted := dev.poll(t)
	if code := dev.result(t, dev.key, posted.ID, "looked"); code != http.StatusOK {
		t.Fatalf("result: HTTP %d", code)
	}
	ft.next(t)
	if r := taskRow(t, s, posted.ID); r.source != "mention" || r.sender != "U1" || r.files != 1 || r.in == 0 || r.out == 0 || !r.leased || r.outcome != outPosted {
		t.Fatalf("posted task: %+v", r)
	}

	if code := dev.trigger(t, dev.key, "R1", proto.TriggerBody{Trigger: "alerts", Key: "k1", Text: "an alert"}); code != http.StatusOK {
		t.Fatalf("trigger: HTTP %d", code)
	}
	silent := dev.poll(t)
	dev.result(t, dev.key, silent.ID, "")
	if r := taskRow(t, s, silent.ID); r.source != "alerts" || r.sender != dev.DeviceID || r.outcome != outEmpty {
		t.Fatalf("triggered task: %+v", r)
	}

	sendEvent(t, base, mention(ft, "R1", "U2", "@Agent Link taken, never answered"))
	taken := dev.poll(t)
	sendEvent(t, base, mention(ft, "R1", "U2", "@Agent Link never taken"))
	var waiting string
	s.db.QueryRow(`SELECT id FROM task_log WHERE outcome IS NULL AND leased_at IS NULL`).Scan(&waiting)
	s.db.Exec(`INSERT INTO task_log (id, bot, room_id, device_id, source, sender, attachments, task_bytes, queued_at) VALUES ('t-old', 'main', 'R1', ?, 'mention', 'U1', 0, 1, ?)`,
		dev.DeviceID, time.Now().Add(-logRetention-time.Hour).UnixMilli())
	s.reapExpired(time.Now().Add(time.Hour))
	ft.next(t)
	ft.next(t)
	if r := taskRow(t, s, taken.ID); r.outcome != outNoResult {
		t.Fatalf("lease ended: %+v", r)
	}
	if r := taskRow(t, s, waiting); r.outcome != outNotTaken {
		t.Fatalf("never taken: %+v", r)
	}
	var old int
	if s.db.QueryRow(`SELECT COUNT(*) FROM task_log WHERE id = 't-old'`).Scan(&old); old != 0 {
		t.Fatal("a task older than the retention was kept")
	}

	var out bytes.Buffer
	if err := Stats(&out, s.cfg.DBPath, 7, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]string{
		{dev.DeviceID, "main", "laptop"}, {"v9.9.9", "0/0", "1/2", "2", "@"}, {"claude", "seven_day", "until"},
		{"main", "R1", "Test", "room", "4", "empty", "1,", "no_result", "1,", "not_taken", "1,", "posted", "1"},
		{"alerts", dev.DeviceID, "1", "empty", "1"}, {"mention", "U1", "1", "posted", "1"}, {"mention", "U2", "2"},
		{"WAIT", "(queued", "→", "taken)", "p50"},
	} {
		if !hasRow(out.String(), want...) {
			t.Errorf("stats lacks a row with %q:\n%s", want, out.String())
		}
	}
	if os.Getenv("AGL_SHOW") != "" {
		t.Log("\n" + out.String())
	}
	if !strings.Contains(out.String(), until.Local().Format("01-02 15:04 MST")) {
		t.Errorf("stats lacks when the limit lifts:\n%s", out.String())
	}
	if strings.Contains(out.String(), "extra") {
		t.Error("stats shows a status field the server does not know")
	}
	out.Reset()
	if err := Stats(&out, s.cfg.DBPath, 7, posted.ID, time.Now()); err != nil || !hasRow(out.String(), "outcome", "posted") || !hasRow(out.String(), "attachments", "1") {
		t.Fatalf("task detail: %v\n%s", err, out.String())
	}

	// The peak is the highest yet, not the latest.
	s.logQueued("t-later", "main", dev.DeviceID, proto.Task{RoomID: "R1"}, "U1", 1, 1)
	var peak int
	if s.db.QueryRow(`SELECT peak_queue FROM device_stats WHERE device_id = ?`, dev.DeviceID).Scan(&peak); peak != 2 {
		t.Fatalf("peak = %d, want 2", peak)
	}

	s.Close()
	for _, p := range []string{s.cfg.DBPath, s.cfg.DBPath + "-wal"} {
		b, _ := os.ReadFile(p)
		for _, plain := range []string{"look at this", "never taken", "an alert", "secret-file-77", "shot.png", "/hooks/response/tok"} {
			if bytes.Contains(b, []byte(plain)) {
				t.Errorf("%s holds %q", p, plain)
			}
		}
	}
}

// TestRestartTellsRoomsWhatWasLost: tasks still waiting at a restart are logged as lost and
// their room hears how many, not what they were; a task already taken is not lost.
func TestRestartTellsRoomsWhatWasLost(t *testing.T) {
	s, base, ft := newTestServer(t, func(c *Config) {
		c.Bots[0].RoomWebhooks = map[string]string{"R1": strings.TrimSuffix(c.ResponseURLPrefix, "/hooks/response/") + "/hooks/webhook"}
	})
	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)
	dev.poll(t)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link kiwi-canary"))
	taken := dev.poll(t)
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link plum-canary"))
	dev.trigger(t, dev.key, "R1", proto.TriggerBody{Trigger: "alerts", Key: "k", Text: "fig-canary"})
	s.Close()

	s2, err := New(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	p := ft.next(t)
	if p.path != "/hooks/webhook" || !strings.Contains(p.text, "took 2 task(s) for this room (`alerts` ×1, mention ×1)") ||
		strings.Contains(p.text, "plum") || strings.Contains(p.text, "fig") {
		t.Fatalf("lost notice: %+v", p)
	}
	var lost int
	s2.db.QueryRow(`SELECT COUNT(*) FROM task_log WHERE outcome = ?`, outLost).Scan(&lost)
	if r := taskRow(t, s2, taken.ID); lost != 2 || r.outcome != "" || !r.leased {
		t.Fatalf("lost %d, taken %+v", lost, r)
	}

	hs := httptest.NewServer(s2.Handler())
	defer hs.Close()
	dev.base = hs.URL
	if code := dev.result(t, dev.key, taken.ID, "done after restart"); code != http.StatusOK {
		t.Fatalf("result after restart: HTTP %d", code)
	}
	ft.next(t)
	if r := taskRow(t, s2, taken.ID); r.outcome != outPosted {
		t.Fatalf("taken task after its result: %+v", r)
	}

	// A third start finds nothing new to report.
	s2.Close()
	s3, err := New(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	s3.Close()
	ft.quiet(t)
}

// hasRow reports whether a line of out holds fields, in order and next to each other.
func hasRow(out string, fields ...string) bool {
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		for i := 0; i+len(fields) <= len(f); i++ {
			if slices.Equal(f[i:i+len(fields)], fields) {
				return true
			}
		}
	}
	return false
}
