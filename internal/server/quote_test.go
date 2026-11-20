package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

// objectID is a Tanka message id sent at t.
func objectID(t time.Time) string { return fmt.Sprintf("%08x%016x", t.Unix(), 42) }

func TestObjectIDTime(t *testing.T) {
	sent := time.Date(2026, 11, 20, 10, 42, 10, 0, time.Local)
	if got, ok := objectIDTime(objectID(sent)); !ok || !got.Equal(sent) {
		t.Fatalf("objectIDTime = %v, %v", got, ok)
	}
	for _, bad := range []string{"", "m1", "6a1f3c2a00000000000000", "6a1f3c2a000000000000000000", "zz1f3c2a0000000000000000"} {
		if _, ok := objectIDTime(bad); ok {
			t.Errorf("objectIDTime(%q) has a time", bad)
		}
	}
}

// TestQuotedSessionMatch: an exact mention match comes first; otherwise exactly one session of
// the bot posting in the room within postSlack of the id's time matches, and anything else
// does not.
func TestQuotedSessionMatch(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	at := time.Date(2026, 11, 20, 10, 0, 0, 0, time.Local)
	ms := func(sec float64) time.Time { return at.Add(time.Duration(sec * float64(time.Second))) }
	s.logPost("main", "R1", "sess-a", ms(0.5), ms(1.2))   // a reply from session a
	s.logPost("main", "R1", "sess-b", ms(20), ms(21))     // and from b, later
	s.logPost("second", "R1", "sess-x", ms(21.5), ms(22)) // another bot's, overlapping b's slack
	s.logPost("main", "R1", "", ms(40), ms(41))           // a server notice
	s.logPost("second", "R1", "sess-y", ms(60), ms(61))   // only another bot's
	s.logPost("main", "R2", "sess-other-room", ms(80), ms(81))
	s.logMention("human-1", "main", "R1", "sess-m")
	s.logMention("human-2", "second", "R1", "sess-n")
	s.logMention(objectID(ms(1)), "main", "R1", "sess-exact") // a mention's id that a post's window also covers

	for _, tc := range []struct{ id, want string }{
		{"human-1", "sess-m"},
		{"human-2", ""},                 // a mention of the other bot
		{objectID(ms(1)), "sess-exact"}, // exact before time
		{objectID(ms(3)), "sess-a"},     // 1.8s after a's post ended
		{objectID(ms(-1)), "sess-a"},    // 1.5s before it started
		{objectID(ms(-2)), ""},          // 2.5s before
		{objectID(ms(4)), ""},           // 2.8s after
		{objectID(ms(21)), ""},          // b and the other bot's x
		{objectID(ms(40)), ""},          // the server's own notice
		{objectID(ms(60)), ""},          // only another bot posted
		{objectID(ms(80)), ""},          // another room
		{"not-an-object-id", ""},
	} {
		if got := s.quotedSession("main", "R1", tc.id); got != tc.want {
			t.Errorf("quotedSession(%s) = %q, want %q", tc.id, got, tc.want)
		}
	}
}

// TestQuotedMentionRoutes: the session a result names is kept with the post and with the
// person's mention, and a later mention quoting either carries it to the worker.
func TestQuotedMentionRoutes(t *testing.T) {
	s, base, ft := newTestServer(t, nil)
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	s.now = func() time.Time { return time.UnixMilli(clock.Load()) }
	step := func() { clock.Add(10 * time.Second.Milliseconds()) } // apart by more than postSlack, within signatureSkew

	dev := register(t, base, "")
	sendEvent(t, base, mention(ft, "R1", "U1", "@Agent Link pair "+dev.DeviceID))
	ft.next(t)
	dev.poll(t) // online
	step()

	ask := func(text, msgID, quoted string) *proto.Task {
		t.Helper()
		ev := mention(ft, "R1", "U1", "@Agent Link "+text)
		ev.Event.MessageID = msgID
		if quoted != "" {
			ev.Event.Message = &tanka.MessageSnapshot{ReplyMessageID: quoted}
		}
		sendEvent(t, base, ev)
		task := dev.poll(t)
		if task == nil {
			t.Fatalf("%q was not queued", text)
		}
		return task
	}
	answer := func(task *proto.Task, session string) time.Time {
		t.Helper()
		serverKey, _ := seal.ParsePublicKey(dev.ServerKey)
		plain, _ := json.Marshal(proto.Result{Text: "answer", SessionID: session})
		sealed, _ := seal.Seal(dev.key, serverKey, proto.ResultContext(dev.DeviceID, task.ID), plain)
		posted := s.now()
		if code := api(t, http.MethodPost, base+"/api/v1/tasks/"+task.ID+"/result", dev.DeviceToken, proto.Envelope{ID: task.ID, Sealed: sealed, Reply: dev.replies[task.ID]}, nil); code != http.StatusOK {
			t.Fatalf("result: HTTP %d", code)
		}
		ft.next(t)
		step()
		return posted
	}

	first := ask("alert triage", "human-1", "")
	posted := answer(first, "sess-alert")
	answer(ask("redis timeout", "human-2", ""), "sess-redis")

	if got := ask("这个要处理吗", "human-3", "human-1"); got.RouteSessionID != "sess-alert" || got.ReplyMessageID != "human-1" {
		t.Fatalf("quoting a mention: %+v", got)
	}
	answer(ask("x", "human-4", ""), "") // a reply from no session
	if got := ask("这个要处理吗", "human-5", objectID(posted)); got.RouteSessionID != "sess-alert" {
		t.Fatalf("quoting the reply posted at %s: %+v", posted, got)
	}
	if got := ask("这个要处理吗", "human-6", objectID(posted.Add(-time.Hour))); got.RouteSessionID != "" {
		t.Fatalf("quoting a time nothing was posted at: %+v", got)
	}
	if got := ask("plain", "human-7", ""); got.RouteSessionID != "" {
		t.Fatalf("unquoted: %+v", got)
	}

	// A bot's mention is not kept as a message to route by.
	ev := botMention(ft, "R1", "B1", "@Agent Link handed on")
	ev.Event.MessageID = "bot-1"
	sendEvent(t, base, ev)
	answer(dev.poll(t), "sess-bot")
	var n int
	s.db.QueryRow(`SELECT count(*) FROM mentions WHERE message_id = 'bot-1'`).Scan(&n)
	if n != 0 {
		t.Fatal("a bot's mention was kept")
	}
}

func TestRetentionCoversQuoteTables(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	now := time.Now()
	old := now.Add(-logRetention - time.Hour)
	s.logPost("main", "R1", "old", old, old)
	s.logPost("main", "R1", "new", now, now)
	s.now = func() time.Time { return old }
	s.logMention("m-old", "main", "R1", "old")
	s.now = func() time.Time { return now }
	s.logMention("m-new", "main", "R1", "new")

	s.sweepLog(now)
	for _, table := range []string{"posts", "mentions"} {
		var sessions []string
		rows, _ := s.db.Query(`SELECT session_id FROM ` + table)
		for rows.Next() {
			var id string
			rows.Scan(&id)
			sessions = append(sessions, id)
		}
		rows.Close()
		if len(sessions) != 1 || sessions[0] != "new" {
			t.Errorf("%s after the sweep: %v", table, sessions)
		}
	}
}
