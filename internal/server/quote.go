package server

import (
	"encoding/binary"
	"encoding/hex"
	"log"
	"time"
)

// A mention that quotes a message carries only the quoted message's id. To route it to the
// session the quoted message belongs to, the server keeps, for logRetention:
//
//   - mentions: the session each person's mention ran in, by the mention's message id;
//   - posts: when each reply was posted to a room, by which bot, and from which session.
//
// A quoted id found in mentions gives its session. Otherwise the id's time is looked up in
// posts: Tanka message ids are 24-hex ObjectIds whose first four bytes are the send time in
// Unix seconds, so a bot reply's id falls within the time its post took, give or take
// postSlack. Exactly one session posting then is a match; none or several is no match, and
// the worker falls back to the room's current session.

const postSlack = 2 * time.Second

// Quote match outcomes, as logged.
const (
	quoteExact     = "exact"     // the quoted message is a mention that ran in the session
	quoteTime      = "time"      // one session posted at the quoted message's time
	quoteNone      = "none"      // nothing posted then, or the id has no time in it
	quoteAmbiguous = "ambiguous" // several sessions, or the server, posted then
)

// objectIDTime is the send time in a Tanka message id, a 24-hex ObjectId.
func objectIDTime(id string) (time.Time, bool) {
	b, err := hex.DecodeString(id)
	if len(id) != 24 || err != nil {
		return time.Time{}, false
	}
	return time.Unix(int64(binary.BigEndian.Uint32(b[:4])), 0), true
}

// logPost records a reply posted to a room; session is "" for the server's own notices.
func (s *Server) logPost(bot, room, session string, start, end time.Time) {
	s.logExec("post", `INSERT INTO posts (bot, room_id, session_id, post_start, post_end) VALUES (?, ?, ?, ?, ?)`,
		bot, room, session, start.UnixMilli(), end.UnixMilli())
}

// logMention records the session a person's mention ran in.
func (s *Server) logMention(msgID, bot, room, session string) {
	s.logExec("mention", `INSERT INTO mentions (message_id, bot, room_id, session_id, at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (message_id) DO UPDATE SET bot = excluded.bot, room_id = excluded.room_id, session_id = excluded.session_id, at = excluded.at`,
		msgID, bot, room, session, s.now().UnixMilli())
}

// quotedSession is the session of bot that the message msgID in room belongs to, or "". It
// logs the outcome with the id's time, never the id.
func (s *Server) quotedSession(bot, room, msgID string) string {
	sent, how, session := "-", quoteNone, ""
	defer func() {
		log.Printf("bot %s room=%s quote: sent=%s match=%s session=%s", bot, room, sent, how, short(session))
	}()
	var mBot, mSession string
	err := s.db.QueryRow(`SELECT bot, session_id FROM mentions WHERE message_id = ? AND room_id = ?`, msgID, room).Scan(&mBot, &mSession)
	if err == nil {
		if mBot == bot && mSession != "" {
			how, session = quoteExact, mSession
		}
		return session // a mention of another bot is not this bot's to route
	}
	t, ok := objectIDTime(msgID)
	if !ok {
		return ""
	}
	sent = t.Local().Format("2006-01-02 15:04:05")
	ms := t.UnixMilli()
	rows, err := s.db.Query(`SELECT DISTINCT bot, session_id FROM posts WHERE room_id = ? AND post_start <= ? AND post_end >= ?`,
		room, ms+postSlack.Milliseconds(), ms-postSlack.Milliseconds())
	if err != nil {
		log.Printf("quote: %v", err)
		return ""
	}
	defer rows.Close()
	type poster struct{ bot, session string }
	var hits []poster
	for rows.Next() {
		var p poster
		if err := rows.Scan(&p.bot, &p.session); err != nil {
			log.Printf("quote: %v", err)
			return ""
		}
		hits = append(hits, p)
	}
	switch {
	case len(hits) > 1:
		how = quoteAmbiguous
	case len(hits) == 1 && hits[0].bot == bot && hits[0].session != "":
		how, session = quoteTime, hits[0].session
	}
	return session
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
