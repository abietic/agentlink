// Package server receives Tanka app_mention events for one or more bots, queues each one
// for the worker device paired with its room, and posts the worker's result back. Tasks
// can also be triggered without a mention, by a worker (POST /api/v1/trigger) or by a
// Sentry integration (POST /sentry/{bot}); their results go to the room's webhook.
//
// Messages are only passed through: a task waits in memory until its device takes it, sealed
// to that device's key, so nothing between server and worker (the TLS-terminating proxy
// included) sees its content. Where the result goes travels with the task, boxed with the
// server's storage key, and comes back with the result; once a device has a task, the server
// keeps only what it needs to say the task was dropped, and a restart loses nothing but tasks
// still waiting. SQLite holds devices, room bindings, the server's key, and metadata: a log
// of what became of each task (ids, times, sizes, outcome; kept 90 days) and what each worker
// last reported about itself. Neither the database nor the logs hold message text or where a
// reply goes.
package server

import (
	"cmp"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/sqlite"
	"agentlink/internal/tanka"
)

const (
	maxBody        = 1 << 20
	signatureSkew  = 5 * time.Minute
	dedupeTTL      = time.Hour // Tanka retries a delivery for about six minutes
	maxQueuePerDev = 100
	onlineGrace    = 30 * time.Second // a worker re-polls right after each long poll ends
)

const schema = `
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS devices (
	id TEXT PRIMARY KEY,
	bot TEXT NOT NULL,
	name TEXT NOT NULL,
	token_hash TEXT NOT NULL UNIQUE,
	pubkey BLOB NOT NULL,
	created_at INTEGER NOT NULL,
	last_seen INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS bindings (
	bot TEXT NOT NULL,
	room_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	room_name TEXT NOT NULL,
	bound_by TEXT NOT NULL,
	bound_at INTEGER NOT NULL,
	PRIMARY KEY (bot, room_id)
);
-- Tasks and seen event ids were kept here before 0.4; they live in memory now.
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS events;
-- What became of each task, never its content or where its reply goes; see tasklog.go.
CREATE TABLE IF NOT EXISTS task_log (
	id TEXT PRIMARY KEY,
	bot TEXT NOT NULL,
	room_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	source TEXT NOT NULL,          -- "mention", or the trigger's name
	sender TEXT NOT NULL,          -- who mentioned the bot; the device that sent a trigger; "" for a server hook
	attachments INTEGER NOT NULL,
	task_bytes INTEGER NOT NULL,   -- sealed, as sent to the device
	result_bytes INTEGER NOT NULL DEFAULT 0,
	queued_at INTEGER NOT NULL,    -- unix ms
	leased_at INTEGER,
	lease_until INTEGER,
	done_at INTEGER,
	outcome TEXT                   -- NULL while waiting or running
);
CREATE INDEX IF NOT EXISTS task_log_queued_at ON task_log (queued_at);
CREATE TABLE IF NOT EXISTS device_stats (
	device_id TEXT PRIMARY KEY,
	status TEXT NOT NULL DEFAULT '',        -- the worker's last proto.WorkerStatus, JSON
	status_at INTEGER NOT NULL DEFAULT 0,
	peak_queue INTEGER NOT NULL DEFAULT 0,  -- most tasks waiting or running at once
	peak_at INTEGER NOT NULL DEFAULT 0
);
`

const helpText = `**agentlink** runs what you write on the device paired with this room, through its harness (the Claude Agent SDK, or mirasim).

- ` + "`pair <device-id>`" + ` / ` + "`unpair`" + ` — bind this room to a worker (it prints its device id on start)
- ` + "`status`" + ` — device, workdir, session, what is running
- ` + "`cd <path>`" + ` — switch workdir (the next prompt starts a new session)
- ` + "`harness [name]`" + ` — show or switch this room's harness (the next prompt starts a new session)
- ` + "`new [prompt]`" + ` — start a new session
- ` + "`sessions`" + ` — recent sessions in this room
- ` + "`resume <n|session-id> [prompt]`" + ` — switch to an earlier session, or to one started in a terminal
- ` + "`<session-id> <prompt>`" + ` — continue that session without switching the room to it; ids are in ` + "`sessions`" + ` and in reply footers, and different sessions run side by side
- anything else — a prompt, continuing the room's current session`

type device struct {
	ID, Bot, Name string
	Pub           *ecdh.PublicKey
	LastSeen      time.Time
}

type Server struct {
	cfg  Config
	bots map[string]*Bot
	db   *sql.DB
	key  []byte           // storage key
	priv *ecdh.PrivateKey // end-to-end key; workers pin its public half at registration
	http *http.Client
	now  func() time.Time

	mu      sync.Mutex
	wake    map[string]chan struct{} // device → long-poll wakeup
	waiting map[string][]*task       // device → tasks it has not taken yet, oldest first
	leased  map[string]*task         // task id → a task a device has taken (without its content)
	seen    map[string]time.Time     // dedupe key → when to forget it
	hops    map[string]*botHops      // room → bot-sent mentions since a person's
}

// maxBotHops is how many bot-sent mentions in a row a room takes before a person has to
// mention a bot again. Bots that answer each other would otherwise never stop.
const maxBotHops = 3

type botHops struct {
	n    int
	last time.Time
}

// task is a queued or taken task. Its content leaves the server with the device.
type task struct {
	id, dev, bot, room string
	sealed             []byte // proto.Task sealed to the device; nil once taken
	reply              string // response_url; empty for triggered tasks
	created            time.Time
	until              time.Time // lease end; zero while waiting
}

// replyTo is where a taken task's result goes. It travels with the task, boxed with the
// storage key, so a result can be posted after a restart.
type replyTo struct {
	Dev   string `json:"dev"`
	Bot   string `json:"bot"`
	Room  string `json:"room"`
	URL   string `json:"url"`
	Until int64  `json:"until"` // lease end, unix ms: a later result is refused
}

func New(cfg Config) (*Server, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	key, err := seal.LoadOrCreateKey(cfg.KeyPath)
	if err != nil {
		return nil, err
	}
	db, err := sqlite.Open(cfg.DBPath, schema)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`VACUUM`); err != nil { // rewrites the file, so dropped task rows leave no trace in it
		log.Printf("vacuum: %v", err)
	}
	s := &Server{
		cfg:     cfg,
		bots:    map[string]*Bot{},
		db:      db,
		key:     key,
		http:    &http.Client{Timeout: 15 * time.Second},
		now:     time.Now,
		wake:    map[string]chan struct{}{},
		waiting: map[string][]*task{},
		leased:  map[string]*task{},
		seen:    map[string]time.Time{},
		hops:    map[string]*botHops{},
	}
	for i := range s.cfg.Bots {
		s.bots[s.cfg.Bots[i].Name] = &s.cfg.Bots[i]
	}
	if s.priv, err = s.loadKey(); err != nil {
		db.Close()
		return nil, err
	}
	for _, b := range s.cfg.Bots {
		for room, dev := range b.Rooms {
			if err := s.bind(b.Name, room, dev, "", "config"); err != nil {
				db.Close()
				return nil, fmt.Errorf("bot %s room %s: %w", b.Name, room, err)
			}
		}
	}
	s.markLost()
	return s, nil
}

func (s *Server) Close() error { return s.db.Close() }

func (s *Server) loadKey() (*ecdh.PrivateKey, error) {
	var boxed []byte
	err := s.db.QueryRow(`SELECT v FROM meta WHERE k = 'server_key'`).Scan(&boxed)
	if errors.Is(err, sql.ErrNoRows) {
		k := seal.NewPrivateKey()
		if boxed, err = seal.Box(s.key, "server_key", k.Bytes()); err != nil {
			return nil, err
		}
		_, err = s.db.Exec(`INSERT INTO meta (k, v) VALUES ('server_key', ?)`, boxed)
		return k, err
	}
	if err != nil {
		return nil, err
	}
	raw, err := seal.Unbox(s.key, "server_key", boxed)
	if err != nil {
		return nil, fmt.Errorf("server key: %w (key_path must be the key this database was created with)", err)
	}
	return seal.ParsePrivateKey(raw)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tanka/events", s.handleEvent)
	mux.HandleFunc("POST /tanka/events/{bot}", s.handleEvent)
	mux.HandleFunc("POST /api/v1/register", s.handleRegister)
	mux.HandleFunc("GET /api/v1/poll", s.handlePoll)
	mux.HandleFunc("POST /api/v1/tasks/{id}/result", s.handleResult)
	mux.HandleFunc("POST /api/v1/trigger", s.handleTrigger)
	mux.HandleFunc("POST /sentry/{bot}", s.handleSentry)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	return mux
}

// Reap gives up on tasks nobody picked up or answered, until ctx ends.
func (s *Server) Reap(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reapExpired(s.now())
		}
	}
}

func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	bot := s.bots[cmp.Or(r.PathValue("bot"), s.cfg.DefaultBot)]
	if bot == nil {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	err = tanka.Verify([]byte(bot.SigningSecret), r.Header.Get(tanka.HeaderTimestamp), r.Header.Get(tanka.HeaderSignature), body, s.now(), signatureSkew)
	if err != nil {
		log.Printf("bot %s: event rejected: %v", bot.Name, err)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	var env tanka.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if env.Type == "url_verification" {
		writeJSON(w, http.StatusOK, map[string]string{"challenge": env.Challenge})
		return
	}
	if bot.AppID != "" && env.APIAppID != bot.AppID {
		log.Printf("bot %s: event for app %q rejected", bot.Name, env.APIAppID)
		http.Error(w, "wrong app", http.StatusForbidden)
		return
	}
	if env.Type == "event_callback" {
		var raw any
		_ = json.Unmarshal(body, &raw)
		log.Printf("bot %s: event %s shape %q", bot.Name, env.EventID, shape(raw))
	}
	if env.Type == "event_callback" && env.Event != nil && env.Event.Type == "app_mention" && s.firstSeen(bot.Name+"/"+env.EventID, dedupeTTL) {
		s.onMention(bot, *env.Event)
	}
	writeJSON(w, http.StatusOK, map[string]int{"code": 0})
}

// shape renders the keys and value types of a decoded JSON value without any values, so the
// log shows every field Tanka sends (documented or not) without the message text in it.
func shape(v any) string {
	switch v := v.(type) {
	case map[string]any:
		parts := make([]string, 0, len(v))
		for _, k := range slices.Sorted(maps.Keys(v)) {
			parts = append(parts, k+":"+shape(v[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []any:
		if len(v) == 0 {
			return "[]"
		}
		return fmt.Sprintf("[%s]x%d", shape(v[0]), len(v))
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case nil:
		return "null"
	}
	return "?"
}

// firstSeen reports whether key is new, and remembers it for ttl. A restart forgets every
// key; Tanka only retries a delivery that was not answered, so that rarely matters.
func (s *Server) firstSeen(key string, ttl time.Duration) bool {
	if strings.HasSuffix(key, "/") { // no event_id
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = s.now().Add(ttl)
	return true
}

func (s *Server) onMention(bot *Bot, ev tanka.MentionEvent) {
	// sender_type is not in Tanka's docs; its value (not message content) says whether bots' mentions arrive.
	log.Printf("bot %s: mention room=%s user=%s sender_type=%q", bot.Name, ev.RoomID, ev.UserID, ev.SenderType)
	fromBot := ev.SenderType == "bot"
	if len(s.cfg.AllowedUsers) > 0 && !slices.Contains(s.cfg.AllowedUsers, ev.UserID) {
		log.Printf("bot %s room=%s user=%s: not in allowed_users", bot.Name, ev.RoomID, ev.UserID)
		if !fromBot { // a bot gets no answer: it may answer back
			s.replyAsync(bot, ev.RoomID, ev.ResponseURL, fmt.Sprintf("User `%s` is not allowed to use this bot.", ev.UserID))
		}
		return
	}
	text := stripMention(ev.Text, bot.Mentions)
	if fromBot {
		switch n := s.botHop(ev.RoomID); {
		case n == maxBotHops+1:
			log.Printf("bot %s room=%s: %d bot-sent mentions in a row, stopping", bot.Name, ev.RoomID, n)
			s.replyAsync(bot, ev.RoomID, ev.ResponseURL, fmt.Sprintf("Stopped here: %d mentions in a row came from bots. A person has to mention a bot before bots can hand on work in this room again.", maxBotHops))
			return
		case n > maxBotHops:
			log.Printf("bot %s room=%s: bot-sent mention %d in a row dropped", bot.Name, ev.RoomID, n)
			return
		}
		text = afterMention(ev.Text, bot.Mentions)
	} else {
		s.mu.Lock()
		delete(s.hops, ev.RoomID)
		s.mu.Unlock()
	}
	word, arg := proto.CutWord(text)
	switch kw := proto.Keyword(word); {
	case kw == "" && len(ev.Attachments) == 0, kw == "help": // an image alone is a prompt
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, helpText)
	case (kw == "pair" || kw == "unpair") && fromBot:
		// Which device a room drives is for the people in it to decide.
		log.Printf("bot %s room=%s user=%s: %s from a bot ignored", bot.Name, ev.RoomID, ev.UserID, kw)
	case kw == "pair":
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, s.pair(bot, ev, arg))
	case kw == "unpair":
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, s.unpair(bot, ev.RoomID))
	default:
		s.enqueue(bot, ev, text)
	}
}

// botHop counts a bot-sent mention in room and returns how many have arrived in a row. A
// person's mention starts the count again, and so does a quiet spell of two leases: bots
// answering each other are never that slow, since a result later than its lease is refused.
func (s *Server) botHop(room string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hops[room]
	if h == nil || s.now().Sub(h.last) > 2*s.cfg.lease() {
		h = &botHops{}
		s.hops[room] = h
	}
	h.n, h.last = h.n+1, s.now()
	return h.n
}

// afterMention returns what follows the first "@<configured name>" in a bot's message, so the
// bot handing work on can write to its own reader first. Tanka delivers every mention as
// "@Display Name". Without one it falls back to stripMention.
func afterMention(text string, names []string) string {
	at := -1
	var rest string
	for _, n := range names {
		if n == "" {
			continue
		}
		if i := strings.Index(text, "@"+n); i >= 0 && (at < 0 || i < at) {
			at, rest = i, text[i+1+len(n):]
		}
	}
	if at < 0 {
		return stripMention(text, names)
	}
	return strings.TrimSpace(rest)
}

// stripMention removes the leading bot mention: "<@ID>", "@<configured name>", or else "@word".
func stripMention(text string, names []string) string {
	t := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(t, "<@"):
		if i := strings.IndexByte(t, '>'); i > 0 {
			return strings.TrimSpace(t[i+1:])
		}
	case strings.HasPrefix(t, "@"):
		for _, n := range names {
			if n != "" && strings.HasPrefix(t[1:], n) {
				return strings.TrimSpace(t[1+len(n):])
			}
		}
		_, rest := proto.CutWord(t)
		return rest
	}
	return t
}

func (s *Server) pair(bot *Bot, ev tanka.MentionEvent, deviceID string) string {
	if deviceID == "" {
		return "Usage: `pair <device-id>`. The worker prints its device id when it starts."
	}
	d, err := s.deviceWhere(`id = ?`, deviceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Sprintf("No device `%s` is registered with this server.", deviceID)
	case err != nil:
		log.Printf("pair: %v", err)
		return "Pairing failed: the server could not read its database."
	case d.Bot != bot.Name:
		return fmt.Sprintf("Device `%s` is set up for bot `%s`; mention that bot in this room to pair it.", deviceID, d.Bot)
	}
	if err := s.bind(bot.Name, ev.RoomID, deviceID, ev.RoomName, ev.UserID); err != nil {
		log.Printf("pair: %v", err)
		return "Pairing failed: the server could not save it."
	}
	log.Printf("bot %s room %s paired with device %s by %s", bot.Name, ev.RoomID, deviceID, ev.UserID)
	return fmt.Sprintf("Paired this room with device `%s` (%s). Mention me with a prompt to run it there.", deviceID, d.Name)
}

func (s *Server) bind(bot, roomID, deviceID, roomName, by string) error {
	_, err := s.db.Exec(`INSERT INTO bindings (bot, room_id, device_id, room_name, bound_by, bound_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (bot, room_id) DO UPDATE SET device_id = excluded.device_id, room_name = excluded.room_name,
			bound_by = excluded.bound_by, bound_at = excluded.bound_at`,
		bot, roomID, deviceID, roomName, by, s.now().UnixMilli())
	return err
}

func (s *Server) unpair(bot *Bot, roomID string) string {
	var dev string
	err := s.db.QueryRow(`DELETE FROM bindings WHERE bot = ? AND room_id = ? RETURNING device_id`, bot.Name, roomID).Scan(&dev)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "This room is not paired."
	case err != nil:
		log.Printf("unpair: %v", err)
		return "Unpairing failed: the server could not save it."
	}
	log.Printf("bot %s room %s unpaired from device %s", bot.Name, roomID, dev)
	return fmt.Sprintf("Unpaired this room from device `%s`.", dev)
}

func (s *Server) enqueue(bot *Bot, ev tanka.MentionEvent, text string) {
	d, err := s.deviceWhere(`id = (SELECT device_id FROM bindings WHERE bot = ? AND room_id = ?)`, bot.Name, ev.RoomID)
	var msg string
	switch {
	case errors.Is(err, sql.ErrNoRows):
		msg = "This room is not paired with a device. Run `pair <device-id>` first; the worker prints its device id when it starts."
	case err != nil:
		log.Printf("enqueue: %v", err)
		msg = "The server could not read its database, so nothing was queued."
	case !s.online(d):
		// Not queued: a prompt that runs hours later, when the laptop wakes up, would surprise.
		msg = fmt.Sprintf("Device `%s` is offline (last seen %s), so nothing was queued.", d.ID, s.ago(d.LastSeen))
	default:
		t := proto.Task{RoomID: ev.RoomID, RoomName: ev.RoomName, UserID: ev.UserID, UserName: ev.UserName, Text: text, SenderType: ev.SenderType}
		for _, a := range ev.Attachments {
			t.Attachments = append(t.Attachments, proto.Attachment{URL: a.URL, Name: a.Name})
		}
		id, err := s.insertTask(bot, d, t, ev.UserID, ev.ResponseURL)
		switch {
		case err == nil:
			log.Printf("task %s queued bot=%s room=%s user=%s device=%s attachments=%d", id, bot.Name, ev.RoomID, ev.UserID, d.ID, len(t.Attachments))
			return
		case errors.Is(err, errQueueFull):
			msg = fmt.Sprintf("Device `%s` already has %d tasks waiting; try again later.", d.ID, maxQueuePerDev)
		default:
			log.Printf("enqueue: %v", err)
			msg = "The server could not queue the task, so nothing was queued."
		}
	}
	s.replyAsync(bot, ev.RoomID, ev.ResponseURL, msg)
}

var errQueueFull = fmt.Errorf("already has %d tasks waiting or running", maxQueuePerDev)

// insertTask fills in t's id and time, seals it to d and queues it in memory; responseURL
// may be empty. sender is who asked for it, for the task log. It refuses once d has
// maxQueuePerDev tasks waiting or running.
func (s *Server) insertTask(bot *Bot, d *device, t proto.Task, sender, responseURL string) (string, error) {
	id := "t-" + randHex(6)
	t.ID, t.CreatedAt = id, s.now()
	plain, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	sealed, err := seal.Seal(s.priv, d.Pub, proto.TaskContext(d.ID, id), plain)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	n := len(s.waiting[d.ID])
	for _, l := range s.leased {
		if l.dev == d.ID {
			n++
		}
	}
	if n >= maxQueuePerDev {
		s.mu.Unlock()
		return "", errQueueFull
	}
	s.waiting[d.ID] = append(s.waiting[d.ID], &task{id: id, dev: d.ID, bot: bot.Name, room: t.RoomID, sealed: sealed, reply: responseURL, created: s.now()})
	s.logQueued(id, bot.Name, d.ID, t, sender, len(sealed), n+1) // under mu, so a lease cannot log first
	s.mu.Unlock()
	s.signal(d.ID)
	return id, nil
}

// trigger queues a task nobody typed for the device bound to roomID; from is the device
// that asked for it, or "" for a server hook. It is queued even if the device is offline; if
// nobody picks it up, it expires without a reply.
func (s *Server) trigger(bot *Bot, roomID, name, key, text, from string) (string, error) {
	d, err := s.deviceWhere(`id = (SELECT device_id FROM bindings WHERE bot = ? AND room_id = ?)`, bot.Name, roomID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", errors.New("room is not bound to a registered device")
	}
	if err != nil {
		return "", err
	}
	id, err := s.insertTask(bot, d, proto.Task{RoomID: roomID, Text: text, Trigger: name, Key: key}, from, "")
	if err != nil {
		return "", fmt.Errorf("device %s: %w", d.ID, err)
	}
	return id, nil
}

// handleTrigger queues a task a worker's trigger produced. The body is sealed by the
// device, and the room must belong to the device's own bot.
func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	d, ok := s.auth(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req proto.TriggerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil || req.RoomID == "" {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	var body proto.TriggerBody
	plain, err := seal.Open(s.priv, d.Pub, proto.TriggerContext(d.ID, req.RoomID), req.Sealed)
	if err == nil {
		err = json.Unmarshal(plain, &body)
	}
	if err != nil || body.Trigger == "" || strings.TrimSpace(body.Text) == "" {
		http.Error(w, "trigger does not decrypt for this device and room, or is incomplete", http.StatusBadRequest)
		return
	}
	bot := s.bots[d.Bot]
	if bot == nil {
		http.Error(w, "the device's bot is no longer configured", http.StatusConflict)
		return
	}
	id, err := s.trigger(bot, req.RoomID, body.Trigger, body.Key, body.Text, d.ID)
	if err != nil {
		log.Printf("trigger from %s for room %s: %v", d.ID, req.RoomID, err)
		http.Error(w, "not queued: "+err.Error(), http.StatusConflict)
		return
	}
	log.Printf("task %s queued bot=%s room=%s trigger=%s from=%s", id, d.Bot, req.RoomID, body.Trigger, d.ID)
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *Server) online(d *device) bool {
	return !d.LastSeen.IsZero() && s.now().Sub(d.LastSeen) < s.cfg.PollWait()+onlineGrace
}

func (s *Server) ago(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return s.now().Sub(t).Round(time.Second).String() + " ago"
}

func (s *Server) signal(dev string) {
	select {
	case s.wakeCh(dev) <- struct{}{}:
	default:
	}
}

func (s *Server) wakeCh(dev string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.wake[dev]
	if ch == nil {
		ch = make(chan struct{}, 1)
		s.wake[dev] = ch
	}
	return ch
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	tok, ok := bearer(r)
	if !ok || subtle.ConstantTimeCompare([]byte(tok), []byte(s.cfg.EnrollToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req proto.RegisterRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if _, err := seal.ParsePublicKey(req.PublicKey); err != nil {
		http.Error(w, "public_key must be an X25519 public key", http.StatusBadRequest)
		return
	}
	bot := cmp.Or(req.Bot, s.cfg.DefaultBot)
	if s.bots[bot] == nil {
		http.Error(w, "unknown bot "+bot, http.StatusBadRequest)
		return
	}
	id := "dev-" + strings.ToLower(base32.StdEncoding.EncodeToString(randBytes(5)))
	token := randHex(32)
	name := tanka.Truncate(strings.TrimSpace(req.Name), 64)
	_, err := s.db.Exec(`INSERT INTO devices (id, bot, name, token_hash, pubkey, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, bot, name, hashToken(token), req.PublicKey, s.now().UnixMilli())
	if err != nil {
		log.Printf("register: %v", err)
		http.Error(w, "could not save the device", http.StatusInternalServerError)
		return
	}
	log.Printf("device %s registered for bot %s (%s)", id, bot, name)
	writeJSON(w, http.StatusOK, proto.RegisterResponse{DeviceID: id, DeviceToken: token, Bot: bot, ServerKey: s.priv.PublicKey().Bytes()})
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	d, ok := s.auth(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.noteStatus(d.ID, r.Header.Get(proto.HeaderWorkerStatus))
	timer := time.NewTimer(s.cfg.PollWait())
	defer timer.Stop()
	for {
		s.touch(d.ID)
		env, err := s.lease(d.ID)
		if err != nil {
			log.Printf("poll %s: %v", d.ID, err)
			http.Error(w, "storage error", http.StatusInternalServerError)
			return
		}
		if env != nil {
			writeJSON(w, http.StatusOK, env)
			return
		}
		select {
		case <-s.wakeCh(d.ID):
		case <-timer.C:
			s.touch(d.ID)
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) touch(dev string) {
	if _, err := s.db.Exec(`UPDATE devices SET last_seen = ? WHERE id = ?`, s.now().UnixMilli(), dev); err != nil {
		log.Printf("touch %s: %v", dev, err)
	}
}

// lease hands the device its oldest waiting task, with where the result goes boxed in Reply,
// and forgets the task's content.
func (s *Server) lease(dev string) (*proto.Envelope, error) {
	s.mu.Lock()
	q := s.waiting[dev]
	if len(q) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	t := q[0]
	if s.waiting[dev] = q[1:]; len(q) == 1 {
		delete(s.waiting, dev)
	}
	sealed := t.sealed
	t.sealed, t.until = nil, s.now().Add(s.cfg.lease())
	s.leased[t.id] = t
	s.mu.Unlock()
	s.logLeased(t.id, t.until)
	plain, _ := json.Marshal(replyTo{Dev: t.dev, Bot: t.bot, Room: t.room, URL: t.reply, Until: t.until.UnixMilli()})
	boxed, err := seal.Box(s.key, "reply "+t.id, plain)
	if err != nil {
		return nil, err
	}
	return &proto.Envelope{ID: t.id, Sealed: sealed, Reply: boxed}, nil
}

// taken returns where the result of task id, taken by dev, goes: from memory, or after a
// restart from the box the device sent back. It returns nil for a task that was never taken,
// was already answered, or whose lease ended.
func (s *Server) taken(dev, id string, boxed []byte) *replyTo {
	s.mu.Lock()
	t := s.leased[id]
	if t != nil && (t.dev != dev || s.now().After(t.until)) {
		t = nil // a lapsed lease is left for reapExpired, which tells the room
	}
	if t != nil {
		delete(s.leased, id)
	}
	s.mu.Unlock()
	var rt replyTo
	if t != nil {
		rt = replyTo{Dev: t.dev, Bot: t.bot, Room: t.room, URL: t.reply, Until: t.until.UnixMilli()}
	} else if plain, err := seal.Unbox(s.key, "reply "+id, boxed); err != nil || json.Unmarshal(plain, &rt) != nil || rt.Dev != dev {
		return nil
	}
	until := time.UnixMilli(rt.Until)
	if s.now().After(until) || !s.firstSeen("result/"+id, until.Sub(s.now())) {
		return nil
	}
	return &rt
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	d, ok := s.auth(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var env proto.Envelope
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	var res proto.Result
	plain, err := seal.Open(s.priv, d.Pub, proto.ResultContext(d.ID, id), env.Sealed)
	if err == nil {
		err = json.Unmarshal(plain, &res)
	}
	if err != nil {
		log.Printf("task %s: result from %s does not open: %v", id, d.ID, err)
		http.Error(w, "result does not decrypt for this device and task", http.StatusBadRequest)
		return
	}
	rt := s.taken(d.ID, id, env.Reply)
	if rt == nil {
		http.Error(w, "unknown task", http.StatusNotFound)
		return
	}
	if res.Text == "" {
		log.Printf("task %s done room=%s (nothing to post)", id, rt.Room)
		s.logDone(id, outEmpty, len(env.Sealed))
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if err := s.reply(s.bots[rt.Bot], rt.Room, rt.URL, res.Text); err != nil {
		log.Printf("task %s: reply failed: %v", id, err)
		s.logDone(id, outReplyFailed, len(env.Sealed))
		http.Error(w, "reply failed", http.StatusBadGateway)
		return
	}
	log.Printf("task %s done room=%s", id, rt.Room)
	s.logDone(id, outPosted, len(env.Sealed))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) reapExpired(now time.Time) {
	var dropped []*task
	s.mu.Lock()
	for dev, q := range s.waiting {
		keep := q[:0]
		for _, t := range q {
			if t.created.Before(now.Add(-s.cfg.queueTTL())) {
				dropped = append(dropped, t)
			} else {
				keep = append(keep, t)
			}
		}
		if s.waiting[dev] = keep; len(keep) == 0 {
			delete(s.waiting, dev)
		}
	}
	for id, t := range s.leased {
		if t.until.Before(now) {
			dropped = append(dropped, t)
			delete(s.leased, id)
		}
	}
	for k, until := range s.seen {
		if until.Before(now) {
			delete(s.seen, k)
		}
	}
	s.mu.Unlock()
	s.sweepLog(now)
	for _, t := range dropped {
		msg, outcome := fmt.Sprintf("Device `%s` did not pick this task up within %s, so it was dropped.", t.dev, s.cfg.queueTTL()), outNotTaken
		if !t.until.IsZero() {
			msg, outcome = fmt.Sprintf("Device `%s` took this task but sent no result within %s, so it was dropped.", t.dev, s.cfg.lease()), outNoResult
		}
		s.logDone(t.id, outcome, 0)
		log.Printf("task %s expired on device %s", t.id, t.dev)
		// Only a mention gets told: a triggered task has no response_url, and an asleep
		// laptop would otherwise fill the room with notices.
		if bot := s.bots[t.bot]; bot != nil && t.reply != "" {
			s.replyAsync(bot, t.room, t.reply, msg)
		}
	}
}

func (s *Server) replyAsync(bot *Bot, roomID, responseURL, text string) {
	go func() {
		if err := s.reply(bot, roomID, responseURL, text); err != nil {
			log.Printf("reply bot=%s room=%s failed: %v", bot.Name, roomID, err)
		}
	}()
}

// reply posts to the event's response_url (single use, threads under the mention) and
// falls back to the room's incoming webhook.
func (s *Server) reply(bot *Bot, roomID, responseURL, text string) error {
	if bot == nil {
		return errors.New("the task's bot is no longer configured")
	}
	msg := tanka.Message{Text: tanka.Truncate(text, s.cfg.MaxReplyChars), TextType: "markdown"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var errs []error
	switch {
	case responseURL == "":
	case !strings.HasPrefix(responseURL, s.cfg.ResponseURLPrefix):
		errs = append(errs, errors.New("response_url does not start with response_url_prefix"))
	default:
		err := tanka.Post(ctx, s.http, responseURL, msg)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("response_url: %w", err))
	}
	hook := bot.RoomWebhooks[roomID]
	if hook == "" {
		return errors.Join(append(errs, errors.New("no room webhook configured"))...)
	}
	if err := tanka.Post(ctx, s.http, hook, msg); err != nil {
		return errors.Join(append(errs, fmt.Errorf("room webhook: %w", err))...)
	}
	return nil
}

func (s *Server) auth(r *http.Request) (*device, bool) {
	tok, ok := bearer(r)
	if !ok {
		return nil, false
	}
	d, err := s.deviceWhere(`token_hash = ?`, hashToken(tok))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Printf("auth: %v", err)
	}
	return d, err == nil
}

func (s *Server) deviceWhere(cond string, args ...any) (*device, error) {
	var d device
	var pub []byte
	var seen int64
	err := s.db.QueryRow(`SELECT id, bot, name, pubkey, last_seen FROM devices WHERE `+cond, args...).
		Scan(&d.ID, &d.Bot, &d.Name, &pub, &seen)
	if err != nil {
		return nil, err
	}
	if seen > 0 {
		d.LastSeen = time.UnixMilli(seen)
	}
	d.Pub, err = seal.ParsePublicKey(pub)
	return &d, err
}

func bearer(r *http.Request) (string, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return tok, ok && tok != ""
}

func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func randHex(n int) string { return hex.EncodeToString(randBytes(n)) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
