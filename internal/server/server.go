// Package server receives Tanka app_mention events for one or more bots, queues each one
// for the worker device paired with its room, and posts the worker's result back.
//
// State lives in SQLite. A task's content is sealed to its device's key as soon as it
// arrives, so the database and everything between server and worker (the TLS-terminating
// proxy included) only see ciphertext; response_urls are encrypted with the server's
// storage key. Logs carry ids, never message text.
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
CREATE TABLE IF NOT EXISTS tasks (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL UNIQUE,
	device_id TEXT NOT NULL,
	bot TEXT NOT NULL,
	room_id TEXT NOT NULL,
	payload BLOB NOT NULL,  -- proto.Task sealed to the device
	reply_to BLOB NOT NULL, -- response_url boxed with the storage key
	created_at INTEGER NOT NULL,
	leased_until INTEGER NOT NULL DEFAULT 0 -- 0 = waiting
);
CREATE INDEX IF NOT EXISTS tasks_by_device ON tasks (device_id, seq);
CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, seen_at INTEGER NOT NULL);
`

const helpText = `**agentlink** runs what you write on the device paired with this room, through the Claude Agent SDK.

- ` + "`pair <device-id>`" + ` / ` + "`unpair`" + ` — bind this room to a worker (it prints its device id on start)
- ` + "`status`" + ` — device, workdir, session, what is running
- ` + "`cd <path>`" + ` — switch workdir (the next prompt starts a new session)
- ` + "`new [prompt]`" + ` — start a new session
- ` + "`sessions`" + ` — recent sessions in this room
- ` + "`resume <n|session-id> [prompt]`" + ` — switch to an earlier session, or to one started in a terminal
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

	mu   sync.Mutex
	wake map[string]chan struct{} // device → long-poll wakeup
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
	s := &Server{
		cfg:  cfg,
		bots: map[string]*Bot{},
		db:   db,
		key:  key,
		http: &http.Client{Timeout: 15 * time.Second},
		now:  time.Now,
		wake: map[string]chan struct{}{},
	}
	for i := range s.cfg.Bots {
		s.bots[s.cfg.Bots[i].Name] = &s.cfg.Bots[i]
	}
	if s.priv, err = s.loadKey(); err != nil {
		db.Close()
		return nil, err
	}
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
	if env.Type == "event_callback" && env.Event != nil && env.Event.Type == "app_mention" {
		first, err := s.firstSeen(bot.Name + "/" + env.EventID)
		if err != nil {
			log.Printf("bot %s: dedupe: %v", bot.Name, err)
			http.Error(w, "storage error", http.StatusInternalServerError) // Tanka retries
			return
		}
		if first {
			s.onMention(bot, *env.Event)
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"code": 0})
}

func (s *Server) firstSeen(key string) (bool, error) {
	if strings.HasSuffix(key, "/") { // no event_id
		return true, nil
	}
	res, err := s.db.Exec(`INSERT INTO events (id, seen_at) VALUES (?, ?) ON CONFLICT (id) DO NOTHING`, key, s.now().UnixMilli())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Server) onMention(bot *Bot, ev tanka.MentionEvent) {
	if len(s.cfg.AllowedUsers) > 0 && !slices.Contains(s.cfg.AllowedUsers, ev.UserID) {
		log.Printf("bot %s room=%s user=%s: not in allowed_users", bot.Name, ev.RoomID, ev.UserID)
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, fmt.Sprintf("User `%s` is not allowed to use this bot.", ev.UserID))
		return
	}
	text := stripMention(ev.Text, bot.Mentions)
	word, arg := proto.CutWord(text)
	switch proto.Keyword(word) {
	case "", "help":
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, helpText)
	case "pair":
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, s.pair(bot, ev, arg))
	case "unpair":
		s.replyAsync(bot, ev.RoomID, ev.ResponseURL, s.unpair(bot, ev.RoomID))
	default:
		s.enqueue(bot, ev, text)
	}
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
	_, err = s.db.Exec(`INSERT INTO bindings (bot, room_id, device_id, room_name, bound_by, bound_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (bot, room_id) DO UPDATE SET device_id = excluded.device_id, room_name = excluded.room_name,
			bound_by = excluded.bound_by, bound_at = excluded.bound_at`,
		bot.Name, ev.RoomID, deviceID, ev.RoomName, ev.UserID, s.now().UnixMilli())
	if err != nil {
		log.Printf("pair: %v", err)
		return "Pairing failed: the server could not save it."
	}
	log.Printf("bot %s room %s paired with device %s by %s", bot.Name, ev.RoomID, deviceID, ev.UserID)
	return fmt.Sprintf("Paired this room with device `%s` (%s). Mention me with a prompt to run it there.", deviceID, d.Name)
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
	var queued int
	if err == nil {
		err = s.db.QueryRow(`SELECT count(*) FROM tasks WHERE device_id = ?`, d.ID).Scan(&queued)
	}
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
	case queued >= maxQueuePerDev:
		msg = fmt.Sprintf("Device `%s` already has %d tasks waiting; try again later.", d.ID, maxQueuePerDev)
	default:
		id, err := s.insertTask(bot, d, ev, text)
		if err == nil {
			log.Printf("task %s queued bot=%s room=%s user=%s device=%s", id, bot.Name, ev.RoomID, ev.UserID, d.ID)
			return
		}
		log.Printf("enqueue: %v", err)
		msg = "The server could not store the task, so nothing was queued."
	}
	s.replyAsync(bot, ev.RoomID, ev.ResponseURL, msg)
}

func (s *Server) insertTask(bot *Bot, d *device, ev tanka.MentionEvent, text string) (string, error) {
	id := "t-" + randHex(6)
	plain, err := json.Marshal(proto.Task{
		ID: id, RoomID: ev.RoomID, RoomName: ev.RoomName,
		UserID: ev.UserID, UserName: ev.UserName, Text: text, CreatedAt: s.now(),
	})
	if err != nil {
		return "", err
	}
	payload, err := seal.Seal(s.priv, d.Pub, proto.TaskContext(d.ID, id), plain)
	if err != nil {
		return "", err
	}
	replyTo, err := seal.Box(s.key, "reply "+id, []byte(ev.ResponseURL))
	if err != nil {
		return "", err
	}
	_, err = s.db.Exec(`INSERT INTO tasks (id, device_id, bot, room_id, payload, reply_to, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, d.ID, bot.Name, ev.RoomID, payload, replyTo, s.now().UnixMilli())
	if err != nil {
		return "", err
	}
	s.signal(d.ID)
	return id, nil
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

// lease hands the device's oldest waiting task to it, in one statement so two polls
// cannot both take it.
func (s *Server) lease(dev string) (*proto.Envelope, error) {
	var env proto.Envelope
	err := s.db.QueryRow(`UPDATE tasks SET leased_until = ?
		WHERE seq = (SELECT seq FROM tasks WHERE device_id = ? AND leased_until = 0 ORDER BY seq LIMIT 1)
		RETURNING id, payload`, s.now().Add(s.cfg.lease()).UnixMilli(), dev).Scan(&env.ID, &env.Sealed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &env, err
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
	var botName, room string
	var replyTo []byte
	err = s.db.QueryRow(`DELETE FROM tasks WHERE id = ? AND device_id = ? AND leased_until > 0 RETURNING bot, room_id, reply_to`,
		id, d.ID).Scan(&botName, &room, &replyTo)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "unknown task", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("task %s: %v", id, err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	if err := s.reply(s.bots[botName], room, s.responseURL(id, replyTo), res.Text); err != nil {
		log.Printf("task %s: reply failed: %v", id, err)
		http.Error(w, "reply failed", http.StatusBadGateway)
		return
	}
	log.Printf("task %s done room=%s", id, room)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) reapExpired(now time.Time) {
	rows, err := s.db.Query(`DELETE FROM tasks
		WHERE (leased_until > 0 AND leased_until < ?) OR (leased_until = 0 AND created_at < ?)
		RETURNING id, device_id, bot, room_id, reply_to, leased_until`,
		now.UnixMilli(), now.Add(-s.cfg.queueTTL()).UnixMilli())
	if err != nil {
		log.Printf("reap: %v", err)
		return
	}
	type dropped struct {
		id, dev, bot, room string
		replyTo            []byte
		leased             int64
	}
	var all []dropped
	for rows.Next() {
		var d dropped
		if err := rows.Scan(&d.id, &d.dev, &d.bot, &d.room, &d.replyTo, &d.leased); err != nil {
			log.Printf("reap: %v", err)
			continue
		}
		all = append(all, d)
	}
	rows.Close()
	for _, d := range all {
		msg := fmt.Sprintf("Device `%s` did not pick this task up within %s, so it was dropped.", d.dev, s.cfg.queueTTL())
		if d.leased > 0 {
			msg = fmt.Sprintf("Device `%s` took this task but sent no result within %s, so it was dropped.", d.dev, s.cfg.lease())
		}
		log.Printf("task %s expired on device %s", d.id, d.dev)
		if bot := s.bots[d.bot]; bot != nil {
			s.replyAsync(bot, d.room, s.responseURL(d.id, d.replyTo), msg)
		}
	}
	if _, err := s.db.Exec(`DELETE FROM events WHERE seen_at < ?`, now.Add(-dedupeTTL).UnixMilli()); err != nil {
		log.Printf("reap events: %v", err)
	}
}

func (s *Server) responseURL(taskID string, boxed []byte) string {
	u, err := seal.Unbox(s.key, "reply "+taskID, boxed)
	if err != nil {
		log.Printf("task %s: response_url: %v", taskID, err)
	}
	return string(u)
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
