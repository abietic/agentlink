// Package server receives Tanka app_mention events, queues each one for the worker device
// paired with its room, and posts the worker's result back to that room.
//
// Message text lives only in memory and is dropped once the result is posted; logs carry
// ids, never text. Only devices (with hashed tokens) and room bindings are persisted.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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

	"agentlink/internal/jsonfile"
	"agentlink/internal/proto"
	"agentlink/internal/tanka"
)

const (
	maxBody        = 1 << 20
	signatureSkew  = 5 * time.Minute
	dedupeTTL      = time.Hour // Tanka retries a delivery for about six minutes
	maxQueuePerDev = 100
	onlineGrace    = 30 * time.Second // a worker re-polls right after each long poll ends
)

const helpText = `**agentlink** runs what you write on the device paired with this room, through Claude Code.

- ` + "`pair <device-id>`" + ` / ` + "`unpair`" + ` — bind this room to a worker (it prints its device id on start)
- ` + "`status`" + ` — device, workdir, session, what is running
- ` + "`cd <path>`" + ` — switch workdir (the next prompt starts a new session)
- ` + "`new [prompt]`" + ` — start a new session
- ` + "`sessions`" + ` — recent sessions in this room
- ` + "`resume <n|session-id> [prompt]`" + ` — switch to an earlier session, or to one started in a terminal
- anything else — a prompt, continuing the room's current session`

type Device struct {
	Name      string    `json:"name"`
	TokenHash string    `json:"token_hash"`
	CreatedAt time.Time `json:"created_at"`
}

type Binding struct {
	DeviceID string    `json:"device_id"`
	RoomName string    `json:"room_name"`
	BoundBy  string    `json:"bound_by"`
	BoundAt  time.Time `json:"bound_at"`
}

type state struct {
	Devices  map[string]*Device  `json:"devices"`
	Bindings map[string]*Binding `json:"bindings"` // by room_id
}

type job struct {
	task        proto.Task
	device      string
	responseURL string
	deadline    time.Time
}

type Server struct {
	cfg  Config
	http *http.Client
	now  func() time.Time

	mu       sync.Mutex
	st       state
	lastSeen map[string]time.Time     // device → last poll
	queues   map[string][]*job        // device → waiting jobs
	leased   map[string]*job          // task id → job a worker holds
	wake     map[string]chan struct{} // device → long-poll wakeup
	seen     map[string]time.Time     // event_id → first delivery
}

func New(cfg Config) (*Server, error) {
	cfg.applyDefaults()
	st := state{Devices: map[string]*Device{}, Bindings: map[string]*Binding{}}
	if _, err := jsonfile.Load(cfg.StatePath, &st); err != nil {
		return nil, err
	}
	if st.Devices == nil {
		st.Devices = map[string]*Device{}
	}
	if st.Bindings == nil {
		st.Bindings = map[string]*Binding{}
	}
	return &Server{
		cfg:      cfg,
		http:     &http.Client{Timeout: 15 * time.Second},
		now:      time.Now,
		st:       st,
		lastSeen: map[string]time.Time{},
		queues:   map[string][]*job{},
		leased:   map[string]*job{},
		wake:     map[string]chan struct{}{},
		seen:     map[string]time.Time{},
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tanka/events", s.handleEvent)
	mux.HandleFunc("POST /api/v1/register", s.handleRegister)
	mux.HandleFunc("GET /api/v1/poll", s.handlePoll)
	mux.HandleFunc("POST /api/v1/tasks/{id}/result", s.handleResult)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	return mux
}

// Reap gives up on tasks whose worker never answered, until ctx ends.
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
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	err = tanka.Verify([]byte(s.cfg.SigningSecret), r.Header.Get(tanka.HeaderTimestamp), r.Header.Get(tanka.HeaderSignature), body, s.now(), signatureSkew)
	if err != nil {
		log.Printf("event rejected: %v", err)
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
	if env.Type == "event_callback" && env.Event != nil && env.Event.Type == "app_mention" && s.firstSeen(env.EventID) {
		s.onMention(*env.Event)
	}
	writeJSON(w, http.StatusOK, map[string]int{"code": 0})
}

func (s *Server) firstSeen(eventID string) bool {
	if eventID == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, t := range s.seen {
		if now.Sub(t) > dedupeTTL {
			delete(s.seen, id)
		}
	}
	if _, dup := s.seen[eventID]; dup {
		return false
	}
	s.seen[eventID] = now
	return true
}

func (s *Server) onMention(ev tanka.MentionEvent) {
	if len(s.cfg.AllowedUsers) > 0 && !slices.Contains(s.cfg.AllowedUsers, ev.UserID) {
		log.Printf("mention room=%s user=%s: not in allowed_users", ev.RoomID, ev.UserID)
		s.replyAsync(ev.RoomID, ev.ResponseURL, fmt.Sprintf("User `%s` is not allowed to use this bot.", ev.UserID))
		return
	}
	text := stripMention(ev.Text, s.cfg.BotNames)
	word, arg := proto.CutWord(text)
	switch proto.Keyword(word) {
	case "", "help":
		s.replyAsync(ev.RoomID, ev.ResponseURL, helpText)
	case "pair":
		s.replyAsync(ev.RoomID, ev.ResponseURL, s.pair(ev, arg))
	case "unpair":
		s.replyAsync(ev.RoomID, ev.ResponseURL, s.unpair(ev.RoomID))
	default:
		s.enqueue(ev, text)
	}
}

// stripMention removes the leading bot mention: "<@ID>", "@<configured name>", or else "@word".
func stripMention(text string, botNames []string) string {
	t := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(t, "<@"):
		if i := strings.IndexByte(t, '>'); i > 0 {
			return strings.TrimSpace(t[i+1:])
		}
	case strings.HasPrefix(t, "@"):
		for _, n := range botNames {
			if n != "" && strings.HasPrefix(t[1:], n) {
				return strings.TrimSpace(t[1+len(n):])
			}
		}
		_, rest := proto.CutWord(t)
		return rest
	}
	return t
}

func (s *Server) pair(ev tanka.MentionEvent, deviceID string) string {
	if deviceID == "" {
		return "Usage: `pair <device-id>`. The worker prints its device id when it starts."
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.st.Devices[deviceID]
	if d == nil {
		return fmt.Sprintf("No device `%s` is registered with this server.", deviceID)
	}
	prev := s.st.Bindings[ev.RoomID]
	s.st.Bindings[ev.RoomID] = &Binding{DeviceID: deviceID, RoomName: ev.RoomName, BoundBy: ev.UserID, BoundAt: s.now()}
	if err := jsonfile.Save(s.cfg.StatePath, &s.st); err != nil {
		if prev == nil {
			delete(s.st.Bindings, ev.RoomID)
		} else {
			s.st.Bindings[ev.RoomID] = prev
		}
		log.Printf("pair room=%s: save state: %v", ev.RoomID, err)
		return "Pairing failed: the server could not save its state."
	}
	log.Printf("room %s paired with device %s by %s", ev.RoomID, deviceID, ev.UserID)
	return fmt.Sprintf("Paired this room with device `%s` (%s). Mention me with a prompt to run it there.", deviceID, d.Name)
}

func (s *Server) unpair(roomID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.st.Bindings[roomID]
	if b == nil {
		return "This room is not paired."
	}
	delete(s.st.Bindings, roomID)
	if err := jsonfile.Save(s.cfg.StatePath, &s.st); err != nil {
		s.st.Bindings[roomID] = b
		log.Printf("unpair room=%s: save state: %v", roomID, err)
		return "Unpairing failed: the server could not save its state."
	}
	log.Printf("room %s unpaired from device %s", roomID, b.DeviceID)
	return fmt.Sprintf("Unpaired this room from device `%s`.", b.DeviceID)
}

func (s *Server) enqueue(ev tanka.MentionEvent, text string) {
	s.mu.Lock()
	b := s.st.Bindings[ev.RoomID]
	var msg string
	switch {
	case b == nil:
		msg = "This room is not paired with a device. Run `pair <device-id>` first; the worker prints its device id when it starts."
	case !s.onlineLocked(b.DeviceID):
		// Not queued: a prompt that runs hours later, when the laptop wakes up, would surprise.
		msg = fmt.Sprintf("Device `%s` is offline (last seen %s), so nothing was queued.", b.DeviceID, s.lastSeenLocked(b.DeviceID))
	case len(s.queues[b.DeviceID]) >= maxQueuePerDev:
		msg = fmt.Sprintf("Device `%s` already has %d tasks waiting; try again later.", b.DeviceID, maxQueuePerDev)
	default:
		j := &job{
			task: proto.Task{
				ID: "t-" + randHex(6), RoomID: ev.RoomID, RoomName: ev.RoomName,
				UserID: ev.UserID, UserName: ev.UserName, Text: text, CreatedAt: s.now(),
			},
			device:      b.DeviceID,
			responseURL: ev.ResponseURL,
		}
		s.queues[b.DeviceID] = append(s.queues[b.DeviceID], j)
		s.signalLocked(b.DeviceID)
		s.mu.Unlock()
		log.Printf("task %s queued room=%s user=%s device=%s", j.task.ID, ev.RoomID, ev.UserID, b.DeviceID)
		return
	}
	s.mu.Unlock()
	s.replyAsync(ev.RoomID, ev.ResponseURL, msg)
}

func (s *Server) onlineLocked(dev string) bool {
	t, ok := s.lastSeen[dev]
	return ok && s.now().Sub(t) < s.cfg.PollWait()+onlineGrace
}

func (s *Server) lastSeenLocked(dev string) string {
	t, ok := s.lastSeen[dev]
	if !ok {
		return "never since the server started"
	}
	return s.now().Sub(t).Round(time.Second).String() + " ago"
}

func (s *Server) signalLocked(dev string) {
	select {
	case s.wakeLocked(dev) <- struct{}{}:
	default:
	}
}

func (s *Server) wakeLocked(dev string) chan struct{} {
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
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	id := "dev-" + strings.ToLower(base32.StdEncoding.EncodeToString(randBytes(5)))
	token := randHex(32)
	name := tanka.Truncate(strings.TrimSpace(req.Name), 64)
	s.mu.Lock()
	s.st.Devices[id] = &Device{Name: name, TokenHash: hashToken(token), CreatedAt: s.now()}
	err := jsonfile.Save(s.cfg.StatePath, &s.st)
	if err != nil {
		delete(s.st.Devices, id)
	}
	s.mu.Unlock()
	if err != nil {
		log.Printf("register: save state: %v", err)
		http.Error(w, "could not save state", http.StatusInternalServerError)
		return
	}
	log.Printf("device %s registered (%s)", id, name)
	writeJSON(w, http.StatusOK, proto.RegisterResponse{DeviceID: id, DeviceToken: token})
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	dev, ok := s.device(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	timer := time.NewTimer(s.cfg.PollWait())
	defer timer.Stop()
	for {
		s.mu.Lock()
		now := s.now()
		s.lastSeen[dev] = now
		if q := s.queues[dev]; len(q) > 0 {
			j := q[0]
			s.queues[dev] = q[1:]
			j.deadline = now.Add(s.cfg.lease())
			s.leased[j.task.ID] = j
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, j.task)
			return
		}
		wake := s.wakeLocked(dev)
		s.mu.Unlock()
		select {
		case <-wake:
		case <-timer.C:
			s.mu.Lock()
			s.lastSeen[dev] = s.now()
			s.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	dev, ok := s.device(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var res proto.Result
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&res); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	s.mu.Lock()
	j := s.leased[id]
	if j == nil || j.device != dev {
		s.mu.Unlock()
		http.Error(w, "unknown task", http.StatusNotFound)
		return
	}
	delete(s.leased, id)
	s.mu.Unlock()
	if err := s.reply(j.task.RoomID, j.responseURL, res.Text); err != nil {
		log.Printf("task %s: reply failed: %v", id, err)
		http.Error(w, "reply failed", http.StatusBadGateway)
		return
	}
	log.Printf("task %s done room=%s", id, j.task.RoomID)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) reapExpired(now time.Time) {
	var expired []*job
	s.mu.Lock()
	for id, j := range s.leased {
		if now.After(j.deadline) {
			expired = append(expired, j)
			delete(s.leased, id)
		}
	}
	s.mu.Unlock()
	for _, j := range expired {
		log.Printf("task %s expired on device %s", j.task.ID, j.device)
		s.replyAsync(j.task.RoomID, j.responseURL, fmt.Sprintf("Device `%s` took this task but sent no result within %s, so it was dropped.", j.device, s.cfg.lease()))
	}
}

func (s *Server) replyAsync(roomID, responseURL, text string) {
	go func() {
		if err := s.reply(roomID, responseURL, text); err != nil {
			log.Printf("reply room=%s failed: %v", roomID, err)
		}
	}()
}

// reply posts to the event's response_url (single use, threads under the mention) and
// falls back to the room's incoming webhook.
func (s *Server) reply(roomID, responseURL, text string) error {
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
	hook := s.cfg.RoomWebhooks[roomID]
	if hook == "" {
		return errors.Join(append(errs, errors.New("no room webhook configured"))...)
	}
	if err := tanka.Post(ctx, s.http, hook, msg); err != nil {
		return errors.Join(append(errs, fmt.Errorf("room webhook: %w", err))...)
	}
	return nil
}

func (s *Server) device(r *http.Request) (string, bool) {
	tok, ok := bearer(r)
	if !ok {
		return "", false
	}
	h := []byte(hashToken(tok))
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, d := range s.st.Devices {
		if subtle.ConstantTimeCompare([]byte(d.TokenHash), h) == 1 {
			return id, true
		}
	}
	return "", false
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
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func randHex(n int) string { return hex.EncodeToString(randBytes(n)) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
