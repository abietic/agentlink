// Package worker pulls tasks for this device from agentlink-server, runs them with the
// Claude Agent SDK in the room's workdir and session, and sends the result back.
//
// Tasks and results travel sealed between this device's and the server's X25519 keys;
// local state (identity, keys, rooms, session history) is kept encrypted in SQLite.
package worker

import (
	"cmp"
	"context"
	"crypto/ecdh"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"agentlink/internal/proto"
	"agentlink/internal/seal"
	"agentlink/internal/tanka"
)

const (
	maxSessions   = 10
	roomQueueSize = 16
)

type Session struct {
	ID       string    `json:"id"`
	Workdir  string    `json:"workdir"`
	Title    string    `json:"title"` // start of the first prompt
	LastUsed time.Time `json:"last_used"`
}

// RoomState is what a room has selected on this device.
type RoomState struct {
	Workdir   string    `json:"workdir"`
	SessionID string    `json:"session_id,omitempty"` // continued by default; empty = next prompt starts one
	Sessions  []Session `json:"sessions,omitempty"`   // most recent first
}

// Identity is this device's registration with one server.
type Identity struct {
	ServerURL   string `json:"server_url"`
	Bot         string `json:"bot"`
	DeviceID    string `json:"device_id"`
	DeviceToken string `json:"device_token"`
	DeviceKey   []byte `json:"device_key"` // X25519 private key
	ServerKey   []byte `json:"server_key"` // pinned at registration
}

type running struct {
	since  time.Time
	prompt string
}

type Worker struct {
	cfg    Config
	client *Client
	runner Runner
	store  *store
	sem    chan struct{}
	now    func() time.Time
	wg     sync.WaitGroup

	mu        sync.Mutex
	id        Identity
	key       *ecdh.PrivateKey
	serverKey *ecdh.PublicKey
	state     map[string]*RoomState // by room_id
	rooms     map[string]chan proto.Task
	running   map[string]running
}

func New(cfg Config) (*Worker, error) {
	st, err := openStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	w := &Worker{
		cfg:     cfg,
		client:  NewClient(cfg.ServerURL),
		runner:  SDK{Command: cfg.Runner, Options: cfg.SDKOptions, Timeout: cfg.timeout()},
		store:   st,
		sem:     make(chan struct{}, cfg.MaxConcurrency),
		now:     time.Now,
		rooms:   map[string]chan proto.Task{},
		running: map[string]running{},
	}
	if w.state, err = st.rooms(); err != nil {
		return nil, err
	}
	var id Identity
	if _, err := st.get("identity", &id); err != nil {
		return nil, err
	}
	if id.DeviceToken != "" {
		if err := w.setIdentity(id); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *Worker) setIdentity(id Identity) error {
	key, err := seal.ParsePrivateKey(id.DeviceKey)
	if err != nil {
		return fmt.Errorf("device key: %w", err)
	}
	serverKey, err := seal.ParsePublicKey(id.ServerKey)
	if err != nil {
		return fmt.Errorf("server key: %w", err)
	}
	w.id, w.key, w.serverKey = id, key, serverKey
	w.client.Token = id.DeviceToken
	return nil
}

func (w *Worker) DeviceID() string { return w.id.DeviceID }

// Register gets a device id and token from the server unless this device already has one
// for that server and bot. Each registration makes a new key pair.
func (w *Worker) Register(ctx context.Context) error {
	if w.id.DeviceToken != "" && w.id.ServerURL == w.client.BaseURL && (w.cfg.Bot == "" || w.cfg.Bot == w.id.Bot) {
		return nil
	}
	if w.cfg.EnrollToken == "" {
		return errors.New("not registered with " + w.client.BaseURL + " yet: set enroll_token (or AGENTLINK_ENROLL_TOKEN) for the first start")
	}
	key := seal.NewPrivateKey()
	reg, err := w.client.Register(ctx, w.cfg.EnrollToken, proto.RegisterRequest{Name: w.cfg.Name, Bot: w.cfg.Bot, PublicKey: key.PublicKey().Bytes()})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	id := Identity{ServerURL: w.client.BaseURL, Bot: reg.Bot, DeviceID: reg.DeviceID, DeviceToken: reg.DeviceToken, DeviceKey: key.Bytes(), ServerKey: reg.ServerKey}
	if err := w.setIdentity(id); err != nil {
		return err
	}
	return w.store.put("identity", id)
}

// Run polls for tasks until ctx ends, then waits for running tasks to report back.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.cfg.AllowedUsers) == 0 {
		log.Printf("allowed_users is empty: every task is refused, and the reply shows the sender's id")
	}
	backoff := time.Second
	var err error
	for ctx.Err() == nil {
		var env *proto.Envelope
		env, err = w.client.Poll(ctx)
		if errors.Is(err, ErrUnauthorized) {
			break
		}
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("poll: %v (retrying in %s)", err, backoff)
			}
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if env == nil {
			continue
		}
		var t proto.Task
		plain, oerr := seal.Open(w.key, w.serverKey, proto.TaskContext(w.id.DeviceID, env.ID), env.Sealed)
		if oerr == nil {
			oerr = json.Unmarshal(plain, &t)
		}
		if oerr != nil || t.ID != env.ID {
			log.Printf("task %s: does not decrypt for this device (%v); the server will time it out", env.ID, oerr)
			continue
		}
		w.dispatch(ctx, t)
	}
	w.wg.Wait()
	if errors.Is(err, ErrUnauthorized) {
		return err
	}
	return ctx.Err()
}

func (w *Worker) dispatch(ctx context.Context, t proto.Task) {
	if !slices.Contains(w.cfg.AllowedUsers, t.UserID) {
		log.Printf("task %s: user %s is not in allowed_users", t.ID, t.UserID)
		go w.send(t, fmt.Sprintf("User `%s` may not use device `%s`. Its owner can add that id to `allowed_users` in the worker config.", t.UserID, w.id.DeviceID))
		return
	}
	if cmd := parseCommand(t.Text); cmd.name == "status" || cmd.name == "sessions" {
		go w.send(t, w.query(t.RoomID, cmd.name)) // read-only, so it answers even while a prompt runs
		return
	}
	select {
	case w.roomQueue(ctx, t.RoomID) <- t:
	default:
		go w.send(t, fmt.Sprintf("This room already has %d tasks waiting on this device; try again later.", roomQueueSize))
	}
}

func (w *Worker) roomQueue(ctx context.Context, roomID string) chan proto.Task {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := w.rooms[roomID]
	if ch == nil {
		ch = make(chan proto.Task, roomQueueSize)
		w.rooms[roomID] = ch
		w.wg.Add(1)
		go w.roomLoop(ctx, ch)
	}
	return ch
}

// roomLoop runs one room's tasks in order, so a room's prompts never race on its session.
func (w *Worker) roomLoop(ctx context.Context, ch chan proto.Task) {
	defer w.wg.Done()
	for {
		select {
		case t := <-ch:
			if ctx.Err() == nil {
				select {
				case w.sem <- struct{}{}:
					reply := w.handle(ctx, t)
					<-w.sem
					w.send(t, reply)
					continue
				case <-ctx.Done():
				}
			}
			w.send(t, "The worker shut down before running this.")
		case <-ctx.Done():
			for {
				select {
				case t := <-ch:
					w.send(t, "The worker shut down before running this.")
				default:
					return
				}
			}
		}
	}
}

// send seals and posts a result, retrying only when the request may not have reached the server.
func (w *Worker) send(t proto.Task, text string) {
	plain, _ := json.Marshal(proto.Result{Text: text})
	sealed, err := seal.Seal(w.key, w.serverKey, proto.ResultContext(w.id.DeviceID, t.ID), plain)
	if err != nil {
		log.Printf("task %s: seal result: %v", t.ID, err)
		return
	}
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := w.client.Result(ctx, proto.Envelope{ID: t.ID, Sealed: sealed})
		cancel()
		if err == nil {
			return
		}
		var ue *url.Error
		if !errors.As(err, &ue) || attempt == 3 {
			log.Printf("task %s: result not delivered: %v", t.ID, err)
			return
		}
		time.Sleep(time.Duration(attempt) * 2 * time.Second)
	}
}

type command struct{ name, arg, prompt string }

func parseCommand(text string) command {
	word, rest := proto.CutWord(text)
	switch kw := proto.Keyword(word); kw {
	case "status", "sessions":
		if rest == "" {
			return command{name: kw}
		}
	case "new":
		return command{name: "new", prompt: rest}
	case "resume":
		ref, p := proto.CutWord(rest)
		return command{name: "resume", arg: ref, prompt: p}
	case "cd", "workdir":
		return command{name: "cd", arg: rest}
	}
	return command{name: "prompt", prompt: strings.TrimSpace(text)}
}

// handle runs a task that may change room state or start Claude; it returns the reply.
func (w *Worker) handle(ctx context.Context, t proto.Task) string {
	cmd := parseCommand(t.Text)
	switch cmd.name {
	case "cd":
		return w.chdir(t.RoomID, cmd.arg)
	case "new":
		w.mu.Lock()
		w.roomLocked(t.RoomID).SessionID = ""
		w.saveLocked(t.RoomID)
		w.mu.Unlock()
		if cmd.prompt == "" {
			return "Started a new session: the next prompt opens it."
		}
	case "resume":
		if msg, ok := w.resume(t.RoomID, cmd.arg); !ok || cmd.prompt == "" {
			return msg
		}
	}
	return w.prompt(ctx, t, cmd.prompt)
}

func (w *Worker) chdir(roomID, arg string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	rs := w.roomLocked(roomID)
	if arg == "" {
		return fmt.Sprintf("Workdir is `%s`.", tilde(rs.Workdir))
	}
	dir, err := resolveWorkdir(w.cfg.WorkdirRoots, rs.Workdir, arg)
	if err != nil {
		return "Workdir unchanged: " + err.Error()
	}
	rs.Workdir, rs.SessionID = dir, ""
	w.saveLocked(roomID)
	return fmt.Sprintf("Workdir is now `%s`; the next prompt starts a new session there.", tilde(dir))
}

func (w *Worker) resume(roomID, ref string) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rs := w.roomLocked(roomID)
	if ref == "" {
		return "Usage: `resume <n|session-id> [prompt]`; `sessions` lists this room's sessions.", false
	}
	s, ok := findSession(rs.Sessions, ref)
	if !ok {
		if !isUUID(ref) {
			return fmt.Sprintf("No session in this room matches `%s`; `sessions` lists them.", ref), false
		}
		s = Session{ID: ref, Workdir: rs.Workdir} // e.g. one started in a terminal: it runs in the current workdir
	}
	if _, err := resolveWorkdir(w.cfg.WorkdirRoots, "/", s.Workdir); err != nil {
		return "Cannot resume: " + err.Error(), false
	}
	rs.SessionID, rs.Workdir = s.ID, s.Workdir
	w.saveLocked(roomID)
	return fmt.Sprintf("Resumed session `%s` in `%s`.", short(s.ID), tilde(s.Workdir)), true
}

func (w *Worker) prompt(ctx context.Context, t proto.Task, text string) string {
	w.mu.Lock()
	rs := w.roomLocked(t.RoomID)
	run := Run{Workdir: rs.Workdir, SessionID: rs.SessionID, Resume: rs.SessionID != "", Prompt: text}
	if !run.Resume {
		run.SessionID = newUUID()
	}
	w.running[t.RoomID] = running{since: w.now(), prompt: text}
	w.mu.Unlock()

	log.Printf("task %s: running in %s, session %s (resume=%v)", t.ID, run.Workdir, short(run.SessionID), run.Resume)
	res, err := w.runner.Run(ctx, run)

	w.mu.Lock()
	delete(w.running, t.RoomID)
	if res.SessionID != "" || res.Stopped { // a stopped run still leaves a resumable transcript
		sid := cmp.Or(res.SessionID, run.SessionID)
		rs := w.roomLocked(t.RoomID)
		rs.SessionID = sid
		rs.remember(Session{ID: sid, Workdir: run.Workdir, Title: preview(text, 40), LastUsed: w.now()})
		w.saveLocked(t.RoomID)
	}
	w.mu.Unlock()
	log.Printf("task %s: finished in %s (error=%v)", t.ID, res.Duration.Round(time.Second), err != nil || res.IsError)
	return w.format(ctx, run, res, err)
}

func (w *Worker) format(ctx context.Context, run Run, res RunResult, err error) string {
	var b strings.Builder
	switch {
	case res.Stopped && ctx.Err() != nil:
		b.WriteString("**Stopped:** the worker shut down during this run.")
	case res.Stopped:
		fmt.Fprintf(&b, "**Timed out** after %s. The session keeps what was done so far; send a follow-up to continue it.", w.cfg.timeout())
	case err != nil:
		fmt.Fprintf(&b, "**Run failed:** %v", err)
	case res.IsError:
		b.WriteString("**Claude ended with an error:**\n\n" + tanka.Truncate(res.Text, w.cfg.MaxOutputChars))
	default:
		b.WriteString(tanka.Truncate(res.Text, w.cfg.MaxOutputChars))
	}
	if res.Denials > 0 {
		fmt.Fprintf(&b, "\n\n⚠️ %d tool call(s) were denied by this device's permission settings.", res.Denials)
	}
	session := "none"
	if res.SessionID != "" || res.Stopped || run.Resume {
		session = short(cmp.Or(res.SessionID, run.SessionID))
	}
	fmt.Fprintf(&b, "\n\n`%s` · `%s` · session `%s`", w.id.DeviceID, tilde(run.Workdir), session)
	if res.Turns > 0 {
		fmt.Fprintf(&b, " · %d turns", res.Turns)
	}
	fmt.Fprintf(&b, " · %s", res.Duration.Round(time.Second))
	if res.CostUSD > 0 {
		fmt.Fprintf(&b, " · $%.2f", res.CostUSD)
	}
	return b.String()
}

func (w *Worker) query(roomID, name string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	rs := w.roomLocked(roomID)
	var b strings.Builder
	if name == "sessions" {
		if len(rs.Sessions) == 0 {
			return "No sessions in this room yet."
		}
		b.WriteString("**Recent sessions** (`resume <n>` switches to one)\n")
		for i, s := range rs.Sessions {
			mark := ""
			if s.ID == rs.SessionID {
				mark = " ← current"
			}
			fmt.Fprintf(&b, "%d. `%s` · `%s` · %s · %s%s\n", i+1, short(s.ID), tilde(s.Workdir), s.LastUsed.Local().Format("01-02 15:04"), s.Title, mark)
		}
		return strings.TrimRight(b.String(), "\n")
	}
	fmt.Fprintf(&b, "- **Device** `%s` (%s)\n", w.id.DeviceID, w.cfg.Name)
	fmt.Fprintf(&b, "- **Workdir** `%s`\n", tilde(rs.Workdir))
	if rs.SessionID == "" {
		b.WriteString("- **Session** none yet: the next prompt starts one\n")
	} else {
		fmt.Fprintf(&b, "- **Session** `%s`: prompts continue it, `new` starts another\n", short(rs.SessionID))
	}
	if r, ok := w.running[roomID]; ok {
		fmt.Fprintf(&b, "- **Running** for %s: %s\n", w.now().Sub(r.since).Round(time.Second), preview(r.prompt, 60))
	} else {
		b.WriteString("- **Running** nothing\n")
	}
	if n := len(w.rooms[roomID]); n > 0 {
		fmt.Fprintf(&b, "- **Queued** %d\n", n)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (w *Worker) roomLocked(roomID string) *RoomState {
	rs := w.state[roomID]
	if rs == nil {
		rs = &RoomState{Workdir: w.cfg.DefaultWorkdir}
		w.state[roomID] = rs
	}
	return rs
}

func (w *Worker) saveLocked(roomID string) {
	if err := w.store.put("room/"+roomID, w.state[roomID]); err != nil {
		log.Printf("save room %s: %v", roomID, err)
	}
}

// remember moves s to the front of the history, keeping the title it first got.
func (rs *RoomState) remember(s Session) {
	out := []Session{s}
	for _, x := range rs.Sessions {
		if x.ID == s.ID {
			if x.Title != "" {
				out[0].Title = x.Title
			}
			continue
		}
		if len(out) < maxSessions {
			out = append(out, x)
		}
	}
	rs.Sessions = out
}

// findSession matches a 1-based position in the history or a unique id prefix (4+ chars).
func findSession(ss []Session, ref string) (Session, bool) {
	if n, err := strconv.Atoi(ref); err == nil {
		if n >= 1 && n <= len(ss) {
			return ss[n-1], true
		}
		return Session{}, false
	}
	var hit []Session
	for _, s := range ss {
		if len(ref) >= 4 && strings.HasPrefix(s.ID, ref) {
			hit = append(hit, s)
		}
	}
	if len(hit) == 1 {
		return hit[0], true
	}
	return Session{}, false
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return false
		}
	}
	return true
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func preview(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
