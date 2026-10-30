package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

// SentryHook accepts webhooks from a Sentry internal integration at POST /sentry/{bot} and
// turns each issue alert into a triggered task for the device paired with RoomID.
type SentryHook struct {
	// ClientSecret verifies Sentry-Hook-Signature; AGENTLINK_SENTRY_SECRET_<BOT> overrides it.
	ClientSecret string `json:"client_secret"`
	RoomID       string `json:"room_id"` // the task runs for this room's device and posts its result there
	Trigger      string `json:"trigger"` // trigger name on the task (the worker must know it); default "sentry"
	// Prompt is the task text. {issue_id}, {title}, {url}, {rule} and {project} are filled in
	// from the alert; treat them as untrusted in whatever the prompt asks for.
	Prompt string `json:"prompt"`
}

// sentryAlert is the part of an event_alert payload the prompt uses.
type sentryAlert struct {
	Data struct {
		Event struct {
			IssueID json.Number `json:"issue_id"`
			Title   string      `json:"title"`
			WebURL  string      `json:"web_url"`
			Project json.Number `json:"project"`
		} `json:"event"`
		TriggeredRule string `json:"triggered_rule"`
	} `json:"data"`
}

func verifySentry(secret string, body []byte, sig string) bool {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	want := hex.EncodeToString(m.Sum(nil))
	return secret != "" && hmac.Equal([]byte(want), []byte(strings.ToLower(strings.TrimSpace(sig))))
}

func (s *Server) handleSentry(w http.ResponseWriter, r *http.Request) {
	bot := s.bots[r.PathValue("bot")]
	if bot == nil || bot.Sentry == nil {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if !verifySentry(bot.Sentry.ClientSecret, body, r.Header.Get("Sentry-Hook-Signature")) {
		log.Printf("bot %s: sentry webhook rejected: bad signature", bot.Name)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	resource := r.Header.Get("Sentry-Hook-Resource")
	if resource != "event_alert" {
		log.Printf("bot %s: sentry webhook %q ignored", bot.Name, resource)
		w.WriteHeader(http.StatusOK)
		return
	}
	if id := r.Header.Get("Request-ID"); id != "" {
		first, err := s.firstSeen("sentry/" + bot.Name + "/" + id)
		if err != nil {
			http.Error(w, "storage error", http.StatusInternalServerError)
			return
		}
		if !first {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	var a sentryAlert
	if err := json.Unmarshal(body, &a); err != nil || a.Data.Event.IssueID == "" {
		http.Error(w, "not an issue alert", http.StatusBadRequest)
		return
	}
	ev := a.Data.Event
	text := strings.NewReplacer(
		"{issue_id}", ev.IssueID.String(),
		"{title}", oneLine(ev.Title, 200),
		"{url}", ev.WebURL,
		"{rule}", oneLine(a.Data.TriggeredRule, 120),
		"{project}", ev.Project.String(),
	).Replace(bot.Sentry.Prompt)
	h := bot.Sentry
	id, err := s.trigger(bot, h.RoomID, h.Trigger, "sentry:"+ev.IssueID.String(), text)
	if err != nil {
		log.Printf("bot %s: sentry alert for issue %s not queued: %v", bot.Name, ev.IssueID, err)
		http.Error(w, "not queued", http.StatusServiceUnavailable)
		return
	}
	log.Printf("task %s queued bot=%s room=%s trigger=%s issue=%s", id, bot.Name, h.RoomID, h.Trigger, ev.IssueID)
	w.WriteHeader(http.StatusOK)
}

// oneLine flattens untrusted text so it cannot add lines to a prompt template.
func oneLine(s string, n int) string {
	r := []rune(strings.Join(strings.Fields(s), " "))
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return string(r)
}
