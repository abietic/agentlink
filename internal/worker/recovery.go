package worker

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"agentlink/internal/proto"
)

// Recovery is what the worker does when a task's run ends early. Config.Recovery holds one
// per task source: "trigger:<name>" for one trigger, "trigger" for the others. A source
// without one keeps the plain behaviour: the failure is the task's reply and nothing runs
// again. Tasks people send ("mention") cannot have one yet; the ledger already names each
// entry's source so they can be added without changing what is stored.
//
// A task with a Recovery and a key is kept in a ledger, in the worker's encrypted store, from the
// moment it is admitted until it has a final reply, so it also survives the worker being
// killed. A retry is a new task for the same key, asked of the server like a trigger
// command's items are, so its reply reaches the room the usual way.
type Recovery struct {
	// On lists the ways a run can end that are worth another run: "shutdown" (the worker
	// stopped), "stall" (stall_sec passed without activity), "timeout" (timeout_sec ran
	// out), "error" (the harness failed or ended with an error) and "limit" (a usage limit;
	// the next run waits until it resets).
	On []string `json:"on"`
	// Auth waits out a harness that cannot sign in (an expired or revoked login or token):
	// such a run does not count against MaxAttempts, keeps the session it was resuming, and
	// the task runs again every RetryAfterSec until GiveUpAfterSec. The room is told once,
	// when the harness first fails this way. Default true; false treats it as "error".
	Auth *bool `json:"auth,omitempty"`
	// MaxAttempts is how many runs a task gets in all; default 2. A run that never started
	// because of a usage limit does not count.
	MaxAttempts int `json:"max_attempts"`
	// RetryAfterSec is the wait before the next run; default 300. A shutdown is retried as
	// soon as the worker is back, and a usage limit when it resets.
	RetryAfterSec int `json:"retry_after_sec"`
	// Resume continues the session the last run left instead of starting over, with
	// ResumePrompt as the prompt. If that session cannot be continued, the task starts over.
	Resume       bool   `json:"resume"`
	ResumePrompt string `json:"resume_prompt"`
	// Quiet posts nothing for a run that will be retried, so the room sees only the final
	// reply. Without it each early end is posted, with a line saying when the next run is.
	Quiet bool `json:"quiet"`
	// AnnounceLimit keeps, under Quiet, the one line a harness posts when it first reaches a
	// usage limit (which limit and when it resets, once per limit), and the one line saying
	// it could not sign in (once until a run on it finishes again).
	AnnounceLimit bool `json:"announce_limit"`
	// NoticeAfterSec posts one line once a task that had a run end early has gone this long
	// without a final reply; 0 never does.
	NoticeAfterSec int `json:"notice_after_sec"`
	// GiveUpAfterSec ends the retries this long after the task first arrived; default 86400.
	GiveUpAfterSec int `json:"give_up_after_sec"`
}

const (
	endShutdown = "shutdown"
	endStall    = "stall"
	endTimeout  = "timeout"
	endError    = "error"
	endLimit    = "limit"
	endAuth     = "auth"

	defaultResumePrompt = "Your previous run on this task was interrupted before it finished. Continue from where it stopped, without repeating work that is already done, and end with the complete answer to the original task."
)

var endText = map[string]string{
	endShutdown: "was cut off when the worker stopped",
	endStall:    "stalled",
	endTimeout:  "ran out of time",
	endError:    "ended with an error",
	endLimit:    "hit a usage limit",
	endAuth:     "could not sign in to its harness",
}

// endOf is how a harness run ended: "" if it finished, else one of the end* kinds. A run the
// context stopped is a shutdown even if it also printed something.
func endOf(ctx context.Context, res RunResult, err error) string {
	switch {
	case res.Stopped && ctx.Err() != nil:
		return endShutdown
	case res.Stalled:
		return endStall
	case res.Stopped:
		return endTimeout
	case authFailed(res, err):
		return endAuth
	case err != nil || res.IsError:
		return endError
	}
	return ""
}

// authSigns are what a failed run says when the harness could not sign in: the CLI's
// "Not logged in · Please run /login" and "Invalid API key · Please run /login", and the API's
// 401 for an expired, revoked or invalid OAuth token.
var authSigns = []string{
	"please run /login",
	"not logged in",
	"authentication_failed",
	"authentication_error",
	"oauth access token is invalid",
	"oauth token has expired",
	"oauth token revoked",
	"invalid api key",
	"api error: 401",
}

// authFailed reports whether a failed run failed to sign in. The runner says so itself
// (res.AuthFailed); the texts cover runners from before it did, and other harnesses. Only a
// failed run is read, so an answer that is about a 401 is not taken for one.
func authFailed(res RunResult, err error) bool {
	if res.AuthFailed {
		return true
	}
	if err == nil && !res.IsError {
		return false
	}
	text := res.Text
	if err != nil {
		text += "\n" + err.Error()
	}
	text = strings.ToLower(text)
	return slices.ContainsFunc(authSigns, func(s string) bool { return strings.Contains(text, s) })
}

// waitsOnAuth: see Recovery.Auth.
func (r *Recovery) waitsOnAuth() bool { return r.Auth == nil || *r.Auth }

// retries reports whether a run that ended with end runs again under r.
func (r *Recovery) retries(end string) bool {
	if end == endAuth && r.waitsOnAuth() {
		return true
	}
	return slices.Contains(r.On, end)
}

// How often the ledger is checked, and how long a retry the server was asked for may take
// to come back before it is asked for again.
var (
	recoveryTick   = 15 * time.Second
	recoveryResend = 5 * time.Minute
)

func (c *Config) checkRecovery() error {
	for src, r := range c.Recovery {
		name, isTrigger := strings.CutPrefix(src, "trigger:")
		switch {
		case src == "mention":
			return fmt.Errorf("recovery %q: tasks people send cannot be retried yet; only \"trigger\" and \"trigger:<name>\"", src)
		case src != "trigger" && !isTrigger:
			return fmt.Errorf("recovery %q: the source must be \"trigger\" or \"trigger:<name>\"", src)
		case isTrigger && !slices.ContainsFunc(c.Triggers, func(t Trigger) bool { return t.Name == name }):
			return fmt.Errorf("recovery %q: no trigger has that name", src)
		}
		for _, k := range r.On {
			if _, ok := endText[k]; !ok || k == endAuth {
				return fmt.Errorf("recovery %q: on: unknown %q; use shutdown, stall, timeout, error, limit (sign-in failures have their own key, \"auth\")", src, k)
			}
		}
		r.MaxAttempts = defaultInt(r.MaxAttempts, 2)
		r.RetryAfterSec = defaultInt(r.RetryAfterSec, 300)
		r.GiveUpAfterSec = defaultInt(r.GiveUpAfterSec, 86400)
		r.ResumePrompt = cmp.Or(r.ResumePrompt, defaultResumePrompt)
		c.Recovery[src] = r
	}
	return nil
}

// recovery is the Recovery for t's source, or nil.
func (w *Worker) recovery(trigger string) *Recovery {
	if trigger == "" {
		return nil
	}
	for _, src := range []string{"trigger:" + trigger, "trigger"} {
		if r, ok := w.cfg.Recovery[src]; ok {
			return &r
		}
	}
	return nil
}

// pendingRun is a ledger entry: a task that has no final reply yet.
type pendingRun struct {
	Source   string    `json:"source"` // "trigger"
	Trigger  string    `json:"trigger"`
	Key      string    `json:"key"`
	RoomID   string    `json:"room_id"`
	Text     string    `json:"text"`
	Session  string    `json:"session,omitempty"` // what the last run left, to resume
	Harness  string    `json:"harness,omitempty"`
	Attempts int       `json:"attempts"` // runs so far
	First    time.Time `json:"first"`
	Why      string    `json:"why,omitempty"` // how the last run ended
	// State is "queued" or "running" (a task for it is on this worker), "waiting" (the next
	// run is asked for at Next), "failed" (the final reply is still to be asked for), or
	// "sent" (the server was asked at Next for a task that will carry Want).
	State string    `json:"state"`
	Next  time.Time `json:"next,omitempty"`
	Want  string    `json:"want,omitempty"` // "run" or "final"
	// NoticeOut: the server was asked for a task to carry the notice. Noticed: it was posted.
	NoticeOut bool `json:"notice_out,omitempty"`
	Noticed   bool `json:"noticed,omitempty"`
}

// pendingKey names t's ledger entry. A task without a key has none: its retry could not be
// told from any other task of the trigger, so it keeps the plain behaviour.
func pendingKey(t proto.Task) string {
	if t.Key == "" {
		return ""
	}
	return t.Trigger + "/" + t.Key
}

func (w *Worker) savePendingLocked() {
	if err := w.store.put("pending", w.pending); err != nil {
		log.Printf("save pending: %v", err)
	}
}

// outcome is how a prompt ended, for recovery.
type outcome struct {
	end      string    // "" if it finished, else one of the end* kinds
	session  string    // the session a run left, if any
	harness  string    // the harness that session is on
	ran      bool      // a harness was started
	noResult bool      // the harness failed without a result, e.g. it could not resume
	retryAt  time.Time // with endLimit: when the earliest limit resets
	notice   string    // with endLimit: what the room is told about the limit itself, once per limit
}

// claim decides what a triggered task with a Recovery is. It returns a reply to send
// without running anything (ok false), or lets the task be admitted. With bypass the
// trigger's dedupe window does not apply: the task is a retry.
func (w *Worker) claim(t proto.Task) (reply string, bypass, ok bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := pendingKey(t)
	e := w.pending[k]
	switch {
	case e == nil:
		return "", false, true, nil // admit creates the entry once dedupe has passed
	case e.NoticeOut:
		e.NoticeOut, e.Noticed = false, true
		w.savePendingLocked()
		return w.noticeLocked(e), false, false, nil
	case e.State == "sent" && e.Want == "final":
		delete(w.pending, k)
		w.savePendingLocked()
		return finalText(e), false, false, nil
	case e.State == "queued" || e.State == "running":
		return "", false, false, fmt.Errorf("key %s is already %s here", t.Key, e.State)
	}
	e.State = "queued"
	w.savePendingLocked()
	return "", true, true, nil
}

// track adds an admitted task to the ledger.
func (w *Worker) trackLocked(t proto.Task) {
	k := pendingKey(t)
	if k != "" && w.pending[k] == nil {
		w.pending[k] = &pendingRun{Source: "trigger", Trigger: t.Trigger, Key: t.Key, RoomID: t.RoomID, Text: t.Text, First: w.now(), State: "queued"}
		w.savePendingLocked()
	}
}

func (w *Worker) noticeLocked(e *pendingRun) string {
	next := "It is running again now."
	if e.State == "waiting" || e.State == "sent" && e.Want == "run" {
		next = "Its next run is due " + e.Next.Local().Format("15:04") + "."
	}
	return fmt.Sprintf("⏳ `%s` task `%s` has no result yet: a run %s. %s", e.Trigger, e.Key, endText[e.Why], next)
}

func finalText(e *pendingRun) string {
	if e.Why == endAuth {
		s := fmt.Sprintf("**Gave up** on `%s` task `%s`: its harness could not sign in until the retries ran out (give_up_after_sec), after %d counted run(s). Sign the harness in again on this device.", e.Trigger, e.Key, e.Attempts)
		if e.Session != "" {
			s += fmt.Sprintf("\n\nsession `%s` (to follow up, @ me and start with `%s`)", short(e.Session), short(e.Session))
		}
		return s
	}
	s := fmt.Sprintf("**Gave up** on `%s` task `%s` after %d run(s): the last one %s.", e.Trigger, e.Key, e.Attempts, cmp.Or(endText[e.Why], "did not finish"))
	if e.Session != "" {
		s += fmt.Sprintf("\n\nsession `%s` (to follow up, @ me and start with `%s`)", short(e.Session), short(e.Session))
	}
	return s
}

// unrun is the reply for a task the worker shut down before running. One that will run
// again when the worker is back posts nothing.
func (w *Worker) unrun(t proto.Task) string {
	const plain = "The worker shut down before running this."
	pol := w.recovery(t.Trigger)
	if pol == nil {
		return plain
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	k := pendingKey(t)
	if w.pending[k] == nil {
		return plain
	}
	if slices.Contains(pol.On, endShutdown) {
		return "" // still "queued" in the ledger: adopt picks it up
	}
	delete(w.pending, k)
	w.savePendingLocked()
	return plain
}

// triggered runs a triggered task that has a Recovery and settles its ledger entry.
func (w *Worker) triggered(ctx context.Context, t proto.Task, tr *Trigger, pol *Recovery) string {
	k := pendingKey(t)
	fresh := Run{Harness: w.cfg.Harness, Workdir: tr.Workdir, Prompt: t.Text}
	run := fresh
	w.mu.Lock()
	e := w.pending[k]
	if e == nil { // not admitted through dispatch
		w.mu.Unlock()
		return w.prompt(ctx, t, fresh, false)
	}
	e.State = "running"
	if pol.Resume && e.Session != "" && w.runners[e.Harness] != nil {
		run = Run{Harness: e.Harness, Workdir: tr.Workdir, SessionID: e.Session, Resume: true, Prompt: pol.ResumePrompt}
	}
	w.savePendingLocked()
	w.mu.Unlock()

	reply, out := w.run(ctx, t, run, false)
	if out.end == endAuth && !pol.waitsOnAuth() {
		out.end = endError
	}
	auth := out.end == endAuth
	// A run that could not sign in says nothing about the session: it is resumed next time.
	if run.Resume && out.noResult && !auth {
		log.Printf("task %s: session %s could not be continued; starting over", t.ID, short(run.SessionID))
		reply, out = w.run(ctx, t, fresh, false)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if (out.ran || out.end != endLimit) && !auth {
		e.Attempts++
	}
	if out.session != "" {
		e.Session, e.Harness = out.session, out.harness
	}
	age := w.now().Sub(e.First)
	if out.end == "" || !pol.retries(out.end) || e.Attempts >= pol.MaxAttempts || age >= time.Duration(pol.GiveUpAfterSec)*time.Second {
		delete(w.pending, k)
		w.savePendingLocked()
		if auth {
			reply += "\n\n⚠️ The harness could not sign in until the retries ran out (give_up_after_sec); no more runs follow on their own. Sign it in again on this device."
		} else if out.end != "" && e.Attempts > 1 {
			reply += fmt.Sprintf("\n\n⚠️ This was run %d of this task; no more follow on their own.", e.Attempts)
		}
		return reply
	}
	e.State, e.Why = "waiting", out.end
	switch out.end {
	case endShutdown:
		e.Next = w.now()
	case endLimit:
		e.Next = out.retryAt
	default:
		e.Next = w.now().Add(time.Duration(pol.RetryAfterSec) * time.Second)
	}
	w.savePendingLocked()
	log.Printf("task %s: run %d of %s %s %s; next run at %s", t.ID, e.Attempts, t.Trigger, t.Key, endText[out.end], e.Next.Format(time.RFC3339))
	if auth {
		// Said once per harness until a run on it finishes again, not once per retry.
		if w.authDown[out.harness] {
			return ""
		}
		w.authDown[out.harness] = true
		notice := fmt.Sprintf("🔑 `%s` could not sign in on this device. Its triggered tasks wait and run again every %s until their give_up_after_sec (this one next at %s); these runs do not count against max_attempts. Said only this once.",
			out.harness, time.Duration(pol.RetryAfterSec)*time.Second, e.Next.Local().Format("15:04"))
		switch {
		case !pol.Quiet:
			return joinNotes([]string{reply}, notice)
		case pol.AnnounceLimit:
			return notice
		}
		return ""
	}
	if pol.Quiet {
		if pol.AnnounceLimit {
			return out.notice
		}
		return ""
	}
	return joinNotes([]string{reply}, fmt.Sprintf("↻ It runs again on its own at %s (run %d of at most %d).", e.Next.Local().Format("15:04"), e.Attempts+1, pol.MaxAttempts))
}

// adopt takes over the entries a previous worker process left queued or running.
func (w *Worker) adopt() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, e := range w.pending {
		if e.State != "queued" && e.State != "running" {
			continue
		}
		pol := w.recovery(e.Trigger)
		if pol == nil {
			delete(w.pending, k)
			continue
		}
		if e.State == "running" {
			e.Attempts++
			e.Why = endShutdown
		}
		e.State, e.Next = "waiting", w.now()
		if !slices.Contains(pol.On, endShutdown) || e.Attempts >= pol.MaxAttempts {
			e.State, e.Why = "failed", endShutdown
		}
		log.Printf("recovery: %s was left unfinished by the last worker process: %s", k, e.State)
	}
	w.savePendingLocked()
}

// recoverLoop asks the server for the tasks the ledger is due: retries, notices and final
// replies.
func (w *Worker) recoverLoop(ctx context.Context) {
	defer w.wg.Done()
	tick := time.NewTicker(recoveryTick)
	defer tick.Stop()
	for {
		w.recoverOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (w *Worker) recoverOnce(ctx context.Context) {
	type ask struct {
		k string
		e pendingRun
	}
	var asks []ask
	w.mu.Lock()
	now := w.now()
	for k, e := range w.pending {
		pol := w.recovery(e.Trigger)
		if pol == nil {
			log.Printf("recovery: dropping %s: its source has no recovery config any more", k)
			delete(w.pending, k)
			continue
		}
		if e.State == "sent" && now.Sub(e.Next) > recoveryResend { // the task never came back
			e.State = map[string]string{"run": "waiting", "final": "failed"}[e.Want]
		}
		if e.State == "waiting" && now.Sub(e.First) >= time.Duration(pol.GiveUpAfterSec)*time.Second {
			e.State = "failed"
		}
		switch {
		case e.State == "failed":
			e.State, e.Want, e.Next = "sent", "final", now
		case e.State == "waiting" && !now.Before(e.Next):
			e.State, e.Want, e.Next = "sent", "run", now
		case pol.NoticeAfterSec > 0 && e.Why != "" && !e.Noticed && !e.NoticeOut && now.Sub(e.First) >= time.Duration(pol.NoticeAfterSec)*time.Second:
			e.NoticeOut = true
		default:
			continue
		}
		asks = append(asks, ask{k, *e})
	}
	w.savePendingLocked()
	w.mu.Unlock()
	for _, a := range asks {
		// The text is the task's own, so a retry whose ledger entry is gone by the time it
		// comes back is still the task, never a stray prompt.
		if err := w.submit(ctx, a.e.RoomID, a.e.Trigger, a.e.Key, a.e.Text); err != nil {
			log.Printf("recovery: %s: %v (asking again later)", a.k, err)
			continue
		}
		log.Printf("recovery: asked the server for %s (state=%s want=%s notice=%v)", a.k, a.e.State, a.e.Want, a.e.NoticeOut)
	}
}
