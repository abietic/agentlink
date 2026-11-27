package worker

import (
	"fmt"
	"regexp"
	"strings"
)

// redacted replaces every secret this package masks before text leaves the worker.
const redacted = "[REDACTED]"

// The patterns redactSecrets masks. They are compiled once; a prompt-injected run must not
// be able to post a credential into the room, and matching after the fact would cost a
// regexp compile per reply.
var (
	awsKeyRe    = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	pemKeyRe    = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	pemOpenRe   = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*\z`)
	githubRe    = regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}\b`)
	githubPatRe = regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{22,}\b`)
	gitlabRe    = regexp.MustCompile(`\bglpat-[A-Za-z0-9_\-]{20,}`)
	skKeyRe     = regexp.MustCompile(`\bsk-[A-Za-z0-9_\-]{20,}`)
	slackRe     = regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9\-]{10,}`)
	sentryRe    = regexp.MustCompile(`\bsntr[ysu]s?_[A-Za-z0-9+/=_\-]{20,}`)
	googleRe    = regexp.MustCompile(`\bAIza[0-9A-Za-z_\-]{35}\b`)
	jwtRe       = regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}`)

	// Bearer keeps the scheme word and its spacing, masking only the token.
	bearerRe = regexp.MustCompile(`(?i)\bBearer(\s+)([A-Za-z0-9\-._~+/]{16,}=*)`)

	// key=value / key: value keeps the key, any quotes, and the separator; only the value
	// group (6) is masked. Longest key names come first so an alternation cannot stop at
	// their shorter suffix.
	kvSecretRe = regexp.MustCompile(`(?i)(["']?)(\b(?:aws_secret_access_key|secret_access_key|access_token|refresh_token|client_secret|private_key|access_key|authorization|api_key|apikey|api-key|password|passwd|secret|token)\b)(["']?)(\s*[=:]\s*)(["']?)([^\s"',;]{8,})`)

	// URL userinfo keeps scheme, user and host; only the password group (3) is masked.
	urlUserRe = regexp.MustCompile(`\b(https?|postgres(?:ql)?|mysql|redis|amqp|mongodb(?:\+srv)?)://([^\s/@:]+):([^\s/@]+)@`)

	// URL query parameters keep the name and separator; only the value group (2) is masked.
	urlQueryRe = regexp.MustCompile(`(?i)([?&](?:token|access_token|key|api_key|sig|signature|secret|password)=)([^\s&#"']{8,})`)
)

// redactSecrets masks every secret-like string in s and reports how many it masked. It never
// logs or returns the secret itself. Patterns run from the most specific (a whole PEM block,
// a URL's password) to bare tokens, so a value masked by an earlier pattern is not counted
// again.
func redactSecrets(s string) (string, int) {
	n := 0
	var c int
	s, c = replaceAllCount(pemKeyRe, s, redacted)
	n += c
	s, c = replaceAllCount(pemOpenRe, s, redacted)
	n += c
	s, c = maskGroup(urlUserRe, s, 3, false)
	n += c
	s, c = maskGroup(kvSecretRe, s, 6, true)
	n += c
	s, c = maskGroup(urlQueryRe, s, 2, true)
	n += c
	s, c = maskGroup(bearerRe, s, 2, false)
	n += c
	for _, re := range []*regexp.Regexp{awsKeyRe, githubRe, githubPatRe, gitlabRe, skKeyRe, slackRe, sentryRe, googleRe, jwtRe} {
		s, c = replaceAllCount(re, s, redacted)
		n += c
	}
	return s, n
}

// redactionNote is the line appended when redactSecrets masked anything.
func redactionNote(n int) string {
	return fmt.Sprintf("⚠️ 已遮盖 %d 处疑似密钥", n)
}

// withRedactionNote appends the note on a line of its own.
func withRedactionNote(text string, n int) string {
	if n == 0 {
		return text
	}
	if text == "" {
		return redactionNote(n)
	}
	return text + "\n\n" + redactionNote(n)
}

// replaceAllCount replaces every match of re in s with repl and returns the count.
func replaceAllCount(re *regexp.Regexp, s, repl string) (string, int) {
	ms := re.FindAllStringIndex(s, -1)
	if len(ms) == 0 {
		return s, 0
	}
	var b strings.Builder
	b.Grow(len(s))
	last := 0
	for _, m := range ms {
		b.WriteString(s[last:m[0]])
		b.WriteString(repl)
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String(), len(ms)
}

// maskGroup replaces one capturing group of every match of re, keeping the rest of the
// match. skipNumbers leaves values made only of digits alone (a timestamp is not a secret).
// Values already masked are skipped, so overlapping patterns count a secret once.
func maskGroup(re *regexp.Regexp, s string, group int, skipNumbers bool) (string, int) {
	ms := re.FindAllStringSubmatchIndex(s, -1)
	if len(ms) == 0 {
		return s, 0
	}
	var b strings.Builder
	b.Grow(len(s))
	last, n := 0, 0
	for _, m := range ms {
		vs, ve := m[2*group], m[2*group+1]
		if vs < 0 || ve < 0 {
			continue
		}
		val := s[vs:ve]
		if val == redacted || (skipNumbers && isDigits(val)) {
			continue
		}
		b.WriteString(s[last:vs])
		b.WriteString(redacted)
		last = ve
		n++
	}
	if n == 0 {
		return s, 0
	}
	b.WriteString(s[last:])
	return b.String(), n
}

// isDigits reports whether s is non-empty and all ASCII digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
