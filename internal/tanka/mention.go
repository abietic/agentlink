package tanka

import (
	"strings"
	"unicode"
)

// The server and a verifying worker both take a prompt out of a mention's text with these,
// so the worker can derive it from the signed text exactly as the server would, instead of
// trusting the server's copy.

// PromptText is the prompt in a mention's text: after the first "@<name>" for a bot sender
// (see AfterMention), otherwise the text with its leading mention removed (StripMention).
func PromptText(text string, names []string, fromBot bool) string {
	if fromBot {
		return AfterMention(text, names)
	}
	return StripMention(text, names)
}

// AfterMention returns what follows the first "@<configured name>" in a bot's message, so the
// bot handing work on can write to its own reader first. Tanka delivers every mention as
// "@Display Name". Without one it falls back to StripMention.
func AfterMention(text string, names []string) string {
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
		return StripMention(text, names)
	}
	return strings.TrimSpace(rest)
}

// StripMention removes the leading bot mention: "<@ID>", "@<configured name>", or else "@word".
func StripMention(text string, names []string) string {
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
		if i := strings.IndexFunc(t, unicode.IsSpace); i >= 0 {
			return strings.TrimSpace(t[i:])
		}
		return ""
	}
	return t
}
