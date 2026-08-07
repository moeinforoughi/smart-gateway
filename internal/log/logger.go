// Package log provides the structured JSON logger used across Smart Gateway.
//
// It wraps log/slog with:
//   - mandatory JSON output (FR-1300, NFR-161)
//   - secret masking so credentials never reach the logs (NFR-142)
//   - correlation identifier support for request tracing (NFR-162)
//   - duration helpers for long-running operations (NFR-163)
package log

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
)

// Redacted marks a value that must never appear verbatim in logs.
// String() renders it as "***"; LogValuer guarantees slog never sees
// the raw content (NFR-142).
type Redacted string

// String implements fmt.Stringer, always returning the masked form.
func (Redacted) String() string { return maskValue }

// LogValue implements slog.LogValuer so the raw value is masked even when
// passed through slog.Any.
func (Redacted) LogValue() slog.Value { return slog.StringValue(maskValue) }

const maskValue = "***"

// Logger is the gateway logging facade. It is safe for concurrent use.
// A Logger must not be copied after first use.
type Logger struct {
	base *slog.Logger
}

// New creates a JSON logger writing to out.
// Level filtering applies to the underlying handler.
func New(out io.Writer, level slog.Level) *Logger {
	if out == nil {
		out = os.Stderr
	}
	handler := &maskingHandler{
		next: slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}),
	}
	return &Logger{base: slog.New(handler)}
}

// LevelFromString parses a level name into a slog.Level.
// Supported: debug, info, warn, error. Defaults to info.
func LevelFromString(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// With returns a child logger carrying the given structured attributes.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{base: l.base.With(args...)}
}

// Debug logs at debug level.
func (l *Logger) Debug(msg string, args ...any) { l.base.Debug(msg, args...) }

// Info logs at info level.
func (l *Logger) Info(msg string, args ...any) { l.base.Info(msg, args...) }

// Warn logs at warn level.
func (l *Logger) Warn(msg string, args ...any) { l.base.Warn(msg, args...) }

// Error logs at error level.
func (l *Logger) Error(msg string, args ...any) { l.base.Error(msg, args...) }

// WithContext returns a child logger that enriches every entry with
// correlation identifiers found in ctx (see WithRequestID).
func (l *Logger) WithContext(ctx context.Context) *Logger {
	if ctx == nil {
		return l
	}
	if id, ok := ctx.Value(ctxKeyRequestID{}).(string); ok && id != "" {
		return l.With(slog.String("request_id", id))
	}
	return l
}

type ctxKeyRequestID struct{}

// WithRequestID returns a context carrying requestID for correlation.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, requestID)
}

// RequestIDFromContext returns the correlation id stored in ctx, if any.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(ctxKeyRequestID{}).(string)
	return id
}

// NewRequestID generates a short random correlation identifier.
// Not cryptographically strong; sufficient for log correlation.
func NewRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}

// maskingHandler sanitizes sensitive attributes before delegating to slog.
type maskingHandler struct {
	next slog.Handler
}

func (h *maskingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *maskingHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, sanitizeAttr(a))
		return true
	})
	nr.AddAttrs(attrs...)
	return h.next.Handle(ctx, nr)
}

func (h *maskingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		clean = append(clean, sanitizeAttr(a))
	}
	return &maskingHandler{next: h.next.WithAttrs(clean)}
}

func (h *maskingHandler) WithGroup(name string) slog.Handler {
	return &maskingHandler{next: h.next.WithGroup(name)}
}

func sanitizeAttr(a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		children := a.Value.Group()
		clean := make([]slog.Attr, 0, len(children))
		for _, c := range children {
			clean = append(clean, sanitizeAttr(c))
		}
		args := make([]any, 0, len(clean))
		for _, c := range clean {
			args = append(args, c)
		}
		return slog.Group(a.Key, args...)
	}
	if isSensitiveKey(a.Key) {
		return slog.String(a.Key, maskValue)
	}
	if a.Value.Kind() == slog.KindString {
		if cleaned, ok := maskURLUserinfo(a.Value.String()); ok {
			return slog.String(a.Key, cleaned)
		}
	}
	return a
}

// maskURLUserinfo redacts the password embedded in a URL's userinfo
// (http://user:pass@host) so credentials cannot leak under a
// non-sensitive key (NFR-142).
func maskURLUserinfo(s string) (string, bool) {
	if !strings.Contains(s, "@") {
		return s, false
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.User == nil {
		return s, false
	}
	if _, has := u.User.Password(); !has {
		return s, false
	}
	u.User = url.UserPassword(u.User.Username(), maskValue)
	return u.String(), true
}

var sensitiveKeyWords = []string{
	"password", "passwd", "secret", "token", "authorization", "cookie",
	"private_key", "api_key", "uuid", "short_id", "credential", "auth", "sn",
}

func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	for _, w := range sensitiveKeyWords {
		if strings.Contains(lower, w) {
			return true
		}
	}
	return false
}
