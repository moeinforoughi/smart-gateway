package log

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerMasksSecrets(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelDebug)
	l.Info("auth attempt", "password", "hunter2", "node_uuid", "abc123", "ok", true)

	out := buf.String()
	if strings.Contains(out, "hunter2") || strings.Contains(out, "abc123") {
		t.Fatalf("secrets leaked into log: %s", out)
	}
	if !strings.Contains(out, `"***"`) {
		t.Fatalf("expected masked value in log: %s", out)
	}
	if !strings.Contains(out, `"ok":true`) {
		t.Fatalf("non-sensitive attributes must pass through: %s", out)
	}
}

func TestLoggerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo)
	l.Debug("hidden", "a", 1)
	l.Info("shown", "b", 2)
	if strings.Contains(buf.String(), "hidden") {
		t.Fatal("debug line must be filtered at info level")
	}
	if !strings.Contains(buf.String(), "shown") {
		t.Fatal("info line must pass at info level")
	}
}

func TestLoggerJSON(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo)
	l.Info("hello", "node", "n1")
	if !strings.HasPrefix(buf.String(), "{") {
		t.Fatalf("expected JSON object, got: %s", buf.String())
	}
}

func TestWithContextCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo)
	ctx := WithRequestID(context.Background(), "req-123")
	l.WithContext(ctx).Info("served")
	if !strings.Contains(buf.String(), `"request_id":"req-123"`) {
		t.Fatalf("expected correlation id in log: %s", buf.String())
	}
}

func TestRedactedValue(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo)
	l.Info("sub", "url", Redacted("http://user:pass@host"))
	if strings.Contains(buf.String(), "user:pass") {
		t.Fatalf("redacted value leaked: %s", buf.String())
	}
	if !strings.Contains(buf.String(), maskValue) {
		t.Fatalf("expected masked output: %s", buf.String())
	}
}

func TestLevelFromString(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug, "DEBUG": slog.LevelDebug,
		"warn": slog.LevelWarn, "warning": slog.LevelWarn,
		"error": slog.LevelError, "info": slog.LevelInfo, "bogus": slog.LevelInfo, "": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := LevelFromString(in); got != want {
			t.Errorf("LevelFromString(%q) = %v, want %v", in, got, want)
		}
	}
}
