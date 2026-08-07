package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"smart-gateway/internal/config"
	"smart-gateway/internal/log"
)

func newTestApp(t *testing.T) *App {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load(): %v", err)
	}
	logger := log.New(nil, 0)
	a, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return a
}

func TestHealthz(t *testing.T) {
	a := newTestApp(t)
	if err := a.startAll(context.Background()); err != nil {
		t.Fatalf("startAll(): %v", err)
	}
	defer a.Shutdown(context.Background())

	rec := httptest.NewRecorder()
	a.handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", rec.Code)
	}
}

func TestReadyzWhenReady(t *testing.T) {
	a := newTestApp(t)
	if err := a.startAll(context.Background()); err != nil {
		t.Fatalf("startAll(): %v", err)
	}
	defer a.Shutdown(context.Background())

	rec := httptest.NewRecorder()
	a.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200", rec.Code)
	}
}

func TestShutdownCompletesWithinGrace(t *testing.T) {
	a := newTestApp(t)
	if err := a.startAll(context.Background()); err != nil {
		t.Fatalf("startAll(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownGrace)
	defer cancel()
	start := time.Now()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	if elapsed := time.Since(start); elapsed > ShutdownGrace {
		t.Fatalf("shutdown took %s, over %s budget", elapsed, ShutdownGrace)
	}
}
