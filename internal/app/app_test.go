package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"smart-gateway/internal/config"
	"smart-gateway/internal/log"
)

func newTestApp(t *testing.T) *App {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load(): %v", err)
	}
	cfg.Storage.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.Subscriptions.File = filepath.Join(t.TempDir(), "no-such-subscriptions.txt")
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

func TestHealthAndSpeedComponentsStartAndStop(t *testing.T) {
	a := newTestApp(t)
	if a.monitor == nil || a.collector == nil || a.speedTester == nil {
		t.Fatal("M3 monitoring components must be constructed")
	}
	if err := a.startAll(context.Background()); err != nil {
		t.Fatalf("startAll(): %v", err)
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
}

func TestHealthDisabledSkipsComponent(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load(): %v", err)
	}
	cfg.Storage.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.Subscriptions.File = filepath.Join(t.TempDir(), "no-such-subscriptions.txt")
	cfg.Health.Enabled = false
	cfg.Speed.Enabled = false
	logger := log.New(nil, 0)
	a, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer a.Shutdown(context.Background())
	for _, c := range a.components {
		if c.name == "health" || c.name == "speed" {
			t.Fatalf("disabled subsystem %q must not be registered", c.name)
		}
	}
}
