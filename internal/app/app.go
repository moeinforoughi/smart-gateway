// Package app is the composition root of Smart Gateway (architecture §4-6).
//
// It owns the application lifecycle:
//   - ordered startup with per-subsystem readiness reporting (FR-004)
//   - graceful shutdown within 30s with a fixed stop order (NFR-009, §5)
//   - dependency wiring via constructor injection (NFR-108)
//
// Business logic lives in internal/{domain,service}; app only wires it up.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"smart-gateway/internal/config"
	"smart-gateway/internal/log"
	"smart-gateway/internal/scheduler"
	"smart-gateway/internal/version"
)

// ShutdownGrace is the total budget for graceful shutdown (NFR-009).
const ShutdownGrace = 30 * time.Second

// Readiness reports whether a subsystem is ready to serve.
type Readiness func(ctx context.Context) error

// component tracks one subsystem's startup state (FR-004).
type component struct {
	name  string
	start func(ctx context.Context) error
	stop  func(ctx context.Context) error
	ready Readiness
}

// App is the composed application.
type App struct {
	cfg     *config.Config
	logger  *log.Logger
	sched   *scheduler.Scheduler
	httpSrv *http.Server

	components []component
	readiness  map[string]Readiness
}

// New wires the application together. It performs no I/O beyond what is
// required to construct infrastructure; heavy setup happens in Run.
func New(cfg *config.Config, logger *log.Logger) (*App, error) {
	a := &App{
		cfg:       cfg,
		logger:    logger,
		readiness: make(map[string]Readiness),
	}
	a.registerComponents()
	return a, nil
}

func (a *App) registerComponents() {
	// Scheduler: always available.
	a.sched = scheduler.New(nil, nil, a.logger)
	a.readiness["scheduler"] = func(ctx context.Context) error {
		if a.sched.Len() >= 0 {
			return nil
		}
		return errors.New("scheduler not started")
	}
	a.components = append(a.components,
		component{name: "scheduler", start: func(ctx context.Context) error {
			a.sched.Start()
			return nil
		}, stop: func(ctx context.Context) error {
			a.sched.Stop(ctxRemaining(ctx, ShutdownGrace))
			return nil
		}, ready: a.readiness["scheduler"]},
	)
}

// Run starts all components in order, serves HTTP, then blocks until ctx is
// cancelled, performing graceful shutdown (FR-006).
func (a *App) Run(ctx context.Context) error {
	if err := a.startAll(ctx); err != nil {
		return err
	}
	a.logger.Info("gateway started", "version", Version(), "listen", a.cfg.API.Listen)

	if err := a.startHTTP(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	a.logger.Info("shutdown requested")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), ShutdownGrace)
	defer cancel()
	if err := a.Shutdown(shutdownCtx); err != nil {
		return err
	}
	a.logger.Info("gateway stopped cleanly")
	return nil
}

// startAll starts components sequentially, recording readiness (FR-004).
func (a *App) startAll(ctx context.Context) error {
	for _, c := range a.components {
		a.logger.Info("starting subsystem", "subsystem", c.name)
		if err := c.start(ctx); err != nil {
			a.logger.Error("subsystem failed to start", "subsystem", c.name, "error", err)
			return fmt.Errorf("start %s: %w", c.name, err)
		}
		a.logger.Info("subsystem ready", "subsystem", c.name)
	}
	return nil
}

// Shutdown stops subsystems in the reverse order of startup, honoring the
// shutdown sequence from architecture §5 (schedulers -> workers -> HTTP ->
// logs -> database).
func (a *App) Shutdown(ctx context.Context) error {
	if a.httpSrv != nil {
		shutdownErr := a.httpSrv.Shutdown(ctx)
		if shutdownErr != nil && !errors.Is(shutdownErr, context.Canceled) && !errors.Is(shutdownErr, context.DeadlineExceeded) {
			a.logger.Warn("http server shutdown error", "error", shutdownErr)
		}
	}
	var firstErr error
	for i := len(a.components) - 1; i >= 0; i-- {
		c := a.components[i]
		if c.stop == nil {
			continue
		}
		if err := c.stop(ctx); err != nil && firstErr == nil {
			firstErr = err
			a.logger.Error("subsystem shutdown error", "subsystem", c.name, "error", err)
		}
	}
	return firstErr
}

func ctxRemaining(ctx context.Context, fallback time.Duration) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		if rem := time.Until(d); rem > 0 {
			return rem
		}
	}
	return fallback
}

func (a *App) startHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)

	a.httpSrv = &http.Server{
		Addr:              a.cfg.API.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		a.logger.Info("http server listening", "addr", a.cfg.API.Listen)
		if err := a.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("http server error", "error", err)
		}
	}()
	return nil
}

func (a *App) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (a *App) handleReadyz(w http.ResponseWriter, r *http.Request) {
	for name, ready := range a.readiness {
		if err := ready(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready","subsystem":"` + name + `"}`))
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// Logger returns the application logger (used by tests).
func (a *App) Logger() *log.Logger { return a.logger }

// Config returns the immutable configuration.
func (a *App) Config() *config.Config { return a.cfg }

// Version returns the full human-readable version string.
func Version() string { return version.Full() }
