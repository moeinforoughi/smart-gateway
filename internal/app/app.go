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
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"smart-gateway/internal/config"
	"smart-gateway/internal/domain"
	"smart-gateway/internal/health"
	"smart-gateway/internal/infrastructure/singbox"
	"smart-gateway/internal/log"
	"smart-gateway/internal/repository"
	"smart-gateway/internal/routing"
	"smart-gateway/internal/scheduler"
	"smart-gateway/internal/service/speed"
	"smart-gateway/internal/subscription"
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
	repo    *repository.Repo
	subs    *subscription.Manager
	core    *singbox.Supervisor
	routes  *routing.Manager
	httpSrv *http.Server

	monitor     *health.Monitor
	collector   *health.Collector
	speedTester *speed.Tester

	// bgCtx/bgCancel own the long-running health/speed goroutines; they are
	// cancelled during shutdown (stop order, architecture §5).
	bgCtx    context.Context
	bgCancel context.CancelFunc

	nodeFingerprint string

	components []component
	readiness  map[string]Readiness
}

// New wires the application together. It performs no I/O beyond what is
// required to construct infrastructure; heavy setup happens in Run.
func New(cfg *config.Config, logger *log.Logger) (*App, error) {
	repo, err := repository.New(cfg.Storage.Path, nil)
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	a := &App{
		cfg:       cfg,
		repo:      repo,
		logger:    logger,
		readiness: make(map[string]Readiness),
	}
	if err := a.buildCore(); err != nil {
		return nil, err
	}
	a.buildHealth()
	a.subs = subscription.New(subscription.Config{
		RefreshInterval: cfg.Subscriptions.Refresh,
		RequestTimeout:  cfg.Subscriptions.RequestTimeout,
		MaxBytes:        cfg.Subscriptions.MaxBytes,
		UserAgent:       cfg.Subscriptions.UserAgent,
		VerifyTLS:       cfg.Subscriptions.VerifyTLS,
		BackoffBase:     cfg.Subscriptions.BackoffBase,
		BackoffMax:      cfg.Subscriptions.BackoffMax,
		RetryLimit:      cfg.Subscriptions.RetryLimit,
	}, repo, repo, nil, nil, logger, nil)
	a.registerComponents()
	return a, nil
}

// buildCore constructs the sing-box supervisor and route manager from the
// configured listeners. A listener that cannot be built (e.g. a reality
// inbound without a keypair, which M5 generates automatically) is skipped
// with a warning rather than failing the whole gateway (FR-904).
func (a *App) buildCore() error {
	cfg := a.cfg
	var inbounds []map[string]any
	for _, lc := range cfg.Inbound.Listeners {
		if !lc.Enabled {
			continue
		}
		in, err := singbox.BuildInbound(lc, singbox.RealityKeypair{})
		if err != nil {
			a.logger.Warn("skipping inbound", "protocol", lc.Protocol, "error", err)
			continue
		}
		inbounds = append(inbounds, in)
	}

	onRestart := func(reason string) {
		a.logger.Warn("sing-box restart", "reason", reason)
	}
	a.core = singbox.NewSupervisor(singbox.SupervisorOptions{
		BinaryPath:     cfg.Core.SingboxPath,
		ConfigDir:      cfg.Core.ConfigDir,
		CacheFile:      cfg.Core.CacheFile,
		ClashAPIListen: cfg.Core.APIListen,
		ClashAPISecret: cfg.Core.APISecret,
		LogLevel:       cfg.Log.Level,
		Inbounds:       inbounds,
		RestartLimit:   cfg.Core.RestartLimit,
		OnRestart:      onRestart,
	}, a.logger, nil, nil)

	a.routes = routing.New(a.core, a.repo, routing.Config{}, nil, a.logger, nil)
	return nil
}

// coreAwareDeepProber delegates deep probes to the data plane but reports
// health.ErrDataPlaneDown while sing-box is down, so a core outage is not
// recorded as per-node failures (health monitor skips those).
type coreAwareDeepProber struct{ core *singbox.Supervisor }

func (c coreAwareDeepProber) DeepProbe(ctx context.Context, nodeID string, timeout time.Duration) (time.Duration, error) {
	if c.core == nil || c.core.Failed() {
		return 0, health.ErrDataPlaneDown
	}
	return c.core.DeepProbe(ctx, nodeID, timeout)
}

// buildHealth constructs the M3 monitoring pipeline: direct prober, health
// monitor (candidate/fleet/deep loops), passive traffic collector and the
// speed-test engine. All use the injected clock/random ports; construction is
// side-effect free — the loops only start when the components start in Run.
func (a *App) buildHealth() {
	cfg := a.cfg
	clk := domain.SystemClock{}
	rnd := domain.SystemRandom{}

	prober := health.NewDirectProber(health.ProbeConfig{
		Timeout:   cfg.Health.ProbeTimeout,
		HTTPProbe: false,
		VerifyTLS: true,
	}, clk, a.logger)

	a.monitor = health.New(health.Config{
		Workers:          cfg.Health.Workers,
		ProbeTimeout:     cfg.Health.ProbeTimeout,
		CandidateWindow:  cfg.Health.CandidateWindow,
		FleetInterval:    cfg.Health.FleetInterval,
		DeepInterval:     cfg.Health.DeepInterval,
		CandidatePool:    cfg.Health.CandidatePool,
		MinInterval:      cfg.Health.MinInterval,
		HardLatency:      cfg.Scoring.HardLatency,
		FailoverFailures: cfg.Scoring.FailoverFailures,
	}, prober, coreAwareDeepProber{core: a.core}, a.repo, a.repo,
		domain.NewBudget(int64(cfg.Health.MaxProbeBytes)<<20, clk), clk, rnd, a.logger, nil)

	a.collector = health.NewCollector(a.core, a.repo, cfg.Health.MinInterval, clk, a.logger, nil)

	a.speedTester = speed.New(speed.Config{
		Interval:       cfg.Speed.Interval,
		DownloadSize:   cfg.Speed.DownloadSize,
		Concurrency:    cfg.Speed.Concurrency,
		Target:         cfg.Speed.Target,
		Timeout:        cfg.Speed.Timeout,
		CandidateCount: cfg.Health.CandidatePool,
	}, speed.NewDirectMeasurer(cfg.Speed.Target, cfg.Speed.Timeout, cfg.Speed.DownloadSize, true, clk),
		a.repo, a.repo, domain.NewBudget(cfg.Speed.MaxPerDay, clk), clk, rnd, a.logger, nil)
}

// background returns the shared context owning the health/speed goroutines.
func (a *App) background() context.Context {
	if a.bgCancel == nil {
		a.bgCtx, a.bgCancel = context.WithCancel(context.Background())
	}
	return a.bgCtx
}

// syncActive mirrors the current active outbound into the health monitor so
// the candidate pool always includes it.
func (a *App) syncActive() {
	if a.monitor == nil {
		return
	}
	if id, _ := a.routes.Current(); id != "" {
		a.monitor.SetActive(id)
	}
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

	// Storage: the SQLite database opened during construction.
	a.readiness["storage"] = func(ctx context.Context) error {
		_, err := a.repo.CountNodes(ctx)
		return err
	}
	a.components = append(a.components,
		component{name: "storage", start: func(ctx context.Context) error {
			return nil
		}, stop: func(ctx context.Context) error {
			return a.repo.Close()
		}, ready: a.readiness["storage"]},
	)

	// Core: the sing-box data plane. Starts after storage so persisted nodes
	// are available; the route manager picks the single active outbound.
	a.readiness["core"] = func(ctx context.Context) error {
		if a.core.Failed() {
			return errors.New("sing-box supervisor in failed state")
		}
		return nil
	}
	a.components = append(a.components,
		component{name: "core", start: func(ctx context.Context) error {
			return a.startCore(ctx)
		}, stop: func(ctx context.Context) error {
			return a.core.Stop(ctx)
		}, ready: a.readiness["core"]},
	)

	// Subscriptions: register sources and schedule periodic refreshes.
	a.components = append(a.components,
		component{name: "subscriptions", start: func(ctx context.Context) error {
			if err := a.registerSubscriptions(ctx); err != nil {
				return err
			}
			a.sched.Add(scheduler.Job{
				ID:       "subscription-refresh",
				Interval: 30 * time.Second,
				Func:     func(ctx context.Context) { a.refreshTick(ctx) },
			})
			return nil
		}, stop: nil, ready: func(ctx context.Context) error { return nil }},
	)

	// Health & passive collector: direct probes, deep probes and traffic
	// attribution. Their loops live on the shared background context.
	if a.cfg.Health.Enabled {
		a.components = append(a.components,
			component{name: "health", start: func(ctx context.Context) error {
				bg := a.background()
				go a.monitor.Run(bg)
				go a.collector.Run(bg)
				return nil
			}, stop: func(ctx context.Context) error {
				if a.bgCancel != nil {
					a.bgCancel()
				}
				return nil
			}, ready: func(ctx context.Context) error { return nil }},
		)
	}

	// Speed tests: rare, budgeted throughput measurements.
	if a.cfg.Speed.Enabled {
		a.components = append(a.components,
			component{name: "speed", start: func(ctx context.Context) error {
				go a.speedTester.Run(a.background())
				return nil
			}, stop: func(ctx context.Context) error { return nil },
				ready: func(ctx context.Context) error { return nil }},
		)
	}
}

// startCore applies the persisted nodes to the data plane and activates the
// best node. With no nodes the data plane stays down until a refresh lands.
func (a *App) startCore(ctx context.Context) error {
	nodes, err := a.repo.ListAllNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	if len(nodes) == 0 {
		a.logger.Info("no nodes yet; data plane down until first refresh")
		return nil
	}
	if err := a.core.Apply(ctx, nodes); err != nil {
		return err
	}
	a.nodeFingerprint = fingerprintNodes(nodes)
	if err := a.routes.EnsureActive(ctx, nodes); err != nil {
		return err
	}
	a.syncActive()
	return nil
}

// refreshTick runs a subscription refresh then re-applies the node set to the
// data plane only when membership actually changed (avoids needless restarts
// on a 30s cadence). Selection re-evaluation is cheap and runs every tick.
func (a *App) refreshTick(ctx context.Context) {
	a.subs.RefreshDue(ctx)
	nodes, err := a.repo.ListAllNodes(ctx)
	if err != nil {
		a.logger.Error("list nodes after refresh", "error", err)
		return
	}
	fp := fingerprintNodes(nodes)
	if fp != a.nodeFingerprint || !a.core.Running() {
		if len(nodes) > 0 {
			if err := a.core.Apply(ctx, nodes); err != nil {
				a.logger.Error("re-apply core config", "error", err)
			}
		}
		a.nodeFingerprint = fp
	}
	if err := a.routes.EnsureActive(ctx, nodes); err != nil {
		a.logger.Warn("ensure active outbound", "error", err)
	}
	a.syncActive()
}

// fingerprintNodes derives a stable membership fingerprint so the app only
// restarts the data plane when the node set actually changed.
func fingerprintNodes(nodes []domain.Node) string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID+":"+n.DedupeHash)
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return string(sum[:])
}

// registerSubscriptions merges file and inline sources into the store.
// A missing subscriptions file is not an error (only inline defs apply).
func (a *App) registerSubscriptions(ctx context.Context) error {
	var specs []subscription.Spec
	if _, err := os.Stat(a.cfg.Subscriptions.File); err == nil {
		fileSpecs, err := subscription.ParseSpecsFile(a.cfg.Subscriptions.File)
		if err != nil {
			return fmt.Errorf("parse subscriptions file: %w", err)
		}
		specs = append(specs, fileSpecs...)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat subscriptions file: %w", err)
	}
	for _, d := range a.cfg.Subscriptions.Defs {
		specs = append(specs, subscription.Spec{
			Name:        d.Name,
			URL:         d.URL,
			Priority:    d.Priority,
			Tags:        d.Tags,
			Headers:     d.Headers,
			UpdateEvery: derefDuration(d.UpdateEvery),
			Timeout:     derefDuration(d.Timeout),
		})
	}
	return a.subs.EnsureRegistered(ctx, specs)
}

func derefDuration(d *time.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return *d
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
