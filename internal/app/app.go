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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"smart-gateway/internal/api"
	"smart-gateway/internal/config"
	"smart-gateway/internal/domain"
	"smart-gateway/internal/health"
	"smart-gateway/internal/infrastructure/bus"
	"smart-gateway/internal/infrastructure/singbox"
	"smart-gateway/internal/log"
	"smart-gateway/internal/repository"
	"smart-gateway/internal/routing"
	"smart-gateway/internal/scheduler"
	"smart-gateway/internal/scoring"
	"smart-gateway/internal/selector"
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
	api     *api.Server

	// realityKeypair is loaded once and used both for server inbounds and the
	// published client configs (M5).
	realityKeypair singbox.RealityKeypair

	clock domain.Clock
	bus   *bus.Bus

	monitor     *health.Monitor
	collector   *health.Collector
	speedTester *speed.Tester

	scoreEngine *scoring.Engine
	selPolicy   *selector.Policy

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
	a.clock = domain.SystemClock{}
	a.bus = bus.New()
	a.buildScoring()
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
	a.ensureAPIToken()
	a.buildAPI()
	a.registerComponents()
	return a, nil
}

// buildCore constructs the sing-box supervisor and route manager from the
// configured listeners. The reality keypair is loaded/generated once (M5) so
// vless-reality inbounds work out of the box. A listener that cannot be built
// is skipped with a warning rather than failing the whole gateway (FR-904).
func (a *App) buildCore() error {
	cfg := a.cfg
	a.realityKeypair = a.loadRealityKeypair()
	var inbounds []map[string]any
	for _, lc := range cfg.Inbound.Listeners {
		if !lc.Enabled {
			continue
		}
		in, err := singbox.BuildInbound(lc, a.realityKeypair)
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

	a.routes = routing.New(a.core, a.repo, routing.Config{}, a.selPolicy, a.repo, a.clock, a.logger, a.bus)
	return nil
}

// loadRealityKeypair returns the persisted keypair (or generates and persists
// a fresh one). A failure degrades gracefully: vless-reality listeners are
// skipped with a warning, everything else keeps working.
func (a *App) loadRealityKeypair() singbox.RealityKeypair {
	kp, err := singbox.LoadOrCreateRealityKeypair(context.Background(), a.cfg.Core.SingboxPath, a.keypairPath(), a.logger)
	if err != nil {
		a.logger.Warn("reality keypair unavailable; vless-reality inbounds will be skipped", "error", err)
	}
	return kp
}

// keypairPath locates the persisted reality keypair next to the SQLite store.
func (a *App) keypairPath() string {
	return filepath.Join(filepath.Dir(a.cfg.Storage.Path), "reality-keypair.json")
}

// ensureAPIToken fills api.token with an auto-generated, persisted value so
// the dashboard token stays stable across restarts (M5, FR-1102).
func (a *App) ensureAPIToken() {
	if a.cfg.API.Token != "" {
		return
	}
	tokenPath := filepath.Join(filepath.Dir(a.cfg.Storage.Path), "admin-token")
	if b, err := os.ReadFile(tokenPath); err == nil {
		if t := strings.TrimSpace(string(b)); t != "" {
			a.cfg.API.Token = t
			return
		}
	}
	token := randomToken(32)
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		a.logger.Warn("persist admin token", "error", err, "file", tokenPath)
	}
	a.cfg.API.Token = token
	a.logger.Warn("admin token auto-generated; pin it with api.token to keep it stable",
		"token", token, "file", tokenPath)
}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failure: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// buildAPI wires the M5 REST API + dashboard. The client-config closure reads
// the persisted keypair so the published URIs always match the server.
func (a *App) buildAPI() {
	a.api = api.New(api.Deps{
		Config:     a.cfg,
		Store:      a.repo,
		Routes:     a.routes,
		Subscriber: a.subs,
		Core:       a.core,
		ClientConfig: func() (*singbox.ClientConfig, error) {
			return singbox.BuildClientConfig(a.cfg.Inbound.Listeners, a.realityKeypair, a.cfg.API.PublicHost)
		},
		Reload:  a.reload,
		Version: version.Full(),
		Logger:  a.logger,
	})
}

// reload implements the config-only reload endpoint (SIGHUP equivalent). It
// re-reads and validates the YAML, re-registers changed subscription sources
// at runtime, and reports every other changed section as requiring a restart.
func (a *App) reload(ctx context.Context) (api.ReloadResult, error) {
	if a.cfg.SourcePath == "" {
		return api.ReloadResult{}, errors.New("no config file to reload (started with defaults)")
	}
	fresh, err := config.Load(a.cfg.SourcePath)
	if err != nil {
		return api.ReloadResult{}, fmt.Errorf("reload config: %w", err)
	}

	var res api.ReloadResult
	if fresh.Subscriptions.File != a.cfg.Subscriptions.File ||
		!reflect.DeepEqual(fresh.Subscriptions.Defs, a.cfg.Subscriptions.Defs) {
		if err := a.registerSpecs(ctx, fresh); err != nil {
			return api.ReloadResult{}, fmt.Errorf("apply subscriptions: %w", err)
		}
		res.Reloaded = append(res.Reloaded, "subscriptions")
	}

	if !reflect.DeepEqual(fresh.Inbound.Listeners, a.cfg.Inbound.Listeners) || !reflect.DeepEqual(fresh.Core, a.cfg.Core) {
		res.RequiresRestart = append(res.RequiresRestart, "core")
	}
	// An empty token in the file means "keep the running (auto-generated) one".
	if fresh.API.Token == "" {
		fresh.API.Token = a.cfg.API.Token
	}
	if !reflect.DeepEqual(fresh.API, a.cfg.API) {
		res.RequiresRestart = append(res.RequiresRestart, "api")
	}
	if !reflect.DeepEqual(fresh.Health, a.cfg.Health) || !reflect.DeepEqual(fresh.Speed, a.cfg.Speed) ||
		!reflect.DeepEqual(fresh.Scoring, a.cfg.Scoring) || !reflect.DeepEqual(fresh.Selector, a.cfg.Selector) {
		res.RequiresRestart = append(res.RequiresRestart, "monitoring")
	}
	if !reflect.DeepEqual(fresh.Storage, a.cfg.Storage) {
		res.RequiresRestart = append(res.RequiresRestart, "storage")
	}
	if !reflect.DeepEqual(fresh.Alerting, a.cfg.Alerting) {
		res.RequiresRestart = append(res.RequiresRestart, "alerting")
	}
	if !reflect.DeepEqual(fresh.Log, a.cfg.Log) {
		res.RequiresRestart = append(res.RequiresRestart, "logging")
	}
	if len(res.Reloaded) == 0 && len(res.RequiresRestart) == 0 {
		res.Reloaded = append(res.Reloaded, "none")
	}
	return res, nil
}

// buildScoring constructs the M4 selection machinery: the score engine
// (rescoring on health events, persisting via the store, publishing
// EvtScoreUpdated) and the selector policy (hysteresis + failover) consumed by
// the route manager. Construction is side-effect free; the engine and the
// selection loop start with the components in Run.
func (a *App) buildScoring() {
	cfg := a.cfg
	sc := scoring.DefaultConfig()
	sc.LatencyWeight = cfg.Scoring.LatencyWeight
	sc.StabilityWeight = cfg.Scoring.StabilityWeight
	sc.ThroughputWeight = cfg.Scoring.ThroughputWeight
	sc.LossWeight = cfg.Scoring.LossWeight
	sc.MinSamples = cfg.Scoring.MinSamples
	sc.StabilityWindow = cfg.Scoring.StabilityWindow
	sc.HardLatency = cfg.Scoring.HardLatency
	sc.FailoverFailures = cfg.Scoring.FailoverFailures
	sc.StaleAfter = cfg.Scoring.StaleAfter

	a.scoreEngine = scoring.NewEngine(a.bus, a.repo, a.repo, a.bus, sc, a.clock, a.logger)

	sel := selector.DefaultConfig()
	sel.SwitchDelta = cfg.Scoring.SwitchDelta
	sel.DwellTime = cfg.Scoring.DwellTime
	sel.Manual = cfg.Selector.Mode == "manual"
	a.selPolicy = selector.NewPolicy(a.repo, sel, a.clock, a.logger)
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
	clk := a.clock
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

// runSelectorLoop reacts to health/score events by re-evaluating the active
// outbound. Events are coalesced and evaluated at most once per second so a
// burst of probes (candidate pool sweep) triggers one cheap decision instead
// of one per node (NFR: selection well under the 1s budget).
func (a *App) runSelectorLoop(ctx context.Context) {
	ch, err := a.bus.Subscribe(ctx)
	if err != nil {
		a.logger.Error("selector loop subscribe", "error", err)
		return
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	pending := false
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			switch ev.Type {
			case domain.EvtNodeHealthChanged, domain.EvtScoreUpdated:
				pending = true
			}
		case <-tick.C:
			if pending {
				pending = false
				a.evaluateSelection(ctx)
			}
		}
	}
}

// evaluateSelection asks the route manager to re-run the selection policy over
// the current fleet. It is a no-op while the data plane is down (a core
// outage must not churn selection, cf. ErrDataPlaneDown handling).
func (a *App) evaluateSelection(ctx context.Context) {
	if a.core == nil || !a.core.Running() {
		return
	}
	nodes, err := a.repo.ListAllNodes(ctx)
	if err != nil {
		a.logger.Warn("selector: list nodes", "error", err)
		return
	}
	if len(nodes) == 0 {
		return
	}
	if err := a.routes.EnsureActive(ctx, nodes); err != nil {
		a.logger.Warn("selector: re-evaluate active outbound", "error", err)
	}
	a.syncActive()
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

	// Scoring engine: keeps the materialized scores in sync with health events.
	a.components = append(a.components,
		component{name: "scoring", start: func(ctx context.Context) error {
			go a.scoreEngine.Run(a.background())
			return nil
		}, stop: func(ctx context.Context) error {
			if a.bgCancel != nil {
				a.bgCancel()
			}
			return nil
		}, ready: func(ctx context.Context) error { return nil }},
	)

	// Selector: re-evaluates the active outbound as scores change (failover,
	// hysteresis switches), debounced to at most once per second.
	a.components = append(a.components,
		component{name: "selector", start: func(ctx context.Context) error {
			go a.runSelectorLoop(a.background())
			return nil
		}, stop: func(ctx context.Context) error {
			if a.bgCancel != nil {
				a.bgCancel()
			}
			return nil
		}, ready: func(ctx context.Context) error { return nil }},
	)

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
	return a.registerSpecs(ctx, a.cfg)
}

func (a *App) registerSpecs(ctx context.Context, cfg *config.Config) error {
	var specs []subscription.Spec
	if _, err := os.Stat(cfg.Subscriptions.File); err == nil {
		fileSpecs, err := subscription.ParseSpecsFile(cfg.Subscriptions.File)
		if err != nil {
			return fmt.Errorf("parse subscriptions file: %w", err)
		}
		specs = append(specs, fileSpecs...)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat subscriptions file: %w", err)
	}
	for _, d := range cfg.Subscriptions.Defs {
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
	if a.api != nil {
		mux.Handle("/", a.api.Handler())
		go a.api.Run(ctx)
	}

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
