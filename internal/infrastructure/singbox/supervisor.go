package singbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// SupervisorOptions configures the sing-box subprocess supervisor.
type SupervisorOptions struct {
	BinaryPath     string
	ConfigDir      string
	ConfigFileName string // default: config.json
	CacheFile      string
	ClashAPIListen string
	ClashAPISecret string
	LogLevel       string
	Inbounds       []map[string]any
	RestartMin     time.Duration // backoff floor
	RestartMax     time.Duration // backoff ceiling
	RestartLimit   int           // max auto-restarts per hour before stopping
	OnRestart      func(reason string)
}

// Supervisor owns the sing-box subprocess lifecycle: config generation,
// `sing-box check` validation, atomic writes, launch, crash watchdog with
// backoff and selection re-assertion (FR-7xx, NFR-009).
type Supervisor struct {
	opts   SupervisorOptions
	gen    *Generator
	logger *log.Logger
	clock  domain.Clock
	rand   domain.RandomSource
	client *ClashClient

	mu            sync.Mutex
	nodes         []domain.Node
	cmd           *exec.Cmd
	procDone      chan struct{}
	running       bool
	stopping      bool
	failed        bool
	lastSelection string
	restarts      []time.Time
}

// NewSupervisor builds a supervisor with defaults applied.
func NewSupervisor(opts SupervisorOptions, logger *log.Logger, clock domain.Clock, rnd domain.RandomSource) *Supervisor {
	if opts.ConfigFileName == "" {
		opts.ConfigFileName = "config.json"
	}
	if opts.LogLevel == "" {
		opts.LogLevel = "info"
	}
	if opts.RestartMin <= 0 {
		opts.RestartMin = time.Second
	}
	if opts.RestartMax <= 0 {
		opts.RestartMax = time.Minute
	}
	if opts.RestartLimit <= 0 {
		opts.RestartLimit = 5
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if rnd == nil {
		rnd = domain.SystemRandom{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &Supervisor{
		opts: opts,
		gen: NewGenerator(GeneratorOptions{
			SelectorTag:    DefaultSelectorTag,
			Inbounds:       opts.Inbounds,
			ClashAPIListen: opts.ClashAPIListen,
			ClashAPISecret: opts.ClashAPISecret,
			CacheFile:      opts.CacheFile,
			LogLevel:       opts.LogLevel,
		}),
		logger: logger,
		clock:  clock,
		rand:   rnd,
		client: NewClashClient("http://"+opts.ClashAPIListen, opts.ClashAPISecret, 5*time.Second),
	}
}

// Close releases the Clash client.
func (s *Supervisor) Close() { s.client.Close() }

// ConfigPath returns the on-disk config location.
func (s *Supervisor) ConfigPath() string {
	return filepath.Join(s.opts.ConfigDir, s.opts.ConfigFileName)
}

// Running reports whether the subprocess is currently up.
func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Failed reports whether the watchdog gave up (restart rate limit).
func (s *Supervisor) Failed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// Start writes the config (if nodes exist), validates it and launches the
// subprocess. With no usable nodes it leaves the data plane down (the
// route manager triggers Apply once nodes are ingested).
func (s *Supervisor) Start(ctx context.Context) error {
	if err := os.MkdirAll(s.opts.ConfigDir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	s.mu.Lock()
	hasNodes := len(s.nodes) > 0
	s.mu.Unlock()
	if !hasNodes {
		s.logger.Warn("no nodes to configure; data plane stays down until a refresh lands")
		return nil
	}
	if err := s.writeConfig(ctx); err != nil {
		return err
	}
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.launch()
}

// Apply replaces the node set and restarts the data plane when it is up
// (subscription refresh path: regenerate → check → supervised restart).
// The config is validated before the process is touched.
func (s *Supervisor) Apply(ctx context.Context, nodes []domain.Node) error {
	s.mu.Lock()
	s.nodes = nodes
	running := s.running
	s.mu.Unlock()

	if err := os.MkdirAll(s.opts.ConfigDir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	if err := s.writeConfig(ctx); err != nil {
		return err
	}
	if err := s.check(ctx); err != nil {
		return err
	}
	if !running {
		return s.launch()
	}
	if err := s.Stop(ctx); err != nil {
		return err
	}
	return s.launch()
}

// Stop shuts the subprocess down gracefully (SIGTERM, kill fallback).
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = true
	cmd := s.cmd
	done := s.procDone
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if runtime.GOOS != "windows" {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-done:
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-done
	}
	return nil
}

// ApplySelection delegates to the Clash API and remembers the choice so a
// watchdog restart can re-assert it.
func (s *Supervisor) ApplySelection(ctx context.Context, nodeID string) error {
	if err := s.client.ApplySelection(ctx, DefaultSelectorTag, nodeID); err != nil {
		return err
	}
	s.mu.Lock()
	s.lastSelection = nodeID
	s.mu.Unlock()
	return nil
}

// Current returns the active selector member, or "" when the data plane is
// down.
func (s *Supervisor) Current(ctx context.Context) (string, error) {
	s.mu.Lock()
	running := s.running
	last := s.lastSelection
	s.mu.Unlock()
	if !running {
		return last, nil
	}
	now, err := s.client.Current(ctx, DefaultSelectorTag)
	if err != nil {
		return "", err
	}
	return now, nil
}

// RestartsInLastHour returns the number of watchdog restarts in the window.
func (s *Supervisor) RestartsInLastHour() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	cutoff := now.Add(-time.Hour)
	n := 0
	for _, t := range s.restarts {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

// DeepProbe measures end-to-end latency through the sing-box data plane for
// the given outbound node (full protocol handshake; deep probe, M3). QUIC
// nodes (hysteria2/tuic) require this because direct TCP probes cannot reach
// them.
func (s *Supervisor) DeepProbe(ctx context.Context, nodeID string, timeout time.Duration) (time.Duration, error) {
	return s.client.Delay(ctx, nodeID, timeout)
}

// Connections returns cumulative per-node byte counters from /connections
// (passive throughput attribution, M3). Implements domain.ConnectionsSampler.
func (s *Supervisor) Connections(ctx context.Context) ([]domain.ConnMetric, error) {
	return s.client.NodeTraffic(ctx)
}

func (s *Supervisor) writeConfig(ctx context.Context) error {
	s.mu.Lock()
	nodes := append([]domain.Node(nil), s.nodes...)
	s.mu.Unlock()
	cfg, err := s.gen.Generate(nodes)
	if err != nil {
		return fmt.Errorf("generate core config: %w", err)
	}
	data, err := cfg.JSON()
	if err != nil {
		return fmt.Errorf("marshal core config: %w", err)
	}
	tmp := s.ConfigPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write core config: %w", err)
	}
	if err := os.Rename(tmp, s.ConfigPath()); err != nil {
		return fmt.Errorf("install core config: %w", err)
	}
	s.logger.Info("core config written", "path", s.ConfigPath(), "nodes", len(nodes))
	return nil
}

func (s *Supervisor) check(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, s.opts.BinaryPath, "check", "-c", s.ConfigPath())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sing-box check failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (s *Supervisor) launch() error {
	s.mu.Lock()
	if s.failed {
		s.mu.Unlock()
		return errors.New("supervisor in failed state; manual intervention required")
	}
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.stopping = false
	s.mu.Unlock()

	cmd := exec.Command(s.opts.BinaryPath, "run", "-c", s.ConfigPath())
	cmd.Stdout = &lineWriter{logger: s.logger.With("stream", "stdout"), prefix: "sing-box"}
	cmd.Stderr = &lineWriter{logger: s.logger.With("stream", "stderr"), prefix: "sing-box"}
	if err := cmd.Start(); err != nil {
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
		return fmt.Errorf("start sing-box: %w", err)
	}
	done := make(chan struct{})
	s.mu.Lock()
	s.cmd = cmd
	s.procDone = done
	s.running = true
	s.mu.Unlock()
	s.logger.Info("sing-box started", "pid", cmd.Process.Pid, "config", s.ConfigPath())
	go s.watch(cmd, done)
	s.reassertSelection()
	return nil
}

func (s *Supervisor) watch(cmd *exec.Cmd, done chan struct{}) {
	defer close(done)
	waitErr := cmd.Wait()
	s.mu.Lock()
	running := s.running
	stopping := s.stopping
	s.running = false
	s.cmd = nil
	s.procDone = nil
	s.mu.Unlock()
	if !running || stopping {
		return
	}
	s.logger.Warn("sing-box process exited unexpectedly",
		"error", waitErr, "pid", cmd.Process.Pid, "restarts_1h", s.RestartsInLastHour())
	if s.opts.OnRestart != nil {
		s.opts.OnRestart("crash")
	}
	s.backoffRestart(1)
}

// backoffDelay returns the jittered exponential backoff for the n-th restart
// attempt, clamped to [RestartMin, RestartMax]. Pure so it stays testable.
func (s *Supervisor) backoffDelay(attempt int) time.Duration {
	delay := s.opts.RestartMin
	for i := 1; i < attempt && delay < s.opts.RestartMax; i++ {
		delay *= 2
		if delay > s.opts.RestartMax {
			delay = s.opts.RestartMax
		}
	}
	return s.jitter(delay)
}

func (s *Supervisor) backoffRestart(attempt int) {
	delay := s.backoffDelay(attempt)
	ctx, cancel := context.WithTimeout(context.Background(), delay)
	defer cancel()
	<-ctx.Done()

	s.mu.Lock()
	stopping := s.stopping
	now := s.clock.Now()
	cutoff := now.Add(-time.Hour)
	kept := s.restarts[:0]
	for _, t := range s.restarts {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.restarts = kept
	withinLimit := len(s.restarts) < s.opts.RestartLimit
	s.mu.Unlock()
	if stopping {
		return
	}
	if !withinLimit {
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
		s.logger.Error("sing-box restart rate limit reached; giving up",
			"limit_1h", s.opts.RestartLimit)
		if s.opts.OnRestart != nil {
			s.opts.OnRestart("rate-limited")
		}
		return
	}
	s.mu.Lock()
	s.restarts = append(s.restarts, s.clock.Now())
	s.mu.Unlock()
	if err := s.launch(); err != nil {
		s.logger.Error("sing-box restart failed", "error", err, "attempt", attempt)
		s.backoffRestart(attempt + 1)
	}
}

// reassertSelection replays the last known selection once the Clash API is
// reachable after a restart (watchdog recovery).
func (s *Supervisor) reassertSelection() {
	s.mu.Lock()
	sel := s.lastSelection
	s.mu.Unlock()
	if sel == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for i := 0; i < 20; i++ {
			if _, err := s.client.Current(ctx, DefaultSelectorTag); err == nil {
				if err := s.client.ApplySelection(ctx, DefaultSelectorTag, sel); err == nil {
					s.logger.Info("selection re-asserted after restart", "node", sel)
				}
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		s.logger.Warn("could not re-assert selection: clash api unreachable")
	}()
}

func (s *Supervisor) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := s.rand.Float64()*0.4 - 0.2 // ±20%
	return d + time.Duration(float64(d)*f)
}

// lineWriter splits subprocess output into log lines.
type lineWriter struct {
	logger *log.Logger
	prefix string
	buf    bytes.Buffer
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			w.buf.WriteString(line)
			break
		}
		w.logger.Debug(w.prefix, "line", strings.TrimRight(line, "\r\n"))
	}
	return len(p), nil
}

var _ io.Writer = (*lineWriter)(nil)
