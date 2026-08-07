package health

import (
	"context"
	"errors"
	"sync"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// ErrDataPlaneDown reports a deep probe that could not run because the
// sing-box data plane is unavailable. It is not a node failure: a core
// outage must not mass-mark nodes FAILED, so the monitor skips recording it.
var ErrDataPlaneDown = errors.New("data plane down")

// NodeSource lists nodes to probe (fleet view).
type NodeSource interface {
	ListAllNodes(ctx context.Context) ([]domain.Node, error)
}

// Store persists probe outcomes and health state (M3). Implemented by the
// SQLite repository.
type Store interface {
	AddMeasurement(ctx context.Context, nodeID string, latency time.Duration, success bool) error
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
	SetNodeState(ctx context.Context, id string, state domain.NodeState, at time.Time) error
}

// DeepProber measures full protocol handshakes through the sing-box data
// plane. Implemented by the sing-box supervisor.
type DeepProber interface {
	DeepProbe(ctx context.Context, nodeID string, timeout time.Duration) (time.Duration, error)
}

// Config carries the health engine knobs resolved from the gateway config.
type Config struct {
	Workers           int
	ProbeTimeout      time.Duration
	CandidateWindow   time.Duration
	FleetInterval     time.Duration
	DeepInterval      time.Duration
	CandidatePool     int
	MinInterval       time.Duration
	HardLatency       time.Duration // above this a node is unhealthy (default: 3s)
	FailoverFailures  int           // consecutive failures before FAILED (default: 3)
	DeepBytesEstimate int64         // budget charge per deep probe
}

// Monitor drives the tiered probing schedule (candidate window, fleet sweep,
// deep probes), persists measurements, evolves health state and publishes
// events. All intervals are jittered (FR-410); node order is randomized
// (FR-412).
type Monitor struct {
	cfg    Config
	prober Prober
	deep   DeepProber
	nodes  NodeSource
	store  Store
	budget *domain.Budget
	clock  domain.Clock
	rand   domain.RandomSource
	logger *log.Logger
	bus    domain.Bus
	pool   *Pool

	mu        sync.Mutex
	lastProbe map[string]time.Time
	activeID  string
}

// New builds a monitor with defaults applied.
func New(cfg Config, prober Prober, deep DeepProber, nodes NodeSource, store Store,
	budget *domain.Budget, clock domain.Clock, rnd domain.RandomSource,
	logger *log.Logger, bus domain.Bus) *Monitor {
	if cfg.Workers < 1 {
		cfg.Workers = 50
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 3 * time.Second
	}
	if cfg.CandidateWindow <= 0 {
		cfg.CandidateWindow = time.Minute
	}
	if cfg.FleetInterval <= 0 {
		cfg.FleetInterval = 24 * time.Hour
	}
	if cfg.DeepInterval <= 0 {
		cfg.DeepInterval = 15 * time.Minute
	}
	if cfg.CandidatePool < 10 {
		cfg.CandidatePool = 100
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 30 * time.Second
	}
	if cfg.HardLatency <= 0 {
		cfg.HardLatency = 3 * time.Second
	}
	if cfg.FailoverFailures < 1 {
		cfg.FailoverFailures = 3
	}
	if cfg.DeepBytesEstimate <= 0 {
		cfg.DeepBytesEstimate = 4096
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
	return &Monitor{
		cfg:       cfg,
		prober:    prober,
		deep:      deep,
		nodes:     nodes,
		store:     store,
		budget:    budget,
		clock:     clock,
		rand:      rnd,
		logger:    logger,
		bus:       bus,
		pool:      NewPool(cfg.Workers),
		lastProbe: make(map[string]time.Time),
	}
}

// SetActive records the current active outbound so it always stays in the
// deep-probe candidate pool.
func (m *Monitor) SetActive(id string) {
	m.mu.Lock()
	m.activeID = id
	m.mu.Unlock()
}

// Active returns the last SetActive value.
func (m *Monitor) Active() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeID
}

// Run starts the candidate, fleet and deep loops and blocks until ctx is
// cancelled. Candidate and fleet run once immediately (fast first signal);
// deep waits one jittered interval so the data plane settles.
func (m *Monitor) Run(ctx context.Context) {
	go m.loop(ctx, "candidate", m.cfg.CandidateWindow, m.probeCandidates)
	go m.loop(ctx, "fleet", m.cfg.FleetInterval, m.probeFleet)
	go m.loop(ctx, "deep", m.cfg.DeepInterval, m.probeDeep)
	<-ctx.Done()
}

func (m *Monitor) loop(ctx context.Context, name string, interval time.Duration, fn func(context.Context)) {
	// First run is quick so startup isn't quiet; the "deep" loop waits a full
	// jittered interval so the data plane settles. Subsequent runs use the
	// jittered interval (FR-410: randomized, never fixed).
	if name == "deep" {
		if !m.sleep(ctx, m.jitter(interval)) {
			return
		}
	} else if !m.sleep(ctx, m.shortDelay(interval)) {
		return
	}
	for {
		fn(ctx)
		if !m.sleep(ctx, m.jitter(interval)) {
			return
		}
	}
}

// shortDelay returns a randomized first-run delay in [interval/20, interval/4].
func (m *Monitor) shortDelay(interval time.Duration) time.Duration {
	floor := interval / 20
	return floor + time.Duration(m.rand.Float64()*float64(interval/4))
}

func (m *Monitor) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// probeCandidates probes the rotating candidate pool (active + new + top-N).
func (m *Monitor) probeCandidates(ctx context.Context) {
	nodes := m.listNodes(ctx)
	if len(nodes) == 0 {
		return
	}
	m.mu.Lock()
	active := m.activeID
	m.mu.Unlock()
	pick := PickCandidates(nodes, m.healthFor, active, m.cfg.CandidatePool)
	if len(pick) == 0 {
		return
	}
	m.probeNodes(ctx, pick)
}

// probeFleet sweeps every node with a direct probe.
func (m *Monitor) probeFleet(ctx context.Context) {
	nodes := m.listNodes(ctx)
	if len(nodes) == 0 {
		return
	}
	m.probeNodes(ctx, nodes)
}

// probeDeep deep-probes the candidate pool through the data plane (full
// protocol handshake; required for QUIC nodes).
func (m *Monitor) probeDeep(ctx context.Context) {
	nodes := m.listNodes(ctx)
	if len(nodes) == 0 {
		return
	}
	m.mu.Lock()
	active := m.activeID
	m.mu.Unlock()
	pick := PickCandidates(nodes, m.healthFor, active, m.cfg.CandidatePool)
	if len(pick) == 0 {
		return
	}
	m.probeNodesDeep(ctx, pick)
}

func (m *Monitor) listNodes(ctx context.Context) []domain.Node {
	nodes, err := m.nodes.ListAllNodes(ctx)
	if err != nil {
		m.logger.Warn("list nodes for probing", "error", err)
		return nil
	}
	return nodes
}

func (m *Monitor) healthFor(id string) domain.NodeHealth {
	h, err := m.store.GetNodeHealth(context.Background(), id)
	if err != nil {
		return domain.NodeHealth{NodeID: id}
	}
	return h
}

// probeNodes direct-probes the given set, honoring the per-node min interval
// and randomized order (FR-412, FR-406).
func (m *Monitor) probeNodes(ctx context.Context, nodes []domain.Node) {
	jobs := m.dueNodes(nodes)
	for _, n := range jobs {
		n := n
		m.pool.Go(ctx, func() { m.probeNode(ctx, n, false) })
	}
	m.pool.Wait()
}

func (m *Monitor) probeNodesDeep(ctx context.Context, nodes []domain.Node) {
	jobs := m.dueNodes(nodes)
	for _, n := range jobs {
		n := n
		m.pool.Go(ctx, func() { m.probeNode(ctx, n, true) })
	}
	m.pool.Wait()
}

// dueNodes returns nodes that are past their per-node min interval, shuffled.
func (m *Monitor) dueNodes(nodes []domain.Node) []domain.Node {
	now := m.clock.Now()
	m.mu.Lock()
	var out []domain.Node
	for _, n := range nodes {
		last, ok := m.lastProbe[n.ID]
		if !ok || now.Sub(last) >= m.cfg.MinInterval {
			out = append(out, n)
		}
	}
	// Fisher-Yates with the injected random source (anti-abuse order).
	for i := len(out) - 1; i > 0; i-- {
		j := m.rand.Intn(i + 1)
		out[i], out[j] = out[j], out[i]
	}
	m.mu.Unlock()
	return out
}

func (m *Monitor) probeNode(ctx context.Context, n domain.Node, deep bool) {
	var (
		lat time.Duration
		ok  bool
		res Result
	)
	if deep || needsDeep(n.Protocol) {
		if m.budget == nil || !m.budget.Allow(m.cfg.DeepBytesEstimate) {
			m.logger.Debug("deep probe skipped: budget exhausted", "node", n.ID)
			return
		}
		d, err := m.deep.DeepProbe(ctx, n.ID, m.cfg.ProbeTimeout)
		if err != nil {
			if errors.Is(err, ErrDataPlaneDown) {
				m.logger.Debug("deep probe skipped: data plane down", "node", n.ID)
				return
			}
			ok = false
		} else {
			ok = true
		}
		lat = d
		if ok {
			m.budget.Note(m.cfg.DeepBytesEstimate)
		}
	} else {
		res = m.prober.Probe(ctx, n)
		ok, lat = res.OK, res.Latency
	}
	m.record(ctx, n, lat, ok, res)
}

func (m *Monitor) record(ctx context.Context, n domain.Node, lat time.Duration, ok bool, res Result) {
	now := m.clock.Now()
	m.mu.Lock()
	m.lastProbe[n.ID] = now
	m.mu.Unlock()

	if err := m.store.AddMeasurement(ctx, n.ID, lat, ok); err != nil {
		m.logger.Warn("persist measurement", "node", n.ID, "error", err)
	}
	h, err := m.store.GetNodeHealth(ctx, n.ID)
	if err != nil {
		m.logger.Warn("read node health", "node", n.ID, "error", err)
		return
	}
	m.applyState(ctx, n, h)
	if !ok && h.ConsecutiveFailures >= m.cfg.FailoverFailures {
		m.publish(ctx, domain.EvtNodeFailed, map[string]any{
			"node_id": n.ID,
			"state":   string(domain.StateFailed),
		})
	}
	m.publish(ctx, domain.EvtNodeProbed, map[string]any{
		"node_id":    n.ID,
		"ok":         ok,
		"latency_ms": lat.Milliseconds(),
		"stage":      string(res.Stage),
	})
}

// applyState evolves the node's health state through the documented machine.
func (m *Monitor) applyState(ctx context.Context, n domain.Node, h domain.NodeHealth) {
	desired := healthStateFor(h, m.cfg)
	target := coerceTransition(n.State, desired)
	if target == n.State {
		return
	}
	if !domain.CanTransitionHealth(n.State, target) {
		m.logger.Warn("health transition rejected",
			"node", n.ID, "from", n.State, "to", target)
		return
	}
	if err := m.store.SetNodeState(ctx, n.ID, target, m.clock.Now()); err != nil {
		m.logger.Warn("persist health state", "node", n.ID, "error", err)
		return
	}
	m.publish(ctx, domain.EvtNodeHealthChanged, map[string]any{
		"node_id": n.ID,
		"from":    string(n.State),
		"to":      string(target),
	})
}

func (m *Monitor) publish(ctx context.Context, t domain.EventType, payload map[string]any) {
	if m.bus == nil {
		return
	}
	e := domain.Event{Type: t, At: m.clock.Now(), Payload: payload}
	if err := m.bus.Publish(ctx, e); err != nil {
		m.logger.Warn("publish event", "type", t, "error", err)
	}
}

// healthStateFor maps a folded health record to a desired state (rule-based;
// refined by the scorer in M4).
func healthStateFor(h domain.NodeHealth, cfg Config) domain.NodeState {
	if h.Probes == 0 {
		return domain.StateUnknown
	}
	if h.ConsecutiveFailures >= cfg.FailoverFailures {
		return domain.StateFailed
	}
	hard := cfg.HardLatency
	if hard <= 0 {
		hard = 3 * time.Second
	}
	if h.LossRate >= 0.5 || (h.EWMA > 0 && h.EWMA >= hard) {
		return domain.StateUnstable
	}
	if h.LossRate >= 0.25 || (h.EWMA > 0 && h.EWMA >= hard/2) {
		return domain.StateDegraded
	}
	if h.SuccessCount > 0 {
		return domain.StateHealthy
	}
	return domain.StateUnknown
}

// coerceTransition maps a desired state to a legal move of the health machine.
// Failed nodes must recover through RECOVERING before becoming healthy again.
func coerceTransition(current, desired domain.NodeState) domain.NodeState {
	if current == "" {
		current = domain.StateUnknown
	}
	if domain.CanTransitionHealth(current, desired) {
		return desired
	}
	switch current {
	case domain.StateFailed, domain.StateRecovering:
		// Recovery path: FAILED/RECOVERING cannot jump straight to
		// HEALTHY/DEGRADED/UNSTABLE; stage through RECOVERING.
		switch desired {
		case domain.StateHealthy, domain.StateDegraded, domain.StateUnstable:
			return domain.StateRecovering
		}
	}
	return current
}

// jitter scales d by ±10% using the injected random source (FR-410).
func (m *Monitor) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := m.rand.Float64()*0.2 - 0.1
	return d + time.Duration(float64(d)*f)
}
