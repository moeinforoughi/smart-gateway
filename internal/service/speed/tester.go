// Package speed implements the rare throughput tests (M3, FR-500 series).
// Speed tests are much rarer than health checks, bounded by their own daily
// bandwidth budget and concurrency limit, and measure raw gateway↔server
// bandwidth for plain HTTP/SOCKS proxy nodes. Obfuscated protocols rely on
// deep probes + passive attribution (documented approximation, FR-506).
package speed

import (
	"context"
	"sort"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Measurer produces one throughput estimate (bytes/sec) for a node.
type Measurer interface {
	Measure(ctx context.Context, n domain.Node, size int64) (bps float64, err error)
}

// Store persists throughput estimates. Implemented by the repository.
type Store interface {
	RecordThroughput(ctx context.Context, nodeID string, bps float64, at time.Time) error
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
}

// NodeSource lists the fleet.
type NodeSource interface {
	ListAllNodes(ctx context.Context) ([]domain.Node, error)
}

// Config carries the speed-test knobs resolved from the gateway config.
type Config struct {
	Interval       time.Duration
	DownloadSize   int64
	Concurrency    int
	Target         string
	Timeout        time.Duration
	CandidateCount int
}

// Tester schedules bounded, budgeted throughput measurements.
type Tester struct {
	cfg     Config
	measure Measurer
	nodes   NodeSource
	store   Store
	budget  *domain.Budget
	clock   domain.Clock
	rand    domain.RandomSource
	logger  *log.Logger
	bus     domain.Bus
}

// New builds a speed tester with defaults applied.
func New(cfg Config, measure Measurer, nodes NodeSource, store Store, budget *domain.Budget,
	clock domain.Clock, rnd domain.RandomSource, logger *log.Logger, bus domain.Bus) *Tester {
	if cfg.Interval <= 0 {
		cfg.Interval = 6 * time.Hour
	}
	if cfg.DownloadSize <= 0 {
		cfg.DownloadSize = 1 << 20
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 3
	}
	if cfg.CandidateCount <= 0 {
		cfg.CandidateCount = 100
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
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
	return &Tester{
		cfg: cfg, measure: measure, nodes: nodes, store: store, budget: budget,
		clock: clock, rand: rnd, logger: logger, bus: bus,
	}
}

// Run loops forever: sleep a jittered interval, then measure the candidate
// set under the concurrency limit and daily budget.
func (t *Tester) Run(ctx context.Context) {
	if !t.sleepJittered(ctx, t.cfg.Interval) {
		return
	}
	for {
		t.measureOnce(ctx)
		if !t.sleepJittered(ctx, t.cfg.Interval) {
			return
		}
	}
}

func (t *Tester) measureOnce(ctx context.Context) {
	nodes, err := t.nodes.ListAllNodes(ctx)
	if err != nil {
		t.logger.Warn("list nodes for speed test", "error", err)
		return
	}
	pick := t.pickCandidates(nodes)
	if len(pick) == 0 {
		return
	}
	sem := make(chan struct{}, t.cfg.Concurrency)
	for _, n := range pick {
		if t.budget == nil || !t.budget.Allow(t.cfg.DownloadSize) {
			t.logger.Info("speed test skipped: daily budget exhausted")
			return
		}
		n := n
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			t.measureNode(ctx, n)
		}()
	}
	for i := 0; i < t.cfg.Concurrency; i++ {
		sem <- struct{}{}
	}
}

// pickCandidates returns up to CandidateCount nodes sorted by best EWMA
// latency (only nodes with at least one successful probe).
func (t *Tester) pickCandidates(nodes []domain.Node) []domain.Node {
	type cand struct {
		node domain.Node
		ewma time.Duration
	}
	var list []cand
	for _, n := range nodes {
		h, err := t.store.GetNodeHealth(context.Background(), n.ID)
		if err != nil || h.SuccessCount == 0 {
			continue
		}
		list = append(list, cand{node: n, ewma: h.EWMA})
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].ewma != list[j].ewma {
			return list[i].ewma < list[j].ewma
		}
		return list[i].node.ID < list[j].node.ID
	})
	if len(list) > t.cfg.CandidateCount {
		list = list[:t.cfg.CandidateCount]
	}
	out := make([]domain.Node, 0, len(list))
	for _, c := range list {
		out = append(out, c.node)
	}
	return out
}

func (t *Tester) measureNode(ctx context.Context, n domain.Node) {
	bps, err := t.measure.Measure(ctx, n, t.cfg.DownloadSize)
	if err != nil {
		t.logger.Debug("speed test skipped", "node", n.ID, "error", err)
		return
	}
	if bps <= 0 {
		return
	}
	if t.budget != nil {
		t.budget.Note(t.cfg.DownloadSize)
	}
	now := t.clock.Now()
	if err := t.store.RecordThroughput(ctx, n.ID, bps, now); err != nil {
		t.logger.Warn("persist throughput", "node", n.ID, "error", err)
	}
	t.logger.Info("speed test complete", "node", n.ID, "bps", int64(bps))
	t.publish(ctx, n.ID, bps)
}

func (t *Tester) publish(ctx context.Context, nodeID string, bps float64) {
	if t.bus == nil {
		return
	}
	e := domain.Event{
		Type:    domain.EvtSpeedTested,
		At:      t.clock.Now(),
		Payload: map[string]any{"node_id": nodeID, "bytes_per_sec": bps},
	}
	if err := t.bus.Publish(ctx, e); err != nil {
		t.logger.Warn("publish speed event", "error", err)
	}
}

// sleepJittered sleeps a randomized first delay then a jittered interval
// (FR-410). First run waits only a short random delay so a fresh install gets
// a baseline quickly.
func (t *Tester) sleepJittered(ctx context.Context, interval time.Duration) bool {
	delay := interval/20 + time.Duration(t.rand.Float64()*float64(interval/4))
	if t.rand.Float64() < 0.5 {
		delay = t.jitter(interval)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (t *Tester) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := t.rand.Float64()*0.2 - 0.1
	return d + time.Duration(float64(d)*f)
}
