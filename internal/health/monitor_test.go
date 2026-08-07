package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeProber struct {
	mu   sync.Mutex
	res  map[string]Result // per-node canned result
	call int
}

func (f *fakeProber) Probe(_ context.Context, n domain.Node) Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.call++
	if r, ok := f.res[n.ID]; ok {
		return r
	}
	return Result{OK: true, Latency: 10 * time.Millisecond}
}

func (f *fakeProber) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.call
}

type fakeDeepProber struct {
	lat time.Duration
	err error
}

func (f *fakeDeepProber) DeepProbe(context.Context, string, time.Duration) (time.Duration, error) {
	return f.lat, f.err
}

type fakeNodeSource struct {
	nodes []domain.Node
}

func (f *fakeNodeSource) ListAllNodes(context.Context) ([]domain.Node, error) {
	return append([]domain.Node(nil), f.nodes...), nil
}

type fakeHealthStore struct {
	mu      sync.Mutex
	meas    []probeRec
	states  map[string]domain.NodeState
	measErr error
}

type probeRec struct {
	node    string
	latency time.Duration
	success bool
}

func (f *fakeHealthStore) AddMeasurement(_ context.Context, nodeID string, latency time.Duration, success bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.measErr != nil {
		return f.measErr
	}
	f.meas = append(f.meas, probeRec{node: nodeID, latency: latency, success: success})
	return nil
}

// GetNodeHealth folds the recorded measurements like the real repository.
func (f *fakeHealthStore) GetNodeHealth(_ context.Context, nodeID string) (domain.NodeHealth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var samples []domain.ProbeSample
	for i, m := range f.meas {
		if m.node != nodeID {
			continue
		}
		samples = append(samples, domain.ProbeSample{
			At:      time.Unix(0, int64(i+1)),
			Latency: m.latency,
			Success: m.success,
		})
	}
	if len(samples) > 0 {
		return domain.FoldHealth(nodeID, samples, domain.FoldConfig{}, time.Unix(0, int64(len(samples)))), nil
	}
	return domain.NodeHealth{NodeID: nodeID}, nil
}

func (f *fakeHealthStore) SetNodeState(_ context.Context, id string, state domain.NodeState, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states == nil {
		f.states = map[string]domain.NodeState{}
	}
	f.states[id] = state
	return nil
}

func (f *fakeHealthStore) state(id string) domain.NodeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[id]
}

func (f *fakeHealthStore) measurementCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.meas)
}

func newHealthStore() *fakeHealthStore {
	return &fakeHealthStore{states: map[string]domain.NodeState{}}
}

// fixedRand returns a deterministic random source (all Float64=0.5, Intn=i%n).
type fixedRand struct {
	i int
}

func (r *fixedRand) Float64() float64 { return 0.5 }
func (r *fixedRand) Intn(n int) int   { r.i++; return (r.i * 7) % n }

// fixedClock always reports a given instant.
type fixedClock struct {
	t time.Time
}

func (c *fixedClock) Now() time.Time { return c.t }

func TestMonitorSetActiveAndActive(t *testing.T) {
	m := New(Config{}, nil, nil, &fakeNodeSource{}, &fakeHealthStore{}, nil, nil, nil, nil, nil)
	m.SetActive("node-x")
	if got := m.Active(); got != "node-x" {
		t.Fatalf("Active = %q, want node-x", got)
	}
}

func TestMonitorProbeRecordsAndAppliesHealthy(t *testing.T) {
	clk := &fixedClock{t: time.Unix(0, 0)}
	store := newHealthStore()
	m := New(Config{MinInterval: time.Second}, &fakeProber{}, nil,
		&fakeNodeSource{nodes: []domain.Node{mkNode("a")}}, store,
		nil, clk, nil, nil, nil)
	m.probeNodes(context.Background(), []domain.Node{mkNode("a")})

	if store.measurementCount() != 1 {
		t.Fatalf("expected 1 measurement, got %d", store.measurementCount())
	}
	if got := store.state("a"); got != domain.StateHealthy {
		t.Fatalf("state = %s, want healthy", got)
	}
}

func TestMonitorConsecutiveFailuresToFailed(t *testing.T) {
	store := newHealthStore()
	// Seed two prior failures; the probe below is the third.
	store.AddMeasurement(context.Background(), "a", 0, false)
	store.AddMeasurement(context.Background(), "a", 0, false)
	bus := &fakeBus{}
	fp := &fakeProber{res: map[string]Result{
		"a": {OK: false, Stage: StageDial, Detail: "refused"},
	}}
	m := New(Config{FailoverFailures: 3}, fp, nil,
		&fakeNodeSource{}, store, nil, &fixedClock{}, nil, nil, bus)
	m.probeNode(context.Background(), mkNode("a"), false)

	if got := store.state("a"); got != domain.StateFailed {
		t.Fatalf("state = %s, want failed", got)
	}
	types := bus.types()
	if !containsType(types, domain.EvtNodeFailed) {
		t.Fatalf("expected EvtNodeFailed, got %v", types)
	}
}

func TestMonitorFailedRecoversThroughRecovering(t *testing.T) {
	// Node is currently FAILED. A healthy probe must stage through RECOVERING.
	store := newHealthStore()
	fp := &fakeProber{res: map[string]Result{"a": {OK: true, Latency: 5 * time.Millisecond}}}
	m := New(Config{}, fp, nil, &fakeNodeSource{}, store,
		nil, &fixedClock{}, nil, nil, nil)
	n := mkNode("a")
	n.State = domain.StateFailed
	m.probeNode(context.Background(), n, false)

	if got := store.state("a"); got != domain.StateRecovering {
		t.Fatalf("state = %s, want recovering (staged)", got)
	}
}

func TestMonitorDueNodesHonorsMinInterval(t *testing.T) {
	clk := &fakeClock{}
	clk.Set(time.Unix(0, 0))
	m := New(Config{MinInterval: 30 * time.Second}, nil, nil,
		&fakeNodeSource{}, &fakeHealthStore{}, nil, clk, nil, nil, nil)
	// A node probed at t0 is not due again until MinInterval elapses.
	if got := len(m.dueNodes([]domain.Node{mkNode("a")})); got != 1 {
		t.Fatalf("first probe must be due, got %d", got)
	}
	m.lastProbe["a"] = clk.Now()
	if got := len(m.dueNodes([]domain.Node{mkNode("a")})); got != 0 {
		t.Fatalf("dueNodes = %d, want 0 within the min interval", got)
	}
	clk.Advance(31 * time.Second)
	if got := len(m.dueNodes([]domain.Node{mkNode("a")})); got != 1 {
		t.Fatalf("dueNodes = %d, want 1 after the min interval", got)
	}
}

func TestMonitorJitterDeterministic(t *testing.T) {
	m := New(Config{}, nil, nil, &fakeNodeSource{}, &fakeHealthStore{},
		nil, &fixedClock{t: time.Unix(0, 0)}, &fixedRand{}, nil, nil)
	base := 100 * time.Second
	a := m.jitter(base)
	b := m.jitter(base)
	if a != b {
		t.Fatalf("jitter with a fixed rand must be deterministic: %v vs %v", a, b)
	}
	if a < base-10*time.Second || a > base+10*time.Second {
		t.Fatalf("jitter must stay within ±10%%, got %v for %v", a, base)
	}
}

func TestHealthStateFor(t *testing.T) {
	cfg := Config{HardLatency: 3 * time.Second, FailoverFailures: 3}
	cases := []struct {
		name string
		h    domain.NodeHealth
		want domain.NodeState
	}{
		{"empty", domain.NodeHealth{}, domain.StateUnknown},
		{"failed", domain.NodeHealth{Probes: 3, ConsecutiveFailures: 3, FailureCount: 3}, domain.StateFailed},
		{"unstable-loss", domain.NodeHealth{Probes: 2, FailureCount: 1, SuccessCount: 1, LossRate: 0.5}, domain.StateUnstable},
		{"unstable-slow", domain.NodeHealth{Probes: 1, SuccessCount: 1, EWMA: 4 * time.Second}, domain.StateUnstable},
		{"degraded-loss", domain.NodeHealth{Probes: 4, FailureCount: 1, SuccessCount: 3, LossRate: 0.25}, domain.StateDegraded},
		{"degraded-slow", domain.NodeHealth{Probes: 1, SuccessCount: 1, EWMA: 2 * time.Second}, domain.StateDegraded},
		{"healthy", domain.NodeHealth{Probes: 1, SuccessCount: 1, EWMA: 100 * time.Millisecond}, domain.StateHealthy},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := healthStateFor(c.h, cfg); got != c.want {
				t.Fatalf("healthStateFor = %s, want %s", got, c.want)
			}
		})
	}
}

func TestCoerceTransitionRecoveryStaging(t *testing.T) {
	if got := coerceTransition(domain.StateFailed, domain.StateHealthy); got != domain.StateRecovering {
		t.Fatalf("FAILED->HEALTHY must stage through RECOVERING, got %s", got)
	}
	if got := coerceTransition(domain.StateUnknown, domain.StateHealthy); got != domain.StateHealthy {
		t.Fatalf("UNKNOWN->HEALTHY should pass through, got %s", got)
	}
	if got := coerceTransition(domain.StateHealthy, domain.StateDegraded); got != domain.StateDegraded {
		t.Fatalf("HEALTHY->DEGRADED should pass through, got %s", got)
	}
}

func containsType(types []domain.EventType, want domain.EventType) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}
