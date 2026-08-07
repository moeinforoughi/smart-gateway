package speed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeClock struct {
	t time.Time
}

func (f *fakeClock) Now() time.Time { return f.t }

type fixedRand struct {
	f   float64
	seq []float64
}

func (r *fixedRand) Float64() float64 {
	if len(r.seq) > 0 {
		v := r.seq[0]
		r.seq = r.seq[1:]
		return v
	}
	return r.f
}

func (r *fixedRand) Intn(n int) int { return n / 2 }

type fakeMeasurer struct {
	mu   sync.Mutex
	res  map[string]float64 // nodeID -> bps
	err  error
	skip map[string]bool
}

func (f *fakeMeasurer) Measure(_ context.Context, n domain.Node, _ int64) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	if f.skip[n.ID] {
		return 0, ErrNotMeasurable
	}
	if bps, ok := f.res[n.ID]; ok {
		return bps, nil
	}
	return 0, errors.New("no data")
}

type fakeSpeedStore struct {
	mu   sync.Mutex
	rec  map[string]float64
	heal map[string]domain.NodeHealth
}

func (f *fakeSpeedStore) RecordThroughput(_ context.Context, nodeID string, bps float64, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rec == nil {
		f.rec = map[string]float64{}
	}
	f.rec[nodeID] = bps
	return nil
}

func (f *fakeSpeedStore) GetNodeHealth(_ context.Context, nodeID string) (domain.NodeHealth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.heal[nodeID]; ok {
		return h, nil
	}
	return domain.NodeHealth{NodeID: nodeID}, nil
}

func (f *fakeSpeedStore) recorded(nodeID string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.rec[nodeID]
	return v, ok
}

func mkNode(id string) domain.Node {
	return domain.Node{ID: id, Protocol: domain.ProtocolHTTP, Server: "127.0.0.1", Port: 8080}
}

func TestPickCandidatesSortsByEWMA(t *testing.T) {
	store := &fakeSpeedStore{heal: map[string]domain.NodeHealth{
		"slow": {NodeID: "slow", SuccessCount: 1, Probes: 1, EWMA: 300 * time.Millisecond},
		"fast": {NodeID: "fast", SuccessCount: 1, Probes: 1, EWMA: 20 * time.Millisecond},
		"new":  {NodeID: "new"}, // no probes -> excluded
	}}
	tr := New(Config{CandidateCount: 10}, nil, nil, store, nil, &fakeClock{}, &fixedRand{}, nil, nil)
	pick := tr.pickCandidates([]domain.Node{mkNode("slow"), mkNode("fast"), mkNode("new")})
	if len(pick) != 2 {
		t.Fatalf("pick = %d nodes, want 2 (new node excluded)", len(pick))
	}
	if pick[0].ID != "fast" {
		t.Fatalf("fastest EWMA should be first, got %s", pick[0].ID)
	}
}

func TestMeasureNodeRecordsAndPublishes(t *testing.T) {
	store := &fakeSpeedStore{heal: map[string]domain.NodeHealth{
		"a": {NodeID: "a", SuccessCount: 1, Probes: 1},
	}}
	bus := &fakeBus{}
	clk := &fakeClock{t: time.Unix(0, 0)}
	tr := New(Config{}, &fakeMeasurer{res: map[string]float64{"a": 1024}},
		nil, store, domain.NewBudget(1<<30, clk), clk, &fixedRand{}, nil, bus)

	tr.measureNode(context.Background(), mkNode("a"))

	bps, ok := store.recorded("a")
	if !ok || bps != 1024 {
		t.Fatalf("RecordThroughput: ok=%v bps=%v, want 1024", ok, bps)
	}
	if got := bus.types(); len(got) != 1 || got[0] != domain.EvtSpeedTested {
		t.Fatalf("expected EvtSpeedTested, got %v", got)
	}
}

func TestMeasureNodeSkipsUnmeasurable(t *testing.T) {
	store := &fakeSpeedStore{heal: map[string]domain.NodeHealth{
		"a": {NodeID: "a", SuccessCount: 1, Probes: 1},
	}}
	bus := &fakeBus{}
	tr := New(Config{}, &fakeMeasurer{skip: map[string]bool{"a": true}},
		nil, store, nil, &fakeClock{}, &fixedRand{}, nil, bus)
	tr.measureNode(context.Background(), mkNode("a"))
	if got := bus.types(); len(got) != 0 {
		t.Fatalf("unmeasurable node must publish nothing, got %v", got)
	}
}

func TestMeasureOnceHonorsBudget(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	store := &fakeSpeedStore{heal: map[string]domain.NodeHealth{
		"a": {NodeID: "a", SuccessCount: 1, Probes: 1},
	}}
	budget := domain.NewBudget(10, clk) // too small for the download size
	tr := New(Config{DownloadSize: 100, Concurrency: 2}, &fakeMeasurer{res: map[string]float64{"a": 1}},
		&fakeNodeSrc{nodes: []domain.Node{mkNode("a")}}, store, budget, clk, &fixedRand{}, nil, nil)

	tr.measureOnce(context.Background())
	if _, ok := store.recorded("a"); ok {
		t.Fatal("measurement must be skipped when the budget is exhausted")
	}
}

type fakeNodeSrc struct {
	nodes []domain.Node
}

func (f *fakeNodeSrc) ListAllNodes(context.Context) ([]domain.Node, error) {
	return append([]domain.Node(nil), f.nodes...), nil
}
