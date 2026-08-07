package scoring

import (
	"context"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

type fakeClock struct{ t time.Time }

func (c fakeClock) Now() time.Time { return c.t }

type fakeEvents struct {
	ctx context.Context
	ch  chan domain.Event
}

func (f *fakeEvents) Subscribe(_ context.Context) (<-chan domain.Event, error) {
	return f.ch, nil
}

func (f *fakeEvents) emit(e domain.Event) {
	select {
	case f.ch <- e:
	case <-f.ctx.Done():
	}
}

type fakeHealth struct{ h domain.NodeHealth }

func (f *fakeHealth) GetNodeHealth(_ context.Context, nodeID string) (domain.NodeHealth, error) {
	f.h.NodeID = nodeID
	return f.h, nil
}

type fakeStore struct {
	mu    sync.Mutex
	saved []domain.NodeScore
}

func (f *fakeStore) UpsertScore(_ context.Context, ns domain.NodeScore) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, ns)
	return nil
}

func (f *fakeStore) GetNodeScore(context.Context, string) (domain.NodeScore, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saved) == 0 {
		return domain.NodeScore{}, errNoScores
	}
	return f.saved[len(f.saved)-1], nil
}

func (f *fakeStore) TopEligible(context.Context, int) ([]domain.NodeScore, error) {
	return nil, nil
}

func (f *fakeStore) ScoreHistory(context.Context, string, int) ([]domain.NodeScore, error) {
	return nil, nil
}

var errNoScores = context.Canceled

func (f *fakeStore) last() (domain.NodeScore, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saved) == 0 {
		return domain.NodeScore{}, false
	}
	return f.saved[len(f.saved)-1], true
}

type fakeBus struct {
	mu    sync.Mutex
	types []domain.EventType
}

func (f *fakeBus) Publish(_ context.Context, e domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.types = append(f.types, e.Type)
	return nil
}

func (f *fakeBus) has(t domain.EventType) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range f.types {
		if g == t {
			return true
		}
	}
	return false
}

func newTestEngine(t *testing.T, health domain.NodeHealth) (*Engine, *fakeEvents, *fakeStore, *fakeBus, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	events := &fakeEvents{ctx: ctx, ch: make(chan domain.Event, 8)}
	store := &fakeStore{}
	bus := &fakeBus{}
	cfg := DefaultConfig()
	now := time.Unix(1_700_000_000, 0).UTC()
	eng := NewEngine(events, &fakeHealth{h: health}, store, bus, cfg, fakeClock{t: now}, log.New(nil, 0))
	go eng.Run(ctx)
	t.Cleanup(cancel)
	return eng, events, store, bus, cancel
}

func TestEngineRescoresOnProbeEvent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	health := healthyHealth("n1", 20, now)
	_, events, store, bus, _ := newTestEngine(t, health)

	events.emit(domain.Event{Type: domain.EvtNodeProbed, At: now,
		Payload: map[string]any{"node_id": "n1", "ok": true, "latency_ms": 80}})

	deadline := time.After(2 * time.Second)
	for {
		ns, ok := store.last()
		if ok && ns.NodeID == "n1" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("engine did not rescore within timeout")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	ns, _ := store.last()
	if ns.State != domain.ScoreEligible {
		t.Fatalf("expected eligible state, got %s", ns.State)
	}
	if !bus.has(domain.EvtScoreUpdated) {
		t.Fatal("expected EvtScoreUpdated to be published")
	}
}

func TestEngineRescoresOnHealthChangeEvent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, events, store, _, _ := newTestEngine(t, healthyHealth("n1", 20, now))

	events.emit(domain.Event{Type: domain.EvtNodeHealthChanged, At: now,
		Payload: map[string]any{"node_id": "n1", "from": "healthy", "to": "degraded"}})

	deadline := time.After(2 * time.Second)
	for {
		ns, ok := store.last()
		if ok && ns.NodeID == "n1" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("engine did not rescore on health change")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if ns, _ := store.last(); ns.Score.Eligible != true {
		t.Fatalf("expected eligible score for healthy node, got %+v", ns.Score)
	}
}

func TestEngineIgnoresUnknownNodeEvents(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, events, store, _, _ := newTestEngine(t, healthyHealth("n1", 20, now))

	// A node idless event (e.g. metrics) must not cause a rescore.
	events.emit(domain.Event{Type: domain.EvtMetricsUpdated, At: now, Payload: nil})

	deadline := time.After(150 * time.Millisecond)
	for {
		select {
		case <-deadline:
			if ns, ok := store.last(); ok {
				t.Fatalf("unexpected rescore: %+v", ns)
			}
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
