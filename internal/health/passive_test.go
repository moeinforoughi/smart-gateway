package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeSampler struct {
	mu  sync.Mutex
	cur []domain.ConnMetric
	err error
}

func (f *fakeSampler) set(m []domain.ConnMetric) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cur = m
}

func (f *fakeSampler) Connections(context.Context) ([]domain.ConnMetric, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return append([]domain.ConnMetric(nil), f.cur...), nil
}

type fakeThroughputStore struct {
	mu  sync.Mutex
	rec []throughputRec
	err error
}

type throughputRec struct {
	node string
	bps  float64
	at   time.Time
}

func (f *fakeThroughputStore) RecordThroughput(_ context.Context, nodeID string, bps float64, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.rec = append(f.rec, throughputRec{node: nodeID, bps: bps, at: at})
	return nil
}

func (f *fakeThroughputStore) all() []throughputRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]throughputRec(nil), f.rec...)
}

func TestCollectorSeedsFirstSample(t *testing.T) {
	s := &fakeSampler{}
	store := &fakeThroughputStore{}
	c := NewCollector(s, store, time.Minute, &fakeClock{}, nil, nil)

	s.set([]domain.ConnMetric{{NodeID: "a", Upload: 100, Download: 200}})
	if err := c.sampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.all(); len(got) != 0 {
		t.Fatalf("first sample must seed only, got %d records", len(got))
	}
}

func TestCollectorComputesBPS(t *testing.T) {
	clk := &fakeClock{}
	s := &fakeSampler{}
	store := &fakeThroughputStore{}
	c := NewCollector(s, store, time.Minute, clk, nil, nil)

	clk.Set(time.Unix(0, 0))
	s.set([]domain.ConnMetric{{NodeID: "a", Upload: 0, Download: 0}})
	if err := c.sampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 10s later: 1000 new download bytes over 10s -> 100 B/s.
	clk.Advance(10 * time.Second)
	s.set([]domain.ConnMetric{{NodeID: "a", Upload: 0, Download: 1000}})
	if err := c.sampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := store.all()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if recs[0].bps != 100 {
		t.Fatalf("bps = %v, want 100", recs[0].bps)
	}
}

func TestCollectorSkipsOnCounterReset(t *testing.T) {
	clk := &fakeClock{}
	s := &fakeSampler{}
	store := &fakeThroughputStore{}
	c := NewCollector(s, store, time.Minute, clk, nil, nil)

	clk.Set(time.Unix(0, 0))
	s.set([]domain.ConnMetric{{NodeID: "a", Upload: 100, Download: 200}})
	_ = c.sampleOnce(context.Background())

	clk.Advance(10 * time.Second)
	// Core restarted: counters went down -> must reseed, not record.
	s.set([]domain.ConnMetric{{NodeID: "a", Upload: 0, Download: 0}})
	_ = c.sampleOnce(context.Background())

	if got := store.all(); len(got) != 0 {
		t.Fatalf("counter reset must not record, got %d records", len(got))
	}
}

func TestCollectorPublishesMetricsEvent(t *testing.T) {
	s := &fakeSampler{}
	bus := &fakeBus{}
	c := NewCollector(s, &fakeThroughputStore{}, time.Minute, &fakeClock{}, nil, bus)
	if err := c.sampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if types := bus.types(); len(types) != 1 || types[0] != domain.EvtMetricsUpdated {
		t.Fatalf("expected EvtMetricsUpdated, got %v", types)
	}
}
