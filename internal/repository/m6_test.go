package repository

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func TestListHealthsFoldsAllNodes(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s := testSub("https://example.com/healths")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{testNode("a", "a.example.com", 443), testNode("b", "b.example.com", 443)}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}

	clock := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 4; i++ {
		clock = clock.Add(time.Second)
		r.clock = fixedClock{t: clock}
		if err := r.AddMeasurement(ctx, nodes[0].ID, 50*time.Millisecond, true); err != nil {
			t.Fatal(err)
		}
		if err := r.AddMeasurement(ctx, nodes[1].ID, 200*time.Millisecond, false); err != nil {
			t.Fatal(err)
		}
	}
	r.clock = fixedClock{t: clock}
	if err := r.RecordThroughput(ctx, nodes[0].ID, 12_345.0, clock); err != nil {
		t.Fatal(err)
	}

	healths, err := r.ListHealths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(healths) != 2 {
		t.Fatalf("ListHealths = %d, want 2", len(healths))
	}
	byID := map[string]domain.NodeHealth{}
	for _, h := range healths {
		byID[h.NodeID] = h
	}
	if h := byID[nodes[0].ID]; h.Probes != 4 || h.FailureCount != 0 {
		t.Fatalf("node a health = %+v", h)
	}
	if h := byID[nodes[1].ID]; h.Probes != 4 || h.FailureCount != 4 {
		t.Fatalf("node b health = %+v", h)
	}
	if h := byID[nodes[0].ID]; h.ThroughputBps != 12_345.0 {
		t.Fatalf("node a throughput = %f", h.ThroughputBps)
	}

	// Single-node view must agree with the batched fold.
	single, err := r.GetNodeHealth(ctx, nodes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	batched := byID[nodes[0].ID]
	if single.Probes != batched.Probes || single.FailureCount != batched.FailureCount || single.EWMA != batched.EWMA {
		t.Fatalf("single %+v != batched %+v", single, batched)
	}
}

func TestDataSizeTracksWAL(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	before, err := r.DataSize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before <= 0 {
		t.Fatalf("fresh database must occupy disk, got %d bytes", before)
	}

	s := testSub("https://example.com/size")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{testNode("big", "big.example.com", 443)}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		r.clock = fixedClock{t: time.Unix(1_700_000_000+int64(i), 0).UTC()}
		if err := r.AddMeasurement(ctx, nodes[0].ID, 10*time.Millisecond, true); err != nil {
			t.Fatal(err)
		}
	}
	after, err := r.DataSize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("size did not grow: before %d, after %d", before, after)
	}
}
