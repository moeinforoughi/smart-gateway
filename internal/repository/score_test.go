package repository

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func TestUpsertAndGetNodeScore(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	nodeID := seedNode(t, r, ctx)

	ns := domain.NodeScore{
		NodeID: nodeID,
		Score: domain.Score{
			NodeID:      nodeID,
			Latency:     0.9,
			Stability:   0.8,
			Throughput:  0.5,
			Loss:        1.0,
			Total:       0.845,
			Confidence:  5,
			Eligible:    true,
			EvaluatedAt: now,
		},
		State:       domain.ScoreEligible,
		EvaluatedAt: now,
		Reason:      "eligible",
	}
	if err := r.UpsertScore(ctx, ns); err != nil {
		t.Fatalf("UpsertScore: %v", err)
	}

	got, err := r.GetNodeScore(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetNodeScore: %v", err)
	}
	if got.Score.Total != 0.845 || !got.Score.Eligible || got.State != domain.ScoreEligible {
		t.Fatalf("round-trip mismatch: %+v", got.Score)
	}
	if got.Score.Latency != 0.9 || got.Score.Stability != 0.8 || got.Score.Loss != 1.0 {
		t.Fatalf("component mismatch: %+v", got.Score)
	}
	if !got.EvaluatedAt.Equal(now) {
		t.Fatalf("evaluated_at mismatch: %v", got.EvaluatedAt)
	}
}

func TestTopEligibleOrdersAndFilters(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()

	// One subscription with three nodes: a best, a mid, and an ineligible one.
	sub := testSub("https://example.com/score-order")
	if err := r.SaveSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{
		testNode("low", "low.example.com", 443),
		testNode("best", "best.example.com", 443),
		testNode("mid", "mid.example.com", 443),
	}
	res, err := r.IngestNodes(ctx, sub.ID, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 3 {
		t.Fatalf("expected 3 added, got %d", res.Added)
	}
	for i, n := range nodes {
		if n.ID == "" {
			t.Fatalf("node %d has no id", i)
		}
	}

	best, mid, low := nodes[1], nodes[2], nodes[0]
	upsert := func(id string, total float64, eligible bool) {
		t.Helper()
		if err := r.UpsertScore(ctx, domain.NodeScore{
			NodeID: id,
			Score: domain.Score{NodeID: id, Total: total, Eligible: eligible,
				EvaluatedAt: now},
			State:       scoreStateForTest(eligible),
			EvaluatedAt: now,
		}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	upsert(best.ID, 0.9, true)
	upsert(mid.ID, 0.7, true)
	upsert(low.ID, 0.95, false) // ineligible must not appear

	top, err := r.TopEligible(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 2 {
		t.Fatalf("expected 2 eligible, got %d", len(top))
	}
	if top[0].NodeID != best.ID || top[1].NodeID != mid.ID {
		t.Fatalf("expected [%s %s], got [%s %s]", best.ID, mid.ID, top[0].NodeID, top[1].NodeID)
	}
	for _, s := range top {
		if !s.Eligible() {
			t.Fatalf("top candidate %s not eligible", s.NodeID)
		}
	}
}

func TestScoreHistoryNewestFirst(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	nodeID := seedNode(t, r, ctx)

	t1 := time.Unix(1_700_000_000, 0).UTC()
	t2 := t1.Add(2 * time.Minute)
	for i, at := range []time.Time{t1, t2} {
		err := r.UpsertScore(ctx, domain.NodeScore{
			NodeID:      nodeID,
			Score:       domain.Score{NodeID: nodeID, Total: 0.5 + float64(i)*0.1, Eligible: true, EvaluatedAt: at},
			State:       domain.ScoreEligible,
			EvaluatedAt: at,
			Reason:      "probe",
		})
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	hist, err := r.ScoreHistory(ctx, nodeID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(hist))
	}
	if !hist[0].EvaluatedAt.Equal(t2) || !hist[1].EvaluatedAt.Equal(t1) {
		t.Fatalf("expected newest first, got %v then %v", hist[0].EvaluatedAt, hist[1].EvaluatedAt)
	}
}

func TestTopEligibleAfterIneligibleUpdate(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	nodeID := seedNode(t, r, ctx)

	now := time.Unix(1_700_000_000, 0).UTC()
	// Eligible now...
	err := r.UpsertScore(ctx, domain.NodeScore{
		NodeID:      nodeID,
		Score:       domain.Score{NodeID: nodeID, Total: 0.8, Eligible: true, EvaluatedAt: now},
		State:       domain.ScoreEligible,
		EvaluatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	top, err := r.TopEligible(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 {
		t.Fatalf("expected 1 top, got %d", len(top))
	}
	// ...then the node goes stale and becomes ineligible: the materialized row
	// must be replaced, not stacked.
	err = r.UpsertScore(ctx, domain.NodeScore{
		NodeID:      nodeID,
		Score:       domain.Score{NodeID: nodeID, Total: 0.3, Eligible: false, EvaluatedAt: now},
		State:       domain.ScoreCalculated,
		EvaluatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	top, err = r.TopEligible(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 0 {
		t.Fatalf("expected no eligible after update, got %d", len(top))
	}
}

// seedNode creates one subscription and one node, returning the node ID.
func seedNode(t *testing.T, r *Repo, ctx context.Context) string {
	t.Helper()
	sub := testSub("https://example.com/score")
	if err := r.SaveSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	res, err := r.IngestNodes(ctx, sub.ID, []domain.Node{testNode("n1", "n1.example.com", 443)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 {
		t.Fatalf("expected 1 node added, got %d", res.Added)
	}
	ids, err := r.ListAllNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 node, got %d", len(ids))
	}
	return ids[0].ID
}

func scoreStateForTest(eligible bool) domain.ScoreState {
	if eligible {
		return domain.ScoreEligible
	}
	return domain.ScoreCalculated
}
