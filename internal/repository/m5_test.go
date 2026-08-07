package repository

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func TestListSwitchRecordsNewestFirstAndLimit(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()

	for _, rec := range []domain.SwitchRecord{
		{ID: "r1", At: base.Add(time.Minute), FromID: "a", ToID: "b", Reason: "priority", PrevScore: 0.1, NewScore: 0.2},
		{ID: "r2", At: base.Add(3 * time.Minute), FromID: "b", ToID: "c", Reason: "failover", PrevScore: 0.2, NewScore: 0.0},
		{ID: "r3", At: base.Add(2 * time.Minute), FromID: "c", ToID: "d", Reason: "override", PrevScore: 0.3, NewScore: 0.4},
	} {
		if err := r.RecordSwitch(ctx, rec); err != nil {
			t.Fatalf("RecordSwitch %s: %v", rec.ID, err)
		}
	}

	all, err := r.ListSwitchRecords(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].ID != "r2" || all[1].ID != "r3" || all[2].ID != "r1" {
		t.Fatalf("expected newest-first [r2 r3 r1], got %+v", all)
	}

	limited, err := r.ListSwitchRecords(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 || limited[0].ID != "r2" || limited[1].ID != "r3" {
		t.Fatalf("limit ignored: %+v", limited)
	}
}

func TestDeleteSubscriptionCascades(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s := testSub("https://example.com/delete")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{testNode("a", "a.example.com", 443), testNode("b", "b.example.com", 443)}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}

	existed, err := r.DeleteSubscription(ctx, s.ID)
	if err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if !existed {
		t.Fatal("expected first delete to report the subscription existed")
	}
	got, err := r.GetSubscription(ctx, s.ID)
	if err != nil || got != nil {
		t.Fatalf("GetSubscription after delete = %+v, %v; want nil", got, err)
	}
	count, err := r.CountNodes(ctx)
	if err != nil || count != 0 {
		t.Fatalf("nodes after cascade delete = %d, %v; want 0", count, err)
	}

	again, err := r.DeleteSubscription(ctx, s.ID)
	if err != nil || again {
		t.Fatalf("second delete = %v, %v; want false", again, err)
	}
}

func TestCountNodesByState(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()

	s := testSub("https://example.com/states")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{
		testNode("h", "h.example.com", 443),
		testNode("f", "f.example.com", 443),
		testNode("d", "d.example.com", 443),
	}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodeState(ctx, nodes[0].ID, domain.StateHealthy, now); err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodeState(ctx, nodes[1].ID, domain.StateFailed, now); err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodeState(ctx, nodes[2].ID, domain.StateDegraded, now); err != nil {
		t.Fatal(err)
	}

	byState, err := r.CountNodesByState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if byState[domain.StateHealthy] != 1 || byState[domain.StateFailed] != 1 || byState[domain.StateDegraded] != 1 {
		t.Fatalf("by_state = %+v", byState)
	}
}

func TestListScores(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()

	s := testSub("https://example.com/scores")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{testNode("a", "a.example.com", 443), testNode("b", "b.example.com", 443)}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}
	for i, n := range nodes {
		if err := r.UpsertScore(ctx, domain.NodeScore{
			NodeID: n.ID,
			Score:  domain.Score{NodeID: n.ID, Total: 0.4 + float64(i)*0.2, Eligible: true, EvaluatedAt: now},
			State:  domain.ScoreEligible, EvaluatedAt: now,
		}); err != nil {
			t.Fatalf("UpsertScore %d: %v", i, err)
		}
	}

	all, err := r.ListScores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(all))
	}
	totals := map[string]float64{}
	for _, ns := range all {
		totals[ns.NodeID] = ns.Score.Total
	}
	close := func(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }
	if !close(totals[nodes[0].ID], 0.4) || !close(totals[nodes[1].ID], 0.6) {
		t.Fatalf("totals = %+v", totals)
	}
}
