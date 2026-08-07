package repository

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func newTestRepo(t *testing.T) *Repo {
	t.Helper()
	r, err := New(filepath.Join(t.TempDir(), "gateway.db"), fixedClock{t: time.Unix(1_700_000_000, 0).UTC()})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func testSub(url string) *domain.Subscription {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &domain.Subscription{
		URL:         url,
		Name:        "provider-a",
		Provider:    "p-a",
		Priority:    10,
		Enabled:     true,
		UpdateEvery: 6 * time.Hour,
		State:       domain.SubscriptionActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func testNode(name, server string, port int) domain.Node {
	params := map[string]string{"uuid": "abc"}
	return domain.Node{
		DedupeHash: domain.NodeDedupeHash(server, port, domain.ProtocolVless, params),
		Protocol:   domain.ProtocolVless,
		Server:     server,
		Port:       port,
		Name:       name,
		Params:     params,
	}
}

func TestSaveAndGetSubscription(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s := testSub("https://example.com/sub")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatalf("SaveSubscription: %v", err)
	}
	if s.ID == "" {
		t.Fatal("SaveSubscription must assign an ID")
	}
	got, err := r.GetSubscription(ctx, s.ID)
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.URL != s.URL || got.Priority != 10 || !got.Enabled || got.UpdateEvery != 6*time.Hour {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	// Save again: ID must be preserved (keyed by URL).
	s.Name = "renamed"
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatalf("resave: %v", err)
	}
	list, err := r.ListSubscriptions(ctx)
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	if len(list) != 1 || list[0].ID != s.ID || list[0].Name != "renamed" {
		t.Fatalf("expected single preserved row, got %+v", list)
	}
}

func TestIngestNodesAndSoftDelete(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s := testSub("https://example.com/sub")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}

	nodes := []domain.Node{testNode("a1", "a.example.com", 443), testNode("a2", "b.example.com", 443)}
	res, err := r.IngestNodes(ctx, s.ID, nodes)
	if err != nil {
		t.Fatalf("IngestNodes: %v", err)
	}
	if res.Added != 2 || res.Updated != 0 {
		t.Fatalf("first ingest: %+v", res)
	}
	if nodes[0].ID == "" {
		t.Fatal("node ID must be assigned")
	}

	// Re-ingest the same hashes: updates, IDs preserved.
	ids := []string{nodes[0].ID, nodes[1].ID}
	res, err = r.IngestNodes(ctx, s.ID, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 0 || res.Updated != 2 {
		t.Fatalf("re-ingest: %+v", res)
	}
	if nodes[0].ID != ids[0] || nodes[1].ID != ids[1] {
		t.Fatal("IDs must be stable across refreshes (FR-206)")
	}

	count, err := r.CountNodes(ctx)
	if err != nil || count != 2 {
		t.Fatalf("CountNodes = %d, %v", count, err)
	}

	// One node disappears from the source -> soft deleted (FR-301).
	kept := []string{nodes[0].DedupeHash}
	removed, err := r.SoftDeleteNodesBySub(ctx, s.ID, kept)
	if err != nil {
		t.Fatalf("SoftDeleteNodesBySub: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	list, err := r.ListNodes(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != nodes[0].ID {
		t.Fatalf("expected only kept node, got %+v", list)
	}
}

func TestDedupeAcrossSubscriptionsFirstWins(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s1 := testSub("https://a.example.com/sub")
	s2 := testSub("https://b.example.com/sub")
	if err := r.SaveSubscription(ctx, s1); err != nil {
		t.Fatal(err)
	}
	if err := r.SaveSubscription(ctx, s2); err != nil {
		t.Fatal(err)
	}

	node := testNode("shared", "s.example.com", 443)
	nodes := []domain.Node{node}
	if _, err := r.IngestNodes(ctx, s1.ID, nodes); err != nil {
		t.Fatal(err)
	}
	owned := nodes[0].ID

	node2 := testNode("shared-2", "s.example.com", 443)
	nodes2 := []domain.Node{node2}
	res, err := r.IngestNodes(ctx, s2.ID, nodes2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Skipped != 1 {
		t.Fatalf("expected skip for duplicate across sources, got %+v", res)
	}
	if nodes2[0].ID == owned {
		t.Fatal("skipped node must not own the row")
	}
}

func TestMeasurementsBoundedTo5000(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	s := testSub("https://example.com/sub")
	if err := r.SaveSubscription(ctx, s); err != nil {
		t.Fatal(err)
	}
	nodes := []domain.Node{testNode("n", "m.example.com", 443)}
	if _, err := r.IngestNodes(ctx, s.ID, nodes); err != nil {
		t.Fatal(err)
	}
	id := nodes[0].ID

	clock := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 6000; i++ {
		clock = clock.Add(time.Second)
		r.clock = fixedClock{t: clock}
		if err := r.AddMeasurement(ctx, id, 42*time.Millisecond, i%10 != 0); err != nil {
			t.Fatalf("AddMeasurement: %v", err)
		}
	}

	n, err := r.CountMeasurements(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n > maxMeasurementsPerNode {
		t.Fatalf("measurements = %d, cap is %d", n, maxMeasurementsPerNode)
	}

	h, err := r.GetNodeHealth(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if h.Probes != n {
		t.Fatalf("health probes = %d, want %d", h.Probes, n)
	}
	if h.LastProbedAt.IsZero() {
		t.Fatal("LastProbedAt must be set")
	}
}
