package routing

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

// stubPolicy returns a fixed decision; usable for policy wiring tests.
type stubPolicy struct {
	nodeID string
	reason string
	err    error
}

func (s stubPolicy) Decide(_ context.Context, _ []domain.Node, _ string) (string, string, error) {
	return s.nodeID, s.reason, s.err
}

// keepCurrentPolicy deliberately returns no change.
type keepCurrentPolicy struct{}

func (keepCurrentPolicy) Decide(_ context.Context, _ []domain.Node, _ string) (string, string, error) {
	return "", "keep current", nil
}

func policyTestManager(policy domain.SelectionPolicy, bus *fakeBus) *Manager {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return New(&fakeApplier{}, &fakeStore{}, Config{}, policy, nil, fakeClock{now: now}, nil, bus)
}

func TestEnsureActiveAppliesPolicyDecision(t *testing.T) {
	applier := &fakeApplier{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := New(applier, &fakeStore{}, Config{}, stubPolicy{nodeID: "n3", reason: "switch: candidate beats current"},
		nil, fakeClock{now: now}, nil, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1), node("n2", "s1", 2), node("n3", "s2", 3)}

	if err := m.EnsureActive(context.Background(), nodes); err != nil {
		t.Fatal(err)
	}
	cur, reason := m.Current()
	if cur != "n3" || reason != "switch: candidate beats current" {
		t.Fatalf("expected policy decision n3, got %s (reason %s)", cur, reason)
	}
	selections := applier.selections()
	if len(selections) != 1 || selections[0] != "n3" {
		t.Fatalf("expected exactly one apply to n3, got %v", selections)
	}
}

func TestEnsureActiveKeepsCurrentWhenPolicyReturnsEmpty(t *testing.T) {
	applier := &fakeApplier{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := New(applier, &fakeStore{}, Config{}, keepCurrentPolicy{}, nil,
		fakeClock{now: now}, nil, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1), node("n2", "s2", 10)}

	// First call has no current → priority bootstrap picks n2.
	if err := m.EnsureActive(context.Background(), nodes); err != nil {
		t.Fatal(err)
	}
	if cur, _ := m.Current(); cur != "n2" {
		t.Fatalf("expected bootstrap n2, got %s", cur)
	}
	// Policy returns empty while a current node exists → no change.
	if err := m.EnsureActive(context.Background(), nodes); err != nil {
		t.Fatal(err)
	}
	if cur, _ := m.Current(); cur != "n2" {
		t.Fatalf("expected current kept as n2, got %s", cur)
	}
	if got := applier.selections(); len(got) != 1 {
		t.Fatalf("expected a single apply, got %v", got)
	}
}

func TestEnsureActivePolicyErrorPropagates(t *testing.T) {
	m := policyTestManager(stubPolicy{err: context.Canceled}, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1)}
	if err := m.EnsureActive(context.Background(), nodes); err == nil {
		t.Fatal("expected policy error to propagate")
	}
}
