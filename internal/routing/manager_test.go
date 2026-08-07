package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeApplier struct {
	mu      sync.Mutex
	applied []string
	err     error
	current string
}

func (f *fakeApplier) ApplySelection(_ context.Context, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.applied = append(f.applied, nodeID)
	f.current = nodeID
	return nil
}

func (f *fakeApplier) Current(_ context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current, nil
}

func (f *fakeApplier) selections() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...)
}

type fakeStore struct {
	mu   sync.Mutex
	recs []domain.SwitchRecord
	err  error
}

func (f *fakeStore) RecordSwitch(_ context.Context, rec domain.SwitchRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.recs = append(f.recs, rec)
	return nil
}

func (f *fakeStore) records() []domain.SwitchRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.SwitchRecord(nil), f.recs...)
}

type fakeBus struct {
	mu  sync.Mutex
	evt []domain.Event
}

func (f *fakeBus) Publish(_ context.Context, e domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evt = append(f.evt, e)
	return nil
}

func (f *fakeBus) types() []domain.EventType {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.EventType
	for _, e := range f.evt {
		out = append(out, e.Type)
	}
	return out
}

type fakeClock struct{ now time.Time }

func (c fakeClock) Now() time.Time { return c.now }

func node(id, sub string, priority int) domain.Node {
	return domain.Node{
		ID:             id,
		SubscriptionID: sub,
		Priority:       priority,
		Protocol:       domain.ProtocolVless,
	}
}

func newTestManager(applier *fakeApplier, store *fakeStore, bus *fakeBus) *Manager {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return New(applier, store, Config{}, nil, nil, fakeClock{now: now}, nil, bus)
}

func TestEnsureActivePicksHighestPriority(t *testing.T) {
	applier := &fakeApplier{}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	nodes := []domain.Node{
		node("n1", "s1", 1),
		node("n2", "s2", 10),
		node("n3", "s1", 10),
	}
	if err := m.EnsureActive(context.Background(), nodes); err != nil {
		t.Fatalf("EnsureActive: %v", err)
	}
	// Highest priority wins; tie broken by earliest CreatedAt (zero here, so n2).
	cur, reason := m.Current()
	if cur != "n2" {
		t.Fatalf("expected n2, got %s (reason %s)", cur, reason)
	}
	sel := applier.selections()
	if len(sel) != 1 || sel[0] != "n2" {
		t.Fatalf("expected single apply of n2, got %v", sel)
	}
}

func TestEnsureActiveNoOpWhenUnchanged(t *testing.T) {
	applier := &fakeApplier{}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1)}
	ctx := context.Background()
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	if got := applier.selections(); len(got) != 1 {
		t.Fatalf("expected no re-apply, got %v", got)
	}
}

func TestOverrideForcesNodeAndReleaseRestoresPolicy(t *testing.T) {
	applier := &fakeApplier{}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1), node("n2", "s2", 10)}
	ctx := context.Background()
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	if err := m.Override(ctx, nodes, "n1", "admin"); err != nil {
		t.Fatalf("override: %v", err)
	}
	if cur, _ := m.Current(); cur != "n1" {
		t.Fatalf("expected override n1, got %s", cur)
	}
	if err := m.Release(ctx, nodes); err != nil {
		t.Fatalf("release: %v", err)
	}
	if cur, _ := m.Current(); cur != "n2" {
		t.Fatalf("expected policy restore n2, got %s", cur)
	}
}

func TestApplyFailureDoesNotChangeCurrent(t *testing.T) {
	applier := &fakeApplier{err: errors.New("clash unreachable")}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1)}
	if err := m.EnsureActive(context.Background(), nodes); err == nil {
		t.Fatal("expected error from applier")
	}
	if cur, _ := m.Current(); cur != "" {
		t.Fatalf("expected no current after failure, got %s", cur)
	}
}

func TestSwitchRecordedAndEventsPublished(t *testing.T) {
	applier := &fakeApplier{}
	store := &fakeStore{}
	bus := &fakeBus{}
	m := newTestManager(applier, store, bus)
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1", 1)}
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	// First selection records no switch (nothing to switch from).
	if recs := store.records(); len(recs) != 0 {
		t.Fatalf("expected no record on first selection, got %d", len(recs))
	}
	if err := m.Override(ctx, []domain.Node{node("n1", "s1", 1), node("n2", "s2", 10)}, "n2", "admin"); err != nil {
		t.Fatal(err)
	}
	recs := store.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 switch record, got %d", len(recs))
	}
	if recs[0].FromID != "n1" || recs[0].ToID != "n2" || recs[0].Reason != "admin" {
		t.Fatalf("unexpected record: %+v", recs[0])
	}
	got := bus.types()
	for _, want := range []domain.EventType{domain.EvtSelectionChanged, domain.EvtOutboundChanged} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing event %s in %v", want, got)
		}
	}
}

func TestOverrideUnknownNodeRejected(t *testing.T) {
	applier := &fakeApplier{}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	nodes := []domain.Node{node("n1", "s1", 1)}
	ctx := context.Background()
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	before := applier.selections()
	if err := m.Override(ctx, nodes, "nX", "admin"); err == nil {
		t.Fatal("expected error for unknown override node")
	}
	if cur, _ := m.Current(); cur != "n1" {
		t.Fatalf("expected unchanged n1, got %s", cur)
	}
	if got := applier.selections(); len(got) != len(before) {
		t.Fatalf("override applied unexpectedly: %v", got)
	}
}

// fakePolicy drives the M4 selection policy port deterministically.
type fakePolicy struct {
	nodeID string
	reason string
	err    error
}

func (p *fakePolicy) Decide(_ context.Context, _ []domain.Node, _ string) (string, string, error) {
	return p.nodeID, p.reason, p.err
}

func TestEvaluateNoPolicyReturnsError(t *testing.T) {
	m := newTestManager(&fakeApplier{}, &fakeStore{}, &fakeBus{}) // policy is nil
	_, err := m.Evaluate(context.Background(), []domain.Node{node("n1", "s1", 1)})
	if err == nil {
		t.Fatal("expected error without a selection policy")
	}
}

func TestEvaluateConfirmsCandidateWithoutApplying(t *testing.T) {
	applier := &fakeApplier{}
	m := newTestManager(applier, &fakeStore{}, &fakeBus{})
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1", 1), node("n2", "s2", 10)}
	if err := m.EnsureActive(ctx, nodes); err != nil { // current becomes n2
		t.Fatal(err)
	}
	m.policy = &fakePolicy{nodeID: "n1", reason: "scored better"}
	d, err := m.Evaluate(ctx, nodes)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d.CandidateID != "n1" || d.CurrentID != "n2" || d.Outcome != domain.SwitchConfirmed {
		t.Fatalf("decision = %+v", d)
	}
	// Dry-run: nothing may be applied.
	if got := applier.selections(); len(got) != 1 {
		t.Fatalf("Evaluate must not apply anything, got %v", got)
	}
	if cur, _ := m.Current(); cur != "n2" {
		t.Fatalf("current must stay n2, got %s", cur)
	}
}

func TestEvaluateMapsCooldownAndHysteresis(t *testing.T) {
	m := newTestManager(&fakeApplier{}, &fakeStore{}, &fakeBus{})
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1", 1), node("n2", "s2", 10)}
	if err := m.EnsureActive(ctx, nodes); err != nil { // current becomes n2
		t.Fatal(err)
	}
	for _, tc := range []struct {
		nodeID string
		reason string
		want   domain.SwitchOutcome
	}{
		// Policy keeps the current node, explaining why (cooldown gate).
		{"n2", "switch blocked by dwell-time cooldown", domain.SwitchCooldown},
		{"n2", "candidate fails hysteresis gate", domain.SwitchNotBetter},
		{"", "no eligible candidates", domain.SwitchNone},
	} {
		m.policy = &fakePolicy{nodeID: tc.nodeID, reason: tc.reason}
		d, err := m.Evaluate(ctx, nodes)
		if err != nil {
			t.Fatalf("%s: Evaluate: %v", tc.reason, err)
		}
		if d.Outcome != tc.want {
			t.Fatalf("%q -> outcome %q, want %q", tc.reason, d.Outcome, tc.want)
		}
	}
}

func TestEvaluatePolicyErrorBecomesSwitchFailed(t *testing.T) {
	m := newTestManager(&fakeApplier{}, &fakeStore{}, &fakeBus{})
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1", 1)}
	if err := m.EnsureActive(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	m.policy = &fakePolicy{err: errors.New("scoring store unreachable")}
	d, err := m.Evaluate(ctx, nodes)
	if err != nil {
		t.Fatalf("Evaluate must surface policy errors in the decision, got %v", err)
	}
	if d.Outcome != domain.SwitchFailed || d.Reason == "" {
		t.Fatalf("decision = %+v", d)
	}
}

func TestEvaluateEmptyFleet(t *testing.T) {
	m := newTestManager(&fakeApplier{}, &fakeStore{}, &fakeBus{})
	m.policy = &fakePolicy{nodeID: "n1", reason: "x"}
	d, err := m.Evaluate(context.Background(), nil)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d.Outcome != domain.SwitchNone || d.Reason == "" {
		t.Fatalf("decision = %+v", d)
	}
}
