package selector

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeScores struct {
	top []domain.NodeScore
}

func (f *fakeScores) TopEligible(_ context.Context, _ int) ([]domain.NodeScore, error) {
	return f.top, nil
}

func (f *fakeScores) UpsertScore(context.Context, domain.NodeScore) error { return nil }
func (f *fakeScores) GetNodeScore(context.Context, string) (domain.NodeScore, error) {
	return domain.NodeScore{}, context.Canceled
}
func (f *fakeScores) ScoreHistory(context.Context, string, int) ([]domain.NodeScore, error) {
	return nil, nil
}

func newPolicy(store domain.ScoreStore, clock *fakeClock, cfg Config) *Policy {
	if cfg.SwitchDelta == 0 {
		cfg.SwitchDelta = 0.10
	}
	if cfg.DwellTime == 0 {
		cfg.DwellTime = 5 * time.Minute
	}
	if cfg.RecoveryWindow == 0 {
		cfg.RecoveryWindow = 15 * time.Minute
	}
	if cfg.TopN == 0 {
		cfg.TopN = 20
	}
	return NewPolicy(store, cfg, clock, log.New(nil, 0))
}

func node(id, sub string) domain.Node {
	return domain.Node{ID: id, SubscriptionID: sub, Protocol: domain.ProtocolVless}
}

func eligible(id string, total float64) domain.NodeScore {
	return domain.NodeScore{
		NodeID: id,
		Score:  domain.Score{NodeID: id, Total: total, Eligible: true},
		State:  domain.ScoreEligible,
	}
}

func TestSelectsBestWhenNoCurrent(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.8), eligible("n2", 0.9)}}
	p := newPolicy(store, clock, Config{})
	got, reason, err := p.Decide(context.Background(),
		[]domain.Node{node("n1", "s1"), node("n2", "s1")}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "n2" {
		t.Fatalf("expected n2, got %q (reason %s)", got, reason)
	}
}

func TestHysteresisBlocksSmallImprovement(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.80), eligible("n2", 0.81)}}
	p := newPolicy(store, clock, Config{})
	got, reason, err := p.Decide(context.Background(),
		[]domain.Node{node("n1", "s1"), node("n2", "s1")}, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("expected no switch, got %q (reason %s)", got, reason)
	}
}

func TestSwitchWhenDeltaExceeds(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.80), eligible("n2", 0.95)}}
	p := newPolicy(store, clock, Config{})
	got, _, err := p.Decide(context.Background(),
		[]domain.Node{node("n1", "s1"), node("n2", "s1")}, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "n2" {
		t.Fatalf("expected switch to n2, got %q", got)
	}
}

func TestFailoverOnUnhealthyCurrent(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Current node n1 is not eligible (absent from top): replace immediately.
	store := &fakeScores{top: []domain.NodeScore{eligible("n2", 0.5), eligible("n3", 0.55)}}
	p := newPolicy(store, clock, Config{})
	got, reason, err := p.Decide(context.Background(),
		[]domain.Node{node("n1", "s1"), node("n2", "s1"), node("n3", "s1")}, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "n3" {
		t.Fatalf("expected failover to n3, got %q", got)
	}
	if reason == "" {
		t.Fatal("failover must be explainable")
	}
}

func TestCooldownBlocksSecondSwitch(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	cfg := Config{DwellTime: 5 * time.Minute, RecoveryWindow: time.Second}
	store := &fakeScores{}
	p := newPolicy(store, clock, cfg)
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1"), node("n2", "s1")}

	// Phase 1: n2 clearly better → switch n1→n2.
	store.top = []domain.NodeScore{eligible("n1", 0.90), eligible("n2", 1.00)}
	if got, _, err := p.Decide(ctx, nodes, "n1"); err != nil || got != "n2" {
		t.Fatalf("phase 1: got %q err %v", got, err)
	}
	// Phase 2: n1 is clearly better again; hysteresis passes, but the dwell
	// cooldown blocks the immediate switch back.
	store.top = []domain.NodeScore{eligible("n1", 1.00), eligible("n2", 0.90)}
	got, reason, err := p.Decide(ctx, nodes, "n2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("phase 2: expected cooldown block, got %q (reason %s)", got, reason)
	}
	// Phase 3: after dwell (and the 1s recovery window) elapse, the switch
	// back to the clearly better n1 is allowed.
	clock.advance(cfg.DwellTime + time.Second)
	got, _, err = p.Decide(ctx, nodes, "n2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "n1" {
		t.Fatalf("phase 3: expected switch back to n1, got %q", got)
	}
}

func TestRecoveryStaging(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	cfg := Config{RecoveryWindow: 15 * time.Minute}
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.90), eligible("n2", 0.60)}}
	p := newPolicy(store, clock, cfg)
	ctx := context.Background()
	nodes := []domain.Node{node("n1", "s1"), node("n2", "s1")}

	// n1 is down (not eligible), fail over to n2.
	store.top = []domain.NodeScore{eligible("n2", 0.60)}
	if got, _, err := p.Decide(ctx, nodes, "n1"); err != nil || got != "n2" {
		t.Fatalf("failover: got %q err %v", got, err)
	}
	// n1 recovers and is now the best — but recovery staging blocks it.
	store.top = []domain.NodeScore{eligible("n1", 0.90), eligible("n2", 0.60)}
	if got, reason, err := p.Decide(ctx, nodes, "n2"); err != nil || got != "" {
		t.Fatalf("expected staging block, got %q (reason %s, err %v)", got, reason, err)
	}
	// After the recovery window, n1 may be selected again (n1 is 0.9 vs n2
	// 0.6, delta 0.3 > 0.06).
	clock.advance(cfg.RecoveryWindow + time.Second)
	if got, _, err := p.Decide(ctx, nodes, "n2"); err != nil || got != "n1" {
		t.Fatalf("expected recovery selection of n1, got %q (err %v)", got, err)
	}
}

func TestProviderDownPenalty(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// s1 has 3 nodes but only one eligible (n1) → provider considered down,
	// n1's score is halved (0.80 → 0.40); s2's n2 (0.60) wins.
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.80), eligible("n2", 0.60)}}
	p := newPolicy(store, clock, Config{})
	got, reason, err := p.Decide(context.Background(),
		[]domain.Node{node("n1", "s1"), node("x", "s1"), node("y", "s1"), node("n2", "s2")}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "n2" {
		t.Fatalf("expected n2 (healthy provider), got %q (reason %s)", got, reason)
	}
}

func TestManualMode(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := &fakeScores{top: []domain.NodeScore{eligible("n1", 0.99)}}
	p := newPolicy(store, clock, Config{Manual: true})
	got, reason, err := p.Decide(context.Background(), []domain.Node{node("n1", "s1")}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" || reason == "" {
		t.Fatalf("manual mode must refuse switching, got %q (%s)", got, reason)
	}
}

func TestNoEligibleCandidates(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := newPolicy(&fakeScores{}, clock, Config{})
	got, _, err := p.Decide(context.Background(), []domain.Node{node("n1", "s1")}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("expected no candidate, got %q", got)
	}
}
