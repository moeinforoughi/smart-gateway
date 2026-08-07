package scoring

import (
	"math"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func healthyHealth(nodeID string, probes int, last time.Time) domain.NodeHealth {
	return domain.NodeHealth{
		NodeID:        nodeID,
		Probes:        probes,
		SuccessCount:  probes,
		EWMA:          80 * time.Millisecond,
		Jitter:        5 * time.Millisecond,
		LossRate:      0,
		LastProbedAt:  last,
		LastSuccessAt: last,
	}
}

func TestScoreHealthyNode(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()
	s := Score(healthyHealth("n1", 40, now), cfg, now)

	if !s.Eligible {
		t.Fatal("healthy node must be eligible")
	}
	// 80ms against a 3s hard ceiling → latency near 1.0.
	if s.Latency < 0.97 || s.Latency > 1.0 {
		t.Fatalf("latency out of range: %v", s.Latency)
	}
	// No loss.
	if s.Loss != 1.0 {
		t.Fatalf("loss should be 1.0, got %v", s.Loss)
	}
	// Throughput neutral without a speed test.
	if s.Throughput != 0.5 {
		t.Fatalf("throughput should be neutral 0.5, got %v", s.Throughput)
	}
	if s.Total < 0.8 || s.Total > 1.0 {
		t.Fatalf("total out of range: %v", s.Total)
	}
	if s.Confidence != 40 {
		t.Fatalf("confidence should equal probe count, got %d", s.Confidence)
	}
}

func TestEligibilityGates(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()

	cases := []struct {
		name string
		mod  func(*domain.NodeHealth)
		want bool
	}{
		{"healthy", func(h *domain.NodeHealth) {}, true},
		{"too few samples", func(h *domain.NodeHealth) { h.Probes, h.SuccessCount = 2, 2 }, false},
		{"failover threshold", func(h *domain.NodeHealth) {
			h.ConsecutiveFailures = cfg.FailoverFailures
		}, false},
		{"high loss", func(h *domain.NodeHealth) { h.LossRate = 0.6 }, false},
		{"stale", func(h *domain.NodeHealth) { h.LastProbedAt = now.Add(-2 * cfg.StaleAfter) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := healthyHealth("n1", 10, now)
			tc.mod(&h)
			if got := Score(h, cfg, now).Eligible; got != tc.want {
				t.Fatalf("Eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStaleScorePenalized(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()

	healthy := Score(healthyHealth("n1", 10, now), cfg, now)
	stale := Score(healthyHealth("n1", 10, now.Add(-2*cfg.StaleAfter)), cfg, now)

	if !healthy.Eligible || stale.Eligible {
		t.Fatal("sanity: healthy must be eligible, stale must not")
	}
	// stalePenalty multiplies the total.
	if math.Abs(stale.Total-healthy.Total*stalePenalty) > 1e-9 {
		t.Fatalf("stale total %v != healthy*0.5 %v", stale.Total, healthy.Total*stalePenalty)
	}
}

func TestScoreDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()
	h := healthyHealth("n1", 12, now)
	a := Score(h, cfg, now)
	b := Score(h, cfg, now)
	if a != b {
		t.Fatalf("scoring not deterministic: %v != %v", a, b)
	}
}

func TestWeightsNormalizeToSum(t *testing.T) {
	cfg := Config{
		LatencyWeight: 1, StabilityWeight: 1, ThroughputWeight: 1, LossWeight: 1,
		MinSamples: 3, StabilityWindow: 30, HardLatency: 3 * time.Second,
		FailoverFailures: 3, StaleAfter: 24 * time.Hour,
	}
	now := time.Now()
	s := Score(healthyHealth("n1", 10, now), cfg, now)
	// Equal weights → total is the plain average of the four components.
	want := (s.Latency + s.Stability + s.Throughput + s.Loss) / 4
	if math.Abs(s.Total-want) > 1e-9 {
		t.Fatalf("equal-weight total %v != average %v", s.Total, want)
	}
}

func TestStabilityCreditFromHistory(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()

	h := healthyHealth("n1", 1, now)
	young := Score(h, cfg, now)

	h.Probes = cfg.StabilityWindow + 5
	h.SuccessCount = h.Probes
	mature := Score(h, cfg, now)

	if young.Stability >= mature.Stability {
		t.Fatalf("expected mature node to have more stability credit: %v vs %v",
			young.Stability, mature.Stability)
	}
}
