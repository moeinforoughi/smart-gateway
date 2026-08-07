package domain

import (
	"testing"
	"time"
)

func testTime(sec int64) time.Time { return time.Unix(sec, 0) }

func TestFoldHealthEmpty(t *testing.T) {
	h := FoldHealth("n1", nil, FoldConfig{}, testTime(100))
	if h.Probes != 0 {
		t.Fatalf("Probes = %d, want 0", h.Probes)
	}
	if h.LossRate != 0 {
		t.Fatalf("LossRate = %v, want 0", h.LossRate)
	}
	if h.EWMA != 0 {
		t.Fatalf("EWMA = %v, want 0", h.EWMA)
	}
}

func TestFoldHealthCounters(t *testing.T) {
	samples := []ProbeSample{
		{At: testTime(1), Latency: 100 * time.Millisecond, Success: true},
		{At: testTime(2), Latency: 0, Success: false},
		{At: testTime(3), Latency: 120 * time.Millisecond, Success: true},
		{At: testTime(4), Latency: 0, Success: false},
		{At: testTime(5), Latency: 0, Success: false},
	}
	h := FoldHealth("n1", samples, FoldConfig{}, testTime(6))
	if h.SuccessCount != 2 || h.FailureCount != 3 {
		t.Fatalf("success=%d failure=%d, want 2/3", h.SuccessCount, h.FailureCount)
	}
	if h.ConsecutiveFailures != 2 {
		t.Fatalf("ConsecutiveFailures = %d, want 2 (trailing failures only)", h.ConsecutiveFailures)
	}
	if h.LastLatency != 0 {
		t.Fatalf("LastLatency = %v, want 0 (last sample failed)", h.LastLatency)
	}
	if got := h.LossRate; got != 0.6 {
		t.Fatalf("LossRate = %v, want 0.6", got)
	}
	if h.LastProbedAt != testTime(5) {
		t.Fatalf("LastProbedAt = %v, want sample 5", h.LastProbedAt)
	}
	if h.LastSuccessAt != testTime(3) {
		t.Fatalf("LastSuccessAt = %v, want sample 3", h.LastSuccessAt)
	}
}

func TestFoldHealthEWMA(t *testing.T) {
	samples := []ProbeSample{
		{At: testTime(1), Latency: 100 * time.Millisecond, Success: true},
		{At: testTime(2), Latency: 100 * time.Millisecond, Success: true},
	}
	// With 2 samples and default N=20, alpha = 2/21 ≈ 0.0952. The EWMA seeds
	// at the first value, so two identical latencies keep it at 100ms.
	h := FoldHealth("n1", samples, FoldConfig{}, testTime(3))
	if h.EWMA != 100*time.Millisecond {
		t.Fatalf("EWMA = %v, want 100ms", h.EWMA)
	}
	// Jitter for identical samples: alpha*0 on the second sample only, but the
	// first sample's deviation vs the seeded EWMA is 0 too, so jitter = 0.
	if h.Jitter != 0 {
		t.Fatalf("Jitter = %v, want 0 for identical latencies", h.Jitter)
	}
}

func TestFoldHealthEWMAConvergence(t *testing.T) {
	samples := []ProbeSample{
		{At: testTime(1), Latency: 100 * time.Millisecond, Success: true},
		{At: testTime(2), Latency: 100 * time.Millisecond, Success: true},
		{At: testTime(3), Latency: 300 * time.Millisecond, Success: true},
		{At: testTime(4), Latency: 100 * time.Millisecond, Success: true},
	}
	h := FoldHealth("n1", samples, FoldConfig{EWMASamples: 2}, testTime(5))
	if h.EWMA >= 300*time.Millisecond {
		t.Fatalf("EWMA = %v, must be dampened below the 300ms spike", h.EWMA)
	}
	if h.EWMA <= 100*time.Millisecond {
		t.Fatalf("EWMA = %v, must be above the 100ms floor", h.EWMA)
	}
	if h.Jitter <= 0 {
		t.Fatalf("Jitter = %v, want > 0 when latencies vary", h.Jitter)
	}
}

func TestFoldHealthZeroLatencySuccess(t *testing.T) {
	// A successful probe with zero latency contributes no EWMA sample.
	samples := []ProbeSample{
		{At: testTime(1), Latency: 0, Success: true},
	}
	h := FoldHealth("n1", samples, FoldConfig{}, testTime(2))
	if h.SuccessCount != 1 {
		t.Fatalf("SuccessCount = %d, want 1", h.SuccessCount)
	}
	if h.EWMA != 0 {
		t.Fatalf("EWMA = %v, want 0 (no latency sample)", h.EWMA)
	}
}
