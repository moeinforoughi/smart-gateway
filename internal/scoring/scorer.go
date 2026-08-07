// Package scoring implements the deterministic score engine (M4, FR-400
// series, FR-607/608). It folds one NodeHealth into a [0,1]^4 Score, applies
// the configured weights, penalties and the freshness + confidence gates, and
// decides eligibility. Everything is pure and clock-injected so the engine is
// exhaustively unit-testable.
package scoring

import (
	"time"

	"smart-gateway/internal/domain"
)

// referenceThroughput is the throughput that scores 1.0 (100 Mbps). Values
// above it clamp to 1.0; a node with no throughput sample scores the neutral
// 0.5 so untested nodes are not penalised for lacking a speed test.
const referenceThroughput = 100.0 * 1000 * 1000 / 8 // 100 Mbps in bytes/s

// stalePenalty multiplies Total when a node is past its freshness gate; the
// node is also marked ineligible (FR gates: freshness).
const stalePenalty = 0.5

// Config carries the score engine knobs resolved from the gateway config.
type Config struct {
	LatencyWeight    float64
	StabilityWeight  float64
	ThroughputWeight float64
	LossWeight       float64
	MinSamples       int           // confidence gate: probes required to be selectable
	StabilityWindow  int           // history length that earns full stability credit
	HardLatency      time.Duration // latency above which the node is effectively down
	FailoverFailures int           // consecutive failures that exhaust the stability credit
	StaleAfter       time.Duration // freshness gate: no probe within this → stale
}

// DefaultConfig mirrors the scoring block defaults in docs/configuration.md.
func DefaultConfig() Config {
	return Config{
		LatencyWeight:    0.45,
		StabilityWeight:  0.30,
		ThroughputWeight: 0.15,
		LossWeight:       0.10,
		MinSamples:       3,
		StabilityWindow:  30,
		HardLatency:      3 * time.Second,
		FailoverFailures: 3,
		StaleAfter:       24 * time.Hour,
	}
}

// Score folds a node's health record into an explainable Score. It is pure
// and deterministic: same inputs, same output.
func Score(h domain.NodeHealth, cfg Config, now time.Time) domain.Score {
	hard := cfg.HardLatency
	if hard <= 0 {
		hard = 3 * time.Second
	}
	failover := cfg.FailoverFailures
	if failover < 1 {
		failover = 3
	}
	win := cfg.StabilityWindow
	if win < 1 {
		win = 30
	}

	latency := 0.0
	if h.EWMA > 0 {
		latency = 1 - clamp(h.EWMA.Seconds()/hard.Seconds(), 0, 1)
	}
	loss := 1 - clamp(h.LossRate, 0, 1)
	stability := 1 - clamp(h.Jitter.Seconds()/hard.Seconds(), 0, 1)
	// Consecutive failures exhaust the stability credit.
	stability *= 1 - clamp(float64(h.ConsecutiveFailures)/float64(failover), 0, 1)
	// Historical-stability credit: a longer clean history earns the full
	// credit (capped by the stability window).
	stability *= clamp(float64(h.Probes)/float64(win), 0, 1)

	throughput := 0.5 // neutral when no sample (FR-500 attribution is optional)
	if h.ThroughputBps > 0 {
		throughput = clamp(h.ThroughputBps/referenceThroughput, 0, 1)
	}

	weights := normalizeWeights(cfg.LatencyWeight, cfg.StabilityWeight, cfg.ThroughputWeight, cfg.LossWeight)
	total := weights[0]*latency + weights[1]*stability + weights[2]*throughput + weights[3]*loss

	s := domain.Score{
		NodeID:      h.NodeID,
		Latency:     latency,
		Stability:   stability,
		Throughput:  throughput,
		Loss:        loss,
		Total:       total,
		Confidence:  h.Probes,
		EvaluatedAt: now,
	}
	s.Eligible = eligible(h, cfg, now)
	if !s.Eligible {
		s.Total *= stalePenalty
	}
	return s
}

// eligible applies the freshness + confidence gates (FR gates). A node is
// selectable only when it has enough samples, has not hit the failover
// threshold, was probed recently and is not seeing catastrophic loss.
func eligible(h domain.NodeHealth, cfg Config, now time.Time) bool {
	minSamples := cfg.MinSamples
	if minSamples < 1 {
		minSamples = 3
	}
	if h.Probes < minSamples {
		return false
	}
	if cfg.FailoverFailures >= 1 && h.ConsecutiveFailures >= cfg.FailoverFailures {
		return false
	}
	if h.LossRate > 0.5 {
		return false
	}
	stale := cfg.StaleAfter
	if stale <= 0 {
		stale = 24 * time.Hour
	}
	if !h.Fresh(now, stale) {
		return false
	}
	return true
}

// normalizeWeights rescales weights so they sum to 1, falling back to the
// documented defaults when all are zero.
func normalizeWeights(lat, stab, thr, loss float64) [4]float64 {
	def := DefaultConfig()
	all := lat + stab + thr + loss
	if all <= 0 {
		lat, stab, thr, loss = def.LatencyWeight, def.StabilityWeight, def.ThroughputWeight, def.LossWeight
		all = lat + stab + thr + loss
	}
	if all <= 0 {
		return [4]float64{0.45, 0.30, 0.15, 0.10}
	}
	return [4]float64{lat / all, stab / all, thr / all, loss / all}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
