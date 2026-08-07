package domain

import "time"

// ProbeSample is one retained probe observation (repository row).
type ProbeSample struct {
	At      time.Time
	Latency time.Duration // 0 when the probe failed
	Success bool
}

// FoldConfig controls the derived metrics computed by FoldHealth.
type FoldConfig struct {
	// EWMASamples is the EWMA half-life window (alpha = 2/(N+1)); default 20.
	EWMASamples int
}

// FoldHealth folds an ordered (oldest→newest) window of probe samples into a
// NodeHealth record. Pure and deterministic so the repository, the health
// pipeline and the scorer share one definition (FR-300 series).
func FoldHealth(nodeID string, samples []ProbeSample, cfg FoldConfig, now time.Time) NodeHealth {
	h := NodeHealth{NodeID: nodeID, Probes: len(samples)}
	if len(samples) == 0 {
		return h
	}
	n := cfg.EWMASamples
	if n < 2 {
		n = 20
	}
	alpha := 2.0 / float64(n+1)

	var (
		ewma   float64
		jitter float64
		hasEW  bool
	)
	consec := 0
	last := samples[0]
	for i := range samples {
		s := samples[i]
		if !s.At.IsZero() {
			last = s
		}
		if s.Success {
			h.SuccessCount++
			consec = 0
			h.LastSuccessAt = s.At
		} else {
			h.FailureCount++
			consec++
			h.LastLatency = 0
		}
		if s.Success && s.Latency > 0 {
			ms := float64(s.Latency.Milliseconds())
			if !hasEW {
				ewma = ms
				hasEW = true
			} else {
				ewma = alpha*ms + (1-alpha)*ewma
			}
			jitter = alpha*absF(ms-ewma) + (1-alpha)*jitter
			h.LastLatency = s.Latency
		}
	}
	h.ConsecutiveFailures = consec
	h.LastProbedAt = last.At
	if h.FailureCount == 0 && h.Probes == 0 {
		h.LossRate = 0
	} else {
		h.LossRate = float64(h.FailureCount) / float64(h.Probes)
	}
	if hasEW {
		h.EWMA = time.Duration(ewma) * time.Millisecond
		h.Jitter = time.Duration(jitter) * time.Millisecond
	}
	return h
}

func absF(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// NodeHealth aggregates the raw probe history of a node (FR-300 series).
// The repository keeps a bounded window of measurements per node; these
// counters are the fold of that window plus the current observation.
type NodeHealth struct {
	NodeID              string
	Probes              int
	SuccessCount        int
	FailureCount        int
	ConsecutiveFailures int
	LastLatency         time.Duration
	// EWMA is the exponentially weighted moving average of probe latency,
	// computed by the health pipeline (spike dampening, FR-6xx).
	EWMA time.Duration
	// Jitter estimates latency variance (RTT jitter) in milliseconds.
	Jitter time.Duration
	// LossRate estimates packet loss in [0,1]: connect-failure rate plus
	// retransmission share where TCP_INFO is available (documented
	// approximation).
	LossRate float64
	// ThroughputBps is the latest throughput estimate in bytes/second
	// (speed tests, FR-500 series). Zero until the first measurement.
	ThroughputBps float64
	// ThroughputAt is when the throughput estimate was captured.
	ThroughputAt  time.Time
	LastProbedAt  time.Time
	LastSuccessAt time.Time
}

// SuccessRatio returns success/total, or 0 when no probes exist.
func (h NodeHealth) SuccessRatio() float64 {
	if h.Probes == 0 {
		return 0
	}
	return float64(h.SuccessCount) / float64(h.Probes)
}

// Fresh returns true when the last probe is newer than age (freshness gate).
func (h NodeHealth) Fresh(now time.Time, maxAge time.Duration) bool {
	return !h.LastProbedAt.IsZero() && now.Sub(h.LastProbedAt) <= maxAge
}

// Score is the output of the scoring engine (FR-400 series).
// Each component is in [0,1] where higher is better; Total is the weighted
// combination. Confidence is the number of samples behind the score. Eligible
// reports whether the freshness + confidence gates passed (selectable, M4).
type Score struct {
	NodeID      string
	Latency     float64
	Stability   float64
	Throughput  float64
	Loss        float64
	Total       float64
	Confidence  int
	Eligible    bool
	EvaluatedAt time.Time
}
