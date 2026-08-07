package domain

import "time"

// NodeHealth aggregates the raw probe history of a node (FR-300 series).
// The repository keeps a bounded window of measurements per node; these
// counters are the fold of that window plus the current observation.
// M2 extends the fold with derived metrics consumed by the scorer.
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
// combination. Confidence is the number of samples behind the score.
type Score struct {
	NodeID      string
	Latency     float64
	Stability   float64
	Throughput  float64
	Loss        float64
	Total       float64
	Confidence  int
	EvaluatedAt time.Time
}
