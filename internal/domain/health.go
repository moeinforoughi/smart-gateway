package domain

import "time"

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
	LastProbedAt        time.Time
	LastSuccessAt       time.Time
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
