package domain

import "context"

// ConnMetric attributes cumulative traffic to a single outbound node. It is
// produced by the passive collector sampling the Clash API /connections
// endpoint (zero-cost throughput attribution, M3).
type ConnMetric struct {
	NodeID   string
	Upload   int64 // cumulative bytes uploaded through the node
	Download int64 // cumulative bytes downloaded through the node
}

// ConnectionsSampler is the passive traffic source port. Implementations
// aggregate live connections per outbound node.
type ConnectionsSampler interface {
	// Connections returns cumulative per-node byte counters at this instant.
	Connections(ctx context.Context) ([]ConnMetric, error)
}
