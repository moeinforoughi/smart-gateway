// Package domain holds the Smart Gateway domain entities, value objects,
// ports and state machines. It imports nothing from internal/ — per the
// dependency rule, every other layer may import domain, never the reverse.
package domain

import "time"

// Clock abstracts the source of time so business logic stays deterministic
// in tests (architecture §22: no package may call time.Now() directly).
type Clock interface {
	Now() time.Time
}

// RandomSource abstracts randomness so scheduling jitter, backoff and probe
// order stay reproducible in tests (architecture §23).
type RandomSource interface {
	// Float64 returns a value in [0,1).
	Float64() float64
	// Intn returns a value in [0,n). Panics if n <= 0.
	Intn(n int) int
}

// NodeState models the health lifecycle (architecture §16).
type NodeState string

const (
	StateUnknown    NodeState = "unknown"
	StateHealthy    NodeState = "healthy"
	StateDegraded   NodeState = "degraded"
	StateUnstable   NodeState = "unstable"
	StateFailed     NodeState = "failed"
	StateRecovering NodeState = "recovering"
)

// Valid reports whether s is a known node state.
func (s NodeState) Valid() bool {
	switch s {
	case StateUnknown, StateHealthy, StateDegraded, StateUnstable, StateFailed, StateRecovering:
		return true
	}
	return false
}
