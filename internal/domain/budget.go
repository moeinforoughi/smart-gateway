package domain

import (
	"sync"
	"time"
)

// Budget is a sliding 24-hour bandwidth budget shared by probe and speed-test
// schedulers (FR-411, FR-506). It never counts negative or non-positive
// amounts and always clamps usage to the configured maximum so downstream
// math (e.g. Prometheus counters) stays bounded. The clock is injected so the
// rollover logic is deterministic in tests.
type Budget struct {
	max   int64
	clock Clock

	mu          sync.Mutex
	windowStart time.Time
	used        int64
}

// NewBudget builds a budget capped at maxBytes per rolling 24h window.
func NewBudget(maxBytes int64, clock Clock) *Budget {
	if maxBytes < 0 {
		maxBytes = 0
	}
	if clock == nil {
		clock = SystemClock{}
	}
	now := clock.Now()
	return &Budget{
		max:         maxBytes,
		clock:       clock,
		windowStart: now,
	}
}

// Allow reports whether recording bytes would stay within the cap. It rolls
// the window first so a new day starts fresh (FR-411).
func (b *Budget) Allow(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetIfDue(b.clock.Now())
	return b.used+bytes <= b.max
}

// Note records bytes against the budget, clamped so used never exceeds max.
func (b *Budget) Note(bytes int64) {
	if bytes <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetIfDue(b.clock.Now())
	b.used += bytes
	if b.used > b.max {
		b.used = b.max
	}
}

// Used returns the bytes consumed in the current window.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetIfDue(b.clock.Now())
	return b.used
}

// Remaining returns the bytes still available in the current window.
func (b *Budget) Remaining() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetIfDue(b.clock.Now())
	if b.max <= 0 {
		return 0
	}
	if r := b.max - b.used; r > 0 {
		return r
	}
	return 0
}

// resetIfDue opens a fresh window when the current one is older than 24h.
func (b *Budget) resetIfDue(now time.Time) {
	if now.Sub(b.windowStart) >= 24*time.Hour {
		b.windowStart = now
		b.used = 0
	}
}
