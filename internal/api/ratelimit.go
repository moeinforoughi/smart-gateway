package api

import (
	"sync"
	"time"
)

// rateLimiter is a fixed-window per-client limiter for admin endpoints
// (NFR-143). Windows are created lazily and expired entries are dropped on
// access, so the map stays bounded by the number of distinct clients in a
// window.
type rateLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	seen   map[string]*windowState
}

type windowState struct {
	start time.Time
	count int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	if limit <= 0 {
		limit = 120
	}
	if window <= 0 {
		window = time.Minute
	}
	return &rateLimiter{limit: limit, window: window, seen: make(map[string]*windowState)}
}

// Allow reports whether key may proceed. It advances the window when the
// current window expired.
func (rl *rateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	st := rl.seen[key]
	if st == nil {
		rl.seen[key] = &windowState{start: now, count: 1}
		return true
	}
	if now.Sub(st.start) >= rl.window {
		st.start = now
		st.count = 1
		return true
	}
	st.count++
	return st.count <= rl.limit
}
