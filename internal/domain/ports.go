package domain

import (
	"math/rand/v2"
	"time"
)

// SystemClock implements Clock using the real wall clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// SystemRandom implements RandomSource using the standard library PRNG.
type SystemRandom struct{}

// Float64 returns a value in [0,1).
func (SystemRandom) Float64() float64 { return rand.Float64() }

// Intn returns a value in [0,n).
func (SystemRandom) Intn(n int) int { return rand.IntN(n) }
