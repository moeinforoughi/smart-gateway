package health

import (
	"context"
	"sync"
)

// Pool is a bounded worker pool for probe jobs (FR-403/404). Jobs run with
// their own context, which carries the per-probe timeout, so a stuck probe
// ends when its deadline fires and the worker returns to the pool — stuck
// jobs never leak the pool (FR-406). Pool is safe for concurrent use.
type Pool struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

// NewPool builds a pool with at most n concurrent jobs.
func NewPool(n int) *Pool {
	if n < 1 {
		n = 1
	}
	return &Pool{sem: make(chan struct{}, n)}
}

// Go schedules f under the pool, blocking the caller when the pool is full.
// A job is never scheduled on an already-cancelled context.
func (p *Pool) Go(ctx context.Context, f func()) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	p.wg.Add(1)
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		p.wg.Done()
		return
	}
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()
		f()
	}()
}

// Wait blocks until all scheduled jobs have finished.
func (p *Pool) Wait() { p.wg.Wait() }
