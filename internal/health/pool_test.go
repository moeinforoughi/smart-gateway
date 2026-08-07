package health

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolRunsJobs(t *testing.T) {
	p := NewPool(4)
	var n int64
	for i := 0; i < 8; i++ {
		p.Go(context.Background(), func() { atomic.AddInt64(&n, 1) })
	}
	p.Wait()
	if got := atomic.LoadInt64(&n); got != 8 {
		t.Fatalf("ran %d jobs, want 8", got)
	}
}

func TestPoolLimitsConcurrency(t *testing.T) {
	const workers = 3
	p := NewPool(workers)
	var cur, max int64
	var mu sync.Mutex
	for i := 0; i < workers*4; i++ {
		p.Go(context.Background(), func() {
			mu.Lock()
			cur++
			if cur > max {
				max = cur
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			cur--
			mu.Unlock()
		})
	}
	p.Wait()
	if max > workers {
		t.Fatalf("max concurrency = %d, want <= %d", max, workers)
	}
}

func TestPoolCancelledContextSkipsJob(t *testing.T) {
	p := NewPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: job must not be scheduled
	var ran int64
	p.Go(ctx, func() { atomic.AddInt64(&ran, 1) })
	p.Wait()
	if ran != 0 {
		t.Fatal("job must not run with a cancelled context")
	}
}

func TestNewPoolMinOne(t *testing.T) {
	p := NewPool(0)
	if p == nil {
		t.Fatal("NewPool(0) must return a pool")
	}
}
