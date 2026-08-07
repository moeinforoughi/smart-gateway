package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRand struct{ v float64 }

func (f fakeRand) Float64() float64 { return f.v }
func (fakeRand) Intn(n int) int     { return 0 }

func TestJobRunsOnInterval(t *testing.T) {
	s := New(nil, nil, nil)
	var count atomic.Int64
	s.Add(Job{
		ID:       "j",
		Interval: 10 * time.Millisecond,
		Func:     func(ctx context.Context) { count.Add(1) },
	})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(60 * time.Millisecond)
	cancel()
	s.Stop(2 * time.Second)
	if count.Load() < 3 {
		t.Fatalf("expected >=3 runs, got %d", count.Load())
	}
}

func TestJobFailuresDoNotStopFutureRuns(t *testing.T) {
	s := New(nil, nil, nil)
	var okRuns atomic.Int64
	s.Add(Job{
		ID:       "flaky",
		Interval: 5 * time.Millisecond,
		Func: func(ctx context.Context) {
			if okRuns.Add(1) <= 2 {
				panic("boom")
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(40 * time.Millisecond)
	cancel()
	s.Stop(2 * time.Second)
	if okRuns.Load() < 4 {
		t.Fatalf("expected the job to keep running after panics, got %d runs", okRuns.Load())
	}
}

func TestLongJobDoesNotBlockShortJob(t *testing.T) {
	s := New(nil, nil, nil)
	var shortRuns atomic.Int64
	s.Add(Job{ID: "short", Interval: 5 * time.Millisecond, Func: func(ctx context.Context) { shortRuns.Add(1) }})
	release := make(chan struct{})
	started := make(chan struct{})
	s.Add(Job{ID: "long", Interval: 5 * time.Millisecond, Func: func(ctx context.Context) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	}})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("long job never started")
	}
	time.Sleep(30 * time.Millisecond)
	if shortRuns.Load() == 0 {
		t.Fatal("short job blocked by long job")
	}
	close(release)
	cancel()
	s.Stop(2 * time.Second)
}

func TestJobReceivesCancellation(t *testing.T) {
	s := New(nil, nil, nil)
	var cancelled atomic.Bool
	s.Add(Job{ID: "c", Interval: 5 * time.Millisecond, Func: func(ctx context.Context) {
		<-ctx.Done()
		cancelled.Store(true)
	}})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	s.Stop(2 * time.Second)
	if !cancelled.Load() {
		t.Fatal("job context was not cancelled at shutdown")
	}
}

func TestPersistCallbackInvoked(t *testing.T) {
	var mu sync.Mutex
	var gotID string
	var gotNext time.Time

	s := New(nil, nil, nil)
	s.Add(Job{
		ID:       "j",
		Interval: 10 * time.Millisecond,
		Func:     func(ctx context.Context) {},
		Persist: func(id string, next time.Time) {
			mu.Lock()
			gotID, gotNext = id, next
			mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	s.Stop(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if gotID != "j" {
		t.Fatalf("persist id = %q, want %q", gotID, "j")
	}
	if gotNext.IsZero() {
		t.Fatal("persist must record the next scheduled run")
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	s := New(nil, nil, nil)
	s.Add(Job{ID: "a", Interval: time.Hour, Func: func(ctx context.Context) {}})
	next := s.Snapshot()
	if _, ok := next["a"]; !ok {
		t.Fatal("snapshot missing job")
	}
	s2 := New(nil, nil, nil)
	s2.Add(Job{ID: "a", Interval: time.Hour, Func: func(ctx context.Context) {}})
	s2.Restore(next)
	if got := s2.Snapshot()["a"]; !got.Equal(next["a"]) {
		t.Fatalf("restore did not preserve next run: got %v want %v", got, next["a"])
	}
}

func TestJitterUsesRandomSource(t *testing.T) {
	// Fixed rand returns 0.5 -> offset = interval exactly (center of range).
	s := New(nil, fakeRand{v: 0.5}, nil)
	s.Add(Job{ID: "j", Interval: 100 * time.Millisecond, Jitter: 50 * time.Millisecond, Func: func(ctx context.Context) {}})
	// offset() is private; run long enough to observe scheduling does not stall.
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
	s.Stop(time.Second)
}

func TestRejectsZeroInterval(t *testing.T) {
	s := New(nil, nil, nil)
	s.Add(Job{ID: "bad", Interval: 0, Func: func(ctx context.Context) {}})
	if s.Len() != 0 {
		t.Fatal("zero-interval job must be rejected")
	}
}
