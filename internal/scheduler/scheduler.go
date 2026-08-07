// Package scheduler executes scheduled jobs.
//
// It owns NO business logic (architecture §9). It only:
//   - runs each job independently (FR-1000)
//   - isolates job failures and panics (FR-1001, NFR-007)
//   - supports restoring schedules after restart (FR-1002)
//   - applies configurable random jitter (FR-1003)
//   - dispatches every job in its own goroutine so long jobs never
//     block short ones (FR-1004)
//   - supports cancellation and orderly shutdown (NFR-008, NFR-009, NFR-010)
//
// A binary min-heap keyed by next-run time keeps thousands of jobs cheap
// (NFR-047): the loop sleeps until the earliest deadline and wakes only on
// insertions, so per-tick work is O(log n), not O(n).
package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Func is a scheduled job body. It receives a context that is cancelled at
// shutdown and must respect it (NFR-008).
type Func func(ctx context.Context)

// Job describes one scheduled task.
type Job struct {
	// ID uniquely identifies the job, used for restore and metrics.
	ID string
	// Interval is the nominal period between runs.
	Interval time.Duration
	// Jitter is the maximum random offset applied around Interval.
	// The effective delay is Interval ± Jitter/2 (FR-1003).
	Jitter time.Duration
	// Func is the job body.
	Func Func
	// Persist, when set, records the next scheduled run so schedules can
	// survive a restart (FR-1002). It runs after each execution.
	Persist func(id string, next time.Time)
}

type entry struct {
	job  Job
	next time.Time
	seq  uint64
}

type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	return h[i].next.Before(h[j].next) || (h[i].next.Equal(h[j].next) && h[i].seq < h[j].seq)
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)   { *h = append(*h, x.(*entry)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// Scheduler is a concurrency-safe job scheduler.
type Scheduler struct {
	clock  domain.Clock
	rand   domain.RandomSource
	logger *log.Logger

	mu   sync.Mutex
	heap entryHeap
	seq  uint64
	byID map[string]*entry

	wake chan struct{}
	done chan struct{}

	wg      sync.WaitGroup
	running atomic.Int64
}

// New creates a scheduler. clock and rand may be nil, in which case the
// real system implementations are used.
func New(clock domain.Clock, rand domain.RandomSource, logger *log.Logger) *Scheduler {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if rand == nil {
		rand = domain.SystemRandom{}
	}
	if logger == nil {
		logger = log.New(nil, log.LevelFromString("warn"))
	}
	s := &Scheduler{
		clock:  clock,
		rand:   rand,
		logger: logger,
		byID:   make(map[string]*entry),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	return s
}

// Add registers jobs. Duplicate IDs replace the previous entry.
func (s *Scheduler) Add(jobs ...Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range jobs {
		if j.Interval <= 0 {
			s.logger.Error("scheduler: rejected job with non-positive interval", "job", j.ID, "interval", j.Interval.String())
			continue
		}
		now := s.clock.Now()
		next := now.Add(s.offset(j.Interval, j.Jitter))
		if old, ok := s.byID[j.ID]; ok {
			old.job = j
			old.next = next
			s.fixLocked(old)
			continue
		}
		e := &entry{job: j, next: next, seq: s.nextSeqLocked()}
		s.byID[j.ID] = e
		heap.Push(&s.heap, e)
	}
	s.signalLocked()
}

// Restore seeds next-run times recorded before a restart (FR-1002).
// Jobs must be Add'ed first; Restore overrides their initial deadline.
func (s *Scheduler) Restore(nextByID map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, next := range nextByID {
		e, ok := s.byID[id]
		if !ok || next.IsZero() {
			continue
		}
		e.next = next
		s.fixLocked(e)
	}
	s.signalLocked()
}

func (s *Scheduler) nextSeqLocked() uint64 {
	s.seq++
	return s.seq
}

func (s *Scheduler) fixLocked(e *entry) {
	for i := 0; i < s.heap.Len(); i++ {
		if s.heap[i] == e {
			heap.Fix(&s.heap, i)
			return
		}
	}
}

func (s *Scheduler) offset(interval, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return interval
	}
	f := s.rand.Float64()
	return interval + time.Duration((2*f-1)*float64(jitter))
}

func (s *Scheduler) signalLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run starts the dispatch loop and blocks until ctx is cancelled.
// Use Stop or ctx cancellation to shut down.
func (s *Scheduler) Run(ctx context.Context) {
	go s.loop(ctx)
	<-ctx.Done()
	s.Stop(30 * time.Second)
}

// Start launches the dispatch loop in the background. Call Stop to shut down.
func (s *Scheduler) Start() {
	go s.loop(context.Background())
}

// Stop cancels the loop and waits up to grace for running jobs to finish
// (NFR-009). Jobs that ignore cancellation are not waited on beyond grace.
func (s *Scheduler) Stop(grace time.Duration) {
	select {
	case <-s.done:
	default:
		close(s.done)
	}

	waited := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(grace):
		s.logger.Warn("scheduler: shutdown grace period elapsed with running jobs")
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	for {
		s.mu.Lock()
		var timer *time.Timer
		var timerC <-chan time.Time
		if s.heap.Len() > 0 {
			delay := time.Until(s.heap[0].next)
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		s.mu.Unlock()

		select {
		case <-s.done:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			s.Stop(0)
			return
		case <-s.wake:
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
			s.dispatch()
		}
	}
}

func (s *Scheduler) dispatch() {
	s.mu.Lock()
	now := s.clock.Now()
	var due []*entry
	for s.heap.Len() > 0 && !s.heap[0].next.After(now) {
		e := heap.Pop(&s.heap).(*entry)
		due = append(due, e)
	}
	for _, e := range due {
		next := now.Add(s.offset(e.job.Interval, e.job.Jitter))
		e.next = next
		heap.Push(&s.heap, e)
	}
	s.mu.Unlock()

	for _, e := range due {
		select {
		case <-s.done:
			return
		default:
		}
		s.runOne(e)
	}
}

func (s *Scheduler) runOne(e *entry) {
	s.wg.Add(1)
	s.running.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.running.Add(-1)
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("scheduler: job panicked and was recovered",
					"job", e.job.ID, "panic", r)
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-s.done
			cancel()
		}()
		e.job.Func(ctx)
		if e.job.Persist != nil {
			s.mu.Lock()
			next := e.next
			s.mu.Unlock()
			e.job.Persist(e.job.ID, next)
		}
	}()
}

// Snapshot returns copyable schedule state: each job's ID and next run time.
func (s *Scheduler) Snapshot() map[string]time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]time.Time, len(s.byID))
	for id, e := range s.byID {
		out[id] = e.next
	}
	return out
}

// Len returns the number of registered jobs.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// Running returns the number of jobs executing right now.
func (s *Scheduler) Running() int64 { return s.running.Load() }
