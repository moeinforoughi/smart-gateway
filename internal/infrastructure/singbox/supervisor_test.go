package singbox

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

type fixedRand struct{ v float64 }

func (r fixedRand) Float64() float64 { return r.v }
func (fixedRand) Intn(n int) int     { return n / 2 }

type stepClock struct{ now time.Time }

func (c *stepClock) Now() time.Time { return c.now }

func newSupervisorForTest(opts SupervisorOptions, rnd domain.RandomSource) *Supervisor {
	if opts.RestartMin == 0 {
		opts.RestartMin = time.Second
	}
	if opts.RestartMax == 0 {
		opts.RestartMax = time.Minute
	}
	if opts.RestartLimit == 0 {
		opts.RestartLimit = 5
	}
	return NewSupervisor(opts, log.New(nil, 0), &stepClock{now: time.Unix(0, 0)}, rnd)
}

func TestNewSupervisorDefaults(t *testing.T) {
	s := NewSupervisor(SupervisorOptions{BinaryPath: "sing-box", ConfigDir: "data/core"}, log.New(nil, 0), nil, nil)
	if s.opts.ConfigFileName != "config.json" || s.opts.LogLevel != "info" {
		t.Fatalf("defaults wrong: %+v", s.opts)
	}
	if s.opts.RestartMin != time.Second || s.opts.RestartMax != time.Minute || s.opts.RestartLimit != 5 {
		t.Fatalf("restart defaults wrong: %+v", s.opts)
	}
	if want := filepath.Join("data/core", "config.json"); s.ConfigPath() != want {
		t.Fatalf("ConfigPath=%s want %s", s.ConfigPath(), want)
	}
	if s.gen.SelectorTag() != "smart" {
		t.Fatalf("selector tag=%s", s.gen.SelectorTag())
	}
}

func TestBackoffDelayDoublesAndClamps(t *testing.T) {
	s := newSupervisorForTest(SupervisorOptions{
		RestartMin: time.Second, RestartMax: 8 * time.Second,
	}, fixedRand{v: 0.5}) // no jitter at 0.5
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 8 * time.Second}, // clamped at RestartMax
		{10, 8 * time.Second},
	}
	for _, c := range cases {
		if got := s.backoffDelay(c.attempt); got != c.want {
			t.Errorf("backoffDelay(%d)=%v want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffDelayJitterBounds(t *testing.T) {
	s := newSupervisorForTest(SupervisorOptions{
		RestartMin: time.Second, RestartMax: 8 * time.Second,
	}, fixedRand{v: 0.0}) // -20%
	low := s.backoffDelay(3)
	s2 := newSupervisorForTest(SupervisorOptions{
		RestartMin: time.Second, RestartMax: 8 * time.Second,
	}, fixedRand{v: 1.0}) // +20%
	high := s2.backoffDelay(3)
	if low != 3200*time.Millisecond || high != 4800*time.Millisecond {
		t.Fatalf("jitter bounds: low=%v high=%v (want 3.2s/4.8s)", low, high)
	}
}

func TestRestartsInLastHourWindowing(t *testing.T) {
	clock := &stepClock{now: time.Unix(100000, 0)}
	s := NewSupervisor(SupervisorOptions{ConfigDir: "data/core"}, log.New(nil, 0), clock, nil)
	now := clock.Now()
	s.mu.Lock()
	s.restarts = []time.Time{
		now.Add(-30 * time.Minute), // inside window
		now.Add(-90 * time.Minute), // outside window
	}
	s.mu.Unlock()
	if got := s.RestartsInLastHour(); got != 1 {
		t.Fatalf("RestartsInLastHour=%d want 1", got)
	}
	clock.now = now.Add(31 * time.Minute)
	if got := s.RestartsInLastHour(); got != 0 {
		t.Fatalf("after window slides, RestartsInLastHour=%d want 0", got)
	}
}

func TestCurrentFallsBackToLastSelectionWhenDown(t *testing.T) {
	s := newSupervisorForTest(SupervisorOptions{ConfigDir: "data/core"}, fixedRand{v: 0.5})
	s.mu.Lock()
	s.lastSelection = "node-x"
	s.mu.Unlock()
	got, err := s.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "node-x" {
		t.Fatalf("Current=%q want node-x", got)
	}
}

func TestRunningAndFailedInitially(t *testing.T) {
	s := newSupervisorForTest(SupervisorOptions{ConfigDir: "data/core"}, fixedRand{v: 0.5})
	if s.Running() {
		t.Fatal("should not be running")
	}
	if s.Failed() {
		t.Fatal("should not be failed")
	}
}
