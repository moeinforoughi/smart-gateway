package health

import (
	"context"
	"sync"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// ThroughputStore persists throughput estimates (passive collector + speed
// tests) so GetNodeHealth can fold them into the score inputs.
type ThroughputStore interface {
	RecordThroughput(ctx context.Context, nodeID string, bps float64, at time.Time) error
}

// Collector samples the connections sampler every interval and attributes
// bytes to each node (passive, zero-bandwidth throughput + real usage).
type Collector struct {
	sample   domain.ConnectionsSampler
	store    ThroughputStore
	interval time.Duration
	clock    domain.Clock
	logger   *log.Logger
	bus      domain.Bus

	mu     sync.Mutex
	last   map[string]domain.ConnMetric
	lastAt time.Time
}

// NewCollector builds a passive collector.
func NewCollector(sample domain.ConnectionsSampler, store ThroughputStore, interval time.Duration,
	clock domain.Clock, logger *log.Logger, bus domain.Bus) *Collector {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &Collector{
		sample:   sample,
		store:    store,
		interval: interval,
		clock:    clock,
		logger:   logger,
		bus:      bus,
		last:     make(map[string]domain.ConnMetric),
	}
}

// Run samples until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	if err := c.sampleOnce(ctx); err != nil {
		c.logger.Warn("passive sample failed", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.sampleOnce(ctx); err != nil {
				c.logger.Warn("passive sample failed", "error", err)
			}
		}
	}
}

func (c *Collector) sampleOnce(ctx context.Context) error {
	metrics, err := c.sample.Connections(ctx)
	if err != nil {
		return err
	}
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, m := range metrics {
		prev, ok := c.last[m.NodeID]
		if !ok {
			continue // first observation: seed only
		}
		if m.Download < prev.Download || m.Upload < prev.Upload {
			continue // counters reset (core restart): reseed
		}
		if !c.lastAt.IsZero() {
			elapsed := now.Sub(c.lastAt).Seconds()
			if elapsed > 0 {
				bps := (float64(m.Download-prev.Download) + float64(m.Upload-prev.Upload)) / elapsed
				if bps > 0 {
					if err := c.store.RecordThroughput(ctx, m.NodeID, bps, now); err != nil {
						c.logger.Warn("record throughput", "node", m.NodeID, "error", err)
					}
				}
			}
		}
	}
	// Replace the baseline with the current snapshot atomically.
	c.last = make(map[string]domain.ConnMetric, len(metrics))
	for _, m := range metrics {
		c.last[m.NodeID] = m
	}
	c.lastAt = now
	c.publish(ctx)
	return nil
}

func (c *Collector) publish(ctx context.Context) {
	if c.bus == nil {
		return
	}
	e := domain.Event{Type: domain.EvtMetricsUpdated, At: c.clock.Now()}
	if err := c.bus.Publish(ctx, e); err != nil {
		c.logger.Warn("publish metrics event", "error", err)
	}
}
