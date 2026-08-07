package scoring

import (
	"context"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// HealthLookup reads a node's folded health on demand. Implemented by the
// repository.
type HealthLookup interface {
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
}

// Engine keeps the materialized scores in sync with health: it subscribes to
// probe and health-state events and re-scores the affected node (FR-607),
// persisting via the ScoreStore and publishing EvtScoreUpdated.
type Engine struct {
	events domain.EventSource
	health HealthLookup
	store  domain.ScoreStore
	bus    domain.Bus
	cfg    Config
	clock  domain.Clock
	logger *log.Logger
}

// NewEngine wires a scoring engine over the given ports.
func NewEngine(events domain.EventSource, health HealthLookup, store domain.ScoreStore,
	bus domain.Bus, cfg Config, clock domain.Clock, logger *log.Logger) *Engine {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &Engine{events: events, health: health, store: store, bus: bus,
		cfg: cfg, clock: clock, logger: logger}
}

// Run consumes events until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	ch, err := e.events.Subscribe(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			switch ev.Type {
			case domain.EvtNodeProbed, domain.EvtNodeHealthChanged:
				e.onHealthChange(ctx, ev)
			}
		}
	}
}

func (e *Engine) onHealthChange(ctx context.Context, ev domain.Event) {
	nodeID := ev.KeyedByID("node_id")
	if nodeID == "" {
		return
	}
	h, err := e.health.GetNodeHealth(ctx, nodeID)
	if err != nil {
		e.logger.Warn("read health for rescore", "node", nodeID, "error", err)
		return
	}
	now := e.clock.Now()
	s := Score(h, e.cfg, now)
	ns := domain.NodeScore{
		NodeID:      nodeID,
		Score:       s,
		State:       scoreStateFor(s.Eligible),
		EvaluatedAt: now,
		Reason:      reasonFor(h, s, e.cfg),
	}
	if err := e.store.UpsertScore(ctx, ns); err != nil {
		e.logger.Warn("persist score", "node", nodeID, "error", err)
		return
	}
	if e.bus != nil {
		e.bus.Publish(ctx, domain.Event{
			Type: domain.EvtScoreUpdated,
			At:   now,
			Payload: map[string]any{
				"node_id":  nodeID,
				"total":    s.Total,
				"eligible": s.Eligible,
			},
		})
	}
}

// scoreStateFor maps the gated eligibility to the score state machine.
func scoreStateFor(eligible bool) domain.ScoreState {
	if eligible {
		return domain.ScoreEligible
	}
	return domain.ScoreCalculated
}

// reasonFor produces an explainable one-line reason (FR-804 style).
func reasonFor(h domain.NodeHealth, s domain.Score, cfg Config) string {
	switch {
	case !s.Eligible && h.Probes < cfg.MinSamples:
		return "confidence gate: not enough samples"
	case !s.Eligible && h.ConsecutiveFailures >= cfg.FailoverFailures:
		return "failover threshold reached"
	case !s.Eligible && h.LossRate > 0.5:
		return "loss rate too high"
	case !s.Eligible:
		return "stale: no recent probe"
	default:
		return "eligible"
	}
}
