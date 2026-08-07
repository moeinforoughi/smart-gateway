// Package selector implements the M4 selection policy: hysteresis + dwell
// cooldown + hard failover + recovery staging over the materialized scores
// (FR-705/706, FR-804). The policy is pure: it reads the ScoreStore and the
// fleet, and returns the desired active outbound; routing applies it.
package selector

import (
	"context"
	"sync"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Config is the selector policy knobs.
type Config struct {
	// SwitchDelta is the fractional improvement a candidate must show over
	// the current node before a switch (hysteresis). Default 0.10.
	SwitchDelta float64
	// DwellTime is the minimum time between automatic switches. Default 5m.
	DwellTime time.Duration
	// RecoveryWindow keeps a recovered node out of candidacy for this long
	// after a failover away from it, staging recovery. Default 15m.
	RecoveryWindow time.Duration
	// TopN bounds the candidate pool read from TopEligible. Default 20.
	TopN int
	// Manual disables automatic switching (operator-controlled selection).
	Manual bool
}

// DefaultConfig mirrors the scoring/selector block defaults.
func DefaultConfig() Config {
	return Config{
		SwitchDelta:    0.10,
		DwellTime:      5 * time.Minute,
		RecoveryWindow: 15 * time.Minute,
		TopN:           20,
	}
}

// providerDownRatio marks a provider as down when no more than this fraction
// of its fleet nodes are eligible (provider-wide death penalty, FR-40x).
const providerDownRatio = 0.5

// providerDownPenalty scales the effective score of nodes from a down
// provider so a single healthy node cannot mask a dead provider.
const providerDownPenalty = 0.5

// Policy implements domain.SelectionPolicy.
type Policy struct {
	store  domain.ScoreStore
	cfg    Config
	clock  domain.Clock
	logger *log.Logger

	mu            sync.Mutex
	lastSwitch    time.Time
	recoveryUntil map[string]time.Time
}

// NewPolicy wires a selection policy over the score store.
func NewPolicy(store domain.ScoreStore, cfg Config, clock domain.Clock, logger *log.Logger) *Policy {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	if cfg.SwitchDelta <= 0 {
		cfg.SwitchDelta = DefaultConfig().SwitchDelta
	}
	if cfg.DwellTime <= 0 {
		cfg.DwellTime = DefaultConfig().DwellTime
	}
	if cfg.RecoveryWindow <= 0 {
		cfg.RecoveryWindow = DefaultConfig().RecoveryWindow
	}
	if cfg.TopN <= 0 {
		cfg.TopN = DefaultConfig().TopN
	}
	return &Policy{store: store, cfg: cfg, clock: clock, logger: logger,
		recoveryUntil: make(map[string]time.Time)}
}

// Decide returns the node that should become the active outbound, or the
// empty string when the current selection stays. The reason is
// explainable and directly useful to operators (FR-804).
func (p *Policy) Decide(ctx context.Context, nodes []domain.Node, currentID string) (string, string, error) {
	if p.cfg.Manual {
		return "", "manual mode: operator controls selection", nil
	}
	if len(nodes) == 0 {
		return "", "no nodes in fleet", nil
	}

	top, err := p.store.TopEligible(ctx, p.cfg.TopN)
	if err != nil {
		return "", "", err
	}
	if len(top) == 0 {
		return "", "no eligible candidates", nil
	}

	eligible := make(map[string]bool, len(top))
	best := ""
	var bestTotal, currentTotal float64
	now := p.clock.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	providers := p.providerState(nodes, top)
	for _, ns := range top {
		eligible[ns.NodeID] = true
		if ns.NodeID == currentID {
			currentTotal = ns.Score.Total
			continue
		}
		if rec := p.recoveryUntil[ns.NodeID]; !rec.IsZero() && now.Before(rec) {
			continue // recovery staging: not yet allowed back
		}
		total := ns.Score.Total
		if providers[ns.NodeID] {
			total *= providerDownPenalty
		}
		if total > bestTotal {
			bestTotal = total
			best = ns.NodeID
		}
	}
	if best == "" {
		return "", "no better candidate", nil
	}

	if currentID != "" && eligible[currentID] {
		// Hysteresis: candidate must beat the current node by switch_delta.
		required := currentTotal * p.cfg.SwitchDelta
		if bestTotal-currentTotal < required {
			return "", "hysteresis: candidate not better by switch_delta", nil
		}
		if !p.lastSwitch.IsZero() && now.Sub(p.lastSwitch) < p.cfg.DwellTime {
			return "", "cooldown: dwell time not elapsed", nil
		}
		p.lastSwitch = now
		// The switched-away node is staged for recovery.
		p.recoveryUntil[currentID] = now.Add(p.cfg.RecoveryWindow)
		return best, "switch: candidate beats current by switch_delta", nil
	}

	// Failover: the current node is not eligible (failed/unknown) — replace
	// it immediately, no dwell gate. The failed node cannot return until its
	// recovery window passes while staying healthy.
	p.lastSwitch = now
	p.recoveryUntil[currentID] = now.Add(p.cfg.RecoveryWindow)
	return best, "failover: current node unhealthy", nil
}

// providerState returns, per fleet node, whether its provider is down: no
// more than half of the provider's fleet nodes are eligible.
func (p *Policy) providerState(nodes []domain.Node, top []domain.NodeScore) map[string]bool {
	eligible := make(map[string]bool, len(top))
	for _, ns := range top {
		eligible[ns.NodeID] = true
	}
	members := make(map[string]int)
	healthy := make(map[string]int)
	for _, n := range nodes {
		members[n.SubscriptionID]++
		if eligible[n.ID] {
			healthy[n.SubscriptionID]++
		}
	}
	down := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		total, ok := members[n.SubscriptionID]
		if !ok || total == 0 {
			continue
		}
		down[n.ID] = float64(healthy[n.SubscriptionID])/float64(total) <= providerDownRatio
	}
	return down
}
