// Package routing implements the RouteManager: the single owner of "exactly
// one active outbound". It decides which node the smart selector points at
// and applies it through the domain.OutboundApplier port. The M4 SelectionPolicy
// (scoring/selector) decides WHO should be active; routing owns applying it,
// the admin override, and switch logging. Priority-based selection remains as
// the bootstrap fallback before the first scores exist. Every decision stays
// explainable (FR-705/706, FR-804).
package routing

import (
	"context"
	"fmt"
	"sync"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Config configures the route manager.
type Config struct {
	SelectorTag string // default: smart
}

// Manager keeps exactly one active outbound and logs every switch.
type Manager struct {
	cfg     Config
	applier domain.OutboundApplier
	store   domain.SwitchLogStore
	clock   domain.Clock
	logger  *log.Logger
	bus     domain.Bus
	policy  domain.SelectionPolicy
	scores  domain.ScoreStore

	mu       sync.Mutex
	override string // forced node ID; empty means automatic
	current  string // last applied node ID
	reason   string // why the current selection was made
}

// New builds a route manager.
func New(applier domain.OutboundApplier, store domain.SwitchLogStore, cfg Config,
	policy domain.SelectionPolicy, scores domain.ScoreStore,
	clock domain.Clock, logger *log.Logger, bus domain.Bus) *Manager {
	if cfg.SelectorTag == "" {
		cfg.SelectorTag = "smart"
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &Manager{
		cfg:     cfg,
		applier: applier,
		store:   store,
		clock:   clock,
		logger:  logger,
		bus:     bus,
		policy:  policy,
		scores:  scores,
	}
}

// Current returns the last applied node ID and its reason.
func (m *Manager) Current() (nodeID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current, m.reason
}

// Override forces the active outbound to nodeID (admin override). The override
// persists until Released. nodeID must be a member of nodes, otherwise the
// selection is left untouched and an error is returned.
func (m *Manager) Override(ctx context.Context, nodes []domain.Node, nodeID, reason string) error {
	for _, n := range nodes {
		if n.ID == nodeID {
			m.mu.Lock()
			m.override = nodeID
			m.mu.Unlock()
			return m.apply(ctx, nodeID, reason)
		}
	}
	return fmt.Errorf("override node %q is not in the active node set", nodeID)
}

// Release clears any override and re-evaluates the best node.
func (m *Manager) Release(ctx context.Context, nodes []domain.Node) error {
	m.mu.Lock()
	m.override = ""
	m.mu.Unlock()
	return m.EnsureActive(ctx, nodes)
}

// EnsureActive picks the desired active node and applies it when the current
// selection differs. Callers (node refresh, startup, health updates) invoke
// this to keep "exactly one active" invariant. Priority selection bootstraps
// the first selection; the SelectionPolicy refines it afterwards.
func (m *Manager) EnsureActive(ctx context.Context, nodes []domain.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	m.mu.Lock()
	override := m.override
	cur := m.current
	m.mu.Unlock()

	nodeID, reason := "", ""
	if override != "" {
		for _, n := range nodes {
			if n.ID == override {
				nodeID, reason = override, "override"
				break
			}
		}
		if nodeID == "" {
			return nil // override not in the active set; leave untouched
		}
	} else if m.policy != nil {
		var err error
		nodeID, reason, err = m.policy.Decide(ctx, nodes, cur)
		if err != nil {
			return fmt.Errorf("selection policy: %w", err)
		}
	}
	if nodeID == "" {
		// No policy, or no eligible candidates on first selection: bootstrap
		// with the M2 priority pick. A policy returning empty while a current
		// node exists means "keep current" (hysteresis/cooldown/no candidate).
		if m.policy == nil || cur == "" {
			nodeID = m.pickBest(nodes)
			reason = "priority"
		}
	}
	if nodeID == "" || nodeID == cur {
		return nil
	}
	return m.apply(ctx, nodeID, reason)
}

// pickBest selects the active node: the highest-priority subscription's
// earliest node (bootstrap policy; deterministic). Used only before the first
// scores exist and for admin overrides.
func (m *Manager) pickBest(nodes []domain.Node) string {
	if len(nodes) == 0 {
		return ""
	}
	best := nodes[0]
	for _, n := range nodes[1:] {
		if n.SubscriptionID == "" {
			continue
		}
		if n.Priority > best.Priority || (n.Priority == best.Priority && n.CreatedAt.Before(best.CreatedAt)) {
			best = n
		}
	}
	return best.ID
}

func (m *Manager) apply(ctx context.Context, nodeID, reason string) error {
	m.mu.Lock()
	prev := m.current
	m.mu.Unlock()

	if err := m.applier.ApplySelection(ctx, nodeID); err != nil {
		return err
	}
	m.mu.Lock()
	m.current = nodeID
	m.reason = reason
	m.mu.Unlock()

	if prev != "" && prev != nodeID && m.store != nil {
		now := m.clock.Now()
		prevScore, newScore := m.scoresFor(ctx, prev, nodeID)
		rec := domain.SwitchRecord{
			ID:        domain.NewUUIDv7(now),
			At:        now,
			FromID:    prev,
			ToID:      nodeID,
			Reason:    reason,
			PrevScore: prevScore,
			NewScore:  newScore,
		}
		if err := m.store.RecordSwitch(ctx, rec); err != nil {
			m.logger.Warn("persist switch record", "error", err)
		}
	}
	m.logger.Info("outbound switched", "from", prev, "to", nodeID, "reason", reason)
	m.publish(ctx, domain.EvtSelectionChanged, map[string]any{
		"from": prev, "to": nodeID, "reason": reason,
	})
	m.publish(ctx, domain.EvtOutboundChanged, map[string]any{
		"node": nodeID, "reason": reason,
	})
	return nil
}

func (m *Manager) publish(ctx context.Context, t domain.EventType, payload map[string]any) {
	if m.bus == nil {
		return
	}
	if err := m.bus.Publish(ctx, domain.Event{Type: t, At: m.clock.Now(), Payload: payload}); err != nil {
		m.logger.Warn("publish event", "type", t, "error", err)
	}
}

// scoresFor enriches switch records with the persisted totals, tolerating
// missing scores (nil-safe).
func (m *Manager) scoresFor(ctx context.Context, prev, next string) (prevScore, newScore float64) {
	if m.scores == nil {
		return 0, 0
	}
	if p, err := m.scores.GetNodeScore(ctx, prev); err == nil {
		prevScore = p.Score.Total
	}
	if n, err := m.scores.GetNodeScore(ctx, next); err == nil {
		newScore = n.Score.Total
	}
	return prevScore, newScore
}
