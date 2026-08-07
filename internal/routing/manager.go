// Package routing implements the RouteManager: the single owner of "exactly
// one active outbound". It decides which node the smart selector points at
// and applies it through the domain.OutboundApplier port. Selection policy is
// deliberately simple at M2 (priority-based) and is replaced by the scoring
// engine in M4; every decision stays explainable (FR-705/706, FR-804).
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

	mu       sync.Mutex
	override string // forced node ID; empty means automatic
	current  string // last applied node ID
	reason   string // why the current selection was made
}

// New builds a route manager.
func New(applier domain.OutboundApplier, store domain.SwitchLogStore, cfg Config,
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

// EnsureActive picks the best eligible node and applies it when the current
// selection differs. Callers (node refresh, startup, health updates) invoke
// this to keep "exactly one active" invariant.
func (m *Manager) EnsureActive(ctx context.Context, nodes []domain.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	m.mu.Lock()
	override := m.override
	cur := m.current
	m.mu.Unlock()

	candidate := m.pickBest(nodes, override)
	if candidate == "" || candidate == cur {
		return nil
	}
	reason := "priority"
	if override != "" {
		reason = "override"
	}
	return m.apply(ctx, candidate, reason)
}

// pickBest selects the active node: the override when set, otherwise the
// highest-priority subscription's earliest node (M2 policy; deterministic).
func (m *Manager) pickBest(nodes []domain.Node, override string) string {
	if override != "" {
		for _, n := range nodes {
			if n.ID == override {
				return override
			}
		}
		return ""
	}
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
		rec := domain.SwitchRecord{
			ID:        domain.NewUUIDv7(m.clock.Now()),
			At:        m.clock.Now(),
			FromID:    prev,
			ToID:      nodeID,
			Reason:    reason,
			PrevScore: 0,
			NewScore:  0,
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
