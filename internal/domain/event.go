package domain

import (
	"context"
	"time"
)

// EventType names a domain event.
type EventType string

const (
	EvtSubscriptionRefreshed EventType = "subscription.refreshed"
	EvtSubscriptionFailed    EventType = "subscription.failed"
	EvtNodesIngested         EventType = "nodes.ingested"
	EvtNodeHealthChanged     EventType = "node.health_changed"
	EvtScoreUpdated          EventType = "score.updated"
	EvtSelectionChanged      EventType = "selection.changed"
	EvtOutboundChanged       EventType = "outbound.changed"
	EvtCoreRestarted         EventType = "core.restarted"
	EvtNodeFailed            EventType = "node.failed"
	EvtMetricsUpdated        EventType = "metrics.updated"
	EvtNodeProbed            EventType = "node.probed"
	EvtSpeedTested           EventType = "speed.tested"
)

// Event is an immutable, publishable domain event (architecture §6).
type Event struct {
	Type    EventType
	At      time.Time
	Payload map[string]any
}

// Bus is the outbound port for domain events (architecture §6). Implementors
// (in-memory fan-out, monitoring hooks) may be registered by the composition
// root; publishing must never block producers.
type Bus interface {
	Publish(ctx context.Context, e Event) error
}

// KeyedByID returns the payload value stored under key, if present.
func (e Event) KeyedByID(key string) string {
	if v, ok := e.Payload[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
