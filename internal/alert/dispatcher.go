// Package alert implements Smart Gateway alerting (M6): a Notifier port with
// a generic webhook and a Telegram adapter, plus a Dispatcher that translates
// domain events into alerts, deduplicates and throttles them, and fans them
// out to every configured notifier.
//
// Design decisions:
//   - Non-blocking: Dispatch never blocks producers; notifier failures are
//     logged and never stop the pipeline.
//   - Dedup + throttle: the same (type, key) is suppressed within the dedup
//     window; each alert type is capped per throttle window so a storm of
//     failing subscriptions cannot flood the channel.
//   - Zero notifiers is valid: alerts are then no-ops (logged at debug).
package alert

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Severity is the alert priority.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Valid reports whether s is a known severity.
func (s Severity) Valid() bool {
	switch s {
	case SeverityInfo, SeverityWarning, SeverityCritical:
		return true
	}
	return false
}

// Alert is one notification event.
type Alert struct {
	Severity Severity
	Type     string    // machine type, e.g. "subscription.failed"
	Key      string    // affected entity used for dedup, e.g. subscription id
	Title    string    // short human title
	Message  string    // detailed message
	At       time.Time // when the condition was observed
}

// Notifier delivers alerts to one channel.
type Notifier interface {
	// Name identifies the channel for logs.
	Name() string
	// Notify delivers a. It must respect ctx cancellation.
	Notify(ctx context.Context, a Alert) error
}

// Config tunes dedup and throttling.
type Config struct {
	// DedupWindow suppresses repeats of the same (type, key) within this
	// window. Default 1h.
	DedupWindow time.Duration
	// ThrottleWindow bounds the per-type alert rate. Default 1h.
	ThrottleWindow time.Duration
	// ThrottleLimit caps alerts per type within ThrottleWindow. Default 5.
	ThrottleLimit int
	// FleetDegradedRatio is the failed/total ratio at or above which the
	// periodic fleet check raises a critical alert. Default 0.5.
	FleetDegradedRatio float64
}

// DefaultConfig mirrors the alerting defaults.
func DefaultConfig() Config {
	return Config{
		DedupWindow:        time.Hour,
		ThrottleWindow:     time.Hour,
		ThrottleLimit:      5,
		FleetDegradedRatio: 0.5,
	}
}

// Store is the fleet reader used by the periodic health check.
type Store interface {
	CountNodes(ctx context.Context) (int, error)
	CountNodesByState(ctx context.Context) (map[domain.NodeState]int, error)
}

// Dispatcher deduplicates, throttles and delivers alerts.
type Dispatcher struct {
	cfg       Config
	clock     domain.Clock
	logger    *log.Logger
	notifiers []Notifier
	store     Store
	bus       domain.Bus

	mu       sync.Mutex
	lastSent map[string]time.Time   // type\x00key -> last delivery
	sent     map[string][]time.Time // type -> delivery times in window
}

// NewDispatcher builds a dispatcher with defaults applied. Notifiers are
// added with AddNotifier; the fleet store with WithStore.
func NewDispatcher(cfg Config, clock domain.Clock, logger *log.Logger) *Dispatcher {
	d := DefaultConfig()
	if cfg.DedupWindow > 0 {
		d.DedupWindow = cfg.DedupWindow
	}
	if cfg.ThrottleWindow > 0 {
		d.ThrottleWindow = cfg.ThrottleWindow
	}
	if cfg.ThrottleLimit > 0 {
		d.ThrottleLimit = cfg.ThrottleLimit
	}
	if cfg.FleetDegradedRatio > 0 {
		d.FleetDegradedRatio = cfg.FleetDegradedRatio
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &Dispatcher{
		cfg:      d,
		clock:    clock,
		logger:   logger,
		lastSent: make(map[string]time.Time),
		sent:     make(map[string][]time.Time),
	}
}

// AddNotifier registers a delivery channel.
func (d *Dispatcher) AddNotifier(n Notifier) { d.notifiers = append(d.notifiers, n) }

// WithStore wires the fleet store for the periodic health check.
func (d *Dispatcher) WithStore(s Store) *Dispatcher { d.store = s; return d }

// Notifiers returns the registered channels (tests, dashboard).
func (d *Dispatcher) Notifiers() []Notifier { return d.notifiers }

// Dispatch delivers a unless it is deduplicated or throttled. With no
// notifiers configured an alert is a no-op. Errors from individual notifiers
// are logged and aggregated but never stop delivery to the rest.
func (d *Dispatcher) Dispatch(ctx context.Context, a Alert) error {
	if !a.Severity.Valid() {
		a.Severity = SeverityWarning
	}
	if a.At.IsZero() {
		a.At = d.clock.Now()
	}
	if len(d.notifiers) == 0 {
		d.logger.Debug("alert dropped: no notifiers configured",
			"type", a.Type, "severity", a.Severity)
		return nil
	}
	now := d.clock.Now()
	if !d.allow(now, a) {
		return nil // deduplicated or throttled
	}
	d.record(now, a)

	var firstErr error
	for _, n := range d.notifiers {
		if err := n.Notify(ctx, a); err != nil {
			d.logger.Warn("alert notifier failed",
				"notifier", n.Name(), "type", a.Type, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	d.publishFired(ctx, a)
	return firstErr
}

func (d *Dispatcher) allow(now time.Time, a Alert) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := a.Type + "\x00" + a.Key
	if last, ok := d.lastSent[key]; ok && now.Sub(last) < d.cfg.DedupWindow {
		d.logger.Debug("alert deduplicated", "type", a.Type, "key", a.Key)
		return false
	}
	if d.cfg.ThrottleLimit > 0 {
		kept := d.sent[a.Type][:0]
		for _, t := range d.sent[a.Type] {
			if now.Sub(t) < d.cfg.ThrottleWindow {
				kept = append(kept, t)
			}
		}
		d.sent[a.Type] = kept
		if len(d.sent[a.Type]) >= d.cfg.ThrottleLimit {
			d.logger.Debug("alert throttled", "type", a.Type, "limit", d.cfg.ThrottleLimit)
			return false
		}
	}
	return true
}

func (d *Dispatcher) record(now time.Time, a Alert) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSent[a.Type+"\x00"+a.Key] = now
	if d.cfg.ThrottleLimit > 0 {
		d.sent[a.Type] = append(d.sent[a.Type], now)
	}
}

// publishFired makes the alert observable to metrics subscribers.
func (d *Dispatcher) publishFired(ctx context.Context, a Alert) {
	if d.bus == nil {
		return
	}
	_ = d.bus.Publish(ctx, domain.Event{
		Type: domain.EvtAlertFired,
		At:   a.At,
		Payload: map[string]any{
			"severity": string(a.Severity),
			"type":     a.Type,
		},
	})
}

// Run subscribes to the domain event bus and maps events to alerts. It
// blocks until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context, bus domain.Bus, source domain.EventSource) {
	d.bus = bus
	ch, err := source.Subscribe(ctx)
	if err != nil {
		d.logger.Error("alert dispatcher: subscribe", "error", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if a, ok := d.alertFor(ev); ok {
				if err := d.Dispatch(ctx, a); err != nil {
					d.logger.Warn("alert dispatch failed", "type", a.Type, "error", err)
				}
			}
		}
	}
}

// alertFor maps a domain event to an alert. ok is false for events that are
// not alert-worthy.
func (d *Dispatcher) alertFor(ev domain.Event) (Alert, bool) {
	switch ev.Type {
	case domain.EvtSubscriptionFailed:
		return Alert{
			Severity: SeverityWarning,
			Type:     "subscription.failed",
			Key:      ev.KeyedByID("subscription_id"),
			Title:    "Subscription refresh failing",
			Message: fmt.Sprintf("subscription %s failed: %s (%s consecutive failures)",
				ev.KeyedByID("subscription_id"), ev.KeyedByID("error"), ev.KeyedByID("failures")),
			At: ev.At,
		}, true
	case domain.EvtCoreRestarted:
		return Alert{
			Severity: SeverityCritical,
			Type:     "core.restarted",
			Key:      "core",
			Title:    "sing-box restarted",
			Message:  "sing-box process restarted: " + ev.KeyedByID("reason"),
			At:       ev.At,
		}, true
	case domain.EvtOutboundChanged:
		if reason := ev.KeyedByID("reason"); strings.Contains(strings.ToLower(reason), "failover") {
			return Alert{
				Severity: SeverityCritical,
				Type:     "outbound.failover",
				Key:      ev.KeyedByID("node"),
				Title:    "Outbound failover",
				Message:  "active outbound failed over to " + ev.KeyedByID("node") + " (" + reason + ")",
				At:       ev.At,
			}, true
		}
	}
	return Alert{}, false
}

// Check runs the periodic fleet and storage probes: it raises a critical
// alert when more than FleetDegradedRatio of the fleet is failed and a
// warning when the backing store becomes unreachable.
func (d *Dispatcher) Check(ctx context.Context) {
	if d.store == nil {
		return
	}
	total, err := d.store.CountNodes(ctx)
	if err != nil {
		d.Dispatch(ctx, Alert{
			Severity: SeverityWarning,
			Type:     "storage.error",
			Key:      "store",
			Title:    "Storage read failed",
			Message:  "backing store unreachable: " + err.Error(),
			At:       d.clock.Now(),
		})
		return
	}
	if total == 0 {
		return
	}
	byState, err := d.store.CountNodesByState(ctx)
	if err != nil {
		d.Dispatch(ctx, Alert{
			Severity: SeverityWarning,
			Type:     "storage.error",
			Key:      "store",
			Title:    "Storage read failed",
			Message:  "backing store unreachable: " + err.Error(),
			At:       d.clock.Now(),
		})
		return
	}
	failed := byState[domain.StateFailed] + byState[domain.StateDegraded]
	ratio := float64(failed) / float64(total)
	if ratio >= d.cfg.FleetDegradedRatio {
		d.Dispatch(ctx, Alert{
			Severity: SeverityCritical,
			Type:     "fleet.degraded",
			Key:      "fleet",
			Title:    "Fleet degraded",
			Message: fmt.Sprintf("%d of %d nodes failed/degraded (%.0f%%)",
				failed, total, ratio*100),
			At: d.clock.Now(),
		})
	}
}
