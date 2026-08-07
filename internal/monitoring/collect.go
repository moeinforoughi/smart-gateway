package monitoring

import (
	"context"
	"net/http"
	"strings"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Ports the exporter reads from. They are deliberately narrow so the scrape
// path is testable with fakes; the composition root wires the real subsystems.

// NodeStore lists the live fleet.
type NodeStore interface {
	ListAllNodes(ctx context.Context) ([]domain.Node, error)
}

// ScoreStore lists the materialized per-node scores.
type ScoreStore interface {
	ListScores(ctx context.Context) ([]domain.NodeScore, error)
}

// HealthStore lists the folded per-node health records in one pass.
type HealthStore interface {
	ListHealths(ctx context.Context) ([]domain.NodeHealth, error)
}

// ConnectionsSampler is the live traffic source (core /connections).
type ConnectionsSampler interface {
	Connections(ctx context.Context) ([]domain.ConnMetric, error)
}

// OutboundReader reports the active outbound.
type OutboundReader interface {
	Current() (nodeID, reason string)
}

// SchedulerReader reports scheduler occupancy.
type SchedulerReader interface {
	Len() int
	Running() int64
}

// DataReader reports the backing store footprint.
type DataReader interface {
	DataSize(ctx context.Context) (int64, error)
}

// BudgetReader reports the current probe-bandwidth window usage.
type BudgetReader interface {
	Used() int64
}

// Exporter owns the metric registry and re-samples subsystem state on every
// scrape. Sampling is best-effort: a failing subsystem contributes zeroes and
// a log line, never a failed scrape (M6 acceptance: metric failures never
// stop execution).
type Exporter struct {
	r       *Registry
	clock   domain.Clock
	logger  *log.Logger
	started time.Time

	// Ports (wired by the composition root; nil ports are tolerated).
	nodes    NodeStore
	scores   ScoreStore
	healths  HealthStore
	traffic  ConnectionsSampler
	outbound OutboundReader
	sched    SchedulerReader
	data     DataReader
	probes   BudgetReader

	lastTraffic map[string]int64 // nodeID \x00 direction -> last cumulative bytes

	// Metric handles.
	up           *Gauge
	uptime       *Gauge
	nodesTotal   *GaugeVec
	nodeScore    *GaugeVec
	nodeLatency  *GaugeVec
	healthChecks *CounterVec
	trafficBytes *CounterVec
	outboundCur  *GaugeVec
	switches     *CounterVec
	coreRestarts *CounterVec
	subSuccess   *GaugeVec
	alerts       *CounterVec
	schedJobs    *Gauge
	schedRunning *Gauge
	dbSize       *Gauge
	probeBytes   *Gauge
}

// NewExporter builds an exporter with the full M6 metric set registered.
// Ports may be assigned afterwards via the With* setters.
func NewExporter(clock domain.Clock, logger *log.Logger) *Exporter {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	e := &Exporter{
		r:           NewRegistry(),
		clock:       clock,
		logger:      logger,
		started:     clock.Now(),
		lastTraffic: make(map[string]int64),
	}
	e.up = e.r.NewGauge("smart_gateway_up", "1 if the gateway process is serving.")
	e.uptime = e.r.NewGauge("smart_gateway_uptime_seconds", "Seconds since the gateway started.")
	e.nodesTotal = e.r.NewGaugeVec("smart_gateway_nodes_total",
		"Nodes in the fleet by health status, protocol and owning subscription.", "status", "protocol", "subscription")
	e.nodeScore = e.r.NewGaugeVec("smart_gateway_node_score",
		"Latest materialized total score per node.", "node", "protocol")
	e.nodeLatency = e.r.NewGaugeVec("smart_gateway_node_latency_ms",
		"Latest EWMA probe latency per node in milliseconds.", "node")
	e.healthChecks = e.r.NewCounterVec("smart_gateway_health_checks_total",
		"Probe results by node, probe stage and outcome.", "node", "kind", "result")
	e.trafficBytes = e.r.NewCounterVec("smart_gateway_node_traffic_bytes",
		"Cumulative traffic attributed to each outbound node.", "node", "direction")
	e.outboundCur = e.r.NewGaugeVec("smart_gateway_outbound_current",
		"1 for the active outbound node, 0 for every other node.", "node")
	e.switches = e.r.NewCounterVec("smart_gateway_switch_total",
		"Outbound switches by reason.", "reason")
	e.coreRestarts = e.r.NewCounterVec("smart_gateway_core_restart_total",
		"sing-box watchdog restarts by reason.", "reason")
	e.subSuccess = e.r.NewGaugeVec("smart_gateway_subscription_last_success",
		"Unix seconds of the last successful refresh per subscription.", "subscription")
	e.alerts = e.r.NewCounterVec("smart_gateway_alert_total",
		"Alerts fired by severity and type.", "severity", "type")
	e.schedJobs = e.r.NewGauge("smart_gateway_scheduler_jobs", "Scheduled jobs registered.")
	e.schedRunning = e.r.NewGauge("smart_gateway_scheduler_running", "Scheduled jobs executing right now.")
	e.dbSize = e.r.NewGauge("smart_gateway_db_size_bytes", "Backing SQLite database size in bytes.")
	e.probeBytes = e.r.NewGauge("smart_gateway_probe_traffic_bytes_total",
		"Probe bandwidth consumed in the current 24h window.")
	return e
}

// WithNodes wires the fleet store.
func (e *Exporter) WithNodes(n NodeStore) *Exporter { e.nodes = n; return e }

// WithScores wires the score store.
func (e *Exporter) WithScores(s ScoreStore) *Exporter { e.scores = s; return e }

// WithHealths wires the health store.
func (e *Exporter) WithHealths(h HealthStore) *Exporter { e.healths = h; return e }

// WithTraffic wires the live traffic sampler.
func (e *Exporter) WithTraffic(t ConnectionsSampler) *Exporter { e.traffic = t; return e }

// WithOutbound wires the active-outbound reader.
func (e *Exporter) WithOutbound(o OutboundReader) *Exporter { e.outbound = o; return e }

// WithScheduler wires the scheduler reader.
func (e *Exporter) WithScheduler(s SchedulerReader) *Exporter { e.sched = s; return e }

// WithData wires the backing-store reader.
func (e *Exporter) WithData(d DataReader) *Exporter { e.data = d; return e }

// WithProbeBudget wires the probe bandwidth budget.
func (e *Exporter) WithProbeBudget(b BudgetReader) *Exporter { e.probes = b; return e }

// Registry exposes the underlying registry (tests, custom collectors).
func (e *Exporter) Registry() *Registry { return e.r }

// Collect re-samples every gauge from its subsystem. Errors and panics are
// logged and skipped; the method always completes so a scrape can never be
// aborted (M6 acceptance: metric failures never stop execution).
func (e *Exporter) Collect(ctx context.Context) {
	e.safe(func() {
		e.up.Set(1)
		e.uptime.Set(time.Since(e.started).Seconds())
	})
	e.safe(func() { e.collectNodes(ctx) })
	e.safe(func() { e.collectScores(ctx) })
	e.safe(func() { e.collectHealths(ctx) })
	e.safe(func() { e.collectTraffic(ctx) })
	e.safe(func() { e.collectOutbound(ctx) })
	e.safe(func() { e.collectScheduler() })
	e.safe(func() { e.collectData(ctx) })
	e.safe(func() { e.collectProbeBudget() })
}

// safe runs fn, recovering panics so one bad subsystem cannot kill a scrape.
func (e *Exporter) safe(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("metrics: sampler panic recovered", "panic", r)
		}
	}()
	fn()
}

// nodeSeries is one breakdown key: status \x00 protocol \x00 subscription.
func nodeSeries(n domain.Node) string {
	return string(n.State) + "\x00" + string(n.Protocol) + "\x00" + n.SubscriptionID
}

func (e *Exporter) collectNodes(ctx context.Context) {
	if e.nodes == nil {
		return
	}
	nodes, err := e.nodes.ListAllNodes(ctx)
	if err != nil {
		e.logger.Warn("metrics: list nodes", "error", err)
		return
	}
	counts := make(map[string]int, len(nodes))
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		key := nodeSeries(n)
		counts[key]++
		seen[n.ID] = true
	}
	for key, c := range counts {
		parts := strings.Split(key, "\x00")
		e.nodesTotal.Set(float64(c), parts[0], parts[1], parts[2])
	}
	// Drop breakdown series whose node left the fleet.
	e.nodesTotal.Prune(func(labels ...string) bool {
		return counts[strings.Join(labels, "\x00")] > 0
	})
	_ = seen
}

func (e *Exporter) collectScores(ctx context.Context) {
	if e.scores == nil {
		return
	}
	scores, err := e.scores.ListScores(ctx)
	if err != nil {
		e.logger.Warn("metrics: list scores", "error", err)
		return
	}
	protos := e.nodeProtocols(ctx)
	for _, s := range scores {
		e.nodeScore.Set(s.Score.Total, s.NodeID, protos[s.NodeID])
	}
}

// nodeProtocols maps node ID to protocol for score labels (one cheap query).
func (e *Exporter) nodeProtocols(ctx context.Context) map[string]string {
	protos := make(map[string]string)
	if e.nodes == nil {
		return protos
	}
	nodes, err := e.nodes.ListAllNodes(ctx)
	if err != nil {
		return protos
	}
	for _, n := range nodes {
		protos[n.ID] = string(n.Protocol)
	}
	return protos
}

func (e *Exporter) collectHealths(ctx context.Context) {
	if e.healths == nil {
		return
	}
	healths, err := e.healths.ListHealths(ctx)
	if err != nil {
		e.logger.Warn("metrics: list healths", "error", err)
		return
	}
	for _, h := range healths {
		e.nodeLatency.Set(float64(h.EWMA.Milliseconds()), h.NodeID)
	}
}

func (e *Exporter) collectTraffic(ctx context.Context) {
	if e.traffic == nil {
		return
	}
	metrics, err := e.traffic.Connections(ctx)
	if err != nil {
		e.logger.Warn("metrics: sample traffic", "error", err)
		return
	}
	for _, m := range metrics {
		e.addTrafficDelta(m.NodeID, "upload", m.Upload)
		e.addTrafficDelta(m.NodeID, "download", m.Download)
	}
}

// addTrafficDelta accumulates only positive deltas so a core restart (counters
// reset) never decreases the Prometheus counter (rate() stays well-formed).
// A reset (cumulative < prev) restarts the baseline at the observed value.
func (e *Exporter) addTrafficDelta(nodeID, direction string, cumulative int64) {
	key := nodeID + "\x00" + direction
	prev, ok := e.lastTraffic[key]
	if !ok {
		e.lastTraffic[key] = cumulative
		e.trafficBytes.Add(uint64(cumulative), nodeID, direction)
		return
	}
	if cumulative >= prev {
		e.trafficBytes.Add(uint64(cumulative-prev), nodeID, direction)
	}
	e.lastTraffic[key] = cumulative
}

func (e *Exporter) collectOutbound(ctx context.Context) {
	if e.outbound == nil {
		return
	}
	nodeID, _ := e.outbound.Current()
	if nodeID == "" {
		return
	}
	e.outboundCur.Set(1, nodeID)
	if e.nodes != nil {
		nodes, err := e.nodes.ListAllNodes(ctx)
		if err == nil {
			for _, n := range nodes {
				if n.ID != nodeID {
					e.outboundCur.Set(0, n.ID)
				}
			}
		}
	}
}

func (e *Exporter) collectScheduler() {
	if e.sched == nil {
		return
	}
	e.schedJobs.Set(float64(e.sched.Len()))
	e.schedRunning.Set(float64(e.sched.Running()))
}

func (e *Exporter) collectData(ctx context.Context) {
	if e.data == nil {
		return
	}
	if size, err := e.data.DataSize(ctx); err == nil {
		e.dbSize.Set(float64(size))
	} else {
		e.logger.Warn("metrics: db size", "error", err)
	}
}

func (e *Exporter) collectProbeBudget() {
	if e.probes == nil {
		return
	}
	e.probeBytes.Set(float64(e.probes.Used()))
}

// Run subscribes to the domain event bus and updates the event-driven
// counters. It blocks until ctx is cancelled. A full buffer or a failed
// counter update is logged, never propagated.
func (e *Exporter) Run(ctx context.Context, bus domain.EventSource) {
	ch, err := bus.Subscribe(ctx)
	if err != nil {
		e.logger.Error("metrics subscriber: subscribe", "error", err)
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
			e.handleEvent(ev)
		}
	}
}

func (e *Exporter) handleEvent(ev domain.Event) {
	switch ev.Type {
	case domain.EvtNodeProbed:
		result := "failure"
		if ok, _ := ev.Payload["ok"].(bool); ok {
			result = "success"
		}
		e.healthChecks.Inc(ev.KeyedByID("node_id"), ev.KeyedByID("stage"), result)
	case domain.EvtOutboundChanged:
		e.switches.Inc(ev.KeyedByID("reason"))
	case domain.EvtCoreRestarted:
		e.coreRestarts.Inc(ev.KeyedByID("reason"))
	case domain.EvtSubscriptionRefreshed:
		if sub := ev.KeyedByID("subscription_id"); sub != "" {
			e.subSuccess.Set(float64(ev.At.Unix()), sub)
		}
	case domain.EvtAlertFired:
		e.alerts.Inc(ev.KeyedByID("severity"), ev.KeyedByID("type"))
	}
}

// ServeHTTP implements the public Prometheus /metrics endpoint.
func (e *Exporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.Collect(r.Context())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := e.r.Render(w); err != nil {
		e.logger.Warn("metrics: render", "error", err)
	}
}
