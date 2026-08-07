// Package monitoring exposes Smart Gateway state in the Prometheus text
// exposition format (version 0.0.4) and drives the alert pipeline (M6).
//
// Design decisions:
//   - Dependency-free: a small hand-rolled registry keeps the dependency tree
//     minimal (consistent with the rest of the project). The output is fully
//     compatible with Prometheus scraping: `# HELP`, `# TYPE`, sorted metric
//     names, label escaping, integer tokens for counters.
//   - Pull sampling: gauges (nodes, scores, traffic, scheduler, DB size) are
//     re-sampled on every `/metrics` scrape so the registry is always fresh.
//   - Event-driven counters: switches, health checks, core restarts and alerts
//     are counted by a bus subscriber (Subscriber.Run), so they survive
//     restart-free scrapes.
//   - Failures never stop execution: every sampler swallows and logs errors;
//     a scrape always renders whatever state was collected.
package monitoring

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// labelNameRE matches the Prometheus label-name grammar.
var labelNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// metricNameRE matches the Prometheus metric-name grammar.
var metricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// Metric is any registered Prometheus metric.
type Metric interface {
	// Name is the fully-qualified metric name.
	Name() string
	// Help is the single-line # HELP description.
	Help() string
	// Type is the # TYPE token (counter | gauge).
	Type() string
	// Write emits every sample line of this metric.
	Write(w io.Writer) error
}

// Registry owns a set of uniquely-named metrics and renders them in the
// Prometheus text exposition format, sorted by name. Safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	metrics map[string]Metric
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]Metric)}
}

// Register adds a metric. A duplicate or malformed name is rejected.
func (r *Registry) Register(m Metric) error {
	if !metricNameRE.MatchString(m.Name()) {
		return fmt.Errorf("invalid metric name %q", m.Name())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.metrics[m.Name()]; ok {
		return fmt.Errorf("metric %q already registered", m.Name())
	}
	r.metrics[m.Name()] = m
	return nil
}

// Render writes the full exposition to w.
func (r *Registry) Render(w io.Writer) error {
	r.mu.Lock()
	names := make([]string, 0, len(r.metrics))
	for n := range r.metrics {
		names = append(names, n)
	}
	sort.Strings(names)
	metrics := make([]Metric, 0, len(names))
	for _, n := range names {
		metrics = append(metrics, r.metrics[n])
	}
	r.mu.Unlock()

	bw := bufio.NewWriter(w)
	for _, m := range metrics {
		if _, err := fmt.Fprintf(bw, "# HELP %s %s\n", m.Name(), m.Help()); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(bw, "# TYPE %s %s\n", m.Name(), m.Type()); err != nil {
			return err
		}
		if err := m.Write(bw); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// escapeLabelValue escapes backslash, double-quote and newline per the text
// format specification.
func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

// Gauge is a single settable scalar.
type Gauge struct {
	name string
	help string
	v    atomic.Int64 // stored as the IEEE-754 bit pattern of the float value
}

// NewGauge registers a single-value gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{name: name, help: help}
	_ = r.Register(g)
	return g
}

// Set stores v.
func (g *Gauge) Set(v float64) { g.v.Store(int64FromBits(v)) }

// Add adds delta to the current value.
func (g *Gauge) Add(delta float64) {
	for {
		cur := g.v.Load()
		if g.v.CompareAndSwap(cur, int64FromBits(bitsToFloat(cur)+delta)) {
			return
		}
	}
}

// Value returns the current value.
func (g *Gauge) Value() float64 { return bitsToFloat(g.v.Load()) }

func (g *Gauge) Name() string { return g.name }
func (g *Gauge) Help() string { return g.help }
func (g *Gauge) Type() string { return "gauge" }
func (g *Gauge) Write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s %s\n", g.name, strconv.FormatFloat(g.Value(), 'g', -1, 64))
	return err
}

// Counter is a single monotonic counter.
type Counter struct {
	name string
	help string
	v    atomic.Uint64
}

// NewCounter registers a single-value counter.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	_ = r.Register(c)
	return c
}

// Add adds delta (clamped at zero).
func (c *Counter) Add(delta uint64) { c.v.Add(delta) }

// Inc increments by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value returns the current value.
func (c *Counter) Value() uint64 { return c.v.Load() }

func (c *Counter) Name() string { return c.name }
func (c *Counter) Help() string { return c.help }
func (c *Counter) Type() string { return "counter" }
func (c *Counter) Write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s %d\n", c.name, c.v.Load())
	return err
}

// labelSet is an ordered list of label names shared by every child of a vec.
type labelSet []string

func (ls labelSet) key(values []string) string { return strings.Join(values, "\x00") }

// sample is one rendered labelled series.
type sample struct {
	labels string // already-escaped `{...}` block, empty for unlabelled
	value  string // pre-rendered token
}

// CounterVec counts labelled series (e.g. switch_total{reason}).
type CounterVec struct {
	name   string
	help   string
	labels labelSet

	mu     sync.Mutex
	values map[string]*atomic.Uint64
}

// NewCounterVec registers a labelled counter. labelNames must be valid label
// names.
func (r *Registry) NewCounterVec(name, help string, labelNames ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labelSet(labelNames), values: make(map[string]*atomic.Uint64)}
	_ = r.Register(c)
	return c
}

// Add increments the series identified by labelValues by delta. The number of
// values must equal the registered label names; mismatches are ignored so a
// bad call can never break a scrape.
func (c *CounterVec) Add(delta uint64, labelValues ...string) {
	if len(labelValues) != len(c.labels) {
		return
	}
	k := c.labels.key(labelValues)
	c.mu.Lock()
	cell, ok := c.values[k]
	if !ok {
		cell = &atomic.Uint64{}
		c.values[k] = cell
	}
	c.mu.Unlock()
	cell.Add(delta)
}

// Inc increments the series by one.
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Value returns the current count of the series, or 0.
func (c *CounterVec) Value(labelValues ...string) uint64 {
	if len(labelValues) != len(c.labels) {
		return 0
	}
	c.mu.Lock()
	cell, ok := c.values[c.labels.key(labelValues)]
	c.mu.Unlock()
	if !ok {
		return 0
	}
	return cell.Load()
}

func (c *CounterVec) Name() string { return c.name }
func (c *CounterVec) Help() string { return c.help }
func (c *CounterVec) Type() string { return "counter" }
func (c *CounterVec) Write(w io.Writer) error {
	c.mu.Lock()
	pairs := make([]struct {
		key   string
		value uint64
	}, 0, len(c.values))
	for k, cell := range c.values {
		pairs = append(pairs, struct {
			key   string
			value uint64
		}{k, cell.Load()})
	}
	c.mu.Unlock()
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	for _, p := range pairs {
		if _, err := fmt.Fprintf(w, "%s%s %d\n", c.name, renderLabels(c.labels, p.key), p.value); err != nil {
			return err
		}
	}
	return nil
}

// GaugeVec holds labelled scalar series (e.g. nodes_total{status,protocol}).
type GaugeVec struct {
	name   string
	help   string
	labels labelSet

	mu     sync.Mutex
	values map[string]*atomic.Int64
}

// NewGaugeVec registers a labelled gauge.
func (r *Registry) NewGaugeVec(name, help string, labelNames ...string) *GaugeVec {
	g := &GaugeVec{name: name, help: help, labels: labelSet(labelNames), values: make(map[string]*atomic.Int64)}
	_ = r.Register(g)
	return g
}

// Set stores v for the series identified by labelValues.
func (g *GaugeVec) Set(v float64, labelValues ...string) {
	if len(labelValues) != len(g.labels) {
		return
	}
	k := g.labels.key(labelValues)
	g.mu.Lock()
	cell, ok := g.values[k]
	if !ok {
		cell = &atomic.Int64{}
		g.values[k] = cell
	}
	g.mu.Unlock()
	cell.Store(int64FromBits(v))
}

// Value returns the last set value of the series, or 0.
func (g *GaugeVec) Value(labelValues ...string) float64 {
	if len(labelValues) != len(g.labels) {
		return 0
	}
	g.mu.Lock()
	cell, ok := g.values[g.labels.key(labelValues)]
	g.mu.Unlock()
	if !ok {
		return 0
	}
	return bitsToFloat(cell.Load())
}

// Delete removes a series (used to drop nodes that left the fleet).
func (g *GaugeVec) Delete(labelValues ...string) {
	if len(labelValues) != len(g.labels) {
		return
	}
	g.mu.Lock()
	delete(g.values, g.labels.key(labelValues))
	g.mu.Unlock()
}

// Prune removes every series whose label values are not accepted by keep.
func (g *GaugeVec) Prune(keep func(labelValues ...string) bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.values {
		values := strings.Split(k, "\x00")
		if !keep(values...) {
			delete(g.values, k)
		}
	}
}

func (g *GaugeVec) Name() string { return g.name }
func (g *GaugeVec) Help() string { return g.help }
func (g *GaugeVec) Type() string { return "gauge" }
func (g *GaugeVec) Write(w io.Writer) error {
	g.mu.Lock()
	pairs := make([]struct {
		key   string
		value float64
	}, 0, len(g.values))
	for k, cell := range g.values {
		pairs = append(pairs, struct {
			key   string
			value float64
		}{k, bitsToFloat(cell.Load())})
	}
	g.mu.Unlock()
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	for _, p := range pairs {
		if _, err := fmt.Fprintf(w, "%s%s %s\n", g.name, renderLabels(g.labels, p.key),
			strconv.FormatFloat(p.value, 'g', -1, 64)); err != nil {
			return err
		}
	}
	return nil
}

// renderLabels reconstructs a `{a="x",b="y"}` block from the ordered label
// names and their NUL-joined values.
func renderLabels(names labelSet, joined string) string {
	if len(names) == 0 {
		return ""
	}
	values := strings.Split(joined, "\x00")
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// int64FromBits stores a float64 as its IEEE-754 bit pattern in an int64 so
// atomic stores stay lock-free.
func int64FromBits(f float64) int64 { return int64(math.Float64bits(f)) }

func bitsToFloat(i int64) float64 { return math.Float64frombits(uint64(i)) }
