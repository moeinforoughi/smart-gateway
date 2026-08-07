package monitoring

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/infrastructure/bus"
	"smart-gateway/internal/log"
)

func TestRenderSortedWithHelpType(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("z_metric", "the z metric")
	g := r.NewGauge("a_metric", "the a metric")
	c.Inc()
	g.Set(42.5)

	var buf bytes.Buffer
	if err := r.Render(&buf); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "# HELP a_metric the a metric\n# TYPE a_metric gauge\na_metric 42.5\n") {
		t.Fatalf("gauge block missing:\n%s", got)
	}
	if !strings.Contains(got, "# HELP z_metric the z metric\n# TYPE z_metric counter\nz_metric 1\n") {
		t.Fatalf("counter block missing:\n%s", got)
	}
	if strings.Index(got, "a_metric") > strings.Index(got, "z_metric") {
		t.Fatalf("metrics not sorted by name:\n%s", got)
	}
}

func TestRenderCounterIntegerTokens(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("smart_gateway_tests_total", "tests")
	c.Add(1234567890)
	var buf bytes.Buffer
	_ = r.Render(&buf)
	if !strings.Contains(buf.String(), "smart_gateway_tests_total 1234567890\n") {
		t.Fatalf("expected integer token:\n%s", buf.String())
	}
}

func TestDuplicateRegistrationRejected(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("dup", "d")
	if err := r.Register(c); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestLabelEscaping(t *testing.T) {
	r := NewRegistry()
	v := r.NewGaugeVec("labelled", "l", "node", "reason")
	v.Set(1, `a"b\c`, "line\nbreak")
	var buf bytes.Buffer
	_ = r.Render(&buf)
	want := `labelled{node="a\"b\\c",reason="line\nbreak"} 1`
	if !strings.Contains(buf.String(), want) {
		t.Fatalf("escaping wrong.\nwant substring: %s\ngot:\n%s", want, buf.String())
	}
}

func TestCounterVecAddAndLabelMismatchIgnored(t *testing.T) {
	r := NewRegistry()
	v := r.NewCounterVec("switch_total", "switches", "reason")
	v.Inc("failover")
	v.Add(3, "cooldown")
	if v.Value("failover") != 1 || v.Value("cooldown") != 3 {
		t.Fatalf("values wrong: failover=%d cooldown=%d", v.Value("failover"), v.Value("cooldown"))
	}
	v.Inc("failover", "extra") // mismatched label count: must be ignored
	if v.Value("failover") != 1 {
		t.Fatalf("mismatched label call mutated state: %d", v.Value("failover"))
	}
	if v.Value("missing") != 0 {
		t.Fatal("unknown series must read 0")
	}
}

func TestCounterVecRenderSorted(t *testing.T) {
	r := NewRegistry()
	v := r.NewCounterVec("sorted_switch_total", "s", "reason")
	v.Inc("zzz")
	v.Inc("aaa")
	var buf bytes.Buffer
	_ = r.Render(&buf)
	got := buf.String()
	if strings.Index(got, `reason="aaa"`) > strings.Index(got, `reason="zzz"`) {
		t.Fatalf("series not sorted:\n%s", got)
	}
}

func TestGaugeVecPrune(t *testing.T) {
	r := NewRegistry()
	v := r.NewGaugeVec("prune_total", "p", "node")
	v.Set(1, "a")
	v.Set(1, "b")
	v.Set(1, "c")
	v.Prune(func(labels ...string) bool { return labels[0] == "a" || labels[0] == "c" })
	var buf bytes.Buffer
	_ = r.Render(&buf)
	if strings.Contains(buf.String(), `node="b"`) {
		t.Fatalf("pruned series still rendered:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), `node="a"`) || !strings.Contains(buf.String(), `node="c"`) {
		t.Fatalf("kept series missing:\n%s", buf.String())
	}
}

func TestInvalidMetricNameRejected(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("bad name", "h")
	if err := r.Register(c); err == nil {
		t.Fatal("expected invalid name error")
	}
}

func TestGaugeAtomicFloat(t *testing.T) {
	g := &Gauge{name: "g", help: "h"}
	g.Set(1.5)
	if g.Value() != 1.5 {
		t.Fatalf("gauge value %v", g.Value())
	}
	g.Add(0.25)
	if g.Value() != 1.75 {
		t.Fatalf("gauge value after add %v", g.Value())
	}
	g.Set(-3)
	if g.Value() != -3 {
		t.Fatalf("gauge negative value %v", g.Value())
	}
}

// ---- exporter sampling ----

type fakeNodes struct{ nodes []domain.Node }

func (f fakeNodes) ListAllNodes(context.Context) ([]domain.Node, error) { return f.nodes, nil }

type fakeScores struct{ scores []domain.NodeScore }

func (f fakeScores) ListScores(context.Context) ([]domain.NodeScore, error) { return f.scores, nil }

type fakeHealths struct{ healths []domain.NodeHealth }

func (f fakeHealths) ListHealths(context.Context) ([]domain.NodeHealth, error) { return f.healths, nil }

type fakeTraffic struct{ metrics []domain.ConnMetric }

func (f fakeTraffic) Connections(context.Context) ([]domain.ConnMetric, error) { return f.metrics, nil }

type fakeOutbound struct{ id string }

func (f fakeOutbound) Current() (string, string) { return f.id, "failover" }

type fakeSched struct {
	jobs    int
	running int64
}

func (f fakeSched) Len() int       { return f.jobs }
func (f fakeSched) Running() int64 { return f.running }

type fakeData struct{ size int64 }

func (f fakeData) DataSize(context.Context) (int64, error) { return f.size, nil }

type fakeBudget struct{ used int64 }

func (f fakeBudget) Used() int64 { return f.used }

func newNode(id, protocol, sub, state string) domain.Node {
	return domain.Node{
		ID:             id,
		Protocol:       domain.Protocol(protocol),
		SubscriptionID: sub,
		State:          domain.NodeState(state),
	}
}

func TestExporterCollectSamplesAllGauges(t *testing.T) {
	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	e.WithNodes(fakeNodes{nodes: []domain.Node{
		newNode("n1", "vless", "s1", "healthy"),
		newNode("n2", "trojan", "s1", "failed"),
	}}).WithScores(fakeScores{scores: []domain.NodeScore{
		{NodeID: "n1", Score: domain.Score{Total: 0.9}},
	}}).WithHealths(fakeHealths{healths: []domain.NodeHealth{
		{NodeID: "n1", EWMA: 45 * time.Millisecond},
	}}).WithTraffic(fakeTraffic{metrics: []domain.ConnMetric{
		{NodeID: "n1", Upload: 100, Download: 500},
	}}).WithOutbound(fakeOutbound{id: "n1"}).WithScheduler(fakeSched{jobs: 3, running: 1}).
		WithData(fakeData{size: 4096}).WithProbeBudget(fakeBudget{used: 1024})

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptestReq())
	got := rec.Body.String()
	for _, want := range []string{
		"smart_gateway_up 1",
		"smart_gateway_nodes_total{status=\"healthy\",protocol=\"vless\",subscription=\"s1\"} 1",
		"smart_gateway_nodes_total{status=\"failed\",protocol=\"trojan\",subscription=\"s1\"} 1",
		"smart_gateway_node_score{node=\"n1\",protocol=\"vless\"} 0.9",
		"smart_gateway_node_latency_ms{node=\"n1\"} 45",
		"smart_gateway_node_traffic_bytes{node=\"n1\",direction=\"upload\"} 100",
		"smart_gateway_node_traffic_bytes{node=\"n1\",direction=\"download\"} 500",
		"smart_gateway_outbound_current{node=\"n1\"} 1",
		"smart_gateway_outbound_current{node=\"n2\"} 0",
		"smart_gateway_scheduler_jobs 3",
		"smart_gateway_scheduler_running 1",
		"smart_gateway_db_size_bytes 4096",
		"smart_gateway_probe_traffic_bytes_total 1024",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestExporterNilPortsRender(t *testing.T) {
	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptestReq())
	if !strings.Contains(rec.Body.String(), "smart_gateway_up 1") {
		t.Fatalf("nil ports must still render:\n%s", rec.Body.String())
	}
}

func TestExporterSamplingErrorNeverAborts(t *testing.T) {
	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	e.WithNodes(panicNodes{})
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptestReq())
	if !strings.Contains(rec.Body.String(), "smart_gateway_up 1") {
		t.Fatalf("failing sampler must not abort the scrape:\n%s", rec.Body.String())
	}
}

type panicNodes struct{}

func (panicNodes) ListAllNodes(context.Context) ([]domain.Node, error) {
	panic("boom")
}

func TestTrafficDeltasMonotonicAcrossReset(t *testing.T) {
	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	e.WithTraffic(fakeTraffic{metrics: []domain.ConnMetric{{NodeID: "n1", Upload: 100}}})

	e.Collect(context.Background())
	if got := e.trafficBytes.Value("n1", "upload"); got != 100 {
		t.Fatalf("first sample %d", got)
	}
	e.Collect(context.Background())
	if got := e.trafficBytes.Value("n1", "upload"); got != 100 {
		t.Fatalf("no delta must not double-count: %d", got)
	}
	// Core restart resets the cumulative counters; the delta must be 0.
	e.WithTraffic(fakeTraffic{metrics: []domain.ConnMetric{{NodeID: "n1", Upload: 0}}})
	e.Collect(context.Background())
	if got := e.trafficBytes.Value("n1", "upload"); got != 100 {
		t.Fatalf("reset must not decrease the counter: %d", got)
	}
	// And growth resumes from the new baseline.
	e.WithTraffic(fakeTraffic{metrics: []domain.ConnMetric{{NodeID: "n1", Upload: 25}}})
	e.Collect(context.Background())
	if got := e.trafficBytes.Value("n1", "upload"); got != 125 {
		t.Fatalf("growth after reset: %d", got)
	}
}

// ---- event subscriber ----

func TestSubscriberCountersFromEvents(t *testing.T) {
	b := bus.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	done := make(chan struct{})
	go func() { e.Run(ctx, b); close(done) }()
	waitFor(t, func() bool { return b.SubscriberCount() >= 1 })

	mustPublish(t, b, domain.EvtNodeProbed, map[string]any{"node_id": "n1", "stage": "deep", "ok": true})
	mustPublish(t, b, domain.EvtNodeProbed, map[string]any{"node_id": "n1", "stage": "deep", "ok": false})
	mustPublish(t, b, domain.EvtOutboundChanged, map[string]any{"node": "n1", "reason": "failover"})
	mustPublish(t, b, domain.EvtCoreRestarted, map[string]any{"reason": "crash"})
	mustPublish(t, b, domain.EvtAlertFired, map[string]any{"severity": "warning", "type": "core.restarted"})

	waitFor(t, func() bool {
		return e.healthChecks.Value("n1", "deep", "success") == 1 &&
			e.healthChecks.Value("n1", "deep", "failure") == 1 &&
			e.switches.Value("failover") == 1 &&
			e.coreRestarts.Value("crash") == 1 &&
			e.alerts.Value("warning", "core.restarted") == 1
	})
}

func TestSubscriberSubscriptionSuccessSetsTimestamp(t *testing.T) {
	b := bus.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := NewExporter(domain.SystemClock{}, log.New(nil, 0))
	go e.Run(ctx, b)
	waitFor(t, func() bool { return b.SubscriberCount() >= 1 })

	at := time.Now().Add(-5 * time.Minute)
	if err := b.Publish(ctx, domain.Event{Type: domain.EvtSubscriptionRefreshed, At: at,
		Payload: map[string]any{"subscription_id": "s1"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return e.subSuccess.Value("s1") == float64(at.Unix()) })
}

func mustPublish(t *testing.T, b *bus.Bus, typ domain.EventType, payload map[string]any) {
	t.Helper()
	if err := b.Publish(context.Background(), domain.Event{Type: typ, At: time.Now(), Payload: payload}); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func httptestReq() *http.Request { return httptest.NewRequest("GET", "/metrics", nil) }
