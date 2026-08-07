package alert

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/infrastructure/bus"
	"smart-gateway/internal/log"
)

// fakeClock is a controllable domain.Clock.
type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

// fakeNotifier records delivered alerts.
type fakeNotifier struct {
	name  string
	err   error
	calls []Alert
}

func (f *fakeNotifier) Name() string { return f.name }
func (f *fakeNotifier) Notify(_ context.Context, a Alert) error {
	f.calls = append(f.calls, a)
	return f.err
}

func (f *fakeNotifier) count() int { return len(f.calls) }

func alert(t *testing.T, typ, key string) Alert {
	t.Helper()
	return Alert{Severity: SeverityWarning, Type: typ, Key: key, Title: "t", Message: "m", At: time.Now()}
}

func TestWebhookNotifiesWithJSONAndHeaders(t *testing.T) {
	var got map[string]any
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w := NewWebhook(srv.URL, map[string]string{"Authorization": "secret"}, time.Second)
	a := alert(t, "core.restarted", "core")
	a.Severity = SeverityCritical
	if err := w.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "secret" {
		t.Fatalf("auth header %q", gotAuth)
	}
	if got["type"] != "core.restarted" || got["severity"] != "critical" || got["title"] != "t" {
		t.Fatalf("payload %v", got)
	}
	if _, ok := got["at"]; !ok {
		t.Fatal("missing timestamp")
	}
}

func TestWebhookErrorOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	w := NewWebhook(srv.URL, nil, time.Second)
	if err := w.Notify(context.Background(), alert(t, "x", "k")); err == nil {
		t.Fatal("expected error for 400")
	}
}

func TestTelegramEscapesAndPosts(t *testing.T) {
	var chatID, text string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		chatID = r.Form.Get("chat_id")
		text = r.Form.Get("text")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tg := NewTelegram("bot-token", "chat-1", "[gw]", time.Second)
	tg.Endpoint = srv.URL + "/sendMessage"
	tg.Client = srv.Client()

	a := Alert{Severity: SeverityWarning, Title: "Subscription refresh failing", Message: `download: boom`, At: time.Now()}
	if err := tg.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if chatID != "chat-1" {
		t.Fatalf("chat id %q", chatID)
	}
	if !strings.Contains(text, "<b>[WARNING] Subscription refresh failing</b>") {
		t.Fatalf("message body wrong: %s", text)
	}
	if !strings.Contains(text, "[gw]") {
		t.Fatalf("prefix missing: %s", text)
	}
	if !strings.Contains(text, "download: boom") {
		t.Fatalf("detail missing: %s", text)
	}
}

func TestTelegramRenderEscapesHTML(t *testing.T) {
	tg := NewTelegram("t", "c", "[gw]", time.Second)
	a := Alert{Severity: SeverityCritical, Title: "core <b>boom</b> & co", Message: `token "abc123" <script>`, At: time.Now()}
	got := tg.render(a)
	if strings.Contains(got, "<b>boom</b>") {
		t.Fatalf("title not escaped: %s", got)
	}
	if !strings.Contains(got, "&lt;b&gt;") {
		t.Fatalf("expected escaped title tags: %s", got)
	}
	if !strings.Contains(got, "&quot;abc123&quot;") || !strings.Contains(got, "&lt;script&gt;") {
		t.Fatalf("message not escaped: %s", got)
	}
	if !strings.Contains(got, "[CRITICAL]") {
		t.Fatalf("severity missing: %s", got)
	}
	if !strings.Contains(got, "[gw]") {
		t.Fatalf("prefix missing: %s", got)
	}
}

func TestDispatcherNoNotifiersIsNoop(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	if err := d.Dispatch(context.Background(), alert(t, "x", "k")); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherDeliversToAllAndPublishesFired(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	n1 := &fakeNotifier{name: "one"}
	n2 := &fakeNotifier{name: "two"}
	d.AddNotifier(n1)
	d.AddNotifier(n2)

	b := bus.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var fired atomic.Int64
	go func() {
		ch, _ := b.Subscribe(ctx)
		for ev := range ch {
			if ev.Type == domain.EvtAlertFired {
				fired.Add(1)
			}
		}
	}()
	waitForSubs(t, b, 1)
	d.bus = b

	if err := d.Dispatch(context.Background(), alert(t, "x", "k")); err != nil {
		t.Fatal(err)
	}
	if n1.count() != 1 || n2.count() != 1 {
		t.Fatalf("delivery counts n1=%d n2=%d", n1.count(), n2.count())
	}
	waitFor(t, func() bool { return fired.Load() == 1 })
}

func TestDispatcherDedupsWithinWindow(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{now: now}
	d := NewDispatcher(Config{DedupWindow: time.Hour}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)

	_ = d.Dispatch(context.Background(), alert(t, "x", "k"))
	clk.now = now.Add(10 * time.Minute)
	_ = d.Dispatch(context.Background(), alert(t, "x", "k"))
	if n.count() != 1 {
		t.Fatalf("dedup failed: %d deliveries", n.count())
	}

	// A different key of the same type is NOT deduplicated.
	_ = d.Dispatch(context.Background(), alert(t, "x", "other"))
	if n.count() != 2 {
		t.Fatalf("distinct key must deliver: %d", n.count())
	}
}

func TestDispatcherDedupExpires(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{now: now}
	d := NewDispatcher(Config{DedupWindow: time.Hour}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)

	_ = d.Dispatch(context.Background(), alert(t, "x", "k"))
	clk.now = now.Add(2 * time.Hour)
	_ = d.Dispatch(context.Background(), alert(t, "x", "k"))
	if n.count() != 2 {
		t.Fatalf("dedup window expiry must allow re-delivery: %d", n.count())
	}
}

func TestDispatcherThrottlesPerType(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{now: now}
	d := NewDispatcher(Config{ThrottleLimit: 2, ThrottleWindow: time.Hour}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)

	_ = d.Dispatch(context.Background(), alert(t, "subscription.failed", "s1"))
	_ = d.Dispatch(context.Background(), alert(t, "subscription.failed", "s2"))
	_ = d.Dispatch(context.Background(), alert(t, "subscription.failed", "s3"))
	if n.count() != 2 {
		t.Fatalf("throttle limit breached: %d deliveries", n.count())
	}
	// A different type is not throttled.
	_ = d.Dispatch(context.Background(), alert(t, "core.restarted", "core"))
	if n.count() != 3 {
		t.Fatalf("different type must deliver: %d", n.count())
	}
}

func TestDispatcherThrottleRecoversAfterWindow(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{now: now}
	d := NewDispatcher(Config{ThrottleLimit: 1, ThrottleWindow: time.Hour}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)

	_ = d.Dispatch(context.Background(), alert(t, "x", "a"))
	_ = d.Dispatch(context.Background(), alert(t, "x", "b"))
	clk.now = now.Add(2 * time.Hour)
	_ = d.Dispatch(context.Background(), alert(t, "x", "c"))
	if n.count() != 2 {
		t.Fatalf("throttle window expiry must reset: %d", n.count())
	}
}

func TestDispatcherNotifierErrorStopsNothing(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	bad := &fakeNotifier{name: "bad", err: context.DeadlineExceeded}
	good := &fakeNotifier{name: "good"}
	d.AddNotifier(bad)
	d.AddNotifier(good)

	if err := d.Dispatch(context.Background(), alert(t, "x", "k")); err == nil {
		t.Fatal("expected aggregated error")
	}
	if good.count() != 1 {
		t.Fatalf("good notifier must still deliver: %d", good.count())
	}
}

func TestRunMapsEventsToAlerts(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)

	b := bus.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx, b, b)
	waitForSubs(t, b, 1)

	pub := func(typ domain.EventType, payload map[string]any) {
		t.Helper()
		if err := b.Publish(ctx, domain.Event{Type: typ, At: clk.now, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	pub(domain.EvtSubscriptionFailed, map[string]any{"subscription_id": "s1", "error": "boom", "failures": "5"})
	pub(domain.EvtCoreRestarted, map[string]any{"reason": "crash"})
	pub(domain.EvtOutboundChanged, map[string]any{"node": "n9", "reason": "failover: current node unhealthy"})
	pub(domain.EvtOutboundChanged, map[string]any{"node": "n9", "reason": "switch: candidate beats current"})

	waitFor(t, func() bool { return n.count() >= 3 })

	types := map[string]bool{}
	for _, c := range n.calls {
		types[c.Type] = true
	}
	for _, want := range []string{"subscription.failed", "core.restarted", "outbound.failover"} {
		if !types[want] {
			t.Fatalf("missing alert type %s (got %v)", want, types)
		}
	}
	if types["switch: candidate beats current"] {
		t.Fatal("non-failover switch must not alert")
	}
}

func TestCheckFleetDegraded(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{FleetDegradedRatio: 0.5}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)
	d.WithStore(fakeStore{nodes: 10, byState: map[domain.NodeState]int{domain.StateFailed: 6}})

	d.Check(context.Background())
	if n.count() != 1 || n.calls[0].Type != "fleet.degraded" || n.calls[0].Severity != SeverityCritical {
		t.Fatalf("expected fleet.degraded critical: %+v", n.calls)
	}
}

func TestCheckHealthyFleetNoAlert(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{FleetDegradedRatio: 0.5}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)
	d.WithStore(fakeStore{nodes: 10, byState: map[domain.NodeState]int{domain.StateFailed: 1}})

	d.Check(context.Background())
	if n.count() != 0 {
		t.Fatalf("healthy fleet must not alert: %+v", n.calls)
	}
}

func TestCheckZeroNodesNoAlert(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)
	d.WithStore(fakeStore{nodes: 0, byState: map[domain.NodeState]int{}})

	d.Check(context.Background())
	if n.count() != 0 {
		t.Fatalf("empty fleet must not alert: %+v", n.calls)
	}
}

func TestCheckStorageErrorAlerts(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	d := NewDispatcher(Config{}, clk, log.New(nil, 0))
	n := &fakeNotifier{name: "n"}
	d.AddNotifier(n)
	d.WithStore(fakeStore{err: context.DeadlineExceeded})

	d.Check(context.Background())
	if n.count() != 1 || n.calls[0].Type != "storage.error" {
		t.Fatalf("expected storage.error: %+v", n.calls)
	}
}

type fakeStore struct {
	nodes   int
	byState map[domain.NodeState]int
	err     error
}

func (f fakeStore) CountNodes(context.Context) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.nodes, nil
}

func (f fakeStore) CountNodesByState(context.Context) (map[domain.NodeState]int, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byState, nil
}

func waitForSubs(t *testing.T, b *bus.Bus, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.SubscriberCount() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("subscriber did not register")
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
