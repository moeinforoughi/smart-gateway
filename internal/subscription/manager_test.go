package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeRand struct{ v float64 }

func (f fakeRand) Float64() float64 { return f.v }
func (fakeRand) Intn(n int) int     { return 0 }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type recordingBus struct {
	mu     sync.Mutex
	events []domain.Event
}

func (b *recordingBus) Publish(_ context.Context, e domain.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, e)
	return nil
}

func (b *recordingBus) types() []domain.EventType {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]domain.EventType, len(b.events))
	for i, e := range b.events {
		out[i] = e.Type
	}
	return out
}

type memStores struct {
	mu          sync.Mutex
	byURL       map[string]*domain.Subscription
	ingestCalls int
	nodes       map[string][]domain.Node
}

func newMemStores() *memStores {
	return &memStores{byURL: make(map[string]*domain.Subscription), nodes: make(map[string][]domain.Node)}
}

func (m *memStores) SaveSubscription(_ context.Context, s *domain.Subscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.ID == "" {
		s.ID = domain.NewUUIDv7(time.Unix(1_700_000_000, 0).UTC())
	}
	m.byURL[s.URL] = s
	return nil
}

func (m *memStores) GetSubscriptionByURL(_ context.Context, url string) (*domain.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byURL[url], nil
}

func (m *memStores) GetSubscription(_ context.Context, id string) (*domain.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.byURL {
		if s.ID == id {
			return s, nil
		}
	}
	return nil, nil
}

func (m *memStores) ListSubscriptions(_ context.Context) ([]domain.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Subscription, 0, len(m.byURL))
	for _, s := range m.byURL {
		out = append(out, *s)
	}
	return out, nil
}

func (m *memStores) UpdateSubscriptionResult(_ context.Context, s *domain.Subscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byURL[s.URL] = s
	return nil
}

func (m *memStores) IngestNodes(_ context.Context, subID string, nodes []domain.Node) (domain.IngestResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingestCalls++
	m.nodes[subID] = append([]domain.Node{}, nodes...)
	return domain.IngestResult{Added: len(nodes)}, nil
}

func (m *memStores) SoftDeleteNodesBySub(_ context.Context, _ string, _ []string) (int, error) {
	return 0, nil
}

func newManager(t *testing.T, stores *memStores, server *httptest.Server, bus *recordingBus) (*Manager, *domain.Subscription) {
	t.Helper()
	cfg := Config{
		RefreshInterval: time.Hour,
		RequestTimeout:  5 * time.Second,
		MaxBytes:        1 << 20,
		UserAgent:       "smart-gateway-test",
		VerifyTLS:       true,
		BackoffBase:     time.Minute,
		BackoffMax:      4 * time.Hour,
		RetryLimit:      5,
	}
	m := New(cfg, stores, stores, fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}, fakeRand{v: 0.5}, nil, bus)
	ctx := context.Background()
	if err := m.EnsureRegistered(ctx, []Spec{{Name: "p", URL: server.URL}}); err != nil {
		t.Fatalf("EnsureRegistered: %v", err)
	}
	s, err := stores.GetSubscriptionByURL(ctx, server.URL)
	if err != nil || s == nil {
		t.Fatalf("subscription not registered: %v", err)
	}
	return m, s
}

func TestRefreshDownloadsAndIngests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443#A"))
	}))
	defer server.Close()

	bus := &recordingBus{}
	stores := newMemStores()
	m, s := newManager(t, stores, server, bus)

	res, err := m.RefreshOne(context.Background(), s)
	if err != nil {
		t.Fatalf("RefreshOne: %v", err)
	}
	if res.Added != 1 || res.NodeCount != 1 {
		t.Fatalf("result = %+v", res)
	}
	if s.ETag != `"v1"` || s.State != domain.SubscriptionActive || s.ConsecutiveFailures != 0 {
		t.Fatalf("sub state = %+v", s)
	}
	if s.LastFetchAt.IsZero() || s.NextFetchAt.IsZero() {
		t.Fatal("fetch/schedule times must be set")
	}
	got := bus.types()
	if len(got) != 1 || got[0] != domain.EvtSubscriptionRefreshed {
		t.Fatalf("events = %v", got)
	}
}

func TestConditionalRequestNotModified(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443#A"))
	}))
	defer server.Close()

	stores := newMemStores()
	m, s := newManager(t, stores, server, &recordingBus{})

	if _, err := m.RefreshOne(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	res, err := m.RefreshOne(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Unchanged || res.NodeCount != 1 {
		t.Fatalf("expected unchanged result, got %+v", res)
	}
	if hits != 2 {
		t.Fatalf("server hits = %d, want 2", hits)
	}
	stores.mu.Lock()
	ingests := stores.ingestCalls
	stores.mu.Unlock()
	if ingests != 1 {
		t.Fatalf("ingest calls = %d, want 1 (304 must not re-ingest)", ingests)
	}
}

func TestBackoffOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	stores := newMemStores()
	m, s := newManager(t, stores, server, &recordingBus{})

	if _, err := m.RefreshOne(context.Background(), s); err == nil {
		t.Fatal("expected error")
	}
	if s.State != domain.SubscriptionError || s.ConsecutiveFailures != 1 {
		t.Fatalf("failure state = %+v", s)
	}
	want := time.Unix(1_700_000_000, 0).UTC().Add(time.Minute) // base backoff, zero jitter (rand=0.5)
	if !s.NextFetchAt.Equal(want) {
		t.Fatalf("next fetch = %s, want %s", s.NextFetchAt, want)
	}
}

func TestRetryLimitFiresAlert(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	bus := &recordingBus{}
	stores := newMemStores()
	m, s := newManager(t, stores, server, bus)

	for i := 0; i < 5; i++ {
		_, _ = m.RefreshOne(context.Background(), s)
	}
	got := bus.types()
	if len(got) != 1 || got[0] != domain.EvtSubscriptionFailed {
		t.Fatalf("events = %v, want single subscription.failed", got)
	}
}

func TestRefreshByIDFetchesAndIngests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443#A"))
	}))
	defer server.Close()

	stores := newMemStores()
	m, s := newManager(t, stores, server, &recordingBus{})

	res, err := m.RefreshByID(context.Background(), s.ID)
	if err != nil {
		t.Fatalf("RefreshByID: %v", err)
	}
	if res.Added != 1 || res.NodeCount != 1 {
		t.Fatalf("result = %+v", res)
	}
}

func TestRefreshByIDNotFound(t *testing.T) {
	stores := newMemStores()
	m := New(Config{RefreshInterval: time.Hour, RequestTimeout: time.Second}, stores, stores, nil, nil, nil, nil)
	if _, err := m.RefreshByID(context.Background(), "missing-id"); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestParseSpecsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subs.txt")
	content := `
# comment

ProviderA = https://a.example.com/sub
ProviderB: https://b.example.com/sub
https://bare.example.com/sub
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	specs, err := ParseSpecsFile(path)
	if err != nil {
		t.Fatalf("ParseSpecsFile: %v", err)
	}
	if len(specs) != 3 {
		t.Fatalf("specs = %d, want 3", len(specs))
	}
	if specs[0].Name != "ProviderA" || specs[0].URL != "https://a.example.com/sub" {
		t.Fatalf("spec0 = %+v", specs[0])
	}
	if specs[1].Name != "ProviderB" {
		t.Fatalf("spec1 = %+v", specs[1])
	}
	if specs[2].Name != "bare.example.com" || specs[2].URL != "https://bare.example.com/sub" {
		t.Fatalf("spec2 = %+v", specs[2])
	}
}
