package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"smart-gateway/internal/config"
	"smart-gateway/internal/domain"
	"smart-gateway/internal/infrastructure/singbox"
	"smart-gateway/internal/subscription"
)

const testToken = "test-admin-token"

type fakeStore struct {
	nodes      []domain.Node
	scores     []domain.NodeScore
	health     map[string]domain.NodeHealth
	subs       []domain.Subscription
	switches   []domain.SwitchRecord
	byState    map[domain.NodeState]int
	scoreCount int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		health:     map[string]domain.NodeHealth{},
		byState:    map[domain.NodeState]int{domain.StateHealthy: 1},
		scoreCount: 1,
	}
}

func (f *fakeStore) ListAllNodes(context.Context) ([]domain.Node, error) { return f.nodes, nil }
func (f *fakeStore) CountNodes(context.Context) (int, error)             { return len(f.nodes), nil }
func (f *fakeStore) CountNodesByState(context.Context) (map[domain.NodeState]int, error) {
	return f.byState, nil
}
func (f *fakeStore) CountScores(context.Context) (int, error) { return f.scoreCount, nil }
func (f *fakeStore) ListScores(context.Context) ([]domain.NodeScore, error) {
	return f.scores, nil
}
func (f *fakeStore) GetNodeScore(_ context.Context, id string) (domain.NodeScore, error) {
	for _, ns := range f.scores {
		if ns.NodeID == id {
			return ns, nil
		}
	}
	return domain.NodeScore{}, nil
}
func (f *fakeStore) ScoreHistory(context.Context, string, int) ([]domain.NodeScore, error) {
	return nil, nil
}
func (f *fakeStore) TopEligible(_ context.Context, limit int) ([]domain.NodeScore, error) {
	out := make([]domain.NodeScore, 0, limit)
	for _, ns := range f.scores {
		if ns.Score.Eligible && len(out) < limit {
			out = append(out, ns)
		}
	}
	return out, nil
}
func (f *fakeStore) GetNode(_ context.Context, id string) (*domain.Node, error) {
	for i := range f.nodes {
		if f.nodes[i].ID == id {
			n := f.nodes[i]
			return &n, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) GetNodeHealth(_ context.Context, id string) (domain.NodeHealth, error) {
	return f.health[id], nil
}
func (f *fakeStore) ListSubscriptions(context.Context) ([]domain.Subscription, error) {
	return f.subs, nil
}
func (f *fakeStore) GetSubscription(_ context.Context, id string) (*domain.Subscription, error) {
	for i := range f.subs {
		if f.subs[i].ID == id {
			s := f.subs[i]
			return &s, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) SaveSubscription(_ context.Context, s *domain.Subscription) error {
	if s.ID == "" {
		s.ID = domain.NewUUIDv7(time.Now())
	}
	f.subs = append(f.subs, *s)
	return nil
}
func (f *fakeStore) DeleteSubscription(_ context.Context, id string) (bool, error) {
	for i := range f.subs {
		if f.subs[i].ID == id {
			f.subs = append(f.subs[:i], f.subs[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) ListSwitchRecords(_ context.Context, limit int) ([]domain.SwitchRecord, error) {
	out := f.switches
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type fakeRoutes struct {
	cur     string
	reason  string
	lastD   domain.SwitchDecision
	relased bool
}

func (r *fakeRoutes) Current() (string, string) { return r.cur, r.reason }
func (r *fakeRoutes) Override(_ context.Context, _ []domain.Node, id, reason string) error {
	if id == "missing" {
		return errors.New("not in the active node set")
	}
	r.cur, r.reason = id, reason
	return nil
}
func (r *fakeRoutes) Release(_ context.Context, _ []domain.Node) error {
	r.cur, r.reason, r.relased = "", "", true
	return nil
}
func (r *fakeRoutes) EnsureActive(context.Context, []domain.Node) error { return nil }
func (r *fakeRoutes) Evaluate(_ context.Context, _ []domain.Node) (domain.SwitchDecision, error) {
	r.lastD = domain.SwitchDecision{
		At: time.Now(), CurrentID: "n1", CandidateID: "n2",
		Reason: "scored better", Outcome: domain.SwitchConfirmed,
	}
	return r.lastD, nil
}

type fakeSubscriber struct{ refreshed string }

func (s *fakeSubscriber) RefreshByID(_ context.Context, id string) (*subscription.Result, error) {
	s.refreshed = id
	if id == "missing" {
		return nil, subscription.ErrNotFound
	}
	return &subscription.Result{Added: 1, NodeCount: 1}, nil
}

type fakeCore struct{}

func (fakeCore) Running() bool           { return true }
func (fakeCore) Failed() bool            { return false }
func (fakeCore) RestartsInLastHour() int { return 0 }

func newTestServer(t *testing.T, store *fakeStore) *Server {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.API.Token = testToken
	s := New(Deps{
		Config: cfg, Store: store, Routes: &fakeRoutes{}, Subscriber: &fakeSubscriber{},
		Core: fakeCore{}, Version: "test",
		ClientConfig: func() (*singbox.ClientConfig, error) {
			return &singbox.ClientConfig{Address: "vpn.example.net"}, nil
		},
		Reload: func(context.Context) (ReloadResult, error) {
			return ReloadResult{Reloaded: []string{"subscriptions"}, RequiresRestart: []string{"core"}}, nil
		},
	})
	return s
}

func do(t *testing.T, s *Server, method, path string, body string, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func seedNodes(store *fakeStore) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store.nodes = []domain.Node{
		{ID: "n1", Name: "node-a", Server: "a.example.com", Port: 443, Protocol: domain.ProtocolVless, State: domain.StateHealthy, CreatedAt: now},
		{ID: "n2", Name: "node-b", Server: "b.example.com", Port: 443, Protocol: domain.ProtocolVless, State: domain.StateFailed, CreatedAt: now.Add(time.Second)},
	}
	store.scores = []domain.NodeScore{
		{NodeID: "n1", Score: domain.Score{NodeID: "n1", Total: 0.8, Eligible: true}, State: domain.ScoreEligible},
	}
}

func TestHealthzAndReadyz(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	for _, p := range []string{"/healthz", "/readyz"} {
		if rec := do(t, s, http.MethodGet, p, "", ""); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", p, rec.Code)
		}
	}
}

func TestDashboardServed(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	rec := do(t, s, http.MethodGet, "/dashboard", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("/dashboard = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestCorrelationID(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	rec := do(t, s, http.MethodGet, "/api/v1/status", "", "")
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing X-Request-ID")
	}
}

func TestAdminAuthRequired(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	body := `{"node_id":"n1"}`
	for _, tc := range []struct {
		token string
		want  int
	}{
		{"", http.StatusUnauthorized},
		{"wrong-token", http.StatusUnauthorized},
		{testToken, http.StatusOK},
	} {
		if rec := do(t, s, http.MethodPost, "/api/v1/outbound/override", body, tc.token); rec.Code != tc.want {
			t.Fatalf("token %q -> %d, want %d", tc.token, rec.Code, tc.want)
		}
	}
}

func TestStatusAndMetricsPublic(t *testing.T) {
	store := newFakeStore()
	seedNodes(store)
	s := newTestServer(t, store)
	rec := do(t, s, http.MethodGet, "/api/v1/status", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["version"] != "test" {
		t.Fatalf("version = %v", payload["version"])
	}
	nodes := payload["nodes"].(map[string]any)
	if nodes["total"] != float64(2) {
		t.Fatalf("nodes.total = %v", nodes["total"])
	}
	if rec := do(t, s, http.MethodGet, "/api/v1/metrics", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d", rec.Code)
	}
}

func TestListNodesFilterPagination(t *testing.T) {
	store := newFakeStore()
	seedNodes(store)
	s := newTestServer(t, store)

	rec := do(t, s, http.MethodGet, "/api/v1/nodes?state=failed", "", "")
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	var page struct {
		Total int        `json:"total"`
		Items []nodeItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "n2" {
		t.Fatalf("filter page = %+v", page)
	}

	rec = do(t, s, http.MethodGet, "/api/v1/nodes?page_size=1&page=2", "", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Total != 2 {
		t.Fatalf("pagination page = %+v", page)
	}
}

func TestGetNode(t *testing.T) {
	store := newFakeStore()
	seedNodes(store)
	s := newTestServer(t, store)
	if rec := do(t, s, http.MethodGet, "/api/v1/nodes/n1", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("get node = %d", rec.Code)
	}
	if rec := do(t, s, http.MethodGet, "/api/v1/nodes/nope", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("missing node = %d", rec.Code)
	}
}

func TestOutboundOverrideReleaseEvaluate(t *testing.T) {
	store := newFakeStore()
	seedNodes(store)
	s := newTestServer(t, store)

	rec := do(t, s, http.MethodPost, "/api/v1/outbound/override", `{"node_id":"n1","auto_release_after":"90s"}`, testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("override = %d: %s", rec.Code, rec.Body.String())
	}

	if rec := do(t, s, http.MethodPost, "/api/v1/outbound/override", `{"node_id":"missing"}`, testToken); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad override = %d", rec.Code)
	}

	rec = do(t, s, http.MethodGet, "/api/v1/outbound/evaluate", "", testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("evaluate = %d", rec.Code)
	}
	var d map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d["outcome"] != string(domain.SwitchConfirmed) || d["candidate_id"] != "n2" {
		t.Fatalf("evaluate payload = %+v", d)
	}

	if rec := do(t, s, http.MethodDelete, "/api/v1/outbound/override", "", testToken); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d", rec.Code)
	}
}

func TestSubscriptionsCRUD(t *testing.T) {
	store := newFakeStore()
	s := newTestServer(t, store)

	rec := do(t, s, http.MethodPost, "/api/v1/subscriptions", `{"url":"https://a.example.com/sub","name":"a"}`, testToken)
	if rec.Code != http.StatusCreated {
		t.Fatalf("add = %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		Subscription subscriptionItem     `json:"subscription"`
		Refresh      *subscription.Result `json:"refresh"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	id := created.Subscription.ID

	rec = do(t, s, http.MethodGet, "/api/v1/subscriptions", "", "")
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}

	rec = do(t, s, http.MethodPost, "/api/v1/subscriptions/"+id+"/refresh", "", testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh = %d", rec.Code)
	}

	rec = do(t, s, http.MethodDelete, "/api/v1/subscriptions/"+id, "", testToken)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d", rec.Code)
	}

	if rec := do(t, s, http.MethodDelete, "/api/v1/subscriptions/"+id, "", testToken); rec.Code != http.StatusNotFound {
		t.Fatalf("double delete = %d", rec.Code)
	}
}

func TestAddSubscriptionRequiresURL(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	if rec := do(t, s, http.MethodPost, "/api/v1/subscriptions", `{"name":"x"}`, testToken); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing url = %d", rec.Code)
	}
}

func TestHistoryAndClientConfig(t *testing.T) {
	store := newFakeStore()
	store.switches = []domain.SwitchRecord{{ID: "r1", FromID: "n1", ToID: "n2", Reason: "failover"}}
	s := newTestServer(t, store)

	rec := do(t, s, http.MethodGet, "/api/v1/history?limit=10", "", "")
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	var hist struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &hist); err != nil {
		t.Fatal(err)
	}
	if hist.Total != 1 {
		t.Fatalf("history = %+v", hist)
	}

	rec = do(t, s, http.MethodGet, "/api/v1/client-config", "", "")
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "vpn.example.net") {
		t.Fatalf("client-config = %s", rec.Body.String())
	}
}

func TestReloadEndpoint(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	rec := do(t, s, http.MethodPost, "/api/v1/reload", "", testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("reload = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "subscriptions") {
		t.Fatalf("reload payload = %s", rec.Body.String())
	}
}

func TestRateLimitOnAdminEndpoint(t *testing.T) {
	s := newTestServer(t, newFakeStore())
	var last int
	for i := 0; i <= 125; i++ {
		rec := do(t, s, http.MethodPost, "/api/v1/reload", "", testToken)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after burst, got %d", last)
	}
}
