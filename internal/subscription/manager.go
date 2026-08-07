package subscription

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
	"smart-gateway/internal/parser"
)

// Config carries the subscription settings resolved from the gateway config.
type Config struct {
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
	MaxBytes        int64
	UserAgent       string
	VerifyTLS       bool
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	RetryLimit      int
}

// SubscriptionStore is the persistence port the manager needs.
type SubscriptionStore interface {
	SaveSubscription(ctx context.Context, s *domain.Subscription) error
	GetSubscription(ctx context.Context, id string) (*domain.Subscription, error)
	GetSubscriptionByURL(ctx context.Context, url string) (*domain.Subscription, error)
	ListSubscriptions(ctx context.Context) ([]domain.Subscription, error)
	UpdateSubscriptionResult(ctx context.Context, s *domain.Subscription) error
}

// NodeStore is the persistence port for node ingestion.
type NodeStore interface {
	IngestNodes(ctx context.Context, subID string, nodes []domain.Node) (domain.IngestResult, error)
	SoftDeleteNodesBySub(ctx context.Context, subID string, keepHashes []string) (int, error)
}

// Result reports one refresh outcome.
type Result struct {
	Unchanged bool
	Added     int
	Updated   int
	Skipped   int
	Removed   int
	NodeCount int
}

// Manager refreshes subscriptions on their schedule (FR-109..FR-113).
type Manager struct {
	cfg    Config
	subs   SubscriptionStore
	nodes  NodeStore
	parser *parser.Parser
	client *http.Client
	clock  domain.Clock
	rand   domain.RandomSource
	logger *log.Logger
	bus    domain.Bus
}

// New builds a Manager. The HTTP client is constructed internally with the
// configured timeout and TLS behaviour.
func New(cfg Config, subs SubscriptionStore, nodes NodeStore,
	clock domain.Clock, rnd domain.RandomSource, logger *log.Logger, bus domain.Bus) *Manager {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if rnd == nil {
		rnd = domain.SystemRandom{}
	}
	if logger == nil {
		logger = log.New(nil, log.LevelFromString("info"))
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig.InsecureSkipVerify = !cfg.VerifyTLS
	client := &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: transport,
	}
	return &Manager{
		cfg:    cfg,
		subs:   subs,
		nodes:  nodes,
		parser: parser.New(),
		client: client,
		clock:  clock,
		rand:   rnd,
		logger: logger,
		bus:    bus,
	}
}

// EnsureRegistered persists a subscription for every spec, reusing the
// existing identity when the URL was already known (idempotent startup).
func (m *Manager) EnsureRegistered(ctx context.Context, specs []Spec) error {
	for _, sp := range specs {
		existing, err := m.subs.GetSubscriptionByURL(ctx, sp.URL)
		if err != nil {
			return fmt.Errorf("lookup %s: %w", sp.URL, err)
		}
		if existing != nil {
			continue
		}
		now := m.clock.Now()
		s := &domain.Subscription{
			URL:         sp.URL,
			Name:        sp.Name,
			Provider:    sp.Name,
			Priority:    sp.Priority,
			Tags:        sp.Tags,
			Headers:     sp.Headers,
			UpdateEvery: sp.UpdateEvery,
			Timeout:     sp.Timeout,
			Enabled:     true,
			State:       domain.SubscriptionActive,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := m.subs.SaveSubscription(ctx, s); err != nil {
			return err
		}
		m.logger.Info("subscription registered", "url", s.URL, "name", s.Name, "sub", s.ID)
	}
	return nil
}

// RefreshByID fetches and ingests one subscription immediately (admin
// endpoint). Returns ErrNotFound when no subscription carries id.
func (m *Manager) RefreshByID(ctx context.Context, id string) (*Result, error) {
	s, err := m.subs.GetSubscription(ctx, id)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrNotFound
	}
	return m.RefreshOne(ctx, s)
}

// ErrNotFound reports a missing subscription lookup.
var ErrNotFound = fmt.Errorf("subscription not found")

// RefreshDue refreshes every enabled subscription whose next fetch is due,
// bounded by a small concurrency pool (download latency dominates).
func (m *Manager) RefreshDue(ctx context.Context) {
	subs, err := m.subs.ListSubscriptions(ctx)
	if err != nil {
		m.logger.Error("list subscriptions", "error", err)
		return
	}
	now := m.clock.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := range subs {
		s := subs[i]
		if !s.Enabled {
			continue
		}
		if !s.NextFetchAt.IsZero() && s.NextFetchAt.After(now) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			m.refresh(ctx, &s)
		}()
	}
	wg.Wait()
}

func (m *Manager) refresh(ctx context.Context, s *domain.Subscription) {
	result, err := m.RefreshOne(ctx, s)
	if err != nil {
		m.logger.Warn("subscription refresh failed",
			"sub", s.ID, "url", s.URL, "error", err, "failures", s.ConsecutiveFailures)
		return
	}
	m.logger.Info("subscription refreshed",
		"sub", s.ID, "name", s.Name, "added", result.Added,
		"updated", result.Updated, "removed", result.Removed, "unchanged", result.Unchanged)
}

// RefreshOne fetches and ingests a single subscription (FR-109 conditional
// GET; FR-204/205 dedupe; FR-301 soft delete).
func (m *Manager) RefreshOne(ctx context.Context, s *domain.Subscription) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		m.fail(ctx, s, fmt.Errorf("build request: %w", err))
		return nil, err
	}
	req.Header.Set("User-Agent", m.cfg.UserAgent)
	req.Header.Set("Accept", "*/*")
	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}
	if s.ETag != "" {
		req.Header.Set("If-None-Match", s.ETag)
	}
	if s.LastModified != "" {
		req.Header.Set("If-Modified-Since", s.LastModified)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		m.fail(ctx, s, fmt.Errorf("download: %w", err))
		return nil, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		m.touch(ctx, s, true)
		return &Result{Unchanged: true, NodeCount: s.NodeCount}, nil
	case resp.StatusCode != http.StatusOK:
		err := fmt.Errorf("unexpected HTTP %d", resp.StatusCode)
		m.fail(ctx, s, err)
		return nil, err
	}

	body, err := m.readCapped(resp.Body)
	if err != nil {
		m.fail(ctx, s, err)
		return nil, err
	}

	nodes, parseErrs := m.parser.Parse(body, s.ID, m.clock)
	if len(parseErrs) > 0 {
		m.logger.Warn("subscription parse skipped lines",
			"sub", s.ID, "skipped", len(parseErrs))
	}

	ingested, err := m.nodes.IngestNodes(ctx, s.ID, nodes)
	if err != nil {
		m.fail(ctx, s, fmt.Errorf("ingest: %w", err))
		return nil, err
	}
	keep := make([]string, len(nodes))
	for i := range nodes {
		keep[i] = nodes[i].DedupeHash
	}
	removed, err := m.nodes.SoftDeleteNodesBySub(ctx, s.ID, keep)
	if err != nil {
		m.fail(ctx, s, fmt.Errorf("soft delete: %w", err))
		return nil, err
	}

	now := m.clock.Now()
	s.NodeCount = ingested.Added + ingested.Updated
	s.ETag = resp.Header.Get("ETag")
	s.LastModified = resp.Header.Get("Last-Modified")
	s.State = domain.SubscriptionActive
	s.ConsecutiveFailures = 0
	s.LastError = ""
	s.LastFetchAt = now
	s.UpdatedAt = now
	m.scheduleNext(s, now)
	if err := m.subs.UpdateSubscriptionResult(ctx, s); err != nil {
		m.logger.Error("persist subscription result", "sub", s.ID, "error", err)
	}

	result := &Result{
		Added:     ingested.Added,
		Updated:   ingested.Updated,
		Skipped:   ingested.Skipped,
		Removed:   removed,
		NodeCount: s.NodeCount,
	}
	m.publish(ctx, domain.EvtSubscriptionRefreshed, map[string]any{
		"subscription_id": s.ID,
		"added":           ingested.Added,
		"updated":         ingested.Updated,
		"removed":         removed,
		"node_count":      s.NodeCount,
	})
	return result, nil
}

// readCapped reads the response body, rejecting payloads above the cap.
func (m *Manager) readCapped(body io.Reader) ([]byte, error) {
	limit := m.cfg.MaxBytes
	if limit <= 0 {
		limit = 10 << 20
	}
	r := io.LimitReader(body, limit+1)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("subscription exceeds %d-byte cap", limit)
	}
	return data, nil
}

// fail records an unsuccessful fetch with exponential backoff (FR-110).
func (m *Manager) fail(ctx context.Context, s *domain.Subscription, cause error) {
	now := m.clock.Now()
	s.ConsecutiveFailures++
	s.State = domain.SubscriptionError
	s.LastError = cause.Error()
	s.LastFetchAt = now
	s.NextFetchAt = now.Add(m.backoff(s.ConsecutiveFailures))
	s.UpdatedAt = now
	if err := m.subs.UpdateSubscriptionResult(ctx, s); err != nil {
		m.logger.Error("persist failure state", "sub", s.ID, "error", err)
	}
	if s.ConsecutiveFailures == m.cfg.RetryLimit {
		m.publish(ctx, domain.EvtSubscriptionFailed, map[string]any{
			"subscription_id": s.ID,
			"error":           cause.Error(),
			"failures":        s.ConsecutiveFailures,
		})
	}
}

// touch records a successful 304 round-trip without re-ingesting (FR-109).
func (m *Manager) touch(ctx context.Context, s *domain.Subscription, scheduled bool) {
	now := m.clock.Now()
	s.LastFetchAt = now
	if scheduled {
		m.scheduleNext(s, now)
	}
	s.UpdatedAt = now
	if err := m.subs.UpdateSubscriptionResult(ctx, s); err != nil {
		m.logger.Error("persist unchanged result", "sub", s.ID, "error", err)
	}
}

// scheduleNext sets NextFetchAt to now plus the (per-source or base) interval
// with jitter (FR-112, FR-113).
func (m *Manager) scheduleNext(s *domain.Subscription, now time.Time) {
	interval := m.cfg.RefreshInterval
	if s.UpdateEvery > 0 {
		interval = s.UpdateEvery
	}
	s.NextFetchAt = now.Add(m.jitter(interval, 0.10))
}

// backoff returns the exponential backoff delay for the n-th consecutive
// failure, jittered and capped at BackoffMax (FR-110).
func (m *Manager) backoff(failures int) time.Duration {
	base := m.cfg.BackoffBase
	if base <= 0 {
		base = time.Minute
	}
	delay := base
	for i := 1; i < failures && delay < m.cfg.BackoffMax; i++ {
		delay *= 2
		if delay > m.cfg.BackoffMax {
			delay = m.cfg.BackoffMax
		}
	}
	return m.jitter(delay, 0.20)
}

// jitter scales d by ±fraction using the injected random source.
func (m *Manager) jitter(d time.Duration, fraction float64) time.Duration {
	if d <= 0 {
		return 0
	}
	if fraction <= 0 {
		return d
	}
	f := m.rand.Float64()*2*fraction - fraction
	return d + time.Duration(float64(d)*f)
}

func (m *Manager) publish(ctx context.Context, t domain.EventType, payload map[string]any) {
	if m.bus == nil {
		return
	}
	e := domain.Event{Type: t, At: m.clock.Now(), Payload: payload}
	if err := m.bus.Publish(ctx, e); err != nil {
		m.logger.Warn("publish event", "type", t, "error", err)
	}
}
