package repository

import (
	"context"
	"database/sql"
	"fmt"

	"smart-gateway/internal/domain"
)

// SubscriptionStore is the persistence port for subscriptions.
type SubscriptionStore interface {
	// SaveSubscription inserts a new subscription or updates an existing one,
	// keyed by URL. The ID is generated on first insert and preserved on
	// subsequent saves.
	SaveSubscription(ctx context.Context, s *domain.Subscription) error
	GetSubscription(ctx context.Context, id string) (*domain.Subscription, error)
	// GetSubscriptionByURL returns the subscription registered for url, or
	// nil when none exists yet.
	GetSubscriptionByURL(ctx context.Context, url string) (*domain.Subscription, error)
	ListSubscriptions(ctx context.Context) ([]domain.Subscription, error)
	// DeleteSubscription removes a subscription and, via the foreign-key
	// cascade, its nodes, measurements, throughput and score history (FR-3xx).
	// It reports whether the subscription existed.
	DeleteSubscription(ctx context.Context, id string) (bool, error)
	// UpdateSubscriptionResult records the outcome of a fetch cycle.
	UpdateSubscriptionResult(ctx context.Context, s *domain.Subscription) error
}

const subCols = `id, url, name, provider, priority, tags, enabled, headers,
	update_every, timeout, etag, last_modified, state, consecutive_failures,
	last_fetch_at, next_fetch_at, node_count, last_error, created_at, updated_at`

func (r *Repo) SaveSubscription(ctx context.Context, s *domain.Subscription) error {
	if s.ID == "" {
		s.ID = domain.NewUUIDv7(r.clock.Now())
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO subscriptions (`+subCols+`)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(url) DO UPDATE SET
	name=excluded.name, provider=excluded.provider, priority=excluded.priority,
	tags=excluded.tags, enabled=excluded.enabled, headers=excluded.headers,
	update_every=excluded.update_every, timeout=excluded.timeout,
	updated_at=excluded.updated_at`,
		s.ID, s.URL, s.Name, s.Provider, s.Priority,
		encJSON(s.Tags), boolInt(s.Enabled), encJSON(s.Headers),
		durationText(s.UpdateEvery), durationText(s.Timeout),
		s.ETag, s.LastModified, string(s.State), s.ConsecutiveFailures,
		timeText(s.LastFetchAt), timeText(s.NextFetchAt), s.NodeCount, s.LastError,
		timeText(s.CreatedAt), timeText(s.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save subscription: %w", err)
	}
	return nil
}

func (r *Repo) GetSubscription(ctx context.Context, id string) (*domain.Subscription, error) {
	row := r.db.QueryRowContext(ctx, "SELECT "+subCols+" FROM subscriptions WHERE id=?", id)
	s, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return s, err
}

func (r *Repo) GetSubscriptionByURL(ctx context.Context, url string) (*domain.Subscription, error) {
	row := r.db.QueryRowContext(ctx, "SELECT "+subCols+" FROM subscriptions WHERE url=?", url)
	s, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return s, err
}

func (r *Repo) ListSubscriptions(ctx context.Context) ([]domain.Subscription, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT "+subCols+" FROM subscriptions ORDER BY priority DESC, name ASC")
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()
	var out []domain.Subscription
	for rows.Next() {
		s, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func (r *Repo) UpdateSubscriptionResult(ctx context.Context, s *domain.Subscription) error {
	_, err := r.db.ExecContext(ctx, `
UPDATE subscriptions SET
	state=?, consecutive_failures=?, last_fetch_at=?, next_fetch_at=?,
	node_count=?, last_error=?, etag=?, last_modified=?, updated_at=?
WHERE id=?`,
		string(s.State), s.ConsecutiveFailures, timeText(s.LastFetchAt),
		timeText(s.NextFetchAt), s.NodeCount, s.LastError,
		s.ETag, s.LastModified, timeText(s.UpdatedAt), s.ID)
	if err != nil {
		return fmt.Errorf("update subscription result: %w", err)
	}
	return nil
}

// DeleteSubscription removes the subscription and everything that belongs to
// it (nodes cascade, which cascades measurements/scores). The active outbound
// pointing at a removed node is re-evaluated by the caller.
func (r *Repo) DeleteSubscription(ctx context.Context, id string) (bool, error) {
	res, err := r.db.ExecContext(ctx, "DELETE FROM subscriptions WHERE id=?", id)
	if err != nil {
		return false, fmt.Errorf("delete subscription: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanSubscription(r rowScanner) (*domain.Subscription, error) {
	var s domain.Subscription
	var (
		tags, headers        string
		enabled              int
		updateEvery, timeout string
		lastFetch, nextFetch *string
		created, updated     *string
	)
	if err := r.Scan(&s.ID, &s.URL, &s.Name, &s.Provider, &s.Priority,
		&tags, &enabled, &headers, &updateEvery, &timeout,
		&s.ETag, &s.LastModified, &s.State, &s.ConsecutiveFailures,
		&lastFetch, &nextFetch, &s.NodeCount, &s.LastError,
		&created, &updated); err != nil {
		return nil, err
	}
	decJSON(tags, &s.Tags)
	decJSON(headers, &s.Headers)
	s.Enabled = enabled != 0
	s.UpdateEvery = parseDuration(updateEvery)
	s.Timeout = parseDuration(timeout)
	s.LastFetchAt = parseTime(lastFetch)
	s.NextFetchAt = parseTime(nextFetch)
	s.CreatedAt = parseTime(created)
	s.UpdatedAt = parseTime(updated)
	return &s, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
