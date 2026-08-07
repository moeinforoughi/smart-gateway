package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"smart-gateway/internal/domain"
)

// maxMeasurementsPerNode bounds the retained probe history (FR-302).
const maxMeasurementsPerNode = 5000

// foldEWMASamples is the default EWMA half-life used when folding a node's
// window in the store. The scorer may recompute with its own window.
const foldEWMASamples = 20

// HealthStore is the persistence port for probe measurements.
type HealthStore interface {
	// AddMeasurement records one probe result and prunes the per-node history
	// beyond maxMeasurementsPerNode.
	AddMeasurement(ctx context.Context, nodeID string, latency time.Duration, success bool) error
	// GetNodeHealth folds the retained measurements of a node.
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
	// ListHealths folds the retained measurements of every node in one pass
	// (metrics exporter).
	ListHealths(ctx context.Context) ([]domain.NodeHealth, error)
	// CountMeasurements reports the stored measurement rows.
	CountMeasurements(ctx context.Context) (int, error)
	// RecordThroughput stores the latest passive/speed throughput estimate.
	RecordThroughput(ctx context.Context, nodeID string, bps float64, at time.Time) error
}

// ListHealths folds the retained measurements of every node in a single pass
// (the metrics exporter must not issue one query per node at 10k scale).
func (r *Repo) ListHealths(ctx context.Context) ([]domain.NodeHealth, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT node_id, probed_at, latency_ms, success FROM node_measurements
ORDER BY node_id ASC, probed_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	grouped := make(map[string][]domain.ProbeSample)
	var order []string
	for rows.Next() {
		var (
			nodeID  string
			probed  *string
			latency int
			ok      int
		)
		if err := rows.Scan(&nodeID, &probed, &latency, &ok); err != nil {
			return nil, err
		}
		if _, seen := grouped[nodeID]; !seen {
			order = append(order, nodeID)
		}
		grouped[nodeID] = append(grouped[nodeID], domain.ProbeSample{
			At:      parseTime(probed),
			Latency: time.Duration(latency) * time.Millisecond,
			Success: ok != 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	throughput, err := r.latestThroughput(ctx)
	if err != nil {
		return nil, err
	}
	now := r.clock.Now()
	out := make([]domain.NodeHealth, 0, len(order))
	for _, nodeID := range order {
		h := domain.FoldHealth(nodeID, grouped[nodeID], domain.FoldConfig{EWMASamples: foldEWMASamples}, now)
		if tp, ok := throughput[nodeID]; ok {
			h.ThroughputBps = tp
		}
		out = append(out, h)
	}
	return out, nil
}

// latestThroughput returns the most recent throughput estimate per node.
func (r *Repo) latestThroughput(ctx context.Context) (map[string]float64, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT node_id, bytes_per_sec FROM node_throughput
WHERE (node_id, measured_at) IN (
	SELECT node_id, MAX(measured_at) FROM node_throughput GROUP BY node_id)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]float64)
	for rows.Next() {
		var (
			nodeID string
			thr    sqlNullableFloat
		)
		if err := rows.Scan(&nodeID, &thr); err != nil {
			return nil, err
		}
		out[nodeID] = thr.v
	}
	return out, rows.Err()
}

// CountMeasurements reports the stored measurement rows.
func (r *Repo) AddMeasurement(ctx context.Context, nodeID string, latency time.Duration, success bool) error {
	now := r.clock.Now()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	latencyMS := latency.Milliseconds()
	if latencyMS < 0 {
		latencyMS = 0
	}
	ok := 0
	if success {
		ok = 1
	}
	if _, err := tx.ExecContext(ctx, `
INSERT OR REPLACE INTO node_measurements (node_id, probed_at, latency_ms, success)
VALUES (?,?,?,?)`, nodeID, timeText(now), latencyMS, ok); err != nil {
		return fmt.Errorf("insert measurement: %w", err)
	}
	// Keep the newest maxMeasurementsPerNode rows per node (FR-302).
	if _, err := tx.ExecContext(ctx, `
DELETE FROM node_measurements
WHERE node_id=? AND probed_at <= (
	SELECT probed_at FROM node_measurements WHERE node_id=?
	ORDER BY probed_at DESC LIMIT 1 OFFSET ?)`,
		nodeID, nodeID, maxMeasurementsPerNode-1); err != nil {
		return fmt.Errorf("prune measurements: %w", err)
	}
	return tx.Commit()
}

func (r *Repo) GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error) {
	h := domain.NodeHealth{NodeID: nodeID}
	samples, err := r.measurementSamples(ctx, nodeID)
	if err != nil {
		return h, err
	}
	h = domain.FoldHealth(nodeID, samples, domain.FoldConfig{EWMASamples: foldEWMASamples}, r.clock.Now())

	var (
		thr   sqlNullableFloat
		thrAt *string
	)
	if err := r.db.QueryRowContext(ctx, `
SELECT bytes_per_sec, measured_at FROM node_throughput
WHERE node_id=? ORDER BY measured_at DESC LIMIT 1`, nodeID).Scan(&thr, &thrAt); err != nil && err != sql.ErrNoRows {
		return h, err
	}
	h.ThroughputBps = thr.v
	h.ThroughputAt = parseTime(thrAt)
	return h, nil
}

// measurementSamples loads the retained window (oldest first) for a node.
func (r *Repo) measurementSamples(ctx context.Context, nodeID string) ([]domain.ProbeSample, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT probed_at, latency_ms, success FROM node_measurements
WHERE node_id=? ORDER BY probed_at ASC`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ProbeSample
	for rows.Next() {
		var (
			probed  *string
			latency int
			ok      int
		)
		if err := rows.Scan(&probed, &latency, &ok); err != nil {
			return nil, err
		}
		out = append(out, domain.ProbeSample{
			At:      parseTime(probed),
			Latency: time.Duration(latency) * time.Millisecond,
			Success: ok != 0,
		})
	}
	return out, rows.Err()
}

// RecordThroughput stores a passive or speed-test throughput estimate.
func (r *Repo) RecordThroughput(ctx context.Context, nodeID string, bps float64, at time.Time) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO node_throughput (node_id, measured_at, bytes_per_sec)
VALUES (?,?,?)`, nodeID, timeText(at), bps)
	return err
}

func (r *Repo) CountMeasurements(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM node_measurements").Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

type sqlNullableInt struct{ v int }

func (i *sqlNullableInt) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		i.v = 0
	case int64:
		i.v = int(v)
	case int:
		i.v = v
	case []byte:
		n := 0
		for _, c := range v {
			n = n*10 + int(c-'0')
		}
		i.v = n
	default:
		return fmt.Errorf("unexpected scan type %T", src)
	}
	return nil
}

type sqlNullableFloat struct{ v float64 }

func (f *sqlNullableFloat) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		f.v = 0
	case float64:
		f.v = v
	case int64:
		f.v = float64(v)
	case []byte:
		var x float64
		if _, err := fmt.Sscanf(string(v), "%g", &x); err != nil {
			return err
		}
		f.v = x
	default:
		return fmt.Errorf("unexpected scan type %T", src)
	}
	return nil
}
