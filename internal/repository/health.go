package repository

import (
	"context"
	"fmt"
	"time"

	"smart-gateway/internal/domain"
)

// maxMeasurementsPerNode bounds the retained probe history (FR-302).
const maxMeasurementsPerNode = 5000

// HealthStore is the persistence port for probe measurements.
type HealthStore interface {
	// AddMeasurement records one probe result and prunes the per-node history
	// beyond maxMeasurementsPerNode.
	AddMeasurement(ctx context.Context, nodeID string, latency time.Duration, success bool) error
	// GetNodeHealth folds the retained measurements of a node.
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
	// CountMeasurements reports the stored measurement rows.
	CountMeasurements(ctx context.Context) (int, error)
}

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
	row := r.db.QueryRowContext(ctx, `
SELECT COUNT(*),
	SUM(CASE WHEN success=1 THEN 1 ELSE 0 END),
	SUM(CASE WHEN success=0 THEN 1 ELSE 0 END),
	MAX(probed_at),
	MAX(CASE WHEN success=1 THEN probed_at ELSE NULL END)
FROM node_measurements WHERE node_id=?`, nodeID)
	var count, succ, fail sqlNullableInt
	var lastProbe, lastSuccess *string
	if err := row.Scan(&count, &succ, &fail, &lastProbe, &lastSuccess); err != nil {
		return h, err
	}
	h.Probes = count.v
	h.SuccessCount = succ.v
	h.FailureCount = fail.v
	h.LastProbedAt = parseTime(lastProbe)
	h.LastSuccessAt = parseTime(lastSuccess)
	return h, nil
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
