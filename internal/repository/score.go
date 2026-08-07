package repository

import (
	"context"
	"database/sql"
	"fmt"

	"smart-gateway/internal/domain"
)

// scoreColumns is the shared projection for node_scores and score_history.
const scoreColumns = `total, latency, stability, throughput, loss, confidence, eligible, state, evaluated_at, reason`

// UpsertScore persists the materialized score for a node (FR-607) and appends
// an audit entry to its history (FR-608). Implemented as one transaction so
// the history is never ahead of the current score.
func (r *Repo) UpsertScore(ctx context.Context, ns domain.NodeScore) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
INSERT OR REPLACE INTO node_scores
	(node_id, total, latency, stability, throughput, loss, confidence,
	 eligible, state, evaluated_at, reason, updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		ns.NodeID, ns.Score.Total, ns.Score.Latency, ns.Score.Stability,
		ns.Score.Throughput, ns.Score.Loss, ns.Score.Confidence, boolInt(ns.Score.Eligible),
		string(ns.State), timeText(ns.EvaluatedAt), ns.Reason, timeText(r.clock.Now()))
	if err != nil {
		return fmt.Errorf("upsert node score: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT OR REPLACE INTO score_history
	(node_id, evaluated_at, total, latency, stability, throughput, loss,
	 confidence, eligible, state, reason)
VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		ns.NodeID, timeText(ns.EvaluatedAt), ns.Score.Total, ns.Score.Latency,
		ns.Score.Stability, ns.Score.Throughput, ns.Score.Loss, ns.Score.Confidence,
		boolInt(ns.Score.Eligible), string(ns.State), ns.Reason); err != nil {
		return fmt.Errorf("append score history: %w", err)
	}
	return tx.Commit()
}

// TopEligible returns the highest-scoring eligible nodes (FR-607). The indexed
// query over (eligible, total DESC) keeps selection under 1s at 10k nodes.
func (r *Repo) TopEligible(ctx context.Context, limit int) ([]domain.NodeScore, error) {
	if limit <= 0 {
		limit = 1
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT node_id, `+scoreColumns+` FROM node_scores
WHERE eligible = 1
ORDER BY total DESC, node_id ASC
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScores(rows)
}

// GetNodeScore returns the latest persisted score for a node.
func (r *Repo) GetNodeScore(ctx context.Context, nodeID string) (domain.NodeScore, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT node_id, `+scoreColumns+` FROM node_scores WHERE node_id = ?`, nodeID)
	return scanScore(row)
}

// ScoreHistory returns the most recent evaluations for a node, newest first.
func (r *Repo) ScoreHistory(ctx context.Context, nodeID string, limit int) ([]domain.NodeScore, error) {
	if limit <= 0 {
		limit = 1
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT node_id, `+scoreColumns+` FROM score_history
WHERE node_id = ?
ORDER BY evaluated_at DESC, node_id ASC
LIMIT ?`, nodeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScores(rows)
}

// CountScores reports the materialized score rows (health/ops reporting).
func (r *Repo) CountScores(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM node_scores").Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func scanScore(row rowScanner) (domain.NodeScore, error) {
	var (
		ns               domain.NodeScore
		total, lat, stab sql.NullFloat64
		thr, loss        sql.NullFloat64
		conf, eligible   sql.NullInt64
		state, reason    sql.NullString
		evaluated        *string
	)
	if err := row.Scan(&ns.NodeID, &total, &lat, &stab, &thr, &loss, &conf,
		&eligible, &state, &evaluated, &reason); err != nil {
		return ns, err
	}
	ns.Score = domain.Score{
		NodeID:      ns.NodeID,
		Latency:     lat.Float64,
		Stability:   stab.Float64,
		Throughput:  thr.Float64,
		Loss:        loss.Float64,
		Total:       total.Float64,
		Confidence:  int(conf.Int64),
		Eligible:    eligible.Int64 != 0,
		EvaluatedAt: parseTime(evaluated),
	}
	ns.State = domain.ScoreState(state.String)
	ns.Reason = reason.String
	ns.EvaluatedAt = parseTime(evaluated)
	return ns, nil
}

func scanScores(rows *sql.Rows) ([]domain.NodeScore, error) {
	var out []domain.NodeScore
	for rows.Next() {
		ns, err := scanScore(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ns)
	}
	return out, rows.Err()
}
