package repository

import (
	"context"

	"smart-gateway/internal/domain"
)

// RecordSwitch persists one confirmed outbound switch (FR-705/706).
func (r *Repo) RecordSwitch(ctx context.Context, rec domain.SwitchRecord) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO switch_log (id, at, from_id, to_id, reason, prev_score, new_score)
VALUES (?,?,?,?,?,?,?)`,
		rec.ID, timeText(rec.At), rec.FromID, rec.ToID, rec.Reason,
		rec.PrevScore, rec.NewScore)
	return err
}

// ListSwitchRecords returns the most recent confirmed switches, newest first
// (FR-705/706, FR-804). The switch_log table is indexed on at DESC so the
// history view stays fast as it grows.
func (r *Repo) ListSwitchRecords(ctx context.Context, limit int) ([]domain.SwitchRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT id, at, from_id, to_id, reason, prev_score, new_score
FROM switch_log ORDER BY at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SwitchRecord
	for rows.Next() {
		var rec domain.SwitchRecord
		var at *string
		if err := rows.Scan(&rec.ID, &at, &rec.FromID, &rec.ToID, &rec.Reason,
			&rec.PrevScore, &rec.NewScore); err != nil {
			return nil, err
		}
		rec.At = parseTime(at)
		out = append(out, rec)
	}
	return out, rows.Err()
}
