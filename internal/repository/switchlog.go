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
