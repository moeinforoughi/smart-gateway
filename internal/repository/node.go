package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"smart-gateway/internal/domain"
)

// NodeStore is the persistence port for nodes.
type NodeStore interface {
	// IngestNodes upserts the parsed nodes of one subscription in a single
	// transaction. New identities get a fresh UUIDv7 (FR-206); existing
	// records are refreshed in place; nodes whose dedupe hash already belongs
	// to another subscription are skipped (first-seen wins, FR-204).
	IngestNodes(ctx context.Context, subID string, nodes []domain.Node) (domain.IngestResult, error)
	// SoftDeleteNodesBySub marks every active node of subID not present in
	// keepHashes as deleted (FR-301) and returns how many were removed.
	SoftDeleteNodesBySub(ctx context.Context, subID string, keepHashes []string) (int, error)
	ListNodes(ctx context.Context, subID string) ([]domain.Node, error)
	ListAllNodes(ctx context.Context) ([]domain.Node, error)
	GetNode(ctx context.Context, id string) (*domain.Node, error)
	CountNodes(ctx context.Context) (int, error)
	SetNodeState(ctx context.Context, id string, state domain.NodeState, at time.Time) error
}

const nodeCols = `id, dedupe_hash, subscription_id, protocol, server, port, name,
	transport, tls, sni, fingerprint, host, path, service_name, flow, params,
	raw, tags, state, state_changed_at, created_at, updated_at, deleted_at`

// nodeQueryCols reads nodes joined to their subscription so the selection
// layer sees the owning priority without a denormalized column.
const nodeQueryCols = `n.id, n.dedupe_hash, n.subscription_id, COALESCE(s.priority, 0),
	n.protocol, n.server, n.port, n.name, n.transport, n.tls, n.sni,
	n.fingerprint, n.host, n.path, n.service_name, n.flow, n.params, n.raw,
	n.tags, n.state, n.state_changed_at, n.created_at, n.updated_at, n.deleted_at`

func (r *Repo) IngestNodes(ctx context.Context, subID string, nodes []domain.Node) (domain.IngestResult, error) {
	var res domain.IngestResult
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("begin ingest: %w", err)
	}
	defer tx.Rollback()

	existingStmt, err := tx.PrepareContext(ctx,
		"SELECT id, subscription_id, deleted_at FROM nodes WHERE dedupe_hash=?")
	if err != nil {
		return res, err
	}
	defer existingStmt.Close()

	insertStmt, err := tx.PrepareContext(ctx, `
INSERT INTO nodes (`+nodeCols+`)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return res, err
	}
	defer insertStmt.Close()

	updateStmt, err := tx.PrepareContext(ctx, `
UPDATE nodes SET protocol=?, server=?, port=?, name=?, transport=?, tls=?,
	sni=?, fingerprint=?, host=?, path=?, service_name=?, flow=?, params=?,
	raw=?, tags=?, state=?, state_changed_at=?, updated_at=?, deleted_at=?
WHERE id=?`)
	if err != nil {
		return res, err
	}
	defer updateStmt.Close()

	for i := range nodes {
		n := &nodes[i]
		now := r.clock.Now()
		var existingID, existingSub string
		var deletedAt *string
		err := existingStmt.QueryRowContext(ctx, n.DedupeHash).Scan(&existingID, &existingSub, &deletedAt)
		switch {
		case err == sql.ErrNoRows:
			n.ID = domain.NewUUIDv7(now)
			n.State = domain.StateUnknown
			n.StateChangedAt = now
			n.CreatedAt = now
			n.UpdatedAt = now
			n.DeletedAt = nil
			if _, err := insertStmt.ExecContext(ctx, n.ID, n.DedupeHash, subID,
				string(n.Protocol), n.Server, n.Port, n.Name, n.Transport,
				boolInt(n.TLS), n.SNI, n.Fingerprint, n.Host, n.Path,
				n.ServiceName, n.Flow, encJSON(n.Params), n.Raw, encJSON(n.Tags),
				string(n.State), timeText(n.StateChangedAt), timeText(n.CreatedAt),
				timeText(n.UpdatedAt), nil); err != nil {
				return res, fmt.Errorf("insert node: %w", err)
			}
			res.Added++
		case err != nil:
			return res, err
		case existingSub == subID:
			// Refresh in place; preserve the immutable ID (FR-206).
			n.ID = existingID
			n.UpdatedAt = now
			n.DeletedAt = nil
			if _, err := updateStmt.ExecContext(ctx,
				string(n.Protocol), n.Server, n.Port, n.Name, n.Transport,
				boolInt(n.TLS), n.SNI, n.Fingerprint, n.Host, n.Path,
				n.ServiceName, n.Flow, encJSON(n.Params), n.Raw, encJSON(n.Tags),
				string(n.State), timeText(n.StateChangedAt), timeText(now), nil,
				existingID); err != nil {
				return res, fmt.Errorf("update node: %w", err)
			}
			res.Updated++
		default:
			res.Skipped++ // same node owned by a higher-priority source
		}
	}
	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("commit ingest: %w", err)
	}
	return res, nil
}

func (r *Repo) SoftDeleteNodesBySub(ctx context.Context, subID string, keepHashes []string) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TEMP TABLE keep(h TEXT PRIMARY KEY)"); err != nil {
		return 0, err
	}
	for _, h := range keepHashes {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO keep(h) VALUES (?)", h); err != nil {
			return 0, err
		}
	}
	res, err := tx.ExecContext(ctx, `
UPDATE nodes SET deleted_at=?, updated_at=?
WHERE subscription_id=? AND deleted_at IS NULL
  AND dedupe_hash NOT IN (SELECT h FROM keep)`,
		timeText(r.clock.Now()), timeText(r.clock.Now()), subID)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE keep"); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if n, err := res.RowsAffected(); err == nil {
		return int(n), nil
	}
	return 0, nil
}

func (r *Repo) ListNodes(ctx context.Context, subID string) ([]domain.Node, error) {
	return r.queryNodes(ctx, "WHERE n.subscription_id=? AND n.deleted_at IS NULL ORDER BY n.protocol, n.server", subID)
}

func (r *Repo) ListAllNodes(ctx context.Context) ([]domain.Node, error) {
	return r.queryNodes(ctx, "WHERE n.deleted_at IS NULL ORDER BY n.protocol, n.server")
}

func (r *Repo) GetNode(ctx context.Context, id string) (*domain.Node, error) {
	row := r.db.QueryRowContext(ctx,
		"SELECT "+nodeQueryCols+" FROM nodes n LEFT JOIN subscriptions s ON s.id=n.subscription_id WHERE n.id=?", id)
	n, err := scanNode(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return n, err
}

func (r *Repo) CountNodes(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM nodes WHERE deleted_at IS NULL").Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *Repo) SetNodeState(ctx context.Context, id string, state domain.NodeState, at time.Time) error {
	_, err := r.db.ExecContext(ctx, `
UPDATE nodes SET state=?, state_changed_at=?, updated_at=? WHERE id=?`,
		string(state), timeText(at), timeText(at), id)
	return err
}

func (r *Repo) queryNodes(ctx context.Context, where string, args ...any) ([]domain.Node, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT "+nodeQueryCols+" FROM nodes n LEFT JOIN subscriptions s ON s.id=n.subscription_id "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer rows.Close()
	var out []domain.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *n)
	}
	return out, rows.Err()
}

func scanNode(r rowScanner) (*domain.Node, error) {
	var n domain.Node
	var (
		tls          int
		params, tags string
		state        string
		stateChanged *string
		created      *string
		updated      *string
		deleted      *string
	)
	if err := r.Scan(&n.ID, &n.DedupeHash, &n.SubscriptionID, &n.Priority, &n.Protocol,
		&n.Server, &n.Port, &n.Name, &n.Transport, &tls, &n.SNI,
		&n.Fingerprint, &n.Host, &n.Path, &n.ServiceName, &n.Flow,
		&params, &n.Raw, &tags, &state, &stateChanged, &created, &updated,
		&deleted); err != nil {
		return nil, err
	}
	n.TLS = tls != 0
	decJSON(params, &n.Params)
	decJSON(tags, &n.Tags)
	n.State = domain.NodeState(state)
	n.StateChangedAt = parseTime(stateChanged)
	n.CreatedAt = parseTime(created)
	n.UpdatedAt = parseTime(updated)
	if deleted != nil {
		t := parseTime(deleted)
		n.DeletedAt = &t
	}
	return &n, nil
}
