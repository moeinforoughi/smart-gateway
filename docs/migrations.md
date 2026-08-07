# Smart Gateway — Database Migrations

Version: 1.0
Status: Active

Migrations are applied on open via `PRAGMA user_version`. Each version is
idempotent (`CREATE TABLE IF NOT EXISTS`) and applied inside a transaction.
Downgrades are not supported; backup before reverting to an older binary.

Pragmas set on every open:

- `journal_mode = WAL`
- `synchronous = NORMAL`
- `foreign_keys = ON`
- `busy_timeout = 5000`

## Migration Log

### v1 — Initial schema (M1, FR-300 series)

- `subscriptions` — id (UUIDv7, PK), url (UNIQUE), name, provider, priority,
  tags (JSON), enabled, headers (JSON), update_every, timeout, etag,
  last_modified, state, consecutive_failures, last_fetch_at, next_fetch_at,
  node_count, last_error, created_at, updated_at.
- `nodes` — id (UUIDv7, PK), dedupe_hash (UNIQUE), subscription_id (FK,
  ON DELETE CASCADE), protocol, server, port, name, transport, tls, sni,
  fingerprint, host, path, service_name, flow, params (JSON), raw, tags (JSON),
  state, state_changed_at, created_at, updated_at, deleted_at (soft delete,
  FR-301).
  - Indexes: `idx_nodes_dedupe`, `idx_nodes_sub`, `idx_nodes_server`.
- `node_measurements` — node_id (FK, CASCADE), probed_at, latency_ms, success;
  composite PK (node_id, probed_at). Per-node retention capped at 5000 rows
  (FR-302). Index: `idx_measurements_node`.

### Future

- v2: `scores` / `score_history` (FR-608).
- v3: `switch_history` (FR-705/706, FR-804).
- v4: `subscription_events` rollup.
- v5: rollup tables for `node_measurements` aging (10k × 5k cap).
