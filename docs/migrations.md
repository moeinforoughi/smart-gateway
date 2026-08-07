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

### v2 — Node throughput (M3, FR-500 series)

- `node_throughput` — node_id (FK, CASCADE), measured_at, bytes_per_sec;
  composite PK (node_id, measured_at). Stores speed-test and passively
  attributed throughput estimates (zero-cost via the Clash API /connections).
  Index: `idx_throughput_node`.

### v3 — Materialized scores (M4, FR-607/608)

- `node_scores` — node_id (PK, FK, CASCADE), total, latency, stability,
  throughput, loss, confidence, eligible, state, evaluated_at, reason,
  updated_at. Materialized current score per node; selection runs as the
  indexed query `WHERE eligible=1 ORDER BY total DESC` (NFR-044).
  Index: `idx_scores_total (eligible, total DESC, node_id)`.
- `score_history` — node_id (FK, CASCADE), evaluated_at, total, latency,
  stability, throughput, loss, confidence, eligible, state, reason; composite
  PK (node_id, evaluated_at). Every evaluation is retained for auditability.
  Index: `idx_score_history_node (node_id, evaluated_at DESC)`.

### Future

- v4: `switch_history` (FR-705/706, FR-804).
- v5: `subscription_events` rollup.
- v6: rollup tables for `node_measurements` aging (10k × 5k cap).
