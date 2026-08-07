# Smart Gateway — REST API Reference

Version: 1.0
Status: **Implemented** (M5)

All endpoints are JSON (FR-1103), versioned under `/api/v1` (FR-1104).
Read-only monitoring endpoints are public (FR-1101); admin endpoints require a
Bearer token (FR-1102, NFR-143). The dashboard is served at `/dashboard`
(`api.dashboard`).

## Conventions

- Success: `200 OK` (or `201`/`204`), JSON body.
- Errors: `{ "error": { "code": "...", "message": "..." } }`; generic `500` for
  panics via recovery middleware — stack traces are never exposed.
- Correlation ID: request header `X-Request-ID` echoed in response + logs
  (generated when absent).
- Rate limiting: admin endpoints are limited to **120 requests/min per client
  IP** (fixed window) → `429` + `{"error":{"code":"rate_limited"}}`.
- Auth: `Authorization: Bearer <token>`. The token is `api.token`; when empty
  it is auto-generated and persisted next to the SQLite store
  (`<storage dir>/admin-token`, 0600) — it stays stable across restarts.

## Endpoints

### Health & liveness

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/healthz` | none | Liveness probe (FR-004). |
| GET | `/readyz` | none | Readiness probe; reports component readiness. |
| GET | `/metrics` | none | Prometheus text exposition (M6). |
| GET | `/api/v1/metrics` | none | JSON metric snapshot. |

### Status & nodes

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/status` | none | Gateway + core status, active outbound, node/subscription counts. |
| GET | `/api/v1/nodes` | none | Node list with filter/sort/pagination. |
| GET | `/api/v1/nodes/{id}` | none | Single node detail + score history. |

`GET /api/v1/nodes` query params:

- `state`, `protocol`, `subscription_id` — filters.
- `sort` — `score` (default) | `latency` | `name` | `server` | `created`.
- `order` — `desc` (default) | `asc`.
- `page` (1-based), `page_size` (max 200, default 50).
- Response: `{ "total", "page", "page_size", "items": [ { "id", "name",
  "server", "port", "protocol", "subscription_id", "state", "active",
  "health", "score" } ] }`.

`GET /api/v1/nodes/{id}` — node detail + `health`, `score`, `score_history`
(`?limit=`, max 500).

### Outbound control

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/outbound` | none | Current active outbound + top candidates. |
| POST | `/api/v1/outbound/override` | admin | Force a node as active with optional auto-release. |
| DELETE | `/api/v1/outbound/override` | admin | Release an active override (`204`). |
| GET | `/api/v1/outbound/evaluate` | admin | Dry-run evaluation of a candidate (nothing is applied). |

`POST /api/v1/outbound/override` body:

```json
{ "node_id": "…", "reason": "…", "auto_release_after": "90s" }
```

`auto_release_after` accepts a Go duration (`90s`, `5m`) or plain seconds
(`90`). Response includes `selection` and, when set, `auto_release_at`.

`GET /api/v1/outbound/evaluate` response is a `SwitchDecision`:

```json
{ "at": "…", "candidate_id": "…", "current_id": "…", "reason": "…",
  "outcome": "confirmed|not_better|cooldown|none|failed",
  "candidate_score": 0.0, "current_score": 0.0, "delta": 0.0,
  "candidate_name": "…", "current_name": "…" }
```

### Subscriptions

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/subscriptions` | none | List subscriptions + states. |
| POST | `/api/v1/subscriptions` | admin | Register a new subscription (immediate refresh, best-effort). |
| DELETE | `/api/v1/subscriptions/{id}` | admin | Remove a subscription (`204`; nodes cascade-deleted, selection re-run). |
| POST | `/api/v1/subscriptions/{id}/refresh` | admin | Trigger an immediate refresh. |

`POST /api/v1/subscriptions` body: `url` (required), `name`, `provider`,
`priority`, `tags`, `headers`, `enabled`, `update_every`, `timeout` (durations
as Go strings).

### Operations

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/history` | none | Switch history with node names + reasons (FR-705/706, FR-804). |
| GET | `/api/v1/client-config` | none | Stable client config per inbound (sing-box JSON + v2rayN URIs). |
| POST | `/api/v1/reload` | admin | Config-only reload (SIGHUP equivalent). |

`GET /api/v1/history?limit=` (max 500, default 50) — newest first:
`{ "items": [ { "id", "at", "from_id", "from_name", "to_id", "to_name",
"reason", "prev_score", "new_score" } ], "total" }`.

`GET /api/v1/client-config`:

```json
{ "address": "vpn.example.net", "inbounds": [
  { "protocol": "vless-reality", "tag": "in-vless-reality", "listen": "…",
    "port": 443, "uri": "vless://…", "singbox_json": "{ … }",
    "public_key": "…", "short_id": "00000000" } ] }
```

The `uri` is a v2rayN-style share link; `singbox_json` is an inline sing-box
outbound block. `address` is `api.public_host` (empty when not configured; the
per-inbound host falls back to the reality SNI or bind host). Credentials are
read from the listener's first `uuid`/`password`/user.

`POST /api/v1/reload` re-reads and validates the config file, re-registers
changed subscription sources at runtime, and reports sections that need a
restart:

```json
{ "reloaded": ["subscriptions"], "requires_restart": ["core", "api"] }
```

### Prometheus `/metrics` (M6)

`GET /metrics` exposes the full metric set in the Prometheus text exposition
format (`text/plain; version=0.0.4`). It is public (no auth) so a scraper can
read it. Sampling is best-effort: a failing subsystem contributes zeroes and a
log line, never a failed scrape.

| Metric | Labels | Kind | Purpose |
|---|---|---|---|
| `smart_gateway_up` | — | gauge | 1 while serving. |
| `smart_gateway_uptime_seconds` | — | gauge | Time since process start. |
| `smart_gateway_nodes_total` | `status`,`protocol`,`subscription` | gauge | Fleet breakdown. |
| `smart_gateway_node_score` | `node`,`protocol` | gauge | Latest score per node. |
| `smart_gateway_node_latency_ms` | `node` | gauge | EWMA probe latency. |
| `smart_gateway_health_checks_total` | `node`,`kind`,`result` | counter | Probe results. |
| `smart_gateway_node_traffic_bytes` | `node`,`direction` | counter | Cumulative traffic per outbound. |
| `smart_gateway_outbound_current` | `node` | gauge | 1 for the active outbound. |
| `smart_gateway_switch_total` | `reason` | counter | Outbound switches. |
| `smart_gateway_core_restart_total` | `reason` | counter | sing-box watchdog restarts. |
| `smart_gateway_subscription_last_success` | `subscription` | gauge | Last successful refresh (unix seconds). |
| `smart_gateway_alert_total` | `severity`,`type` | counter | Alerts fired. |
| `smart_gateway_scheduler_jobs` | — | gauge | Registered jobs. |
| `smart_gateway_scheduler_running` | — | gauge | Jobs executing now. |
| `smart_gateway_db_size_bytes` | — | gauge | SQLite footprint including WAL. |
| `smart_gateway_probe_traffic_bytes_total` | — | gauge | 24h probe bandwidth used. |

Fails with `400`/`500` and a descriptive message when the file is missing or
invalid (started with defaults → `not_implemented`).

## Versioning

Breaking changes bump the path prefix (`/api/v2`). Old versions are served
until the next major release.
