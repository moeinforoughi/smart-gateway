# Smart Gateway — REST API Reference

Version: 1.0
Status: Draft (M5 — endpoints not yet implemented)

All endpoints are JSON (FR-1103), versioned under `/api/v1` (FR-1104).
Read-only monitoring endpoints are public (FR-1101); admin endpoints require a
Bearer token (FR-1102, NFR-143). The dashboard is served at `/dashboard`.

## Conventions

- Success: `200 OK` (or `201`/`204`), JSON body.
- Errors: `{ "error": { "code": "...", "message": "..." } }`; generic 500 for
  panics via recovery middleware — stack traces are never exposed.
- Correlation ID: request header `X-Request-ID` echoed in response + logs.
- Rate limiting applies to admin endpoints.

## Endpoints

### Health & liveness

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/healthz` | none | Liveness probe (FR-004). |
| GET | `/readyz` | none | Readiness probe; reports component readiness. |
| GET | `/api/v1/metrics` | none | Prometheus metrics (M6). |

### Status & nodes

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/status` | none | Gateway + core status, active outbound, counts. |
| GET | `/api/v1/nodes` | none | Node list with filter/sort/pagination. |
| GET | `/api/v1/nodes/{id}` | none | Single node detail + score history. |

### Outbound control

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/outbound` | none | Current active outbound + candidates. |
| POST | `/api/v1/outbound/override` | admin | Force a node as active with optional auto-release. |
| DELETE | `/api/v1/outbound/override` | admin | Release an active override. |
| GET | `/api/v1/outbound/evaluate` | admin | Dry-run evaluation of a candidate. |

### Subscriptions

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/v1/subscriptions` | none | List subscriptions + states. |
| POST | `/api/v1/subscriptions` | admin | Register a new subscription. |
| DELETE | `/api/v1/subscriptions/{id}` | admin | Remove a subscription. |
| POST | `/api/v1/subscriptions/{id}/refresh` | admin | Trigger an immediate refresh. |

### Operations

| Method | Path | Auth | Purpose |
|---|---|---|---|
| POST | `/api/v1/reload` | admin | Config-only reload (SIGHUP equivalent). |
| GET | `/api/v1/history` | none | Switch history with reasons (FR-705/706, FR-804). |
| GET | `/api/v1/client-config` | none | Stable client config per inbound (sing-box JSON + v2rayN URIs). |

## Versioning

Breaking changes bump the path prefix (`/api/v2`). Old versions are served
until the next major release.
