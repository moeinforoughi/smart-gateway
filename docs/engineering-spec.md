# Smart Gateway — Engineering Specification

Version: 1.0
Status: Active

## Vision

A self-hosted intelligent proxy gateway that aggregates multiple provider
subscriptions, continuously measures and scores every node, and keeps exactly
one healthy outbound route active behind one stable client-facing endpoint.
Clients configure the endpoint once and never need maintenance: when a node
degrades or dies the gateway transparently fails over to the best available
node, with zero-downtime connection switching.

## Goals

- **Zero client maintenance.** One stable inbound; the client never edits its
  configuration.
- **Aggregation.** Hundreds of provider subscriptions, thousands of nodes,
  ingested, deduplicated and kept fresh.
- **Continuous quality.** Every node health-scored from passive + active +
  deep measurements; a single `smart` selector always points at the best
  eligible node.
- **No-drop switching.** Hot selector switches migrate only new connections;
  live connections are never interrupted (NFR-021).
- **Self-healing.** Watchdog restarts a crashed core and re-asserts the last
  selection.

## Non-Goals

- Not a general-purpose proxy control panel or multi-user service.
- Not a censorship circumvention research project; it operates on top of the
  subscription-provided node inventory.
- No per-connection load balancing (exactly one active outbound, not ECMP).
- No GUI/desktop app; the dashboard is a thin embedded single-page view.

## Non-Functional Targets

| Area | Target |
|---|---|
| Scale | 100+ subscriptions, 10,000 nodes, 5,000 concurrent clients |
| CPU | < 1 core average, < 2 cores peak |
| Memory | < 512 MB at max load |
| Selection latency | < 1 s over 10,000 nodes (NFR-044) |
| Config reload | < 1 s (config-only path) |
| Cold start | < 10 s |
| Shutdown | < 30 s (NFR-009) |
| Bandwidth (probing) | configurable global daily cap; never exceeded |
| Availability | automatic failover on node/provider failure |

## Reliability

- Subsystem isolation: one subsystem failing never terminates the application.
- Panic recovery at every goroutine boundary and HTTP middleware.
- Crash-safe persistence (WAL + checkpoint on shutdown; soft-delete design).
- All long-running goroutines are cancellable; no scheduled task runs after
  shutdown begins.

## Security

- No hardcoded secrets; auto-generated tokens printed once and stored 0600.
- Config validated before startup (fail fast, aggregated errors).
- Credentials never appear in logs (slog masking).
- TLS verification on by default; disabling requires explicit configuration.
- No shell execution from untrusted input (fixed-argument subprocess calls).
- Parameterized SQL only; path traversal prevented; panic stack traces never
  exposed.

## Compliance Gate

Every released milestone must pass `go test -race`, `go vet`, `golangci-lint`
(zero findings), the architecture import checker, and gofmt.
