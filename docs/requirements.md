# Smart Gateway — Requirements

Version: 1.0
Status: Coverage index — authoritative FR/NFR text supplied by project owner

> This file indexes the functional requirements (FR-xxx) and non-functional
> requirements (NFR-xxx) implemented or in flight, mapping each to its code
> location and test. The authoritative requirement text (FR-001…FR-1402,
> NFR-001…NFR-263) is maintained by the project owner and should be merged
> into this file as the traceability source (NFR-185, M8).

## Legend

- **[Done]** implemented, tested, committed
- **[Partial]** in flight / partial coverage
- **[Planned]** scheduled for a later milestone

## Functional requirements — coverage

| ID | Summary | Milestone | Status | Location |
|---|---|---|---|---|
| FR-001 | Single executable entry point | M0 | Done | `cmd/gateway/main.go` |
| FR-002 | Config loaded before subsystems start | M0 | Done | `cmd/gateway/main.go:32` |
| FR-003 | Fail fast on invalid config | M0 | Done | `internal/config/validate.go` |
| FR-004 | Health/readiness endpoints | M0 | Done | `internal/app` |
| FR-006 | Graceful shutdown | M0 | Done | `internal/app` |
| FR-020 | YAML configuration | M0 | Done | `internal/config` |
| FR-021 | Env overrides (SG_*) | M0 | Done | `internal/config` |
| FR-023 | Aggregated validation errors | M0 | Done | `internal/config` |
| FR-024 | Documented defaults/ranges | M0 | Done | `docs/configuration.md` |
| FR-100 | Subscription ingestion | M1 | Done | `internal/subscription` |
| FR-101 | Subscriptions file format | M1 | Done | `internal/subscription/spec.go` |
| FR-103 | Inline sub metadata | M1 | Done | `internal/config` |
| FR-109 | Conditional GET refresh (ETag/304) | M1 | Done | `internal/subscription/manager.go` |
| FR-110 | Exponential backoff on failure | M1 | Done | `internal/subscription/manager.go` |
| FR-112 | Base interval + jitter | M1 | Done | `internal/subscription/manager.go` |
| FR-113 | Per-subscription interval | M1 | Done | `internal/subscription/manager.go` |
| FR-200 | Share-link parsing (incl. SOCKS/HTTP) | M1 | Done | `internal/parser` |
| FR-204/205 | Node deduplication | M1 | Done | `internal/domain/id.go`, `internal/repository` |
| FR-301 | Soft delete on refresh | M1 | Done | `internal/repository/node.go` |
| FR-302 | Measurement cap 5000/node | M1 | Done | `internal/repository/health.go` |
| FR-403/404 | Probe worker pool | M3 | Done | `internal/health/pool.go` |
| FR-405 | Per-probe timeouts | M3 | Done | `internal/health/probe.go` |
| FR-406 | Stuck-worker isolation | M3 | Done | `internal/health/pool.go` |
| FR-407 | DNS + HTTP response checks | M3 | Done | `internal/health/probe.go` |
| FR-410 | Jitter-only scheduling | M3 | Done | `internal/health/monitor.go` |
| FR-411 | Daily probe bandwidth budget | M3 | Done | `internal/domain/budget.go` |
| FR-412 | Per-node min interval | M3 | Done | `internal/health/monitor.go` |
| FR-500 | Passive throughput attribution | M3 | Done | `internal/health/passive.go` |
| FR-503 | Speed-test download size | M3 | Done | `internal/service/speed` |
| FR-504 | Rare throughput tests | M3 | Done | `internal/service/speed` |
| FR-505 | Speed-test concurrency | M3 | Done | `internal/service/speed` |
| FR-506 | Speed-test bandwidth budget | M3 | Done | `internal/service/speed` |
| FR-607 | Rescore after every health update | M4 | Planned | `internal/scoring` |
| FR-608 | Score history retained | M4 | Planned | `internal/scoring` |
| FR-702/703 | Hysteresis (switch_delta, min_ticks) | M4 | Planned | `internal/selector` |
| FR-704 | Min switch interval | M4 | Planned | `internal/selector` |
| FR-705/706 | Switch events + history | M4 | Planned | `internal/selector` |
| FR-802 | Failover confirmation | M4 | Planned | `internal/selector` |
| FR-803 | False-positive minimization | M4 | Planned | `internal/selector` |
| FR-804 | Switch history reasons | M4 | Planned | `internal/selector` |
| FR-805/806 | Auto-recovery + re-entry | M4 | Planned | `internal/selector` |
| FR-900…904 | Inbounds / client config | M5 | Planned | `internal/api` |
| FR-1002 | Scheduler persistence | M1/M2 | Done (wire to storage in M2) | `internal/scheduler` |
| FR-1101 | Public read-only endpoints | M5 | Planned | `internal/api` |
| FR-1102 | Bearer-authed admin endpoints | M5 | Planned | `internal/api` |
| FR-1103 | JSON API | M5 | Planned | `internal/api` |
| FR-1104 | API versioning | M5 | Planned | `internal/api` |

## Non-functional requirements — coverage

| ID | Summary | Milestone | Status | Gate |
|---|---|---|---|---|
| NFR-009 | Shutdown < 30s | M0 | Done | app tests |
| NFR-021 | No-drop selector switch | M4 | Planned | e2e |
| NFR-044 | Selection < 1s @ 10k nodes | M4 | Planned | bench |
| NFR-046 | Cold start < 10s | M2 | Planned | bench |
| NFR-060…066 | Bandwidth + anti-abuse | M3 | Done | budget tests |
| NFR-123 | Race detector | M0 | Done | CI |
| NFR-124 | go vet | M0 | Done | CI |
| NFR-125 | golangci-lint | M0 | Done | CI |
| NFR-126 | gofmt | M0 | Done | CI |
| NFR-140…149 | Security pass | M8 | Planned | audit |
| NFR-160…164 | Observability | M6 | Planned | metrics |
| NFR-184 | Regression tests | M8 | Planned | tests |
| NFR-185 | Acceptance suite (all MUST IDs) | M8 | Planned | CI |
| NFR-202 | Config validation rules | M0 | Done | config tests |
| NFR-220…224 | Deployment | M7 | Planned | containers |
| NFR-241 | Cross-build amd64/arm64 | M0 | Done | CI |
