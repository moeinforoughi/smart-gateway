# Smart Gateway — Full Implementation Plan

Version: 1.0
Status: Active

---

## 1. Purpose

Ship the Smart Gateway to production: a self-hosted intelligent proxy gateway
(Go 1.24+, sing-box v1.13.16 subprocess) that aggregates provider
subscriptions, continuously scores every node, and keeps exactly one healthy
outbound route active behind one stable client-facing endpoint — with zero
client maintenance.

---

## 2. Locked Decisions

| Decision | Choice |
|---|---|
| Routing engine | sing-box v1.13.16 as a **subprocess**, controlled via its Clash API |
| Hot switching | sing-box `selector` outbound tagged `smart`, `interrupt_exist_connections: false` → existing connections never drop; only new ones migrate |
| Selection control | Control plane decides; applies via Clash API `PUT /proxies/smart` |
| Node-set changes | Regenerate config → `sing-box check` → supervised restart (rare; subscription refresh) |
| Recovery | Watchdog restarts core + re-asserts stored selection; `experimental.cache_file` preserves selection across restarts |
| Project location | `D:\repositories\smart-gateway` (local git repo) |
| Dashboard | Embedded, dependency-free single-page UI served by the gateway |
| SQLite driver | `github.com/ncruces/go-sqlite3` (pure Go, CGO-free, race-clean) |
| Language | Go >= 1.25 (spec requires >= 1.24) |

---

## 3. Source-of-Truth Documents

The following documents are the authoritative requirements and MUST be
committed under `docs/`:

| File | Contents |
|---|---|
| `docs/engineering-spec.md` | Original engineering spec (vision, goals, non-goals, perf/reliability/security targets) |
| `docs/requirements.md` | FR-001…FR-1402 and NFR-001…NFR-263 |
| `docs/architecture.md` | Clean + Hexagonal architecture, layer rules, state machines, invariants |
| `docs/PLAN.md` | This plan |
| `docs/configuration.md` | Config reference (every option: purpose, default, range, example) |
| `docs/rest-api.md` | REST endpoint reference |
| `docs/migrations.md` | Database migration log |

---

## 4. Architecture

Clean + Hexagonal + in-process event-driven communication. Dependencies point
strictly inward:

```
                    CLI / config ──► ┌──────────────────────────────┐
                                     │          Application        │
                                     │  app (DI, lifecycle)        │
                                     │  scheduler (jobs only)      │
                                     │  service/* use-cases        │
                                     └──────────────┬───────────────┘
                                                    │ events (immutable)
                                     ┌──────────────▼───────────────┐
                                     │          Domain              │
                                     │  subscription / health       │
                                     │  scoring / selector / routing│
                                     │  entities + ports + machines │
                                     └──────────────┬───────────────┘
                                     ┌──────────────▼───────────────┐
                                     │      Infrastructure          │
                                     │  repository (SQLite)         │
                                     │  singbox adapter + Clash API │
                                     │  httpclient / logger / pools │
                                     │  clock / metrics / alert     │
                                     └──────────────┬───────────────┘
                                                    ▼
                        REST API / Dashboard / Prometheus
                        sing-box process (data plane)
```

Data plane (sing-box process):

- Inbound(s): one listener per enabled protocol (VLESS+Reality default on 443;
  VLESS-TLS, VMess, Trojan, Shadowsocks, Hysteria2, TUIC optional, each on its
  own port).
- Outbound group: single `selector` tagged `smart` listing every provider node.
- Route: all traffic → `smart`.
- `experimental.clash_api`: secret-authed HTTP on 127.0.0.1 (hot switching,
  deep probes, connection/traffic stats).
- `experimental.cache_file`: persists the selection.

### Events (immutable)

`SubscriptionRefreshed → NodesIngested → NodeHealthChanged → ScoreUpdated →
SelectionChanged → OutboundChanged → MetricsUpdated`

In-process pub/sub; synchronous dispatch for ordering; per-subscriber buffered
channels so slow subscribers (metrics) never block the chain.

### State machines

- Health: `UNKNOWN → HEALTHY → DEGRADED → UNSTABLE → FAILED → RECOVERING → HEALTHY`
- Score: `UNKNOWN → CALCULATED → ELIGIBLE → ACTIVE → REPLACED → ARCHIVED`
- Switch: `Current → Candidate? → Better? (hysteresis) → Cooldown? → Switch → Publish`

### Architectural invariants (always true)

- One source of truth for configuration.
- One source of truth for the node repository.
- Exactly one active outbound.
- All health updates produce immutable records.
- Every score is reproducible.
- Every switch decision is explainable.
- Every subsystem is independently testable.
- Infrastructure can be replaced without changing business logic.
- Scheduler never contains domain logic.
- REST API never contains business logic.
- Database never contains routing logic.
- Proxy core never contains selection logic.

---

## 5. Current State (M0 + M1 review)

Repo commits: `aad2ac3` (M0 scaffold), `d4a270c` (M1 subscription domain,
parser, SQLite repo, refresh manager). Repo is clean; nothing untracked.

### Done and solid

- `internal/config` — YAML + `SG_*` env overrides + defaults + aggregated
  fail-fast validation; full schema drafted (log, api, core, subscriptions,
  inbound, health, speed, scoring, selector, storage, alerting).
- `internal/log` — slog JSON, secret masking, correlation IDs.
- `internal/app` — composition root, ordered startup with readiness
  (FR-004), graceful shutdown < 30s (FR-006, NFR-009), `/healthz` `/readyz`.
- `internal/domain` — entities (`Node`, `Subscription`, `NodeHealth`), node
  states, events, ports (`Clock`, `RandomSource`, `Bus`), UUIDv7.
- `internal/parser` — vless/vmess/trojan/ss/ssr/hysteria2/tuic/socks/http;
  dedupe hash; malformed lines skipped + reported.
- `internal/repository` — SQLite WAL + FK, versioned migrations, node
  ingest/soft-delete (FR-301), measurements capped 5000/node (FR-302).
- `internal/subscription` — conditional GET (ETag/Last-Modified), exponential
  backoff, jitter, events, per-sub failure isolation.
- `internal/scheduler` — heap-based (O(log n)), jitter, persist/restore
  (FR-1002), panic isolation, cancellable.
- `scripts/archcheck`, `Makefile`, CI (tests/race/vet/gofmt/lint/cross-build).

### Issues to fix early (start of M2)

1. **CI runner labels broken** — `debian-13` / `rocky-10` are not GitHub-hosted
   runner labels; the matrix expression only remaps `rocky-10`. Fix: run the
   test matrix on `ubuntu-24.04` (amd64 + arm64) and add container smoke-test
   jobs for Debian 13 / Rocky 10.
2. **Go version drift** — `go.mod` is `1.25.0`, CI uses `GO_VERSION: 1.26`.
   Align both (keep 1.25; spec requires >= 1.24).
3. `make run` references `configs/config.yaml`, which is not committed — copy
   from `config.yaml.example` in the run step or commit a base config.
4. `docs/` missing despite README links — create with section 3 files.
5. Add `golangci-lint` `depguard` rules to enforce layer import direction in CI
   (domain must not import repository/infrastructure/api; infra must not import
   api).

---

## 6. Milestones

### M2 — Core engine (sing-box integration)

Deliverables:

- `internal/infrastructure/singbox/`
  - Node → sing-box outbound JSON adapter (`configgen`) for vmess/vless/trojan/
    shadowsocks/hysteria2/tuic/socks/http, preserving uTLS fingerprint,
    transports, QUIC/BBR flags from source nodes.
  - Process supervisor + watchdog (restart w/ backoff, restart metric + alert,
    re-assert selection after restart).
  - Clash API client: `PUT /proxies/smart` (switch), `GET /proxies/{tag}/delay`
    (deep probe), `GET /connections`, `GET /traffic`, config reload.
  - `sing-box check` validation before every apply; config written atomically.
- `internal/routing/` — RouteManager: "exactly one active" state, applies
  selections through the adapter, writes switch log.
- `internal/domain` — extend `NodeHealth` (latency EWMA, jitter, loss estimate,
  throughput, freshness); score entities; Health/Score/Switch state machines.
- CI fixes (section 5), `docs/` created.

Acceptance gate:

- Integration test against a real local `sing-box` binary (pinned version).
- 10k-node startup benchmark (NFR-046); if > 10s, split nodes into
  per-subscription groups behind the outer `smart` selector.
- `go test -race`, `go vet`, `golangci-lint` zero, archcheck pass.

### M3 — Health & speed monitoring (FR-F/G, NFR-060…066)

> Status: **Complete** (commit pending). Pipeline stages tested per stage;
> budget, FoldHealth, candidate pool, passive diff and state-machine
> transitions unit-tested with injected Clock/RandomSource. Deep probes skip
> recording while the data plane is down (ErrDataPlaneDown) so a core outage
> never mass-marks nodes FAILED. Speed tests measure HTTP/SOCKS nodes
> directly; obfuscated protocols rely on passive attribution (documented
> approximation, FR-506).

Deliverables:

- `internal/health/` — pipelines: DNS resolution → TCP connect → TLS handshake →
  HTTP (generate_204) → latency → packet loss → availability.
  - Configurable worker pool (FR-403/404), per-probe timeouts (FR-405),
    stuck-worker isolation (FR-406).
  - Randomized scheduling jitter only; fixed intervals forbidden (FR-410).
  - Global daily bandwidth budget `max_probe_mb_per_day` (FR-411); anti-abuse
    randomized order + per-node min interval (FR-412, NFR-066).
  - Packet loss: connect-failure rate + RTT jitter; Linux `TCP_INFO` retrans
    when available (documented approximation).
  - Deep probes (full protocol handshake through sing-box) restricted to a
    rotating candidate pool (top-N + active + new); required for QUIC nodes
    (Hysteria2/TUIC).
  - Passive collector: sample Clash API `/connections` to attribute bytes per
    node (zero-cost throughput + real usage).
- `internal/service/speed` — rare throughput tests (FR-504), configurable
  download size (FR-503), limited concurrency (FR-505), own daily budget,
  predictable bandwidth (FR-506).

Acceptance gate:

- Budget math test: probe traffic never exceeds the configured cap.
- Determinism tests with injected `Clock` / `RandomSource`.
- Unit tests per pipeline stage (independently testable).

### M4 — Scoring, selection, failover (FR-H/I/J, NFR-044)

> Status: **Complete** (commit pending). Pure scorer + deterministic engine
> (EWMA dampening, penalties, freshness/confidence gates), materialized
> `node_scores` + `score_history` (schema v3, indexed TopEligible), selector
> policy (hysteresis, dwell cooldown, hard failover, recovery staging,
> provider-down penalty, manual mode), wired through an in-memory event bus
> (infrastructure/bus) with a debounced selection loop. Priority bootstrap
> picks the first outbound before scores exist. Unit tests cover scorer gates,
> engine event→rescore, selector hysteresis/cooldown/failover/recovery, repo
> v3 and routing-with-policy.

Deliverables:

- `internal/scoring/` — pure, deterministic scorer: multi-metric configurable
  weights (latency/stability/throughput/loss), EWMA moving averages (spike
  dampening), historical-stability credit, penalties (consecutive failures,
  provider-wide death, stale data), freshness + confidence gates. Recalculated
  after every health update (FR-607); score history retained (FR-608).
- `internal/selector/` — hysteresis (`switch_delta`, `min_ticks`,
  `dwell_time`); small improvements never switch (FR-702/703); configurable min
  switch interval (FR-704); failover confirmation (N consecutive failures +
  fresh probe) (FR-802); false-positive minimization (FR-803); auto-recovery +
  re-entry (FR-805/806); optional admin override with auto-release.
- Materialized eligible-top-N index so selection over 10k nodes completes
  < 1s (NFR-044).
- Switch events + history persisted with reasons (FR-705/706, FR-804).

Acceptance gate:

- Property-style flapping tests (hysteresis/cooldown).
- E2E with 2 local sing-box nodes: kill node A → assert switch to B; live
  connection survives the selector switch (no-drop, NFR-021); recovery +
  switch-back.

### M5 — REST API + dashboard + client config (FR-K/M, FR-1100 series)

> Status: **Complete**. `internal/api` exposes the versioned JSON surface
> (status, metrics, nodes, outbound override/release/evaluate, subscriptions
> CRUD + refresh, history, client-config, reload) with public reads + Bearer
> admin auth, per-IP rate limiting, correlation IDs, panic recovery and request
> logging; the dashboard is served by the API server at `/dashboard`.
> `internal/dashboard` embeds a single-file dependency-free dark-themed SPA
> (status, node table + drawer with score history, override with auto-release,
> dry-run evaluate, subscription management, switch history, client config with
> copy/share). `internal/infrastructure/singbox` builds per-inbound share links
> + inline outbound blocks and persists the Reality keypair (`sing-box generate
> reality-keypair`, 0600). Admin token auto-generates and persists next to the
> store when `api.token` is empty. All layers carry tests; build/vet/gofmt/test
> green.

Deliverables:

- `internal/api/` — `/api/v1/*`, JSON (FR-1103), versioned (FR-1104);
  read-only monitoring endpoints public (FR-1101); admin endpoints Bearer-authed
  (FR-1102, NFR-143); rate limiting; correlation IDs; panic-recovery middleware
  (generic 500, no stack traces leaked); request logging.
  Endpoints: `status`, `nodes` (filter/sort/page), `nodes/{id}`, `outbound`,
  `outbound/override` (POST/DELETE), `outbound/evaluate`, `subscriptions`
  (GET/POST/DELETE/refresh), `reload`, `history`, `client-config`,
  `metrics`, `healthz`, `readyz`, `dashboard`.
- `internal/dashboard/` — embedded single-page UI (no build step): status,
  active outbound + candidates, node table with scores, switch history,
  subscriptions management, override control, client-config download.
- Config generator — one stable client config per inbound (sing-box JSON +
  v2rayN URIs); Reality keypair via `sing-box generate reality-keypair`,
  secrets stored with 0600 perms.

Acceptance gate:

- Endpoint contract tests; FR-900…904, FR-1101…1104 acceptance.
  `internal/api/api_test.go` exercises auth (401), rate limiting (429),
  correlation, status/metrics, node filters/pagination, override/release/
  evaluate, subscription CRUD + refresh, history, client-config and reload.

### M6 — Observability & alerting (FR-N, NFR-160…164)

Deliverables:

- `internal/monitoring/` — Prometheus registry + labelled collectors:
  `smart_gateway_up`, `nodes_total{status,protocol,subscription}`,
  `node_score{node,protocol}`, `node_latency_ms{node}`,
  `health_checks_total{node,kind,result}`, `node_traffic_bytes{node,direction}`,
  `outbound_current{node}`, `switch_total{reason}`,
  `probe_traffic_bytes_total`, `core_restart_total{reason}`,
  `subscription_last_success{subscription}`, `alert_total{severity,type}`,
  scheduler metrics, DB metrics. `/metrics` endpoint.
- `internal/alert/` — `Notifier` port: GenericWebhook + Telegram; events:
  subscription-failure streak, failover, core restart, degraded fleet, storage
  errors; severity levels; dedup + throttle windows.

Acceptance gate:

- Metric label contract tests; alert dedup test; metric failures never stop
  execution.

> Status: **Complete**. Committed as M6. Metric set rendered by
> `internal/monitoring` and served at `GET /metrics`; alert delivery via
> `internal/alert` (webhook + Telegram) with dedup/throttle windows configured
> under `alerting`. Deliberately dependency-free (hand-rolled Prometheus
> text-format registry, 0.0.4).

### M7 — Deployment (NFR-220…224)

Deliverables:

- `deploy/docker/Dockerfile` — multi-stage, Go 1.25, pinned sing-box binary
  (sha256-verified), non-root user.
- `deploy/compose/docker-compose.yml` — ports, volumes, healthcheck, restart
  policy.
- `deploy/systemd/smart-gateway.service` — `DynamicUser`, `Restart=always`,
  hardening options.
- `deploy/install.sh` — one-command install (downloads both binaries, creates
  config + data dirs, installs/enables the service).
- Graceful restart; config validated before startup.

Acceptance gate:

- Container smoke tests on Ubuntu 24.04 / Debian 13 / Rocky 10 images
  (amd64 + arm64) in CI.
- Single-command install verified on a fresh container.

### M8 — Hardening & acceptance

Deliverables:

- `tests/acceptance/` + traceability matrix in `docs/requirements.md`
  (every FR/NFR ID → implementation + test); CI job asserts all MUST IDs
  covered (NFR-185).
- Stress tests: 10k nodes, 5k concurrent clients, 5k measurements/node;
  memory < 512MB, no goroutine leaks; benchmarks documented.
- Security pass (NFR-140…149): no hardcoded secrets; no shell execution of
  user input; parameterized SQL only; path-traversal prevention; TLS verify
  with explicit opt-out; credentials never logged; panic stack traces never
  exposed.
- Regression tests for every confirmed bug (NFR-184).
- Final compliance run: `go vet`, `golangci-lint` zero, `-race`, integration,
  benchmarks, acceptance.

---

## 7. Cross-Cutting Requirements

### Reliability (NFR-001…010)

- Subsystem isolation: one subsystem failing never terminates the app.
- Panic recovery at every goroutine boundary + HTTP middleware.
- All long-running goroutines cancellable.
- Shutdown completes < 30s; no scheduled task runs after shutdown begins.
- Crash-safe persistence: WAL + checkpoint on shutdown; soft-delete design.

### Performance budgets (NFR-040…047)

| Target | Mechanism + gate |
|---|---|
| CPU < 1 core avg / < 2 peak | O(n) probes, heap scheduler, bounded pools |
| RAM < 512MB @ 100 subs / 10k nodes / 5k meas | Bounded working set; never load full history; streaming API pagination |
| Selection < 1s @ 10k nodes | Materialized eligible-top-N index |
| Config reload < 1s | Config-only reload path (SIGHUP / POST /reload) |
| Cold start < 10s | Measured in CI; mitigation = split outbound groups |
| Thousands of jobs | Min-heap scheduler, O(log n) |

### Bandwidth (FR-411/412, NFR-060…066)

- Global probe + speed-test bandwidth budgets, configurable.
- Routine checks never download payloads; deep probes and speed tests are rare
  and bounded.
- Randomized scheduling to avoid recognizable/abusive patterns.

### Security (NFR-140…149)

- No hardcoded secrets; auto-generated tokens printed once, stored 0600.
- Config validation before startup (fail fast, aggregate errors).
- Credentials never appear in logs (slog masking).
- All HTTP clients verify TLS by default; disabling requires explicit config.
- No shell execution from untrusted input (subprocess calls use fixed args).
- Parameterized SQL only; path traversal prevented; no panic traces in API.

### Observability (NFR-160…164)

- Every subsystem exposes metrics and structured JSON logs.
- Request correlation IDs; duration fields on long operations.
- Failures carry sufficient diagnostic context.
- Prometheus exposition at `/metrics` (dependency-free 0.0.4 registry);
  alerts delivered to webhook/Telegram with dedup + throttle windows.

### Future compatibility (NFR-080…084, 260…263)

- Repositories behind ports → SQLite swap = repository-layer only.
- sing-box adapter isolated → Xray swap = adapter only.
- Event-driven internals + no globals → future clustering not invalidated.

---

## 8. Repository Layout (target)

```
smart-gateway/
├── cmd/gateway/main.go
├── internal/
│   ├── app/                  # DI, lifecycle, event-bus wiring, panic guard
│   ├── config/               # YAML + env + validation + reload
│   ├── log/                  # slog JSON, masking, correlation
│   ├── domain/               # entities, state machines, ports, events
│   ├── service/              # use-case orchestrators
│   ├── subscription/         # domain: subscription engine
│   ├── parser/               # share-link parsers
│   ├── repository/           # ports + SQLite adapters
│   ├── scheduler/            # heap job scheduler (no domain logic)
│   ├── health/               # probe pipelines, budget, passive collector
│   ├── scoring/              # pure scorer
│   ├── selector/             # hysteresis, failover, switch-back, override
│   ├── routing/              # RouteManager (one active outbound)
│   ├── infrastructure/
│   │   ├── singbox/          # configgen, supervisor, Clash API client
│   │   ├── bus/              # in-memory domain event fan-out (M4)
│   │   ├── httpclient/       # downloader + probe transports
│   │   ├── clock/            # std Clock/Random
│   │   ├── logger/           # slog wiring
│   │   ├── metrics/          # Prometheus collectors
│   │   ├── alert/            # webhook + telegram
│   │   └── pools/            # generic worker pools
│   ├── monitoring/           # /metrics, /healthz, /readyz, alert dispatch
│   ├── dashboard/            # embedded single-page UI
│   └── api/                  # thin REST handlers
├── pkg/                      # reserved (empty)
├── configs/                  # config.yaml.example, subscriptions.txt.example
├── deploy/                   # docker, compose, systemd, install.sh
├── scripts/                  # build, lint, cross-compile, archcheck
├── docs/                     # section-3 documents + references
└── tests/
    ├── integration/          # real sing-box e2e
    ├── acceptance/           # FR-by-FR acceptance suite
    └── stress/               # 10k nodes / 5k clients benchmarks + soak
```

---

## 9. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| sing-box version drift | Pin v1.13.16; `sing-box check` before every apply; integration tests pinned |
| Clash API `/configs` reload reliability | Fallback to supervised restart for node-set changes; hot selector switching unaffected |
| 10k-outbound startup time | Benchmark at M2; split per-subscription groups behind outer selector |
| 50M measurement rows (10k × 5k) | Per-node cap 5000 + rollup job |
| Health fresh-ness vs bandwidth | Tiered probing: candidates frequent, fleet sparse, passive free |

---

## 10. Requirement Coverage Notes

- FR-200 includes SOCKS + HTTP proxy parsing (already implemented).
- FR-407 includes DNS resolution + HTTP response checks (M3).
- FR-506 / FR-411 bandwidth predictability enforced via budgets.
- FR-1002 schedule persistence implemented (scheduler.Persist/Restore); wire
  to storage in M2.
- NFR-044 selection < 1s gate implemented in M4.
- NFR-185 acceptance suite delivered in M8.

---

End of File
