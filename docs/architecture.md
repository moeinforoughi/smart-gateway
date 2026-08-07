# Smart Gateway — System Architecture

Version: 1.0
Status: Active

## 1. Style

Clean + Hexagonal architecture with in-process event-driven communication.
Dependencies point strictly inward; every layer depends only on layers at the
same or lower rank (dependency rule, §3). All cross-subsystem interaction
flows through domain ports; infrastructure adapters implement those ports and
are replaceable without touching business logic.

## 2. Layer Map

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

## 3. Dependency Rule (enforced by `scripts/archcheck`)

| Layer | Rank | May import |
|---|---|---|
| `shared` | — | nothing internal (`domain`, `log`, `config`, `version`) |
| `domain` | 0 | shared only |
| `service`, `scheduler`, `subscription`, `parser`, `health`, `routing`, `scoring`, `selector` | 1 | rank ≤ 1 + shared |
| `repository` | 2 | rank ≤ 2 |
| `infrastructure`, `monitoring`, `dashboard` | 3 | rank ≤ 3 |
| `api` | 4 | rank ≤ 4 |
| `app` | 5 | rank ≤ 5 |
| `cmd` | 6 | everything |

`scripts/archcheck` walks `internal/*` and rejects any import that points to a
higher rank. The same rules are mirrored in CI via `golangci-lint` `depguard`.

## 4. Data Plane (sing-box subprocess)

- **Inbound(s):** one listener per enabled protocol. VLESS+Reality is the
  default client endpoint (port 443); optional VLESS-TLS, VMess, Trojan,
  Shadowsocks, Hysteria2, TUIC each on their own port.
- **Outbound group:** a single `selector` outbound tagged `smart` listing
  every provider node. Hot switching uses `PUT /proxies/smart` with
  `interrupt_exist_connections: false` → existing connections never drop.
- **Route:** all traffic → `smart`.
- **`experimental.clash_api`:** secret-authenticated HTTP on `127.0.0.1`
  (selector switching, deep delay probes, connection/traffic stats).
- **`experimental.cache_file`:** persists the active selection across restarts.

### Config lifecycle

1. Control plane builds the full JSON config from the node repository.
2. `sing-box check <file>` validates it before any apply.
3. Config is written atomically (temp file + rename, 0600).
4. Node-set changes (subscription refresh) → regenerate → `check` → supervised
   restart. Selector switches never touch the config.

## 5. Events (immutable)

`SubscriptionRefreshed → NodesIngested → NodeHealthChanged → ScoreUpdated →
SelectionChanged → OutboundChanged → MetricsUpdated`

- In-process pub/sub with synchronous dispatch for ordering.
- Per-subscriber buffered channels so slow subscribers (metrics) never block
  the chain.
- Domain ports `Bus` (publish) and `EventSource` (subscribe) are implemented by
  the in-memory fan-out `internal/infrastructure/bus` (M4): non-blocking
  publish, coalesced, never stalls producers; subscribers re-derive state from
  the store when events are dropped.

## 6. State Machines

### Health
`UNKNOWN → HEALTHY → DEGRADED → UNSTABLE → FAILED → RECOVERING → HEALTHY`

### Score
`UNKNOWN → CALCULATED → ELIGIBLE → ACTIVE → REPLACED → ARCHIVED`

### Switch
`Current → Candidate? → Better? (hysteresis) → Cooldown? → Switch → Publish`

## 7. Architectural Invariants (always true)

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
