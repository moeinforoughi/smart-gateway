# Smart Gateway — Configuration Reference

Version: 1.0
Status: Active

Configuration is loaded from a single YAML file (FR-020). Every value can be
overridden by a `SG_*` environment variable (FR-021). The schema is validated
before startup with aggregated, descriptive errors (FR-023). Every option
below documents its purpose, default, allowed range and validation rule
(FR-024). The loaded configuration is immutable; subsystems never read files
directly (architecture §21).

Environment override convention: `SG_<SECTION>_<FIELD>` in SCREAMING_SNAKE
form, e.g. `SG_STORAGE_PATH=data/smart-gateway.db`.

---

## `log`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `level` | `info` | debug\|info\|warn\|error | Minimum log level emitted. |
| `json` | `true` | true | Structured JSON logging (always on). |

## `api`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `listen` | `0.0.0.0:8080` | host:port | REST API + dashboard listen address. |
| `token` | `""` | string | Admin bearer token. Empty → auto-generated on first start, persisted to `<storage dir>/admin-token` (0600) and logged once; re-read on later starts so it stays stable. |
| `public_host` | `""` | host[:port] | Public host/domain clients dial for the generated client configs. Empty → derived per inbound (reality SNI, then the bind host). |
| `dashboard` | `true` | bool | Serve the embedded dashboard at `/dashboard`. |

Environment overrides: `SG_API_LISTEN`, `SG_API_TOKEN`, `SG_API_PUBLIC_HOST`, `SG_API_DASHBOARD`.

## `core`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `singbox_path` | `/usr/local/bin/sing-box` | path | sing-box binary. |
| `api_listen` | `127.0.0.1:9090` | host:port | sing-box Clash API listen address. |
| `api_secret` | `""` | string | Clash API secret. Empty → auto-generated. |
| `cache_file` | `data/core-cache.db` | path | Persists the active selection across restarts. |
| `config_dir` | `data/core` | path | Directory holding generated core configs. |
| `restart_limit` | `5` | 1..1000 | Max core auto-restarts per hour before an alert fires. |

## `subscriptions`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `file` | `subscriptions.txt` | path | Subscription URLs file (one per line; `name = URL` supported). |
| `refresh_interval` | `6h` | >= 1m | Base refresh interval; per-sub jitter applied (FR-112/113). |
| `request_timeout` | `30s` | [1s, 60s] | Per-download timeout. |
| `max_bytes` | `10485760` | [1KiB, 256MiB] | Download size cap. |
| `user_agent` | `SmartGateway/1.0` | string | HTTP user-agent for downloads. |
| `verify_tls` | `true` | bool | TLS verification for downloads; disable only explicitly. |
| `backoff_base` | `1m` | duration | Exponential backoff base on failure. |
| `backoff_max` | `4h` | duration | Backoff ceiling. |
| `retry_limit` | `5` | 1..1000 | Consecutive failures before an alert fires. |
| `defs` | `[]` | list | Inline subscription definitions (FR-103 metadata): `name`, `provider`, `priority`, `tags`, `enabled`, `url`, `update_every`, `headers`, `timeout`. |

## `inbound.listeners`

One listener per enabled client-facing protocol. Stable endpoint: clients only
ever see this.

| Key | Default | Range | Purpose |
|---|---|---|---|
| `protocol` | — | vless-reality\|vless\|vmess\|trojan\|shadowsocks\|hysteria2\|tuic | Inbound protocol. |
| `enabled` | `false` | bool | Enable this listener. |
| `listen` | `0.0.0.0` | host | Bind address. |
| `port` | — | 1..65535 | Listen port. |
| `uuid` | `""` | UUID v4 | Client UUID (vless/vmess/trojan). Empty → auto-generated on first start. |
| `flow` | `xtls-rprx-vision` | string | vless flow. |
| `server_names` | `[]` | list | Reality SNI allow-list. |
| `dest` | — | host:port | Reality fallback target. |
| `short_id_len` | `8` | 0..16 | Reality short ID length (normalized to 8 after parse). |
| `password` | `""` | string | Shadowsocks/tuic password. |
| `method` | — | cipher | Shadowsocks cipher. |
| `users` | `[]` | list | Optional multiple credentials: `uuid`, `password`, `name`. |
| `tls` | — | block | `cert_file`, `key_file` for cert-based listeners. |

## `health`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `enabled` | `true` | bool | Enable health monitoring. |
| `workers` | `50` | [1, 1024] | Probe worker pool size (FR-403/404). |
| `probe_timeout` | `3s` | [100ms, 60s] | Per-probe timeout (FR-405). |
| `candidate_window` | `1m` | duration | Frequency for the active candidate pool. |
| `fleet_interval` | `24h` | duration | Baseline sweep of the full fleet. |
| `deep_interval` | `15m` | duration | Deep probes through the proxy core. |
| `max_probe_mb_per_day` | `100` | MiB | Daily probe bandwidth budget (FR-411). |
| `candidate_pool_size` | `100` | 1..10000 | Size of the deep-probe candidate pool. |
| `min_interval` | `30s` | duration | Minimum gap between probes of the same node (FR-412). |

## `speed`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `enabled` | `true` | bool | Enable throughput estimation. |
| `interval` | `6h` | >= 10m | Much rarer than health checks (FR-504). |
| `download_size_bytes` | `1048576` | [1KiB, 50MiB] | Bytes fetched per test (FR-503). |
| `concurrency` | `3` | [1, 50] | Concurrent speed tests (FR-505). |
| `target_url` | `https://www.gstatic.com/generate_204` | URL | Small test target. |
| `max_bytes_per_day` | `52428800` | bytes | Daily speed-test bandwidth budget (FR-506). |
| `timeout` | `15s` | duration | Per-test timeout. |

## `scoring`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `weight_latency` | `0.45` | 0..1 | Latency score weight. |
| `weight_stability` | `0.30` | 0..1 | Stability score weight. |
| `weight_throughput` | `0.15` | 0..1 | Throughput score weight. |
| `weight_loss` | `0.10` | 0..1 | Loss score weight. |
| `ewma_samples` | `20` | 1..10000 | Moving-average window (spike dampening). |
| `stability_window` | `30` | 1..10000 | Recent checks considered for stability. |
| `min_samples` | `3` | 1..1000 | Confidence gate before a node is selectable. |
| `switch_delta` | `0.10` | (0, 1] | Fractional improvement required before switching (FR-702/703). |
| `min_ticks` | `3` | 1..100 | Consecutive evaluations to confirm a switch (FR-802). |
| `dwell_time` | `5m` | >= 1s | Minimum time between switches (FR-704). |
| `failover_failures` | `3` | 1..100 | Consecutive failures before confirmed failover. |
| `hard_latency_ms` | `3s` | duration | Latency above which a node is considered failed. |
| `stale_after` | `24h` | >= 1m | Score freshness gate: a node with no probe within this window is ineligible (M4). |

Note: if all four weights are zero they normalize to the defaults above.

## `selector`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `mode` | `auto` | auto\|manual | Selection mode. Manual allows admin override. |

## `storage`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `path` | `data/smart-gateway.db` | path | SQLite database path. |

## `alerting`

| Key | Default | Range | Purpose |
|---|---|---|---|
| `webhooks` | `[]` | list | Generic webhooks: `url`, `headers`, `timeout`. |
| `telegram` | — | block | `enabled`, `bot_token`, `chat_id`, `message_prefix`. |
