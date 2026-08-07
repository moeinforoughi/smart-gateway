# Smart Gateway

A self-hosted intelligent proxy gateway. It continuously monitors provider
subscriptions, scores every node, and automatically keeps a single healthy
outbound route active behind one stable client-facing endpoint.

Clients connect only to the gateway. Provider switching happens automatically;
no client configuration ever needs updating.

## Status

Early scaffold (Milestone 0). Implemented so far:

- Go 1.24+ single-binary entrypoint (`cmd/gateway`)
- YAML configuration with `SG_*` environment overrides and fail-fast
  aggregated validation (`internal/config`)
- Structured JSON logging with secret masking and correlation IDs
  (`internal/log`)
- Composition root with ordered startup / graceful shutdown and `/healthz`,
  `/readyz` (`internal/app`)
- Heap-based job scheduler with jitter, panic isolation and schedule restore
  (`internal/scheduler`)
- Domain ports (`Clock`, `RandomSource`) for deterministic tests
  (`internal/domain`)
- Architecture import-direction checker (`scripts/archcheck`)
- CI: tests with race detector, vet, gofmt, golangci-lint, cross-builds
  (Ubuntu/Debian/Rocky × amd64/arm64)

## Quick start

```sh
make build
./bin/smart-gateway -config configs/config.yaml
```

## Documentation

- [Configuration reference](docs/configuration.md)
- [Requirements traceability](docs/requirements.md)
- [Architecture](docs/architecture.md)

## License

TBD
