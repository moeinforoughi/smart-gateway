BINARY      := bin/smart-gateway
MODULE      := smart-gateway
GO          ?= go
LDFLAGS     := -s -w \
	-X 'smart-gateway/internal/version.Version=$(VERSION)' \
	-X 'smart-gateway/internal/version.Commit=$(COMMIT)' \
	-X 'smart-gateway/internal/version.BuildTime=$(BUILDTIME)'

VERSION     ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDTIME   ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

.PHONY: all build test vet lint fmt check run clean cross-build mod-tidy

all: build

build:
	@mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/gateway

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

lint:
	golangci-lint run ./...

mod-tidy:
	$(GO) mod tidy

check: vet fmt-check test lint

fmt-check:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt needed"; exit 1)

run: build
	cp -n configs/config.yaml.example configs/config.yaml || true
	./$(BINARY) -config configs/config.yaml

cross-build:
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/smart-gateway-linux-amd64 ./cmd/gateway
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/smart-gateway-linux-arm64 ./cmd/gateway

clean:
	rm -rf bin data coverage.out
