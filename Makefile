SHELL := /usr/bin/env bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

BIN          := kroxy
PKG          := github.com/bubunyo/kroxy
CMD          := ./cmd/kroxy
BIN_DIR      := bin
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS      := -s -w -X main.version=$(VERSION)
GO           ?= go
GOFLAGS      ?=
DOCKER_IMAGE ?= kroxy:$(VERSION)
COMPOSE      ?= docker compose -f dockerfiles/docker-compose.yml

.PHONY: help build run fmt vet tidy lint test test-race test-integration \
        docker-build compose-up compose-down compose-logs clean

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Compile the kroxy binary into ./bin.
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN) $(CMD)

run: build ## Build and run kroxy with -config dockerfiles/kroxy.yaml.
	$(BIN_DIR)/$(BIN) -config dockerfiles/kroxy.yaml

fmt: ## gofmt the tree.
	$(GO) fmt ./...

vet: ## go vet, including the integration build tag.
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...

tidy: ## go mod tidy.
	$(GO) mod tidy

lint: ## Run golangci-lint if installed.
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed; see https://golangci-lint.run/"; exit 1; }
	golangci-lint run ./...

test: ## Run the unit test suite.
	$(GO) test ./... -count=1

test-race: ## Run the unit test suite with the race detector.
	$(GO) test ./... -race -count=1

test-integration: ## Run the testcontainers-backed integration suite (requires Docker; set DOCKER_HOST if not on the default socket).
	$(GO) test -tags=integration ./integration/... -race -count=1 -timeout=10m

docker-build: ## Build the kroxy docker image.
	docker build -t $(DOCKER_IMAGE) -f dockerfiles/Dockerfile --build-arg VERSION=$(VERSION) .

compose-up: ## Bring up the demo stack (kafka + kroxy) in the background.
	$(COMPOSE) up --build -d

compose-down: ## Tear down the demo stack and remove volumes.
	$(COMPOSE) down -v

compose-logs: ## Follow logs from the demo stack.
	$(COMPOSE) logs -f

clean: ## Remove build artefacts.
	rm -rf $(BIN_DIR)
