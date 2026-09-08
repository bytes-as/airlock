# ephemera - build, test and run
#
# Every target here is also a line in the README, so a reviewer can run what
# they read. `make help` lists them.

SHELL := /bin/bash
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN := bin

GO ?= go
GOFLAGS ?= -trimpath

.DEFAULT_GOAL := help

## help: list available targets
.PHONY: help
help:
	@echo "ephemera $(VERSION)"
	@echo
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /' | sort

## build: build all binaries into ./bin
.PHONY: build
build:
	@mkdir -p $(BIN)
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/ephemerad ./cmd/ephemerad
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/ephemera ./cmd/ephemera
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/ephemera-agent ./cmd/ephemera-agent
	@echo "built $(VERSION) into ./$(BIN)"

## test: run every test that does not need Docker
.PHONY: test
test:
	$(GO) test ./... -timeout 15m

## test-race: run the suite under the race detector (needs cgo)
.PHONY: test-race
test-race:
	CGO_ENABLED=1 $(GO) test -race ./... -timeout 20m

## test-docker: run the integration tests that need a live Docker daemon
.PHONY: test-docker
test-docker:
	$(GO) test -tags docker ./... -timeout 20m -v

## test-all: everything, including race and Docker
.PHONY: test-all
test-all: test test-race test-docker

## lint: formatting and vet
.PHONY: lint
lint:
	@unformatted=$$(gofmt -l . | grep -v '^vendor/' || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...
	@echo "lint ok"

## fmt: format the tree
.PHONY: fmt
fmt:
	gofmt -w .

## cross: verify the tree builds for the platforms we actually deploy to
.PHONY: cross
cross:
	GOOS=linux  GOARCH=amd64 $(GO) build ./...
	GOOS=linux  GOARCH=arm64 $(GO) build ./...
	GOOS=darwin GOARCH=arm64 $(GO) build ./...
	@echo "cross-compile ok"

## run: run the control plane locally with the process driver
.PHONY: run
run: build
	$(BIN)/ephemerad --data-dir ./data --log-level debug

## demo: run one job end to end against a locally running control plane
.PHONY: demo
demo: build
	$(BIN)/ephemera run --command "$(PWD)/$(BIN)/ephemera-agent" --query "ephemeral environments"

## up: build and start the full stack in Docker, with egress isolation
.PHONY: up
up:
	docker compose up --build -d
	@echo "control plane on http://localhost:$${EPHEMERA_PORT:-8080}"

## down: stop the stack and remove its volumes
.PHONY: down
down:
	docker compose down -v

## logs: follow the control plane's logs
.PHONY: logs
logs:
	docker compose logs -f control-plane

## load-test: submit 50 concurrent jobs and report the outcome
.PHONY: load-test
load-test: build
	./scripts/load-test.sh

## tf-validate: format-check and validate the Terraform
.PHONY: tf-validate
tf-validate:
	cd deploy/terraform && terraform fmt -check -recursive && terraform init -backend=false && terraform validate

## tf-fmt: format the Terraform
.PHONY: tf-fmt
tf-fmt:
	cd deploy/terraform && terraform fmt -recursive

## clean: remove build output and local state
.PHONY: clean
clean:
	rm -rf $(BIN) data
	@echo "cleaned"

## ci: everything CI runs
.PHONY: ci
ci: lint cross test tf-validate
