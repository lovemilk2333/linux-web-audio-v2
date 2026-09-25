SHELL := /bin/sh

GO ?= go
PNPM ?= pnpm

BUILD_DIR ?= build
BIN_DIR := $(BUILD_DIR)/bin
WEB_DIR := $(BUILD_DIR)/web
FRONTEND_DIR := frontend
FRONTEND_BASE ?= ./

GOOS ?= linux
GOARCH ?= amd64
GOAMD64 ?= v2
CGO_ENABLED ?= 1
GOMAXPROCS ?= 1
GOEXPERIMENT ?= none
GOTOOLCHAIN ?= local
GO_BUILD_FLAGS ?= -p=1
GO_RELEASE_FLAGS := -trimpath -ldflags="-s -w"
GO_BUILD_ENV := CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) GOAMD64=$(GOAMD64) \
	GOMAXPROCS=$(GOMAXPROCS) GOEXPERIMENT=$(GOEXPERIMENT) GOTOOLCHAIN=$(GOTOOLCHAIN)

.PHONY: all release go-release frontend-release test clean

all: release

release: go-release frontend-release

go-release: $(BIN_DIR)/webaudiod
	@rm -f $(BIN_DIR)/webaudio-client

$(BIN_DIR)/webaudiod: $(shell find cmd core -type f -name '*.go') go.mod go.sum
	@mkdir -p $(BIN_DIR)
	$(GO_BUILD_ENV) $(GO) build $(GO_BUILD_FLAGS) $(GO_RELEASE_FLAGS) -o $@ ./cmd/server

frontend-release:
	$(PNPM) --dir $(FRONTEND_DIR) install --frozen-lockfile
	VITE_BASE_PATH=$(FRONTEND_BASE) $(PNPM) --dir $(FRONTEND_DIR) run build
	@rm -rf $(WEB_DIR)
	@mkdir -p $(WEB_DIR)
	@cp -a $(FRONTEND_DIR)/dist/. $(WEB_DIR)/

test:
	$(GO_BUILD_ENV) $(GO) test $(GO_BUILD_FLAGS) ./...
	$(PNPM) --dir $(FRONTEND_DIR) exec node tests/worklet.test.mjs
	$(PNPM) --dir $(FRONTEND_DIR) exec node tests/protocol.test.mjs

clean:
	rm -rf $(BUILD_DIR)
