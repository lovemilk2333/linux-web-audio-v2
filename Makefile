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
CPU_FLAGS := $(shell \
	if command -v lscpu >/dev/null 2>&1; then \
		LC_ALL=C lscpu | sed -n 's/^Flags:[[:space:]]*//p'; \
	else \
		LC_ALL=C awk '/^flags[[:space:]]*:/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }' /proc/cpuinfo; \
	fi)
GOAMD64 ?= $(shell \
	flags="$(CPU_FLAGS)"; level=v1; missing=0; \
	for feature in cx16 lahf_lm popcnt pni ssse3 sse4_1 sse4_2; do \
		if ! printf '%s\n' "$$flags" | grep -qw "$$feature"; then missing=1; fi; \
	done; \
	if [ "$$missing" -eq 0 ]; then level=v2; fi; \
	missing=0; \
	for feature in avx avx2 bmi1 bmi2 f16c fma movbe osxsave; do \
		if ! printf '%s\n' "$$flags" | grep -qw "$$feature"; then missing=1; fi; \
	done; \
	if ! printf '%s\n' "$$flags" | grep -Eqw 'lzcnt|abm'; then missing=1; fi; \
	if [ "$$missing" -eq 0 ] && [ "$$level" = v2 ]; then level=v3; fi; \
	missing=0; \
	for feature in avx512f avx512bw avx512cd avx512dq avx512vl; do \
		if ! printf '%s\n' "$$flags" | grep -qw "$$feature"; then missing=1; fi; \
	done; \
	if [ "$$missing" -eq 0 ] && [ "$$level" = v3 ]; then level=v4; fi; \
	echo "$$level")
CGO_ENABLED ?= 1
GOMAXPROCS ?= 1
GOEXPERIMENT ?= none
GOTOOLCHAIN ?= local
GO_BUILD_FLAGS ?= -p=1
GO_RELEASE_FLAGS := -trimpath -ldflags="-s -w"
GO_BUILD_ENV := CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) GOAMD64=$(GOAMD64) \
	GOMAXPROCS=$(GOMAXPROCS) GOEXPERIMENT=$(GOEXPERIMENT) GOTOOLCHAIN=$(GOTOOLCHAIN)

.PHONY: all release release-v2 release-v3 go-release frontend-release test clean

all: release

release: go-release frontend-release

release-v2:
	$(MAKE) release GOAMD64=v2

release-v3:
	$(MAKE) release GOAMD64=v3

go-release: $(BIN_DIR)/webaudiod-$(GOAMD64)
	@cp $< $(BIN_DIR)/webaudiod
	@rm -f $(BIN_DIR)/webaudio-client

$(BIN_DIR)/webaudiod-$(GOAMD64): $(shell find cmd core -type f -name '*.go') go.mod go.sum
	@mkdir -p $(BIN_DIR)
	$(GO_BUILD_ENV) $(GO) build $(GO_BUILD_FLAGS) $(GO_RELEASE_FLAGS) -o $(BIN_DIR)/webaudiod ./cmd/server
	@cp $(BIN_DIR)/webaudiod $@

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
