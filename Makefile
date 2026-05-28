# sandbox-sentinel — node-level resource guardian for high-density microVM sandboxes.
#
# node-ctl is a pure-Go daemon: admission control, memory/CPU budget allocation,
# reclaim, per-sandbox state, persistence and audit. It speaks the resource
# control protocol defined in sandbox-runtime/pkg/resource (sandbox-ctl is the
# client).

SHELL := /bin/bash

.PHONY: all build node-ctl test vet bench test-e2e test-e2e-node-ctl clean help

# ---------------------------------------------------------------------------
# Architecture selection (identical block across all kuasar-sandbox repos)
# ---------------------------------------------------------------------------
HOST_ARCH   := $(shell uname -m)
TARGET_ARCH ?= $(HOST_ARCH)
ifeq ($(TARGET_ARCH),amd64)
  override TARGET_ARCH := x86_64
endif
ifeq ($(TARGET_ARCH),arm64)
  override TARGET_ARCH := aarch64
endif
ifeq ($(TARGET_ARCH),x86_64)
  GO_ARCH := amd64
else ifeq ($(TARGET_ARCH),aarch64)
  GO_ARCH := arm64
else
  $(error unsupported TARGET_ARCH=$(TARGET_ARCH); supported: x86_64, aarch64)
endif

# ---------------------------------------------------------------------------
# Build settings
# ---------------------------------------------------------------------------
GO             := go
GO_BUILD_FLAGS := -trimpath
BINDIR         := bin/$(TARGET_ARCH)

define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
all: build

build: node-ctl

node-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/node-ctl ./cmd/node-ctl
	$(call link_bin,node-ctl)

test:
	CGO_ENABLED=0 $(GO) test ./...

vet:
	CGO_ENABLED=0 $(GO) vet ./...

clean:
	rm -rf bin build

# ---------------------------------------------------------------------------
# Tests + benchmarks
# ---------------------------------------------------------------------------
# node-ctl is self-contained binary-wise; the inline-Go client in
# test/e2e/e2e_node_ctl.sh uses sandbox-runtime/pkg/resource as a Go-module
# import (resolved by this repo's `replace runtime => ../sandbox-runtime`).
# SBIN points at the umbrella's assembled bin/ so the script finds node-ctl.
SBIN := $(abspath ../kuasar-sandbox/bin/$(TARGET_ARCH))

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

test-e2e: test-e2e-node-ctl

test-e2e-node-ctl:
	BIN=$(SBIN) bash test/e2e/e2e_node_ctl.sh

help:
	@echo "sandbox-sentinel. Targets:"
	@echo "  build / node-ctl   build the resource controller daemon"
	@echo "  test / vet / clean"
	@echo "  TARGET_ARCH        x86_64 (default) | aarch64"
