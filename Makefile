# orchestrator — node orchestration + e2b-compatible control plane +
# node-level resource control, all in one node-ctl daemon (the e2b host + the
# resource controller, folded in from the former e2b host daemon + sandbox-sentinel).
#
# node-ctl, cluster-ctl, node-stub-ctl, and e2b-key-ctl are pure-Go binaries
# (CGO_ENABLED=0). Guest runtime images are built by the guest-runtime repo.

SHELL := /bin/bash

.PHONY: all build node-ctl cluster-ctl node-stub-ctl e2b-key-ctl \
	        test vet bench test-e2e test-e2e-cluster-stub release test-release clean help

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

GO             := go
GO_BUILD_FLAGS := -trimpath
BINDIR         := bin/$(TARGET_ARCH)
E2E_BIN        ?= $(abspath ../platform/bin/$(TARGET_ARCH))
ZOT_BIN        ?= zot
VGW_BIN        ?= versitygw

define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

all: build

# `build` ships the control-plane binaries.
build: node-ctl cluster-ctl node-stub-ctl e2b-key-ctl

# e2b-key-ctl: pure-derivation tool to derive APISecret and mint API keys from it.
e2b-key-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/e2b-key-ctl ./cmd/e2b-key-ctl
	$(call link_bin,e2b-key-ctl)

# node-ctl: the node daemon — e2b-compatible host (serve / proxy / run-sandbox /
# run-builder / manifest-key / export-sandbox) + the in-process node resource
# controller (serve resource_listen; node-ctl resource verbs, ex sandbox-sentinel).
node-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/node-ctl ./cmd/node-ctl
	$(call link_bin,node-ctl)

# cluster-ctl: the cluster control plane (registry / router / placer, cluster.md).
cluster-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/cluster-ctl ./cmd/cluster-ctl
	$(call link_bin,cluster-ctl)

# node-stub-ctl: controllable node-link stubs for cluster e2e. It simulates node
# control-plane behavior without launching microVMs.
node-stub-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/node-stub-ctl ./cmd/node-stub-ctl
	$(call link_bin,node-stub-ctl)

test:
	CGO_ENABLED=0 $(GO) test ./...

vet:
	CGO_ENABLED=0 $(GO) vet ./...

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

clean:
	rm -rf bin build

# Orchestrator owns both its self-contained cluster stub and the full node,
# proxy, builder, and cluster integration cases. The latter use the assembled
# platform binary set supplied by the platform BMS.
test-e2e:
	BIN="$(E2E_BIN)" ZOT_BIN="$(ZOT_BIN)" VGW_BIN="$(VGW_BIN)" bash test/e2e/run_all.sh

test-e2e-cluster-stub:
	REQUIRE_CLUSTER_STUB=1 BIN="$(CURDIR)/$(BINDIR)" bash test/e2e/e2e_cluster_stub.sh

VERSION ?= v0.1.0

release: build
	@mkdir -p build
	rm -rf build/release-bundle
	SOURCE_DATE_EPOCH="$$(git show -s --format=%ct HEAD)" \
		bash scripts/release.sh package "$(VERSION)" "$(TARGET_ARCH)" build/release-bundle

test-release:
	bash scripts/test-release.sh

help:
	@echo "orchestrator. Targets:"
	@echo "  build / node-ctl           build the node daemon (e2b host + resource control)"
	@echo "  cluster-ctl                build registry/router/placer control plane"
	@echo "  node-ctl                   node resource controller (folded in from sandbox-sentinel)"
	@echo "  node-stub-ctl              build controllable cluster e2e node-link stubs"
	@echo "  test / vet / bench / clean"
	@echo "  test-e2e                   run the orchestrator-owned E2E suite with E2E_BIN"
	@echo "  release                    build a validated orchestrator component bundle"
	@echo "  test-release               test orchestrator component packaging"
	@echo "  TARGET_ARCH                x86_64 (default) | aarch64"
