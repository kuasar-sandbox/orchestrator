# orchestrator — node orchestration, e2b-compatible control plane, independent
# sandbox data Proxy, and node-level resource control. The node-ctl executable
# provides the separately deployed conductor, Proxy, and telemetry roles.
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
E2E_BIN        ?= $(abspath ../kuasar-sandbox/bin/$(TARGET_ARCH))
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

# node-ctl: the node executable — e2b-compatible conductor (serve), independent
# data plane (proxy), telemetry, sandbox/build runners, and resource controller.
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
	bash test/e2e/runtask_privilege_test.sh
	bash test/e2e/vmm_cgroup_test.sh
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_cluster_stub_diagnostics.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_execute_ownership.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_execute_pause_cancellation.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_execute_recovery.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_density_*.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_orchestrator_proxy_go.py'
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_placer_readiness.py'

vet:
	CGO_ENABLED=0 $(GO) vet ./...

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

clean:
	rm -rf bin build

# Orchestrator owns both its self-contained cluster stub and the full node,
# proxy, builder, and cluster integration cases. The latter use the assembled
# platform binary set supplied by Integration E2E.
test-e2e:
	BIN="$(E2E_BIN)" ZOT_BIN="$(ZOT_BIN)" VGW_BIN="$(VGW_BIN)" bash test/e2e/run_all.sh

test-e2e-cluster-stub:
	REQUIRE_CLUSTER_STUB=1 BIN="$(CURDIR)/$(BINDIR)" bash test/e2e/e2e_cluster_stub.sh

VERSION ?= v0.1.0
ACCELERATOR_VERSION ?= v0.1.3
CONNECTOR_VERSION ?= v0.1.2
SANDBOXER_VERSION ?= v0.1.3

release: build
	@mkdir -p build
	rm -rf build/release-bundle
	SOURCE_DATE_EPOCH="$$(git show -s --format=%ct HEAD)" \
	RELEASE_ACCELERATOR_VERSION="$(ACCELERATOR_VERSION)" \
	RELEASE_CONNECTOR_VERSION="$(CONNECTOR_VERSION)" \
	RELEASE_SANDBOXER_VERSION="$(SANDBOXER_VERSION)" \
		bash scripts/release.sh package "$(VERSION)" "$(TARGET_ARCH)" build/release-bundle

test-release:
	bash scripts/test-release.sh

help:
	@echo "orchestrator. Targets:"
	@echo "  build / node-ctl           build conductor, Proxy, telemetry, runners, and resource controller"
	@echo "  cluster-ctl                build registry/router/placer control plane"
	@echo "  node-ctl                   node resource controller (folded in from sandbox-sentinel)"
	@echo "  node-stub-ctl              build controllable cluster e2e node-link stubs"
	@echo "  test / vet / bench / clean"
	@echo "  test-e2e                   run the orchestrator-owned E2E suite with E2E_BIN"
	@echo "  release                    build a validated orchestrator component bundle"
	@echo "  test-release               test orchestrator component packaging"
	@echo "  TARGET_ARCH                x86_64 (default) | aarch64"
