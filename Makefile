# orchestrator — node orchestration, e2b-compatible control plane, independent
# sandbox data Proxy, and node-level resource control. The node-ctl executable
# provides the separately deployed conductor, Proxy, and telemetry roles.
#
# node-ctl, node-stub-ctl and e2b-key-ctl remain pure Go (CGO_ENABLED=0).
# cluster-ctl alone links the pinned FoundationDB native client. Guest runtime
# images are built by the guest-runtime repo.

SHELL := /bin/bash

.PHONY: all build build-core build-cluster-native prepare-cluster-native test-core test-cluster-native node-ctl cluster-ctl node-stub-ctl e2b-key-ctl \
	        test test-source vet bench release test-release clean help

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
# Version is explicitly supplied by release builds; ordinary builds are dev.
BUILD_VERSION ?= dev
VERSION_TAG := $(shell git tag --points-at HEAD --list 'v[0-9]*' --sort=refname 2>/dev/null | \
	grep -E '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8}(\.[1-9][0-9]*)?)?$$' | \
	awk -v preferred="$(BUILD_VERSION)" 'NR == 1 { first = $$0 } $$0 == preferred { exact = $$0 } END { print (exact != "" ? exact : first) }')
VERSION_LDFLAGS := -X github.com/kuasar-sandbox/orchestrator/internal/version.releaseVersion=$(BUILD_VERSION) -X github.com/kuasar-sandbox/orchestrator/internal/version.gitTag=$(VERSION_TAG)
BINDIR         := bin/$(TARGET_ARCH)

define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

all: build

# `build` ships the control-plane binaries.
build: build-core build-cluster-native

build-core: node-ctl node-stub-ctl e2b-key-ctl

# e2b-key-ctl: pure-derivation tool to derive APISecret and mint API keys from it.
e2b-key-ctl:
	@mkdir -p "$(BINDIR)"
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -ldflags '$(VERSION_LDFLAGS)' -o "$(BINDIR)/e2b-key-ctl" ./cmd/e2b-key-ctl
	$(call link_bin,e2b-key-ctl)

# node-ctl: the node executable — e2b-compatible conductor (serve), independent
# data plane (proxy), telemetry, sandbox/build runners, and resource controller.
node-ctl:
	@mkdir -p "$(BINDIR)"
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -ldflags '$(VERSION_LDFLAGS)' -o "$(BINDIR)/node-ctl" ./cmd/node-ctl
	$(call link_bin,node-ctl)

# cluster-ctl: the cluster control plane (registry / router / placer, cluster.md).
cluster-ctl: build-cluster-native

prepare-cluster-native:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/cluster-native.py prepare --arch "$(TARGET_ARCH)"

build-cluster-native:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/cluster-native.py build --arch "$(TARGET_ARCH)" --output "$(BINDIR)" --go "$(GO)" -- $(GO_BUILD_FLAGS) -ldflags '$(VERSION_LDFLAGS)'
	$(call link_bin,cluster-ctl)
	$(call link_bin,run-cluster-ctl)

# The caller supplies an isolated test database; this never connects to the
# machine's default cluster file and fails if FDB_CLUSTER_FILE is absent.
test-cluster-native:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/cluster-native.py test --arch "$(TARGET_ARCH)" --go "$(GO)" -- -count=1 -timeout=10m

# node-stub-ctl: controllable node-link stubs for cluster e2e. It simulates node
# control-plane behavior without launching microVMs.
node-stub-ctl:
	@mkdir -p "$(BINDIR)"
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -ldflags '$(VERSION_LDFLAGS)' -o "$(BINDIR)/node-stub-ctl" ./cmd/node-stub-ctl
	$(call link_bin,node-stub-ctl)

test: test-core

test-core:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
	CGO_ENABLED=0 $(GO) test ./...
	bash test/source/runtask_privilege.sh
	bash test/source/vmm_cgroup.sh
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_*.py'
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-telemetry-image-pull.py

vet:
	CGO_ENABLED=0 $(GO) vet ./...

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

clean:
	rm -rf bin build

# Product E2E is executed by the shared project framework from prepared artifacts.
# Local source checks remain available through the compiler-capable source gate.
test-source:
	bash scripts/ci-source-checks.sh

VERSION ?= v0.1.0
KUASAR_PLATFORM_DIR ?= ../kuasar-sandbox
# Explicit dependency tags select Preview inputs. Otherwise use this module's
# declared release requirement without resolving the module graph.
go_requirement = $$(GOWORK=off $(GO) mod edit -json | \
	jq -er --arg path "github.com/kuasar-sandbox/$(1)" '.Require[] | select(.Path == $$path) | .Version')
ACCELERATOR_VERSION ?= $(call go_requirement,accelerator)
CONNECTOR_VERSION ?= $(call go_requirement,connector)
SANDBOXER_VERSION ?= $(call go_requirement,sandboxer)
RUNTIME_VERSION ?= runtime-$(call go_requirement,guest-runtime)

# Mixed release recipes run sub-makes with the jobserver on real builds. GNU
# Make executes recipes that directly reference $(MAKE) even under --dry-run.
release_make = $(MAKE)
release_recursive = $(if $(findstring n,$(filter-out -% %=%,$(firstword $(MAKEFLAGS)))),,+)

release:
	$(release_recursive)@set -e; accelerator_tag="$(ACCELERATOR_VERSION)"; \
	connector_tag="$(CONNECTOR_VERSION)"; \
	sandboxer_tag="$(SANDBOXER_VERSION)"; \
	runtime_tag="$(RUNTIME_VERSION)"; \
	workspace="$$(python3 -B "$(KUASAR_PLATFORM_DIR)/ci/integration/go_workspace.py" \
		--root .. --owners orchestrator --version "$(VERSION)" --dependency-tag "accelerator=$$accelerator_tag" --dependency-tag "connector=$$connector_tag" --dependency-tag "sandboxer=$$sandboxer_tag" --dependency-tag "guest-runtime=$$runtime_tag")"; \
	$(release_make) build GOWORK="$$workspace" BUILD_VERSION="$(VERSION)" BINDIR="$(abspath $(BINDIR))"; \
	rm -rf "build/release-bundle"; \
	GOWORK="$$workspace" RELEASE_BIN_DIR="$(abspath $(BINDIR))" \
	RELEASE_SOURCE_ROOT="$(CURDIR)" \
	RELEASE_ACCELERATOR_SOURCE_DIR="$(abspath ../accelerator)" \
	RELEASE_CONNECTOR_SOURCE_DIR="$(abspath ../connector)" \
	RELEASE_SANDBOXER_SOURCE_DIR="$(abspath ../sandboxer)" \
	RELEASE_GUEST_RUNTIME_SOURCE_DIR="$(abspath ../guest-runtime)" \
	SOURCE_DATE_EPOCH="$$(git show -s --format=%ct HEAD)" \
	RELEASE_ACCELERATOR_VERSION="$$accelerator_tag" \
	RELEASE_CONNECTOR_VERSION="$$connector_tag" \
	RELEASE_SANDBOXER_VERSION="$$sandboxer_tag" \
	RELEASE_GUEST_RUNTIME_VERSION="$$runtime_tag" \
		bash scripts/release.sh package "$(VERSION)" "$(TARGET_ARCH)" "build/release-bundle"

test-release:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
	bash scripts/test-release.sh

help:
	@echo "orchestrator. Targets:"
	@echo "  build / node-ctl           build conductor, Proxy, telemetry, runners, and resource controller"
	@echo "  build-core                 build the three CGO=0 commands"
	@echo "  build-cluster-native        build cluster-ctl plus its pinned native runtime"
	@echo "  test-core / test-cluster-native  validate pure-Go / real FoundationDB paths"
	@echo "  node-ctl                   node resource controller (folded in from sandbox-sentinel)"
	@echo "  node-stub-ctl              build controllable cluster e2e node-link stubs"
	@echo "  test / vet / bench / clean"
	@echo "  test-source                run compiler-capable orchestrator source integration checks"
	@echo "  release                    build a validated orchestrator component bundle"
	@echo "  test-release               test orchestrator component packaging"
	@echo "  TARGET_ARCH                x86_64 (default) | aarch64"

.PHONY: check-version
# Verify the newly built clean release binaries on their native architecture.
check-version:
	@test "$(HOST_ARCH)" = "$(TARGET_ARCH)"
	@set -e; source="$(VERSION_TAG)"; \
	if [ -z "$$source" ]; then source="$$(git rev-parse HEAD | cut -c1-12)"; fi; \
	for name in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do \
	  binary="$(BINDIR)/$$name"; \
	  if [ "$$name" = cluster-ctl ]; then binary="$(BINDIR)/run-cluster-ctl"; fi; \
	  actual="$$("$$binary" --version)"; \
	  rc=0; "$$binary" --version unexpected >/dev/null 2>&1 || rc=$$?; \
	  if [ "$$rc" -ne 2 ]; then echo "$$name accepted extra version arguments" >&2; exit 1; fi; \
	  expected="$$name $(BUILD_VERSION) ($$source)"; \
	  if [ "$$actual" != "$$expected" ]; then printf 'expected %s, got %s\n' "$$expected" "$$actual" >&2; exit 1; fi; \
	done
