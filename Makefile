# sandbox-orchestrator — node orchestration + e2b-compatible control plane +
# node-level resource control, all in one node-ctl daemon (the e2b host + the
# resource controller, folded in from the former e2b host daemon + sandbox-sentinel).
#
# node-ctl and e2b-key-ctl are pure-Go daemons (CGO_ENABLED=0). It also assembles the
# e2b guest runtime:
#   - sandbox-runtime-e2b   a base sandbox-runtime.erofs with envd injected at
#                           /opt/sandbox-runtime/bin/envd (auto-bind-mounted into
#                           the guest), via `make sandbox-runtime-e2b`.
# envd itself is a native dependency built by sandbox-deps (`make -C sandbox-deps
# envd`), like cloud-hypervisor / mkfs.erofs.

SHELL := /bin/bash

.PHONY: all build node-ctl cluster-ctl e2b-key-ctl sandbox-runtime-e2b sandbox-runtime-builder \
        test vet bench test-e2e test-e2e-cluster-stub clean help

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

# Inputs for `make sandbox-runtime-e2b` (overridable). envd + the base runtime +
# the erofs tools all come from the umbrella's assembled bin/ (sandbox-deps builds
# envd/fsck.erofs/mkfs.erofs; sandbox-runtime builds the base erofs; collect drops
# them there). Point these at the sub-repo bins directly for a standalone build.
BASE_RUNTIME ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/sandbox-runtime.erofs
ENVD         ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/envd
FSCK         ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/fsck.erofs
MKFS         ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/mkfs.erofs
RUNTIME_E2B  ?= $(BINDIR)/sandbox-runtime-e2b.erofs
# Extra inputs for `make sandbox-runtime-builder` (the build-sandbox guest
# flavor: e2b + flatten-ctl + a static mkfs.erofs injected for in-guest use).
FLATTEN_CTL     ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/flatten-ctl
MKFS_GUEST      ?= ../kuasar-sandbox/bin/$(TARGET_ARCH)/mkfs.erofs
RUNTIME_BUILDER ?= $(BINDIR)/sandbox-runtime-builder.erofs

define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

all: build

# `build` ships the daemon + the e2b key tool. sandbox-runtime-e2b is opt-in
# (needs envd + a base runtime), invoked explicitly or by the umbrella's deps stage.
build: node-ctl cluster-ctl e2b-key-ctl

# e2b-key-ctl: pure-derivation tool to mint e2b API keys from a manifest key.
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

# cluster-ctl: the cluster control plane (registry / router / scaler, cluster.md).
cluster-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/cluster-ctl ./cmd/cluster-ctl
	$(call link_bin,cluster-ctl)

# Inject envd into a bare sandbox-runtime.erofs -> sandbox-runtime-e2b.erofs
# (pure shell over fsck.erofs/mkfs.erofs — no node-ctl binary needed).
sandbox-runtime-e2b:
	@[ -f "$(BASE_RUNTIME)" ] || { echo "missing base runtime: $(BASE_RUNTIME) (build sandbox-runtime first or set BASE_RUNTIME=...)" >&2; exit 1; }
	@[ -f "$(ENVD)" ] || { echo "missing envd: $(ENVD) (run 'make -C sandbox-deps envd' or set ENVD=...)" >&2; exit 1; }
	bash deps/build-runtime-e2b.sh --base "$(BASE_RUNTIME)" --envd "$(ENVD)" --out "$(RUNTIME_E2B)" \
	  --fsck-erofs "$(FSCK)" --mkfs-erofs "$(MKFS)"
	$(call link_bin,sandbox-runtime-e2b.erofs)

# The build-sandbox guest flavor: e2b + flatten-ctl + mkfs.erofs under
# /opt/sandbox-runtime/bin (auto-bind-mounted into the guest app root).
sandbox-runtime-builder:
	@[ -f "$(BASE_RUNTIME)" ] || { echo "missing base runtime: $(BASE_RUNTIME) (build sandbox-runtime first or set BASE_RUNTIME=...)" >&2; exit 1; }
	@[ -f "$(ENVD)" ] || { echo "missing envd: $(ENVD) (run 'make -C sandbox-deps envd' or set ENVD=...)" >&2; exit 1; }
	@[ -f "$(FLATTEN_CTL)" ] || { echo "missing flatten-ctl: $(FLATTEN_CTL) (build sandbox-builder first or set FLATTEN_CTL=...)" >&2; exit 1; }
	@[ -f "$(MKFS_GUEST)" ] || { echo "missing mkfs.erofs: $(MKFS_GUEST) (run 'make -C sandbox-deps erofs' or set MKFS_GUEST=...)" >&2; exit 1; }
	bash deps/build-runtime-builder.sh --base "$(BASE_RUNTIME)" --envd "$(ENVD)" \
	  --flatten-ctl "$(FLATTEN_CTL)" --mkfs-binary "$(MKFS_GUEST)" --out "$(RUNTIME_BUILDER)" \
	  --fsck-erofs "$(FSCK)" --mkfs-erofs "$(MKFS)"
	$(call link_bin,sandbox-runtime-builder.erofs)

test:
	CGO_ENABLED=0 $(GO) test ./...

vet:
	CGO_ENABLED=0 $(GO) vet ./...

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

clean:
	rm -rf bin build

# Self-contained cluster e2e: real registry/router/scaler code with a node-link
# stub. No KVM, systemd, root, or microVM artifacts required.
test-e2e: test-e2e-cluster-stub

test-e2e-cluster-stub:
	CGO_ENABLED=0 $(GO) test ./test/e2e/cluster_stub

help:
	@echo "sandbox-orchestrator. Targets:"
	@echo "  build / node-ctl           build the node daemon (e2b host + resource control)"
	@echo "  sandbox-runtime-e2b        inject envd (from sandbox-deps) into a base sandbox-runtime.erofs"
	@echo "  sandbox-runtime-builder    e2b flavor + flatten-ctl + mkfs.erofs (build-sandbox guest runtime)"
	@echo "  node-ctl                   node resource controller (folded in from sandbox-sentinel)"
	@echo "  test / vet / bench / clean"
	@echo "  test-e2e                  run the self-contained cluster stub e2e"
	@echo "  TARGET_ARCH                x86_64 (default) | aarch64"
