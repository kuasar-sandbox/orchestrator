# kuasar-sandbox — complete build entry point for the platform.
#
# `make build` drives every sub-repo's build (via their Makefiles) and then
# assembles all platform runtime files into bin/$(TARGET_ARCH)/ (with native-arch
# symlinks under bin/). `make release` produces merge-safe component tarballs
# under dist/ for the release-v* aggregate.
#
# Cross-repo e2e/perf tests (those that need binaries from multiple sub-repos)
# live in this repo's test/; per-repo tests live in their own repo's test/.
# `make test-e2e` aggregates: drives each sub-repo's test-e2e via its Makefile
# + runs this repo's own cross-repo e2e sub-targets. Same for `perf` and `bench`.

SHELL    := /bin/bash
ORG      := $(abspath $(CURDIR)/../..)
VERSION  ?= v0.1.0

# ---------------------------------------------------------------------------
# Architecture selection (identical normalization across kuasar-sandbox repos)
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
else ifeq ($(TARGET_ARCH),aarch64)
else
  $(error unsupported TARGET_ARCH=$(TARGET_ARCH); supported: x86_64, aarch64)
endif
export TARGET_ARCH

BINDIR         := bin/$(TARGET_ARCH)
SBIN           := $(abspath $(BINDIR))
E2E_TOOL_DIR   := build/e2e-tools/$(TARGET_ARCH)
E2E_ZOT_BIN    := $(abspath $(E2E_TOOL_DIR)/zot)
E2E_VGW_BIN    := $(abspath $(E2E_TOOL_DIR)/versitygw)
BIN_INPUTS_MANIFEST := scripts/bin-inputs.manifest
CI_TIMED       := scripts/ci-timed.sh
GO_REPOS       := accelerator sandboxer guest-runtime connector orchestrator
ZOT_VERSION    ?= v2.1.17

# Cross-repo e2e tests this repo carries (each needs binaries from multiple
# sub-repos: vmlinux/mkfs.erofs from guest-runtime/native-deps,
# sandbox-runtime.erofs from guest-runtime,
# manifest-ctl/store-ctl/cache-ctl from accelerator,
# sandbox-ctl/sandbox-init/cloud-hypervisor from sandboxer, flatten-ctl from
# guest-runtime, connector-ctl from connector,
# node-ctl from orchestrator, etc.).
#
# Auto-derived from the filesystem so `make test-e2e` always runs EVERY e2e script
# (drop an e2e_*.sh into test/e2e/ and it is included — no list to forget). The
# target name maps to the file with '_' → '-': e2e_sandbox_cold.sh → test-e2e-sandbox-cold
# (the pattern rule below reverses it). `test-e2e` is the full suite: missing
# local prerequisites fail the run. e2e_obs is the only environment-gated
# exception because it needs external OBS credentials/bucket configuration.
E2E_SCRIPTS  := $(sort $(wildcard test/e2e/e2e_*.sh))
UMBRELLA_E2E := $(foreach s,$(E2E_SCRIPTS),test-e2e-$(subst _,-,$(patsubst e2e_%.sh,%,$(notdir $(s)))))
UMBRELLA_E2E_RUN := $(patsubst test-e2e-%,run-e2e-%,$(UMBRELLA_E2E))
E2E_REQUIRE_ENV := REQUIRE_KVM=1 REQUIRE_EXEC=1 REQUIRE_CLUSTER_STUB=1 \
	REQUIRE_CLUSTER_REAL=1 REQUIRE_ORCH=1 REQUIRE_PROXY=1 \
	REQUIRE_BUILDER=1 REQUIRE_RUNTASK=1 REQUIRE_CONNECTOR_E2E=1 \
	REQUIRE_GUEST_RUNTIME=1

PERF_TARGETS := perf-sandbox perf-sandbox-manifest perf-density
REPO_E2E_TARGETS := test-e2e-repo-accelerator test-e2e-repo-connector test-e2e-repo-guest-runtime

.PHONY: all build collect e2e-tools release vet test test-ci-tools clean help demo \
        bench test-e2e perf dedup-report \
        $(UMBRELLA_E2E) $(UMBRELLA_E2E_RUN) $(PERF_TARGETS) $(REPO_E2E_TARGETS)

all: build

# Full platform build. Sub-repo order matters: guest-runtime/native-deps leads
# (mkfs.erofs is needed by guest-runtime), sandboxer builds sandbox-init and
# cloud-hypervisor, then guest-runtime packs the guest erofs and builds
# flatten-ctl. Each sub-repo's `build` builds every binary it ships. After all
# sub-builds, `collect` assembles sub-repo artifacts under bin/$(TARGET_ARCH)/.
# Environment tools used only by tests are kept under build/e2e-tools/ and are
# never packaged as release artifacts.
build:
	$(CI_TIMED) build/guest-native $(MAKE) -C $(ORG)/guest-runtime/native-deps build
	$(CI_TIMED) build/accelerator $(MAKE) -C $(ORG)/accelerator build
	$(CI_TIMED) build/sandboxer $(MAKE) -C $(ORG)/sandboxer build
	$(CI_TIMED) build/guest-runtime $(MAKE) -C $(ORG)/guest-runtime build
	$(CI_TIMED) build/connector $(MAKE) -C $(ORG)/connector build
	$(CI_TIMED) build/orchestrator $(MAKE) -C $(ORG)/orchestrator build
	@$(CI_TIMED) build/collect $(MAKE) collect

e2e-tools:
	$(CI_TIMED) tools/zot env BINDIR="$(abspath $(E2E_TOOL_DIR))" TARGET_ARCH="$(TARGET_ARCH)" ZOT_VERSION="$(ZOT_VERSION)" bash scripts/ensure-zot.sh
	$(CI_TIMED) tools/versitygw env BINDIR="$(abspath $(E2E_TOOL_DIR))" TARGET_ARCH="$(TARGET_ARCH)" bash scripts/ensure-versitygw.sh

# Assemble bin/$(TARGET_ARCH)/ from each sub-repo's per-arch bin per the
# binary-input manifest. Native builds drop a bin/<name> symlink to the per-arch
# binary. Idempotent; missing inputs only warn.
collect:
	@rm -rf $(BINDIR); mkdir -p $(BINDIR)
	@while read repo name; do \
	  case "$$repo" in ''|\#*) continue ;; esac; \
	  [ -n "$$repo" ] || continue; \
	  src="$(ORG)/$$repo/bin/$(TARGET_ARCH)/$$name"; \
	  if [ -e "$$src" ]; then \
	    cp -f "$$src" "$(BINDIR)/$$name"; echo "  + $$name"; \
	  else \
	    echo "  ! missing: $$repo/bin/$(TARGET_ARCH)/$$name" >&2; \
	  fi; \
	done < $(BIN_INPUTS_MANIFEST)
	@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
	   find bin -maxdepth 1 -type l -delete 2>/dev/null || true; \
	   while read repo name; do \
	     case "$$repo" in ''|\#*) continue ;; esac; \
	     [ -n "$$repo" ] || continue; \
	     [ -e $(BINDIR)/$$name ] && ln -sfn $(TARGET_ARCH)/$$name bin/$$name; \
	   done < $(BIN_INPUTS_MANIFEST); \
	 fi
	@echo "==> assembled $(BINDIR)/"

# Produce merge-safe component packages. release-v* uploads these original
# component tarballs and checksums directly; it does not create another aggregate
# tarball.
release: build
	bash scripts/release.sh $(VERSION)

# ---------------------------------------------------------------------------
# Tests + benchmarks + perf (cross-repo)
# ---------------------------------------------------------------------------
# Static pattern rule (bound to $(UMBRELLA_E2E)): `test-e2e-<sub>` runs
# test/e2e/e2e_<sub>.sh, with dashes in <sub> mapped to underscores so
# test-e2e-sandbox-cold → e2e_sandbox_cold.sh.
$(UMBRELLA_E2E): test-e2e-%: build e2e-tools
	$(MAKE) run-e2e-$*

$(UMBRELLA_E2E_RUN): run-e2e-%:
	$(CI_TIMED) e2e/orchestrator/$* env $(E2E_REQUIRE_ENV) BIN=$(SBIN) ZOT_BIN=$(E2E_ZOT_BIN) VGW_BIN=$(E2E_VGW_BIN) bash test/e2e/e2e_$(subst -,_,$*).sh

# Accelerator opens many independently selected loopback ports, so keep it
# separate from guest-runtime's registry tests. Connector's netns/BPF resources
# are disjoint from guest-runtime's Docker/loopback resources and can overlap.
test-e2e-repo-accelerator:
	$(CI_TIMED) e2e/accelerator env $(E2E_REQUIRE_ENV) $(MAKE) -C $(ORG)/accelerator test-e2e SBIN=$(SBIN)

test-e2e-repo-connector:
	$(CI_TIMED) e2e/connector env $(E2E_REQUIRE_ENV) $(MAKE) -C $(ORG)/connector test-e2e

test-e2e-repo-guest-runtime:
	$(CI_TIMED) e2e/guest-runtime env $(E2E_REQUIRE_ENV) $(MAKE) -C $(ORG)/guest-runtime test-e2e \
		FLATTEN_CTL=$(SBIN)/flatten-ctl STORE_CTL=$(SBIN)/store-ctl \
		ZOT_BIN=$(E2E_ZOT_BIN) MKFS_EROFS_PATH=$(SBIN)/mkfs.erofs

# Aggregate test-e2e: every umbrella sub-target + each sub-repo's test-e2e.
test-e2e: build e2e-tools
	$(MAKE) -j1 $(UMBRELLA_E2E_RUN)
	$(MAKE) test-e2e-repo-accelerator
	$(MAKE) -j2 test-e2e-repo-connector test-e2e-repo-guest-runtime

# perf harnesses living in this repo (cross-repo binary use).
perf-sandbox: build
	BIN=$(SBIN) bash test/perf/sandbox-perf.sh
perf-sandbox-manifest: build
	BIN=$(SBIN) bash test/perf/sandbox-perf-manifest.sh
perf-density: build
	BIN=$(SBIN) bash test/perf/density-perf.sh

# Aggregate perf: accelerator's perf-cache + this repo's perfs.
perf: build
	$(MAKE) -C $(ORG)/accelerator perf-cache
	$(MAKE) $(PERF_TARGETS)

# Go micro-benchmarks across every Go sub-repo.
bench:
	@for r in $(GO_REPOS); do echo "== bench $$r =="; $(MAKE) -C $(ORG)/$$r bench || exit 1; done

# Dedup analysis report (lives in accelerator).
dedup-report:
	$(MAKE) -C $(ORG)/accelerator dedup-report

# e2b end-to-end demo — the "try it" walkthrough driven by the unmodified e2b CLI
# (build template → boot microVM → exec → pause/resume → kill). DEMO_PAUSE=1 to
# step through and drive the CLI from another terminal; see test/demo/DEMO.md.
demo: build e2e-tools
	BIN=$(SBIN) ZOT_BIN=$(E2E_ZOT_BIN) VGW_BIN=$(E2E_VGW_BIN) bash test/demo/demo_prep.sh   # persistent store/cache/registry (idempotent; honors REGISTRY=…)
	BIN=$(SBIN) bash test/demo/demo_e2b.sh

# ---------------------------------------------------------------------------
# Sub-repo vet/test/clean aggregates
# ---------------------------------------------------------------------------
vet:
	@for r in $(GO_REPOS); do echo "== vet $$r =="; $(MAKE) -C $(ORG)/$$r vet || exit 1; done

test:
	@for r in $(GO_REPOS); do echo "== test $$r =="; $(MAKE) -C $(ORG)/$$r test || exit 1; done

test-ci-tools:
	bash scripts/test-ci-tools.sh

clean:
	@for r in $(GO_REPOS); do $(MAKE) -C $(ORG)/$$r clean 2>/dev/null || true; done
	rm -rf bin build dist

help:
	@echo "kuasar-sandbox — platform build entry. Targets:"
	@echo "  build         build every sub-repo + assemble bin/\$$(TARGET_ARCH)/ (multi-min cold)"
	@echo "  collect       re-assemble bin/\$$(TARGET_ARCH)/ from existing sub-repo outputs"
	@echo "  e2e-tools     ensure local test environment tools under build/e2e-tools/\$$(TARGET_ARCH)/"
	@echo "  release       build + package merge-safe component tarballs under dist/"
	@echo "  test-e2e      aggregate: drive each sub-repo's test-e2e + run this repo's cross-repo e2e ($(words $(UMBRELLA_E2E)))"
	@echo "  test-e2e-<X>  one e2e sub-target (X in {manifest,obs,density,sandbox-{cold,..},orchestrator,run-builder,execute,cluster-real,..})"
	@echo "  demo          run the e2b end-to-end demo (test/demo/demo_e2b.sh; DEMO_PAUSE=1 to step through)"
	@echo "  perf          aggregate: accelerator perf-cache + this repo's perf-sandbox/-manifest/-density"
	@echo "  bench         Go micro-benchmarks across every Go sub-repo"
	@echo "  dedup-report  delegate to accelerator"
	@echo "  vet / test    drive each Go sub-repo's vet/test target"
	@echo "  clean         clean every sub-repo + this repo's bin/ build/ dist/"
	@echo "  TARGET_ARCH   x86_64 (default) | aarch64  (exported to sub-makes)"
