# kuasar-sandbox — complete build entry point for the platform.
#
# `make build` drives every sub-repo's build (via their Makefiles) and then
# assembles all platform artifacts into bin/$(TARGET_ARCH)/ (with native-arch
# symlinks under bin/). `make release` packages that bin/ into a download-
# and-run tarball.
#
# Cross-repo e2e/perf tests (those that need binaries from multiple sub-repos)
# live in this repo's test/; per-repo tests live in their own repo's test/.
# `make test-e2e` aggregates: drives each sub-repo's test-e2e via its Makefile
# + runs this repo's own cross-repo e2e sub-targets. Same for `perf` and `bench`.

SHELL    := /bin/bash
ORG      := $(abspath $(CURDIR)/..)
VERSION  ?= v0.1.1

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
ARTIFACTS_LIST := scripts/artifacts.list
GO_REPOS       := sandbox-accelerator sandbox-runtime sandbox-vswitch sandbox-orchestrator

# Cross-repo e2e tests this repo carries (each needs binaries from multiple
# sub-repos: vmlinux/CH/mkfs.erofs from sandbox-deps, manifest-ctl/store-ctl/
# cache-ctl/flatten-ctl from sandbox-accelerator, node-ctl from sandbox-orchestrator, etc.).
#
# Auto-derived from the filesystem so `make test-e2e` always runs EVERY e2e script
# (drop an e2e_*.sh into test/e2e/ and it is included — no list to forget). The
# target name maps to the file with '_' → '-': e2e_sandbox_cold.sh → test-e2e-sandbox-cold
# (the pattern rule below reverses it). Each script self-skips (exit 0) when its
# prerequisites are absent, so coverage is never silently dropped.
E2E_SCRIPTS  := $(sort $(wildcard test/e2e/e2e_*.sh))
UMBRELLA_E2E := $(foreach s,$(E2E_SCRIPTS),test-e2e-$(subst _,-,$(patsubst e2e_%.sh,%,$(notdir $(s)))))

PERF_TARGETS := perf-sandbox perf-sandbox-manifest perf-density

.PHONY: all build collect release vet test clean help demo \
        bench test-e2e perf dedup-report \
        $(UMBRELLA_E2E) $(PERF_TARGETS)

all: build

# Full platform build. Sub-repo order matters: sandbox-deps leads (mkfs.erofs
# is needed by sandbox-runtime to pack the guest erofs). Each sub-repo's
# `build` builds every binary it ships. After all sub-builds, `collect`
# assembles every artifact under bin/$(TARGET_ARCH)/; then the e2b guest runtime
# (sandbox-runtime-e2b.erofs) is assembled from the collected envd + base erofs +
# erofs tools, and a second `collect` folds it into bin/$(TARGET_ARCH)/.
build:
	$(MAKE) -C $(ORG)/sandbox-deps build
	$(MAKE) -C $(ORG)/sandbox-accelerator build
	$(MAKE) -C $(ORG)/sandbox-runtime build
	$(MAKE) -C $(ORG)/sandbox-vswitch build
	$(MAKE) -C $(ORG)/sandbox-orchestrator build
	@$(MAKE) collect
	$(MAKE) -C $(ORG)/sandbox-orchestrator sandbox-runtime-e2b
	$(MAKE) -C $(ORG)/sandbox-orchestrator sandbox-runtime-builder
	@$(MAKE) collect

# Assemble bin/$(TARGET_ARCH)/ from each sub-repo's per-arch bin per the
# artifacts manifest. Native builds drop a bin/<name> symlink to the per-arch
# binary. Idempotent; missing inputs only warn.
collect:
	@rm -rf $(BINDIR); mkdir -p $(BINDIR)
	@while read repo name; do \
	  [ -n "$$repo" ] || continue; \
	  src="$(ORG)/$$repo/bin/$(TARGET_ARCH)/$$name"; \
	  if [ -e "$$src" ]; then \
	    cp -f "$$src" "$(BINDIR)/$$name"; echo "  + $$name"; \
	  else \
	    echo "  ! missing: $$repo/bin/$(TARGET_ARCH)/$$name" >&2; \
	  fi; \
	done < $(ARTIFACTS_LIST)
	@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
	   while read repo name; do \
	     [ -n "$$repo" ] || continue; \
	     [ -e $(BINDIR)/$$name ] && ln -sfn $(TARGET_ARCH)/$$name bin/$$name; \
	   done < $(ARTIFACTS_LIST); \
	 fi
	@echo "==> assembled $(BINDIR)/"

# Aggregate bin/$(TARGET_ARCH)/ into a download-and-run tarball.
release: build
	bash scripts/release.sh $(VERSION)

# ---------------------------------------------------------------------------
# Tests + benchmarks + perf (cross-repo)
# ---------------------------------------------------------------------------
# Static pattern rule (bound to $(UMBRELLA_E2E)): `test-e2e-<sub>` runs
# test/e2e/e2e_<sub>.sh, with dashes in <sub> mapped to underscores so
# test-e2e-sandbox-cold → e2e_sandbox_cold.sh.
$(UMBRELLA_E2E): test-e2e-%: build
	BIN=$(SBIN) bash test/e2e/e2e_$(subst -,_,$*).sh

# Aggregate test-e2e: every umbrella sub-target + each sub-repo's test-e2e.
test-e2e: build $(UMBRELLA_E2E)
	$(MAKE) -C $(ORG)/sandbox-accelerator test-e2e
	$(MAKE) -C $(ORG)/sandbox-vswitch test-e2e

# perf harnesses living in this repo (cross-repo binary use).
perf-sandbox: build
	BIN=$(SBIN) bash test/perf/sandbox-perf.sh
perf-sandbox-manifest: build
	BIN=$(SBIN) bash test/perf/sandbox-perf-manifest.sh
perf-density: build
	BIN=$(SBIN) bash test/perf/density-perf.sh

# Aggregate perf: accelerator's perf-cache + this repo's perfs.
perf: build
	$(MAKE) -C $(ORG)/sandbox-accelerator perf-cache
	$(MAKE) $(PERF_TARGETS)

# Go micro-benchmarks across every Go sub-repo.
bench:
	@for r in $(GO_REPOS); do echo "== bench $$r =="; $(MAKE) -C $(ORG)/$$r bench || exit 1; done

# Dedup analysis report (lives in accelerator).
dedup-report:
	$(MAKE) -C $(ORG)/sandbox-accelerator dedup-report

# e2b end-to-end demo — the "try it" walkthrough driven by the unmodified e2b CLI
# (build template → boot microVM → exec → pause/resume → kill). DEMO_PAUSE=1 to
# step through and drive the CLI from another terminal; see test/demo/DEMO.md.
demo: build
	BIN=$(SBIN) bash test/demo/demo_prep.sh   # persistent store/cache/registry (idempotent; honors REGISTRY=…)
	BIN=$(SBIN) bash test/demo/demo_e2b.sh

# ---------------------------------------------------------------------------
# Sub-repo vet/test/clean aggregates
# ---------------------------------------------------------------------------
vet:
	@for r in $(GO_REPOS); do echo "== vet $$r =="; $(MAKE) -C $(ORG)/$$r vet || exit 1; done

test:
	@for r in $(GO_REPOS); do echo "== test $$r =="; $(MAKE) -C $(ORG)/$$r test || exit 1; done

clean:
	@for r in $(GO_REPOS) sandbox-deps; do $(MAKE) -C $(ORG)/$$r clean 2>/dev/null || true; done
	rm -rf bin build dist

help:
	@echo "kuasar-sandbox — platform build entry. Targets:"
	@echo "  build         build every sub-repo + assemble bin/\$$(TARGET_ARCH)/ (multi-min cold)"
	@echo "  collect       re-assemble bin/\$$(TARGET_ARCH)/ from existing sub-repo outputs"
	@echo "  release       build + package dist/kuasar-sandbox-\$$(VERSION)-linux-\$$(TARGET_ARCH).tar.gz"
	@echo "  test-e2e      aggregate: drive each sub-repo's test-e2e + run this repo's cross-repo e2e ($(words $(UMBRELLA_E2E)))"
	@echo "  test-e2e-<X>  one e2e sub-target (X in {manifest,obs,density,sandbox-{cold,..},orchestrator,run-builder,execute,..})"
	@echo "  demo          run the e2b end-to-end demo (test/demo/demo_e2b.sh; DEMO_PAUSE=1 to step through)"
	@echo "  perf          aggregate: accelerator perf-cache + this repo's perf-sandbox/-manifest/-density"
	@echo "  bench         Go micro-benchmarks across every Go sub-repo"
	@echo "  dedup-report  delegate to sandbox-accelerator"
	@echo "  vet / test    drive each Go sub-repo's vet/test target"
	@echo "  clean         clean every sub-repo + this repo's bin/ build/ dist/"
	@echo "  TARGET_ARCH   x86_64 (default) | aarch64  (exported to sub-makes)"
