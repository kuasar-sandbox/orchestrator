#!/usr/bin/env bash
# release.sh — package kuasar-sandbox/bin/$(TARGET_ARCH)/ + curated docs +
# cross-repo e2e/perf scripts into a download-and-run tarball:
#   kuasar-sandbox-<version>-linux-<arch>.tar.gz
#
# Layout (mirrors the dev tree's umbrella `{bin,docs,test}` view; sub-repo
# directories are flattened):
#   <release>/
#     bin/                 — every artifact from scripts/artifacts.list
#     README.md            — umbrella README, verbatim
#     docs/                — system-level + module design docs (curated below)
#     test/QUICKSTART.md   — release-targeted e2e/perf entry guide
#     test/e2e/            — cross-repo e2e scripts (all relocate-safe: each
#                            uses ../../bin/ which lands on <release>/bin/)
#     test/perf/           — perf + dedup helpers (same path convention)
#
# `make build` has already populated bin/$ARCH/ from scripts/artifacts.list
# (single source of truth for binaries); this script just copies + tars.
# Canonical invocation is `make release` (depends on `build`).

set -euo pipefail

VERSION="${1:-v0.1.0}"
ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$ARCH" in amd64) ARCH=x86_64 ;; arm64) ARCH=aarch64 ;; esac

UMBRELLA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORG="$(cd "$UMBRELLA_DIR/.." && pwd)"
SRC_BIN="$UMBRELLA_DIR/bin/$ARCH"
NAME="kuasar-sandbox-$VERSION-linux-$ARCH"
OUT="$UMBRELLA_DIR/dist/$NAME"

# ----------------------------------------------------------------------------
# Curation manifests. Edit these to add/remove bundled docs or scripts; keep
# rationale comments in-line so the audit trail lives next to the decision.
# ----------------------------------------------------------------------------

# Docs — "system design + product usage" only. Entries are `src[:dst_basename]`
# rooted at $ORG; rename suffix avoids name collisions in the flat <release>/docs/.
# Excluded (pure build workflow, not user-facing): sandbox-deps/docs/build.md.
DOCS=(
  # umbrella system-level
  "kuasar-sandbox/docs/PROPOSAL.md"
  "kuasar-sandbox/docs/deployment.md"
  "kuasar-sandbox/docs/perf.md"
  # storage accelerator
  "sandbox-accelerator/docs/cache.md"
  "sandbox-accelerator/docs/manifest.md"
  "sandbox-accelerator/docs/store.md"
  # image builder
  "sandbox-builder/docs/flatten.md"
  # microVM runtime
  "sandbox-runtime/docs/sandbox.md"
  "sandbox-runtime/docs/sandbox-runtime.md"
  # node controller
  "sandbox-sentinel/docs/node.md"
  # node orchestrator (e2b-compatible ingress)
  "sandbox-orchestrator/docs/orchestrator.md"
  # virtual switch — rename PROPOSAL.md to avoid collision with umbrella PROPOSAL.md
  "sandbox-vswitch/docs/PROPOSAL.md:vswitch.md"
  "sandbox-vswitch/docs/tapfd.md"
  # platform native dependencies — VMM patches + guest kernel contracts
  "sandbox-deps/docs/cloud-hypervisor.md"
  "sandbox-deps/docs/sandbox-kernel.md"
)

# Cross-repo e2e — all scripts use `SCRIPT_DIR/../../bin` for BIN default, so
# placing them at <release>/test/e2e/ makes BIN resolve to <release>/bin/ with
# no script edits. Excluded: sandbox-sentinel/test/e2e/e2e_node_ctl.sh —
# generates Go drivers via heredoc and runs them with `go run`, requiring a
# checkout of sandbox-{sentinel,runtime} sources. Keep it in the source repo.
E2ES=(
  # umbrella (16)
  "kuasar-sandbox/test/e2e/e2e_density.sh"
  "kuasar-sandbox/test/e2e/e2e_manifest.sh"
  "kuasar-sandbox/test/e2e/e2e_obs.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_cold.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_cold_manifest.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_cold_target.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_diff_template.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_launchspec.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_proto.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_restore.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_restore_files.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_snapshot.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_stdio.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_tapfd.sh"
  "kuasar-sandbox/test/e2e/e2e_sandbox_upload_restore.sh"
  "kuasar-sandbox/test/e2e/e2e_warmpool_dedup.sh"
  "kuasar-sandbox/test/e2e/e2e_orchestrator.sh"
  "kuasar-sandbox/test/e2e/e2e_runtask.sh"
  # accelerator (2)
  "sandbox-accelerator/test/e2e/e2e_cache.sh"
  "sandbox-accelerator/test/e2e/e2e_cluster_rolling.sh"
)

# Perf + dedup-analysis helpers (same `../../bin` path convention).
PERFS=(
  # umbrella (4)
  "kuasar-sandbox/test/perf/sandbox-perf.sh"
  "kuasar-sandbox/test/perf/sandbox-perf-manifest.sh"
  "kuasar-sandbox/test/perf/density-perf.sh"
  "kuasar-sandbox/test/perf/workload.py"
  # accelerator bench/dedup helpers (3)
  "sandbox-accelerator/test/scripts/bench_cache.sh"
  "sandbox-accelerator/test/scripts/bench_cache_remote.sh"
  "sandbox-accelerator/test/scripts/dedup_report.sh"
)

# Deploy assets — operator-facing config + unit examples staged under
# <release>/deploy/. orchestrator-ctl additionally self-installs its sandbox
# template units at startup (see orchestrator.md §5).
DEPLOYS=(
  "sandbox-orchestrator/deploy/config.example.yaml"
  "sandbox-orchestrator/deploy/orchestrator-ctl.service"
)

# ----------------------------------------------------------------------------
# Pre-flight checks (fail early, list every missing input at once)
# ----------------------------------------------------------------------------

missing=()
[ -d "$SRC_BIN" ] && [ -n "$(ls -A "$SRC_BIN" 2>/dev/null || true)" ] \
  || missing+=("$SRC_BIN (empty or absent — run \`make build\`)")
[ -f "$UMBRELLA_DIR/README.md" ] || missing+=("$UMBRELLA_DIR/README.md")
[ -f "$UMBRELLA_DIR/test/QUICKSTART.md" ] || missing+=("$UMBRELLA_DIR/test/QUICKSTART.md")
for spec in "${DOCS[@]}" "${E2ES[@]}" "${PERFS[@]}" "${DEPLOYS[@]}"; do
  src="${spec%%:*}"
  [ -f "$ORG/$src" ] || missing+=("$ORG/$src")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "release.sh: missing inputs:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  exit 1
fi

# ----------------------------------------------------------------------------
# Stage <release>/ tree
# ----------------------------------------------------------------------------

rm -rf "$OUT"
mkdir -p "$OUT/bin" "$OUT/docs" "$OUT/test/e2e" "$OUT/test/perf" "$OUT/deploy"

cp -f "$SRC_BIN"/* "$OUT/bin/"
cp -f "$UMBRELLA_DIR/README.md" "$OUT/README.md"
cp -f "$UMBRELLA_DIR/test/QUICKSTART.md" "$OUT/test/QUICKSTART.md"

stage() {
  # stage <dest_dir> <spec...>  — spec is "src[:dst_basename]" rooted at $ORG
  local dest="$1"; shift
  local spec src dst
  for spec in "$@"; do
    src="${spec%%:*}"
    dst="${spec#*:}"
    [ "$dst" = "$spec" ] && dst="$(basename "$src")"
    cp -f "$ORG/$src" "$dest/$dst"
  done
}

stage "$OUT/docs"      "${DOCS[@]}"
stage "$OUT/test/e2e"  "${E2ES[@]}"
stage "$OUT/test/perf" "${PERFS[@]}"
stage "$OUT/deploy"    "${DEPLOYS[@]}"

# ----------------------------------------------------------------------------
# Tar + summary
# ----------------------------------------------------------------------------

( cd "$UMBRELLA_DIR/dist" && tar czf "$NAME.tar.gz" "$NAME" )

echo "==> $UMBRELLA_DIR/dist/$NAME.tar.gz"
echo "    bin/      ($(ls -1 "$OUT/bin" | wc -l) files)"
ls -1 "$OUT/bin" | sed 's/^/                /'
echo "    docs/     ($(ls -1 "$OUT/docs" | wc -l) files)"
ls -1 "$OUT/docs" | sed 's/^/                /'
echo "    test/e2e/ ($(ls -1 "$OUT/test/e2e" | wc -l) scripts)"
echo "    test/perf/ ($(ls -1 "$OUT/test/perf" | wc -l) scripts)"
echo "    deploy/   ($(ls -1 "$OUT/deploy" | wc -l) files)"
