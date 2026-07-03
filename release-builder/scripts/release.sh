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
#     test/demo/           — the e2b end-to-end demo (demo_e2b.sh + DEMO.md):
#                            the headline "try it" walkthrough, same path convention
#
# `make build` has already populated bin/$ARCH/ from scripts/artifacts.list
# (single source of truth for binaries); this script just copies + tars.
# Canonical invocation is `make release` (depends on `build`).

set -euo pipefail

VERSION="${1:-v0.1.1}"
ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$ARCH" in amd64) ARCH=x86_64 ;; arm64) ARCH=aarch64 ;; esac

UMBRELLA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORG="$(cd "$UMBRELLA_DIR/../.." && pwd)"
SRC_BIN="$UMBRELLA_DIR/bin/$ARCH"
NAME="kuasar-sandbox-$VERSION-linux-$ARCH"
OUT="$UMBRELLA_DIR/dist/$NAME"

# ----------------------------------------------------------------------------
# Curation manifests. Edit these to add/remove bundled docs or scripts; keep
# rationale comments in-line so the audit trail lives next to the decision.
# ----------------------------------------------------------------------------

# Docs — "system design + product usage" only. Entries are `src[:dst_basename]`
# rooted at $ORG; rename suffix avoids name collisions in the flat <release>/docs/.
# Excluded (pure build workflow, not user-facing): guest-runtime/native-deps/docs/build.md.
DOCS=(
  # umbrella system-level
  "orchestrator/release-builder/docs/kuasar-sandbox.md"
  "orchestrator/release-builder/docs/deployment.md"
  "orchestrator/release-builder/docs/perf.md"
  # storage accelerator
  "accelerator/docs/cache.md"
  "accelerator/docs/manifest.md"
  "accelerator/docs/store.md"
  # image builder (folded into the accelerator)
  "accelerator/docs/flatten.md"
  # microVM runtime
  "sandboxer/docs/sandbox.md"
  "sandboxer/docs/sandbox-runtime.md"
  # node orchestrator — single-node control plane + data-plane proxy + resource daemon
  "orchestrator/docs/node.md"
  "orchestrator/docs/node-proxy.md"
  "orchestrator/docs/node-resource.md"
  # cluster tier — registry + router + placer
  "orchestrator/docs/cluster.md"
  "orchestrator/docs/cluster-router.md"
  "orchestrator/docs/cluster-placer.md"
  # virtual switch
  "connector/docs/vswitch.md"
  "connector/docs/tapfd.md"
  # platform native dependencies — VMM patches + guest kernel contracts
  "guest-runtime/native-deps/docs/cloud-hypervisor.md"
  "guest-runtime/native-deps/docs/sandbox-kernel.md"
)

# Cross-repo e2e — all scripts use `SCRIPT_DIR/../../bin` for BIN default, so
# placing them at <release>/test/e2e/ makes BIN resolve to <release>/bin/ with
# no script edits. Excluded: orchestrator/test/e2e/e2e_node_ctl.sh —
# generates Go drivers via heredoc and runs them with `go run`, requiring a
# checkout of sandbox-{orchestrator,runtime} sources. Keep it in the source repo.
E2ES=(
  # umbrella (25)
  "orchestrator/release-builder/test/e2e/e2e_density.sh"
  "orchestrator/release-builder/test/e2e/e2e_manifest.sh"
  "orchestrator/release-builder/test/e2e/e2e_obs.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_cold.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_cold_manifest.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_cold_target.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_diff_template.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_disks.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_launchspec.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_local_merge.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_placeholder.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_proto.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_restore.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_restore_files.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_snapshot.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_stdio.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_tapfd.sh"
  "orchestrator/release-builder/test/e2e/e2e_sandbox_upload_restore.sh"
  "orchestrator/release-builder/test/e2e/e2e_warmpool_dedup.sh"
  "orchestrator/release-builder/test/e2e/e2e_orchestrator.sh"
  "orchestrator/release-builder/test/e2e/e2e_runtask.sh"
  # e2b build + execute (real microVM / Python SDK; self-skip without their prereqs)
  "orchestrator/release-builder/test/e2e/e2e_run_builder.sh"
  "orchestrator/release-builder/test/e2e/e2e_execute.sh"
  "orchestrator/release-builder/test/e2e/e2e_orchestrator_proxy.sh"
  # cluster tier (real cluster processes + node-stub-ctl control-plane/data-forwarding stub)
  "orchestrator/release-builder/test/e2e/e2e_cluster.sh"
  # accelerator (3)
  "accelerator/test/e2e/e2e_cache.sh"
  "accelerator/test/e2e/e2e_cluster_rolling.sh"
  "accelerator/test/e2e/e2e_store_cache_listen.sh"
)

# Perf + dedup-analysis helpers (same `../../bin` path convention).
PERFS=(
  # umbrella (4)
  "orchestrator/release-builder/test/perf/sandbox-perf.sh"
  "orchestrator/release-builder/test/perf/sandbox-perf-manifest.sh"
  "orchestrator/release-builder/test/perf/density-perf.sh"
  "orchestrator/release-builder/test/perf/workload.py"
  # accelerator bench/dedup helpers (5)
  "accelerator/test/scripts/bench_cache.sh"
  "accelerator/test/scripts/bench_cache_remote.sh"
  "accelerator/test/scripts/dedup_report.sh"
  "accelerator/test/scripts/procmon.sh"
  "accelerator/test/scripts/proc_analyze.py"
)

# Deploy assets — operator-facing example configs + systemd units staged under
# <release>/deploy/: one example config per daemon role (node serve/proxy +
# cluster registry/router/placer) and the matching units. node-ctl additionally
# self-installs its sandbox template units at startup (see node.md §5).
DEPLOYS=(
  # node roles
  "orchestrator/deploy/conductor.example.yaml"
  "orchestrator/deploy/proxy.example.yaml"
  "orchestrator/deploy/node-ctl.service"
  "orchestrator/deploy/node-proxy@.service"
  # cluster roles
  "orchestrator/deploy/registry.example.yaml"
  "orchestrator/deploy/router.example.yaml"
  "orchestrator/deploy/placer.example.yaml"
  "orchestrator/deploy/cluster-registry.service"
  "orchestrator/deploy/cluster-router.service"
  "orchestrator/deploy/cluster-placer.service"
)

# The e2b end-to-end demo (prep + run scripts + guide) — the headline "try it"
# walkthrough. demo_e2b.sh requires demo_prep.sh to have run first (it sources
# the prep.env handoff demo_prep writes). Relocate-safe (both use ../../bin).
# Heavier host prereqs than the e2e (e2b CLI + zot + docker + /dev/kvm + openssl
# + mkfs.ext4); checks + skips.
DEMOS=(
  "orchestrator/release-builder/test/demo/demo_prep.sh"
  "orchestrator/release-builder/test/demo/demo_e2b.sh"
  "orchestrator/release-builder/test/demo/DEMO.md"
)

# ----------------------------------------------------------------------------
# Pre-flight checks (fail early, list every missing input at once)
# ----------------------------------------------------------------------------

missing=()
[ -d "$SRC_BIN" ] && [ -n "$(ls -A "$SRC_BIN" 2>/dev/null || true)" ] \
  || missing+=("$SRC_BIN (empty or absent — run \`make build\`)")
[ -f "$UMBRELLA_DIR/README.md" ] || missing+=("$UMBRELLA_DIR/README.md")
[ -f "$UMBRELLA_DIR/test/QUICKSTART.md" ] || missing+=("$UMBRELLA_DIR/test/QUICKSTART.md")
for spec in "${DOCS[@]}" "${E2ES[@]}" "${PERFS[@]}" "${DEPLOYS[@]}" "${DEMOS[@]}"; do
  src="${spec%%:*}"
  [ -f "$ORG/$src" ] || missing+=("$ORG/$src")
done
# Drift guard: every umbrella test/e2e/*.sh must be bundled (E2ES) or explicitly
# excluded here — otherwise a newly-added e2e silently misses the release tarball.
E2E_EXCLUDE=()   # add basenames intentionally kept out of the release, with a reason
for f in "$UMBRELLA_DIR"/test/e2e/*.sh; do
  b="$(basename "$f")"
  printf '%s\n' "${E2ES[@]}" | grep -q "/$b\$" && continue
  printf '%s\n' "${E2E_EXCLUDE[@]:-}" | grep -qx "$b" && continue
  missing+=("test/e2e/$b — not in E2ES or E2E_EXCLUDE (release manifest drift)")
done
# Same drift guard for the accelerator sub-repo scripts the release bundles —
# its e2e (E2ES) and its bench/dedup helpers (PERFS). runtime/vswitch ship no
# test scripts; orchestrator's sole e2e needs a source checkout (kept out).
ACC_E2E_EXCLUDE=( "e2e_flatten.sh" )   # flatten-registry e2e: finds binaries via PATH/env, not the ../../bin convention; stays in the source repo
for f in "$ORG"/accelerator/test/e2e/*.sh; do
  [ -e "$f" ] || continue
  b="$(basename "$f")"
  printf '%s\n' "${E2ES[@]}" | grep -q "/$b\$" && continue
  printf '%s\n' "${ACC_E2E_EXCLUDE[@]:-}" | grep -qx "$b" && continue
  missing+=("accelerator/test/e2e/$b — not in E2ES or ACC_E2E_EXCLUDE (release manifest drift)")
done
ACC_SCRIPTS_EXCLUDE=()   # add basenames intentionally kept out of the release, with a reason
for f in "$ORG"/accelerator/test/scripts/*; do
  [ -e "$f" ] || continue
  b="$(basename "$f")"
  printf '%s\n' "${PERFS[@]}" | grep -q "/$b\$" && continue
  printf '%s\n' "${ACC_SCRIPTS_EXCLUDE[@]:-}" | grep -qx "$b" && continue
  missing+=("accelerator/test/scripts/$b — not in PERFS or ACC_SCRIPTS_EXCLUDE (release manifest drift)")
done
# Same drift guard for the operator deploy assets (DEPLOYS): a newly-added role
# config or unit must be bundled or excluded, else it silently misses the tarball.
DEPLOY_EXCLUDE=()   # add basenames intentionally kept out of the release, with a reason
for f in "$ORG"/orchestrator/deploy/*; do
  [ -e "$f" ] || continue
  b="$(basename "$f")"
  printf '%s\n' "${DEPLOYS[@]}" | grep -q "/$b\$" && continue
  printf '%s\n' "${DEPLOY_EXCLUDE[@]:-}" | grep -qx "$b" && continue
  missing+=("orchestrator/deploy/$b — not in DEPLOYS or DEPLOY_EXCLUDE (release manifest drift)")
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
mkdir -p "$OUT/bin" "$OUT/docs" "$OUT/test/e2e" "$OUT/test/perf" "$OUT/test/demo" "$OUT/deploy"

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
stage "$OUT/test/demo" "${DEMOS[@]}"
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
echo "    test/demo/ ($(ls -1 "$OUT/test/demo" | wc -l) files — e2b walkthrough)"
echo "    deploy/   ($(ls -1 "$OUT/deploy" | wc -l) files)"
