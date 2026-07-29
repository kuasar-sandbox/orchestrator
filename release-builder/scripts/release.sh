#!/usr/bin/env bash
# release.sh — build merge-safe component archives for release-v*.
#
# release-v* uploads these original component archives directly. It does not
# wrap them in a second aggregate tarball. Every archive is rooted at the same
# deployable directory layout so users can extract multiple archives into one
# directory:
#
#   bin/                  shared runtime binaries; file names must be unique
#   docs/                 flat semantic doc names; no component directory layer
#   test/                 runnable e2e/perf/demo scripts keep the ../../bin convention
#   deploy/               flat config and systemd examples with unique names
#   release/<name>.json   package metadata, namespaced by file name

set -euo pipefail

VERSION="${1:-v0.1.0}"
ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$ARCH" in amd64) ARCH=x86_64 ;; arm64) ARCH=aarch64 ;; esac

UMBRELLA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORG="$(cd "$UMBRELLA_DIR/../.." && pwd)"
SRC_BIN="$UMBRELLA_DIR/bin/$ARCH"
DIST="$UMBRELLA_DIR/dist"
STAGE_ROOT="${TMPDIR:-/tmp}/kuasar-release-staging/$VERSION-$ARCH"

mkdir -p "$DIST"
find "$DIST" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
rm -rf "$STAGE_ROOT"
mkdir -p "$STAGE_ROOT"

missing=()
need_file() {
  local path="$1"
  [ -f "$path" ] || missing+=("$path")
}
need_bin() {
  local name="$1"
  need_file "$SRC_BIN/$name"
}

copy_spec() {
  # copy_spec <stage> <prefix> <spec...>
  # spec is "src[:dst]" rooted at $ORG. dst is relative to <prefix>.
  local stage="$1"
  local prefix="$2"
  shift 2
  local spec src dst dest
  for spec in "$@"; do
    src="${spec%%:*}"
    dst="${spec#*:}"
    [ "$dst" = "$spec" ] && dst="$(basename "$src")"
    if [ ! -e "$ORG/$src" ]; then
      missing+=("$ORG/$src")
      continue
    fi
    dest="$stage/$prefix/$dst"
    mkdir -p "$(dirname "$dest")"
    if [ -d "$ORG/$src" ]; then
      rm -rf "$dest"
      cp -a "$ORG/$src" "$dest"
    else
      cp -f "$ORG/$src" "$dest"
    fi
  done
}

write_metadata() {
  local stage="$1"
  local name="$2"
  local archive="$3"
  local repo="$name"
  case "$name" in
    sandbox-runtime|vmlinux) repo="guest-runtime" ;;
  esac
  mkdir -p "$stage/release"
  cat >"$stage/release/$name.json" <<EOF
{
  "name": "$name",
  "version": "$VERSION",
  "arch": "$ARCH",
  "archive": "$archive",
  "commit": "$(git -C "$ORG/$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)",
  "built": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
}

make_archive() {
  local stage="$1"
  local archive="$2"
  ( cd "$stage" && tar --sort=name --owner=0 --group=0 --numeric-owner -czf "$DIST/$archive" . )
  echo "==> $DIST/$archive"
}

package_component() {
  local name="$1"
  local archive="$2"
  shift 2
  local stage="$STAGE_ROOT/$name"
  local missing_before="${#missing[@]}"
  rm -rf "$stage"
  mkdir -p "$stage/bin"

  local section item
  section=""
  for item in "$@"; do
    case "$item" in
      --bins|--docs|--tests|--deploy) section="$item"; continue ;;
    esac
    case "$section" in
      --bins)
        need_bin "$item"
        [ -f "$SRC_BIN/$item" ] && cp -f "$SRC_BIN/$item" "$stage/bin/$item"
        ;;
      --docs)
        copy_spec "$stage" "docs" "$item"
        ;;
      --tests)
        copy_spec "$stage" "test" "$item"
        ;;
      --deploy)
        copy_spec "$stage" "deploy" "$item"
        ;;
      *)
        echo "release.sh: package_component $name: item $item before section marker" >&2
        exit 1
        ;;
    esac
  done

  if [ "${#missing[@]}" -eq "$missing_before" ]; then
    write_metadata "$stage" "$name" "$archive"
    make_archive "$stage" "$archive"
  fi
}

package_runtime_bundle() {
  local name="sandbox-runtime"
  local archive="sandbox-runtime-$ARCH-$VERSION.tar.gz"
  local stage="$STAGE_ROOT/$name"
  local missing_before="${#missing[@]}"
  rm -rf "$stage"
  mkdir -p "$stage/bin"
  need_bin sandbox-runtime.bundle
  if [ -f "$SRC_BIN/sandbox-runtime.bundle" ]; then
    cp -f "$SRC_BIN/sandbox-runtime.bundle" "$stage/bin/sandbox-runtime.bundle"
    cp -f "$SRC_BIN/sandbox-runtime.bundle" "$stage/bin/sandbox-runtime-$ARCH-$VERSION.bundle"
  fi
  copy_spec "$stage" "docs" \
    "guest-runtime/docs/sandbox-runtime.md"
  if [ "${#missing[@]}" -eq "$missing_before" ]; then
    write_metadata "$stage" "$name" "$archive"
    make_archive "$stage" "$archive"
  fi
}

package_vmlinux() {
  local name="vmlinux"
  local archive="vmlinux-$ARCH-$VERSION.tar.gz"
  local stage="$STAGE_ROOT/$name"
  local missing_before="${#missing[@]}"
  rm -rf "$stage"
  mkdir -p "$stage/bin"
  need_bin vmlinux
  if [ -f "$SRC_BIN/vmlinux" ]; then
    cp -f "$SRC_BIN/vmlinux" "$stage/bin/vmlinux"
    cp -f "$SRC_BIN/vmlinux" "$stage/bin/vmlinux-$ARCH-$VERSION"
  fi
  copy_spec "$stage" "docs" \
    "guest-runtime/docs/vmlinux.md"
  if [ "${#missing[@]}" -eq "$missing_before" ]; then
    write_metadata "$stage" "$name" "$archive"
    make_archive "$stage" "$archive"
  fi
}

[ -d "$SRC_BIN" ] && [ -n "$(ls -A "$SRC_BIN" 2>/dev/null || true)" ] \
  || missing+=("$SRC_BIN (empty or absent; run make build)")

package_component "orchestrator" "orchestrator-$VERSION-linux-$ARCH.tar.gz" \
  --bins node-ctl cluster-ctl node-stub-ctl e2b-key-ctl \
  --docs \
    "orchestrator/README.md:orchestrator.md" \
    "orchestrator/docs/node.md:node.md" \
    "orchestrator/docs/node-proxy.md:node-proxy.md" \
    "orchestrator/docs/node-resource.md:node-resource.md" \
    "orchestrator/docs/cluster.md:cluster.md" \
    "orchestrator/docs/cluster-router.md:cluster-router.md" \
    "orchestrator/docs/cluster-placer.md:cluster-placer.md" \
    "orchestrator/release-builder/README.md:release-builder.md" \
    "orchestrator/release-builder/docs/kuasar-sandbox.md:kuasar-sandbox.md" \
    "orchestrator/release-builder/docs/deployment.md:deployment.md" \
    "orchestrator/release-builder/docs/perf.md:perf.md" \
  --tests \
    "orchestrator/release-builder/test/README.md:README.md" \
    "orchestrator/release-builder/test/QUICKSTART.md:QUICKSTART.md" \
    "orchestrator/release-builder/test/e2e/run_all.sh:e2e/run_all.sh" \
    "orchestrator/release-builder/test/e2e/e2e_density.sh:e2e/e2e_density.sh" \
    "orchestrator/release-builder/test/e2e/e2e_manifest.sh:e2e/e2e_manifest.sh" \
    "orchestrator/release-builder/test/e2e/e2e_obs.sh:e2e/e2e_obs.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_cold.sh:e2e/e2e_sandbox_cold.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_cold_manifest.sh:e2e/e2e_sandbox_cold_manifest.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_cold_target.sh:e2e/e2e_sandbox_cold_target.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_diff_template.sh:e2e/e2e_sandbox_diff_template.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_disks.sh:e2e/e2e_sandbox_disks.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_launchspec.sh:e2e/e2e_sandbox_launchspec.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_local_merge.sh:e2e/e2e_sandbox_local_merge.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_placeholder.sh:e2e/e2e_sandbox_placeholder.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_proto.sh:e2e/e2e_sandbox_proto.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_restore.sh:e2e/e2e_sandbox_restore.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_restore_files.sh:e2e/e2e_sandbox_restore_files.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_snapshot.sh:e2e/e2e_sandbox_snapshot.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_stdio.sh:e2e/e2e_sandbox_stdio.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_tapfd.sh:e2e/e2e_sandbox_tapfd.sh" \
    "orchestrator/release-builder/test/e2e/e2e_sandbox_upload_restore.sh:e2e/e2e_sandbox_upload_restore.sh" \
    "orchestrator/release-builder/test/e2e/e2e_warmpool_dedup.sh:e2e/e2e_warmpool_dedup.sh" \
    "orchestrator/release-builder/test/e2e/e2e_orchestrator.sh:e2e/e2e_orchestrator.sh" \
    "orchestrator/release-builder/test/e2e/e2e_runtask.sh:e2e/e2e_runtask.sh" \
    "orchestrator/release-builder/test/e2e/e2e_run_builder.sh:e2e/e2e_run_builder.sh" \
    "orchestrator/release-builder/test/e2e/e2e_execute.sh:e2e/e2e_execute.sh" \
    "orchestrator/release-builder/test/e2e/e2e_orchestrator_proxy.sh:e2e/e2e_orchestrator_proxy.sh" \
    "orchestrator/release-builder/test/e2e/e2e_mmds_endpoints_internal.sh:e2e/e2e_mmds_endpoints_internal.sh" \
    "orchestrator/release-builder/test/e2e/e2e_mmds_endpoints_external.sh:e2e/e2e_mmds_endpoints_external.sh" \
    "orchestrator/release-builder/test/e2e/e2e_mmds_endpoints_cluster.sh:e2e/e2e_mmds_endpoints_cluster.sh" \
    "orchestrator/release-builder/test/e2e/lib/mmds_static_guest.sh:e2e/lib/mmds_static_guest.sh" \
    "orchestrator/release-builder/test/e2e/e2e_cluster_real.sh:e2e/e2e_cluster_real.sh" \
    "orchestrator/test/e2e/e2e_cluster_stub.sh:e2e/e2e_cluster.sh" \
    "orchestrator/release-builder/test/perf/sandbox-perf.sh:perf/sandbox-perf.sh" \
    "orchestrator/release-builder/test/perf/sandbox-perf-manifest.sh:perf/sandbox-perf-manifest.sh" \
    "orchestrator/release-builder/test/perf/density-perf.sh:perf/density-perf.sh" \
    "orchestrator/release-builder/test/perf/workload.py:perf/workload.py" \
    "orchestrator/release-builder/test/demo/demo_prep.sh:demo/demo_prep.sh" \
    "orchestrator/release-builder/test/demo/demo_e2b.sh:demo/demo_e2b.sh" \
    "orchestrator/release-builder/test/demo/DEMO.md:demo/DEMO.md" \
  --deploy \
    "orchestrator/deploy/conductor.example.yaml" \
    "orchestrator/deploy/proxy.example.yaml" \
    "orchestrator/deploy/node-ctl.service" \
    "orchestrator/deploy/node-proxy.service" \
    "orchestrator/deploy/registry.example.yaml" \
    "orchestrator/deploy/router.example.yaml" \
    "orchestrator/deploy/placer.example.yaml" \
    "orchestrator/deploy/cluster-registry.service" \
    "orchestrator/deploy/cluster-router.service" \
    "orchestrator/deploy/cluster-placer.service"

package_component "accelerator" "accelerator-$VERSION-linux-$ARCH.tar.gz" \
  --bins manifest-ctl store-ctl cache-ctl \
  --docs \
    "accelerator/README.md:accelerator.md" \
    "accelerator/docs/cache.md" \
    "accelerator/docs/manifest.md" \
    "accelerator/docs/store.md" \
  --tests \
    "accelerator/test/e2e/e2e_cache.sh:e2e/e2e_cache.sh" \
    "accelerator/test/e2e/e2e_cluster_rolling.sh:e2e/e2e_cluster_rolling.sh" \
    "accelerator/test/e2e/e2e_store_cache_listen.sh:e2e/e2e_store_cache_listen.sh" \
    "accelerator/test/scripts/bench_cache.sh:scripts/bench_cache.sh" \
    "accelerator/test/scripts/bench_cache_remote.sh:scripts/bench_cache_remote.sh" \
    "accelerator/test/scripts/dedup_report.sh:scripts/dedup_report.sh" \
    "accelerator/test/scripts/procmon.sh:scripts/procmon.sh" \
    "accelerator/test/scripts/proc_analyze.py:scripts/proc_analyze.py"

package_component "guest-runtime" "guest-runtime-$VERSION-linux-$ARCH.tar.gz" \
  --bins flatten-ctl mkfs.erofs \
  --docs \
    "guest-runtime/README.md:guest-runtime.md" \
    "guest-runtime/docs/flatten.md" \
    "guest-runtime/native-deps/README.md:native-deps.md" \
    "guest-runtime/native-deps/docs/build.md:native-deps-build.md" \
  --tests \
    "guest-runtime/test/e2e/e2e_flatten.sh:e2e/e2e_flatten.sh" \
    "guest-runtime/test/e2e/README.md:e2e/README.flatten.md"

package_component "sandboxer" "sandboxer-$VERSION-linux-$ARCH.tar.gz" \
  --bins sandbox-ctl sandbox-init cloud-hypervisor \
  --docs \
    "sandboxer/README.md:sandboxer.md" \
    "sandboxer/docs/sandbox.md" \
    "sandboxer/docs/sandbox-init.md" \
    "sandboxer/docs/cloud-hypervisor.md"

package_component "connector" "connector-$VERSION-linux-$ARCH.tar.gz" \
  --bins connector-ctl \
  --docs \
    "connector/README.md:connector.md" \
    "connector/docs/vswitch.md" \
    "connector/docs/tapfd.md" \
  --tests \
    "connector/examples/geneve_eth_test.sh:connector/examples/geneve_eth_test.sh" \
    "connector/examples/geneve_ip_test.sh:connector/examples/geneve_ip_test.sh" \
    "connector/examples/manage_switch.sh:connector/examples/manage_switch.sh" \
    "connector/examples/mgmt_isolation_test.sh:connector/examples/mgmt_isolation_test.sh" \
    "connector/examples/tap_test.sh:connector/examples/tap_test.sh" \
    "connector/examples/perf_bench.sh:connector/examples/perf_bench.sh" \
    "connector/examples/provision_test.sh:connector/examples/provision_test.sh" \
    "connector/examples/start_perf_bench.sh:connector/examples/start_perf_bench.sh" \
  --deploy \
    "connector/dist/connector-vswitch.service" \
    "connector/dist/connector-switch.conf" \
    "connector/dist/NetworkManager-connector.conf"

package_runtime_bundle
package_vmlinux

if [ "${#missing[@]}" -gt 0 ]; then
  echo "release.sh: missing inputs:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  exit 1
fi

( cd "$DIST" && sha256sum \
    "orchestrator-$VERSION-linux-$ARCH.tar.gz" \
    "accelerator-$VERSION-linux-$ARCH.tar.gz" \
    "guest-runtime-$VERSION-linux-$ARCH.tar.gz" \
    "sandboxer-$VERSION-linux-$ARCH.tar.gz" \
    "connector-$VERSION-linux-$ARCH.tar.gz" \
    "sandbox-runtime-$ARCH-$VERSION.tar.gz" \
    "vmlinux-$ARCH-$VERSION.tar.gz" > "SHA256SUMS" )

echo "==> $DIST/SHA256SUMS"
echo "    release-$VERSION should upload these archives directly; no aggregate tarball is produced."
