#!/usr/bin/env bash

set -euo pipefail
umask 022

NAME=orchestrator
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

fail() {
  echo "release: $*" >&2
  exit 1
}

validate_version() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
    || fail "version must match vX.Y.Z or vX.Y.Z-preview.YYYYMMDD"
}

normalize_arch() {
  case "$1" in
    amd64|x86_64) printf 'x86_64\n' ;;
    *) fail "unsupported release architecture: $1; current release target is x86_64" ;;
  esac
}

archive_name() {
  local version="$1" arch
  validate_version "$version"
  arch="$(normalize_arch "$2")"
  printf '%s-%s-linux-%s.tar.gz\n' "$NAME" "$version" "$arch"
}

copy_file() {
  local source="$1" destination="$2"
  [ -f "$ROOT/$source" ] || fail "missing release input: $ROOT/$source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0644 "$ROOT/$source" "$STAGE/$destination"
}

copy_executable() {
  local source="$1" destination="$2"
  [ -x "$source" ] || fail "missing executable release input: $source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0755 "$source" "$STAGE/$destination"
}

copy_root_executable() {
  local source="$1" destination="$2"
  [ -x "$ROOT/$source" ] || fail "missing executable release input: $ROOT/$source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0755 "$ROOT/$source" "$STAGE/$destination"
}

check_go_binary() {
  local file="$1"
  go version -m "$file" >/dev/null 2>&1 \
    || fail "Go build info is missing from $file"
}

validate_archive_paths() {
  local archive="$1" listing="$WORK/listing"
  tar -tzf "$archive" > "$listing"
  awk '
    /^\// { exit 1 }
    { path=$0; sub(/^\.\//, "", path); if (path ~ /(^|\/)\.\.($|\/)/) exit 1 }
  ' "$listing" || fail "$archive contains an unsafe path"
  if grep -E '(^|/)release\.json$|(^|/)release/[^/]+\.json$' "$listing" >/dev/null; then
    fail "$archive contains release metadata JSON"
  fi
  awk '
    { path=$0; sub(/^\.\//, "", path) }
    path != "" && path !~ /\/$/ && path !~ /^(bin|deploy)\// && path !~ /^share\/(licenses|sources)\/orchestrator\// { exit 1 }
  ' "$listing" || fail "$archive contains a file outside the orchestrator release layout"
  tar -tvzf "$archive" | awk '$1 !~ /^[-d]/ { exit 1 }' \
    || fail "$archive contains a non-regular, non-directory entry"
}

validate_bundle() {
  [ "$#" -eq 3 ] || fail "usage: release.sh validate <version> <arch> <bundle-dir>"
  local version="$1" arch archive bundle="$3"
  arch="$(normalize_arch "$2")"
  archive="$(archive_name "$version" "$arch")"
  [ -s "$bundle/release-notes.md" ] || fail "release-notes.md is missing"
  [ -d "$bundle/assets" ] || fail "assets directory is missing"

  local expected="$WORK/expected-assets" actual="$WORK/actual-assets"
  printf '%s\n' "$archive" SHA256SUMS | LC_ALL=C sort > "$expected"
  find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" \
    || { diff -u "$expected" "$actual" >&2 || true; fail "bundle contains an unexpected asset set"; }
  [ "$(grep -cve '^[[:space:]]*$' "$bundle/assets/SHA256SUMS")" -eq 1 ] \
    || fail "SHA256SUMS must contain exactly one entry"
  local digest listed extra
  read -r digest listed extra < "$bundle/assets/SHA256SUMS"
  listed="${listed#\*}"
  if ! [[ "$digest" =~ ^[0-9a-f]{64}$ ]] \
    || [ "$listed" != "$archive" ] || [ -n "${extra:-}" ]; then
    fail "SHA256SUMS does not describe the expected archive"
  fi
  (cd "$bundle/assets" && sha256sum --quiet -c SHA256SUMS) \
    || fail "SHA256SUMS validation failed"

  validate_archive_paths "$bundle/assets/$archive"
  local extract="$WORK/extract"
  rm -rf "$extract"
  mkdir -p "$extract"
  tar -xzf "$bundle/assets/$archive" -C "$extract"
  release_materials_validate "$extract" "$NAME"
  local file
  for file in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
    [ -x "$extract/bin/$file" ] || fail "$archive is missing executable bin/$file"
    check_go_binary "$extract/bin/$file"
  done
  for file in deploy/node-ctl.service deploy/node-proxy.service \
    deploy/cluster-registry.service deploy/cluster-router.service \
    deploy/cluster-placer.service deploy/conductor.example.yaml \
    deploy/proxy.example.yaml deploy/registry.example.yaml \
    deploy/router.example.yaml deploy/placer.example.yaml; do
    [ -f "$extract/$file" ] || fail "$archive is missing $file"
  done
}

package_release() {
  [ "$#" -eq 3 ] || fail "usage: release.sh package <version> <arch> <output-dir>"
  local version="$1" arch output="$3" archive epoch bin_dir project_sha
  local accelerator_source connector_source sandboxer_source accelerator_version connector_version sandboxer_version
  local accelerator_sha connector_sha sandboxer_sha
  arch="$(normalize_arch "$2")"
  archive="$(archive_name "$version" "$arch")"
  if [ -z "$output" ] || [ "$output" = / ] || [ "$output" = . ]; then
    fail "unsafe output directory: $output"
  fi
  [ ! -e "$output" ] || fail "output already exists: $output"
  epoch="${SOURCE_DATE_EPOCH:-0}"
  [[ "$epoch" =~ ^[0-9]+$ ]] || fail "SOURCE_DATE_EPOCH must be an integer"

  STAGE="$WORK/stage"
  rm -rf "$STAGE"
  mkdir -p "$STAGE"
  bin_dir="${RELEASE_BIN_DIR:-$ROOT/bin/$arch}"
  copy_executable "$bin_dir/node-ctl" bin/node-ctl
  copy_executable "$bin_dir/cluster-ctl" bin/cluster-ctl
  copy_executable "$bin_dir/node-stub-ctl" bin/node-stub-ctl
  copy_executable "$bin_dir/e2b-key-ctl" bin/e2b-key-ctl
  check_go_binary "$STAGE/bin/node-ctl"
  check_go_binary "$STAGE/bin/cluster-ctl"
  check_go_binary "$STAGE/bin/node-stub-ctl"
  check_go_binary "$STAGE/bin/e2b-key-ctl"
  copy_file deploy/node-ctl.service deploy/node-ctl.service
  copy_file deploy/node-proxy.service deploy/node-proxy.service
  copy_file deploy/cluster-registry.service deploy/cluster-registry.service
  copy_file deploy/cluster-router.service deploy/cluster-router.service
  copy_file deploy/cluster-placer.service deploy/cluster-placer.service
  copy_file deploy/conductor.example.yaml deploy/conductor.example.yaml
  copy_file deploy/proxy.example.yaml deploy/proxy.example.yaml
  copy_file deploy/registry.example.yaml deploy/registry.example.yaml
  copy_file deploy/router.example.yaml deploy/router.example.yaml
  copy_file deploy/placer.example.yaml deploy/placer.example.yaml

  accelerator_source="${RELEASE_ACCELERATOR_SOURCE_DIR:-$ROOT/../accelerator}"
  connector_source="${RELEASE_CONNECTOR_SOURCE_DIR:-$ROOT/../connector}"
  sandboxer_source="${RELEASE_SANDBOXER_SOURCE_DIR:-$ROOT/../sandboxer}"
  accelerator_version="${RELEASE_ACCELERATOR_VERSION:-${ACCELERATOR_VERSION:-}}"
  connector_version="${RELEASE_CONNECTOR_VERSION:-${CONNECTOR_VERSION:-}}"
  sandboxer_version="${RELEASE_SANDBOXER_VERSION:-${SANDBOXER_VERSION:-}}"
  [[ "$accelerator_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?$ ]] \
    || fail "RELEASE_ACCELERATOR_VERSION must identify the selected accelerator release"
  [[ "$connector_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?$ ]] \
    || fail "RELEASE_CONNECTOR_VERSION must identify the selected connector release"
  [[ "$sandboxer_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?$ ]] \
    || fail "RELEASE_SANDBOXER_VERSION must identify the selected sandboxer release"
  project_sha="$(release_materials_resolve_git_source "$ROOT" "" orchestrator)"
  accelerator_sha="$(release_materials_resolve_git_source "$accelerator_source" \
    "${RELEASE_ACCELERATOR_SOURCE_SHA:-}" accelerator)"
  connector_sha="$(release_materials_resolve_git_source "$connector_source" \
    "${RELEASE_CONNECTOR_SOURCE_SHA:-}" connector)"
  sandboxer_sha="$(release_materials_resolve_git_source "$sandboxer_source" \
    "${RELEASE_SANDBOXER_SOURCE_SHA:-}" sandboxer)"
  release_materials_init "$STAGE" "$WORK/materials" "$NAME"
  release_materials_copy_licenses "$ROOT" project
  release_materials_copy_licenses "$accelerator_source" accelerator
  release_materials_copy_licenses "$connector_source" connector
  release_materials_copy_licenses "$sandboxer_source" sandboxer
  release_materials_record_source 'bin/*,deploy/*' orchestrator "$version" \
    "https://github.com/kuasar-sandbox/orchestrator/commit/$project_sha" \
    "git:$project_sha" project
  release_materials_record_source 'bin/node-ctl,bin/cluster-ctl,bin/node-stub-ctl' accelerator "$accelerator_version" \
    "https://github.com/kuasar-sandbox/accelerator/commit/$accelerator_sha" \
    "git:$accelerator_sha" accelerator
  release_materials_record_source bin/node-ctl connector "$connector_version" \
    "https://github.com/kuasar-sandbox/connector/commit/$connector_sha" \
    "git:$connector_sha" connector
  release_materials_record_source 'bin/node-ctl,bin/cluster-ctl,bin/node-stub-ctl' sandboxer "$sandboxer_version" \
    "https://github.com/kuasar-sandbox/sandboxer/commit/$sandboxer_sha" \
    "git:$sandboxer_sha" sandboxer
  release_materials_add_go_binary "$STAGE/bin/node-ctl" bin/node-ctl
  release_materials_add_go_binary "$STAGE/bin/cluster-ctl" bin/cluster-ctl
  release_materials_add_go_binary "$STAGE/bin/node-stub-ctl" bin/node-stub-ctl
  release_materials_add_go_binary "$STAGE/bin/e2b-key-ctl" bin/e2b-key-ctl
  release_materials_finish

  mkdir -p "$output/assets"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$epoch" \
    --pax-option=delete=atime,delete=ctime -czf "$output/assets/$archive" -C "$STAGE" .
  (cd "$output/assets" && sha256sum "$archive" > SHA256SUMS)
  cat > "$output/release-notes.md" <<EOF
$NAME $version for Linux $arch.

Extract the archive into a Kuasar Sandbox deployment root and verify it with \`SHA256SUMS\`. Documentation and E2E suites from this exact tag are collected by the aggregate platform release.
EOF
  validate_bundle "$version" "$arch" "$output"
  echo "==> prepared $output for $version"
}

command -v go >/dev/null || fail "go is required"
case "${1:-}" in
  archive-name) shift; [ "$#" -eq 2 ] || fail "usage: release.sh archive-name <version> <arch>"; archive_name "$@" ;;
  package) shift; package_release "$@" ;;
  validate) shift; validate_bundle "$@" ;;
  *) fail "usage: release.sh <archive-name|package|validate> ..." ;;
esac
