#!/usr/bin/env bash

set -euo pipefail
umask 022

NAME=orchestrator
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'chmod -R u+w "$WORK"; rm -rf "$WORK"' EXIT
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
  local checkout="$WORK/go-build/orchestrator"
  [ -f "$checkout/$source" ] || fail "missing release input: $source"
  mkdir -p "$(dirname "$STAGE/$destination")"
  install -m 0644 "$checkout/$source" "$STAGE/$destination"
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

stage_release_go_source() {
  local source="$1" sha="$2" destination="$3"
  [ ! -e "$destination" ] || fail "fresh release checkout already exists"
  mkdir -p "$destination"
  local -a git_env=(env -i PATH="$PATH" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null)
  "${git_env[@]}" git -C "$destination" init --quiet --template=
  "${git_env[@]}" git -C "$destination" fetch --quiet --depth=1 "$source" "$sha"
  "${git_env[@]}" git -C "$destination" -c advice.detachedHead=false checkout --quiet --detach "$sha"
}

build_release_go_payloads() {
  local arch="$1" proxy="${GOPROXY:-https://proxy.golang.org,direct}" route variable value
  local sumdb="${GOSUMDB:-sum.golang.org}" sumdb_identity sumdb_url sumdb_extra
  local toolchain="${GOTOOLCHAIN:-local}"
  local -a routes build_env
  IFS=',|' read -r -a routes <<< "$proxy"
  for route in "${routes[@]}"; do
    case "$route" in direct|off) continue ;; esac
    [[ "$route" == https://?* && "$route" != *[@?#[:space:]]* ]] \
      || fail "release Go proxy routing must use credential-free HTTPS"
  done
  [[ "$sumdb" != *$'\n'* && "$sumdb" != *$'\r'* ]] \
    || fail "release checksum database routing must be a single line"
  read -r sumdb_identity sumdb_url sumdb_extra <<< "$sumdb"
  [[ "$sumdb_identity" =~ ^[A-Za-z0-9._+/:=-]+$ && -z "$sumdb_extra" ]] \
    || fail "invalid release checksum database identity"
  if [ -n "$sumdb_url" ]; then
    [[ "$sumdb_url" == https://?* && "$sumdb_url" != *[@?#[:space:]]* ]] \
      || fail "release checksum database routing must use credential-free HTTPS"
  fi
  [[ "$toolchain" =~ ^(local|auto|path|go[0-9]+\.[0-9]+(\.[0-9]+|beta[0-9]+|rc[0-9]+)?(\+(auto|path))?)$ ]] \
    || fail "invalid release Go toolchain selection"
  mkdir -p "$WORK/go-home" "$WORK/go-cache" "$WORK/go-mod"
  chmod 0700 "$WORK/go-home" "$WORK/go-cache" "$WORK/go-mod"
  build_env=(env -i PATH="$PATH" HOME="$WORK/go-home" LANG=C
    GOWORK=off GOENV=off GOFLAGS=-mod=readonly GOPROXY="$proxy" GOSUMDB="$sumdb" GOTOOLCHAIN="$toolchain"
    GOCACHE="$WORK/go-cache" GOMODCACHE="$WORK/go-mod"
    GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null)
  for variable in HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy \
    SSL_CERT_FILE SSL_CERT_DIR; do
    value="${!variable:-}"
    [ -n "$value" ] || continue
    case "$variable" in
      HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|http_proxy|https_proxy|all_proxy)
        [[ "$value" != *[@?#[:space:]]* ]] || fail "release build cannot pass an authenticated proxy"
        ;;
    esac
    build_env+=("$variable=$value")
  done
  RELEASE_MATERIALS_GO_ENV="$WORK/go-build-toolchain.json"
  "${build_env[@]}" go -C "$WORK/go-build/$NAME" env -json GOROOT GOVERSION GOHOSTOS GOHOSTARCH \
    > "$RELEASE_MATERIALS_GO_ENV"
  RELEASE_MATERIALS_WORK="$WORK/go-toolchain-before-build" \
    GOMODCACHE="$WORK/go-mod" GOPROXY="$proxy" GOSUMDB="$sumdb" \
    release_materials_verify_build_go "$RELEASE_MATERIALS_GO_ENV"
  "${build_env[@]}" make --no-print-directory -C "$WORK/go-build/$NAME" TARGET_ARCH="$arch" build
}

check_go_binary() {
  local file="$1" info name
  name="$(basename "$file")"
  case "$name" in node-ctl|cluster-ctl|node-stub-ctl|e2b-key-ctl) ;; *) fail "unexpected Go release payload: $name" ;; esac
  info="$(go version -m "$file" 2>/dev/null)" \
    || fail "Go build info is missing from $file"
  awk -F '\t' -v expected="github.com/kuasar-sandbox/orchestrator/cmd/$name" '
    $2 == "path" { paths++; if ($3 != expected) bad=1 }
    $2 == "mod" { modules++; if ($3 != "github.com/kuasar-sandbox/orchestrator") bad=1 }
    END { exit bad || paths != 1 || modules != 1 }
  ' <<< "$info" || fail "Go release payload must be the $name main package: $file"
  awk -F '\t' '
    $2 == "build" && $3 ~ /^GOOS=/ { os++; if ($3 != "GOOS=linux") bad=1 }
    $2 == "build" && $3 ~ /^GOARCH=/ { arch++; if ($3 != "GOARCH=amd64") bad=1 }
    $2 == "build" && $3 ~ /^CGO_ENABLED=/ { cgo++; if ($3 != "CGO_ENABLED=0") bad=1 }
    END { exit bad || os != 1 || arch != 1 || cgo != 1 }
  ' <<< "$info" || fail "Go release payload must target linux/amd64 with CGO_ENABLED=0: $file"
}

validate_copied_source_files() {
  local extract="$1" sha="$2" file
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "selected project source revision is missing"
  git -C "$ROOT" cat-file -e "$sha^{commit}" 2>/dev/null \
    || fail "selected source commit is unavailable; fetch that exact commit before validation"
  for file in deploy/node-ctl.service deploy/node-proxy.service \
    deploy/cluster-registry.service deploy/cluster-router.service \
    deploy/cluster-placer.service deploy/conductor.example.yaml \
    deploy/proxy.example.yaml deploy/registry.example.yaml \
    deploy/router.example.yaml deploy/placer.example.yaml; do
    [ -f "$extract/$file" ] || fail "archive is missing $file"
    git -C "$ROOT" cat-file blob "$sha:$file" | cmp -s - "$extract/$file" \
      || fail "release deployment bytes differ from selected source: $file"
  done
}

validate_archive_paths() {
  local archive="$1"
  go run "$ROOT/scripts/release-archive-validator.go" "$archive" \
    || fail "$archive contains an unsafe type, mode or ownership, or violates the exact entry contract"
}

requested_dependency_version() {
  local name="$1" binding="${RELEASE_DEPENDENCIES:-}" entry value result=""
  local -a entries
  [ -n "$binding" ] || return 0 # Local source packages can use untagged commits.
  [[ "$binding" != *, && "$binding" != ,* && "$binding" != *,,* ]] \
    || fail "invalid release dependency list"
  IFS=, read -r -a entries <<< "$binding"
  [ "${#entries[@]}" -eq 3 ] || fail "orchestrator release must bind its 3 internal dependencies"
  for entry in "${entries[@]}"; do
    case "${entry%%=*}" in accelerator|connector|sandboxer) ;; *) fail "unexpected orchestrator dependency" ;; esac
    value="${entry#*=}"
    [[ "$value" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?$ ]] \
      || fail "invalid orchestrator dependency version"
    if [ "${entry%%=*}" = "$name" ]; then
      [ -z "$result" ] || fail "duplicate orchestrator dependency: $name"
      result="$value"
    fi
  done
  [ -n "$result" ] || fail "missing orchestrator dependency: $name"
  printf '%s\n' "$result"
}

validate_dependency_source() {
  local extract="$1" name="$2" payload="$3" version="$4"
  local directory_variable="RELEASE_${name^^}_SOURCE_DIR" sha_variable="RELEASE_${name^^}_SOURCE_SHA"
  local source sha tagged
  source="${!directory_variable:-$ROOT/../$name}"
  sha="${!sha_variable:-}"
  [ -n "$sha" ] || sha="$(git -C "$source" rev-parse HEAD)" \
    || fail "cannot resolve selected $name dependency source"
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "$name dependency source must be an exact commit"
  if [ -n "$version" ]; then
    tagged="$(git -C "$source" rev-parse --verify "refs/tags/$version^{commit}")" \
      || fail "selected $name dependency release tag is unavailable"
    [ "$tagged" = "$sha" ] || fail "$name dependency source does not match the selected release tag"
  fi
  release_materials_require_source "$extract" "$NAME" "$payload" "$name" "$version" \
    "https://github.com/kuasar-sandbox/$name/commit/$sha" "git:$sha"
  release_materials_require_git_licenses "$extract" "$NAME" "$source" "$sha" "$name"
}

validate_source_record_keys() {
  # Other fields and uniqueness are authenticated by the existing required-row
  # checks. Do not accept extra attributions merely because those rows exist.
  awk -F '\t' '
    NR == 1 { next }
    $1 == "bin/*,deploy/*" && $2 == "orchestrator" { next }
    $1 == "bin/node-ctl,bin/cluster-ctl,bin/node-stub-ctl" && ($2 == "accelerator" || $2 == "sandboxer") { next }
    $1 == "bin/node-ctl" && $2 == "connector" { next }
    $1 ~ /^bin\/(node-ctl|cluster-ctl|node-stub-ctl|e2b-key-ctl)$/ && $2 == "Go toolchain" { next }
    { exit 1 }
  ' "$1/share/sources/orchestrator/SOURCES.tsv" \
    || fail "source inventory contains an undeclared payload attribution"
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
  local file project_sha
  for file in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
    [ -x "$extract/bin/$file" ] || fail "$archive is missing executable bin/$file"
    check_go_binary "$extract/bin/$file"
  done
  project_sha="$(go version -m "$extract/bin/node-ctl" | \
    awk -F '\t' '$2 == "build" && $3 ~ /^vcs.revision=/ {print substr($3, 14)}')"
  validate_copied_source_files "$extract" "$project_sha"
  release_materials_require_git_licenses "$extract" "$NAME" "$ROOT" "$project_sha" project
  release_materials_require_project_source "$extract" "$NAME" 'bin/*,deploy/*' "$version" \
    bin/node-ctl bin/cluster-ctl bin/node-stub-ctl bin/e2b-key-ctl
  local expected_accelerator expected_connector expected_sandboxer
  expected_accelerator="$(requested_dependency_version accelerator)" || fail "invalid accelerator release binding"
  expected_connector="$(requested_dependency_version connector)" || fail "invalid connector release binding"
  expected_sandboxer="$(requested_dependency_version sandboxer)" || fail "invalid sandboxer release binding"
  validate_dependency_source "$extract" accelerator 'bin/node-ctl,bin/cluster-ctl,bin/node-stub-ctl' "$expected_accelerator"
  validate_dependency_source "$extract" connector 'bin/node-ctl' "$expected_connector"
  validate_dependency_source "$extract" sandboxer 'bin/node-ctl,bin/cluster-ctl,bin/node-stub-ctl' "$expected_sandboxer"
  validate_source_record_keys "$extract"
  release_materials_validate "$extract" "$NAME"
  release_materials_require_go_key "$extract" "$NAME" 'bin/node-ctl'
  release_materials_require_go_key "$extract" "$NAME" 'bin/cluster-ctl'
  release_materials_require_go_key "$extract" "$NAME" 'bin/node-stub-ctl'
  release_materials_require_go_key "$extract" "$NAME" 'bin/e2b-key-ctl'
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
  [ -z "${RELEASE_BIN_DIR:-}" ] || fail "RELEASE_BIN_DIR is not supported: release Go payloads are rebuilt"

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
  local project_version
  project_version="$(release_materials_git_version "$ROOT" "$version" "$project_sha")"
  accelerator_sha="$(release_materials_resolve_git_source "$accelerator_source" \
    "${RELEASE_ACCELERATOR_SOURCE_SHA:-}" accelerator)"
  connector_sha="$(release_materials_resolve_git_source "$connector_source" \
    "${RELEASE_CONNECTOR_SOURCE_SHA:-}" connector)"
  sandboxer_sha="$(release_materials_resolve_git_source "$sandboxer_source" \
    "${RELEASE_SANDBOXER_SOURCE_SHA:-}" sandboxer)"
  stage_release_go_source "$ROOT" "$project_sha" "$WORK/go-build/orchestrator"
  stage_release_go_source "$accelerator_source" "$accelerator_sha" "$WORK/go-build/accelerator"
  stage_release_go_source "$connector_source" "$connector_sha" "$WORK/go-build/connector"
  stage_release_go_source "$sandboxer_source" "$sandboxer_sha" "$WORK/go-build/sandboxer"
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
  build_release_go_payloads "$arch"
  bin_dir="$WORK/go-build/orchestrator/bin/$arch"
  local binary
  for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
    copy_executable "$bin_dir/$binary" "bin/$binary"
    check_go_binary "$STAGE/bin/$binary"
    release_materials_require_go_revision "$STAGE/bin/$binary" "$project_sha"
  done
  accelerator_version="$(release_materials_git_version "$accelerator_source" "$accelerator_version" "$accelerator_sha")"
  connector_version="$(release_materials_git_version "$connector_source" "$connector_version" "$connector_sha")"
  sandboxer_version="$(release_materials_git_version "$sandboxer_source" "$sandboxer_version" "$sandboxer_sha")"
  release_materials_init "$STAGE" "$WORK/materials" "$NAME"
  release_materials_copy_licenses "$ROOT" project
  release_materials_copy_licenses "$accelerator_source" accelerator
  release_materials_copy_licenses "$connector_source" connector
  release_materials_copy_licenses "$sandboxer_source" sandboxer
  release_materials_record_source 'bin/*,deploy/*' orchestrator "$project_version" \
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
  GOMODCACHE="$WORK/go-mod" release_materials_finish

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
