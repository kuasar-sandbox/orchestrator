#!/usr/bin/env bash

set -euo pipefail

# Match the release workflow's checksum mirror and local-only Go policy.
export GOSUMDB=sum.golang.google.cn GOTOOLCHAIN=local

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "test-release: $*" >&2
  exit 1
}

# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

# Reject credential-bearing routing before starting any build subprocess.
# shellcheck disable=SC1090
source <(sed -n '/^build_release_go_payloads() {/,/^}/p' "$ROOT/scripts/release.sh")
for invalid_sumdb in \
  'sum.golang.org https://fixture:fixture@sum.example.invalid' \
  'sum.golang.org https://sum.example.invalid?fixture=value' \
  'sum.golang.org https://sum.example.invalid extra'; do
  if (GOSUMDB="$invalid_sumdb" build_release_go_payloads x86_64 > "$TMP/invalid-sumdb.log" 2>&1); then
    fail "Go build environment accepted unsafe checksum routing"
  fi
  grep -Eq 'release checksum database|invalid release checksum database' "$TMP/invalid-sumdb.log" \
    || fail "unsafe checksum routing failed for an unrelated reason"
done
if (GOTOOLCHAIN='local invalid' build_release_go_payloads x86_64 > "$TMP/invalid-toolchain.log" 2>&1); then
  fail "Go build environment accepted malformed toolchain selection"
fi
grep -Fq 'invalid release Go toolchain selection' "$TMP/invalid-toolchain.log" \
  || fail "invalid toolchain selection failed for an unrelated reason"

export FIXTURE_GO_DISTRIBUTION_CACHE
FIXTURE_GO_DISTRIBUTION_CACHE="$(go env GOMODCACHE)"
bash "$ROOT/scripts/test-release-materials.sh"
bash "$ROOT/scripts/test-release-license-traversal.sh"
GOWORK=off go test -race "$ROOT/scripts/release-go-toolchain.go" "$ROOT/scripts/release-go-toolchain_test.go"

init_fixture_repo() {
  local directory="$1"
  shift
  git -C "$directory" init -q
  git -C "$directory" config --local user.name "Chen Xiaohui"
  git -C "$directory" config --local user.email "graych@gmail.com"
  git -C "$directory" add -- "$@"
  git -C "$directory" commit -q -m "test: create release source fixture"
  git -C "$directory" rev-parse HEAD
}

mkdir -p "$TMP/git-source" \
  "$TMP/material-hash/share/licenses/hash-test/LICENSES" \
  "$TMP/material-hash/share/sources/hash-test"
printf 'fixture license\n' > "$TMP/git-source/LICENSE"
fixture_git_sha="$(init_fixture_repo "$TMP/git-source" LICENSE)"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = "git:$fixture_git_sha" ] \
  || fail "untagged source was recorded as a component release"
git -C "$TMP/git-source" tag v1.2.3 "$fixture_git_sha"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = v1.2.3 ] \
  || fail "matching source tag was not retained"
if (release_materials_git_version "$TMP/git-source" v1.2.3 \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "source version resolver accepted a tag for another commit"
fi
[ "$(release_materials_resolve_git_source "$TMP/git-source" "$fixture_git_sha" fixture)" = "$fixture_git_sha" ] \
  || fail "clean source worktree did not resolve to its selected commit"
if (release_materials_resolve_git_source "$TMP/git-source" \
  0000000000000000000000000000000000000000 fixture >/dev/null 2>&1); then
  fail "source resolver accepted a commit that differs from the selected commit"
fi
printf 'untracked source\n' > "$TMP/git-source/untracked.go"
if (release_materials_resolve_git_source "$TMP/git-source" "" fixture >/dev/null 2>&1); then
  fail "source resolver accepted a dirty source worktree"
fi
printf 'LICENSE.generated\n' > "$TMP/git-source/.git/info/exclude"
printf 'ignored material\n' > "$TMP/git-source/LICENSE.generated"
if (
  release_materials_init "$TMP/ignored-material/stage" "$TMP/ignored-material/work" fixture
  release_materials_copy_licenses "$TMP/git-source" project >/dev/null 2>&1
); then
  fail "license collection accepted material absent from the selected commit"
fi
printf 'nested license manifest\n' \
  > "$TMP/material-hash/share/licenses/hash-test/LICENSES/MATERIALS.sha256"
printf 'generated inventory\n' \
  > "$TMP/material-hash/share/sources/hash-test/MATERIALS.sha256"
release_materials_hash_tree "$TMP/material-hash" hash-test "$TMP/material-hash-actual"
grep -Fq 'share/licenses/hash-test/LICENSES/MATERIALS.sha256' "$TMP/material-hash-actual" \
  || fail "license file named MATERIALS.sha256 was omitted from the material inventory"
if grep -Fq 'share/sources/hash-test/MATERIALS.sha256' "$TMP/material-hash-actual"; then
  fail "generated material inventory included itself"
fi

material_root="$TMP/material-validation"
material_unit=validation
mkdir -p "$material_root/bin"
printf 'payload\n' > "$material_root/bin/tool"
mkdir -p "$material_root/share/licenses/$material_unit/project" \
  "$material_root/share/sources/$material_unit" "$TMP/material-validation-work"
printf 'fixture license\n' > "$material_root/share/licenses/$material_unit/project/LICENSE"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\tshare/licenses/%s/project\n' \
    "$material_unit"
} > "$material_root/share/sources/$material_unit/SOURCES.tsv"
printf 'payload\trecord\tname\tversion_or_value\tchecksum\n' \
  > "$material_root/share/sources/$material_unit/GO-BUILD-INFO.tsv"
printf 'module\tversion\tchecksum\n' \
  > "$material_root/share/sources/$material_unit/GO-MODULES.tsv"
release_materials_hash_tree "$material_root" "$material_unit" \
  "$material_root/share/sources/$material_unit/MATERIALS.sha256"
find "$material_root/share" -type d -exec chmod 0755 {} +
find "$material_root/share" -type f -exec chmod 0644 {} +
(
  WORK="$TMP/material-validation-work"
  release_materials_validate "$material_root" "$material_unit"
)

cp -a "$material_root" "$TMP/material-unsafe-license-path"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\t../../../etc\n'
} > "$TMP/material-unsafe-license-path/share/sources/$material_unit/SOURCES.tsv"
release_materials_hash_tree "$TMP/material-unsafe-license-path" "$material_unit" \
  "$TMP/material-unsafe-license-path/share/sources/$material_unit/MATERIALS.sha256"
chmod 0644 "$TMP/material-unsafe-license-path/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-unsafe-license-path" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a license directory outside its unit"
fi

cp -a "$material_root" "$TMP/material-invalid-record"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\n'
} > "$TMP/material-invalid-record/share/sources/$material_unit/SOURCES.tsv"
release_materials_hash_tree "$TMP/material-invalid-record" "$material_unit" \
  "$TMP/material-invalid-record/share/sources/$material_unit/MATERIALS.sha256"
chmod 0644 "$TMP/material-invalid-record/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-invalid-record" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a SOURCES.tsv row with fewer than six fields"
fi

cp -a "$material_root" "$TMP/material-unsafe-parent-mode"
chmod 0777 "$TMP/material-unsafe-parent-mode/share"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-unsafe-parent-mode" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted an unsafe parent directory mode"
fi

cp -a "$material_root" "$TMP/material-empty-license"
rm "$TMP/material-empty-license/share/licenses/$material_unit/project/LICENSE"
release_materials_hash_tree "$TMP/material-empty-license" "$material_unit" \
  "$TMP/material-empty-license/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-empty-license" "$material_unit" >/dev/null 2>&1
); then
  fail "release material validator accepted an empty license directory"
fi
if (
  release_materials_require_source "$material_root" "$material_unit" bin/tool fixture v2.0.0 \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a different release version"
fi
if (
  release_materials_require_go "$material_root" "$material_unit" bin/tool >/dev/null 2>&1
); then
  fail "release material validator accepted missing Go build records"
fi
cp -a "$material_root" "$TMP/material-missing-payload"
rm "$TMP/material-missing-payload/bin/tool"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-missing-payload" "$material_unit" >/dev/null 2>&1
); then
  fail "release material validator accepted a record for an unshipped payload"
fi

bash "$ROOT/scripts/test-preview-line.sh"
bash "$ROOT/scripts/test-delete-preview.sh"

mkdir -p "$TMP/source-bin"
cat > "$TMP/source-bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = api ] || exit 2
[ "${2:-}" = "repos/$GITHUB_REPOSITORY/git/ref/heads/${FAKE_SOURCE_REF:?}" ] || exit 2
printf '%s\n' "${FAKE_SOURCE_SHA:?}"
EOF
chmod +x "$TMP/source-bin/gh"
env PATH="$TMP/source-bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/orchestrator FAKE_SOURCE_REF=release/v1.2.x FAKE_SOURCE_SHA=1111111111111111111111111111111111111111 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 orchestrator >/dev/null
if env PATH="$TMP/source-bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/orchestrator FAKE_SOURCE_REF=release/v1.2.x FAKE_SOURCE_SHA=1111111111111111111111111111111111111111 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.3.0 orchestrator >/dev/null 2>&1; then
  fail "release source validator accepted a tag from another version line"
fi
bash -n "$ROOT/scripts/delete-preview.sh" "$ROOT/scripts/validate-release-source.sh"

WORKFLOW="$ROOT/.github/workflows/component-release.yml"
grep -Fqx 'run-name: Release ${{ inputs.version }} @${{ inputs.source_sha }} [accelerator=${{ inputs.accelerator_version }},connector=${{ inputs.connector_version }},sandboxer=${{ inputs.sandboxer_version }}]' \
  "$WORKFLOW" || fail "release run identity does not pin source and dependencies"
grep -Fq 'RELEASE_DEPENDENCIES: accelerator=${{ needs.preflight.outputs.accelerator_version }},connector=${{ needs.preflight.outputs.connector_version }},sandboxer=${{ needs.preflight.outputs.sandboxer_version }}' \
  "$WORKFLOW" || fail "Preview publisher does not receive dependency binding"
workflow="$ROOT/.github/workflows/component-release.yml"
for job in build publish; do
  for routing in 'GOPROXY: https://goproxy.cn,direct' 'GOSUMDB: sum.golang.google.cn' 'GOTOOLCHAIN: local'; do
    awk -v job="$job" '
      $0 == "  " job ":" { inside=1; next }
      inside && /^  [A-Za-z0-9_-]+:/ { exit }
      inside && /^    steps:/ { exit }
      inside { print }
    ' "$workflow" | grep -Fx "      $routing" >/dev/null \
      || fail "$workflow $job is missing the verified Go routing policy: $routing"
  done
done
[ "$(grep -Fc 'archive_sha256: ${{ steps.release-archive-digest.outputs.archive_sha256 }}' \
  "$workflow")" -eq 1 ] \
  || fail "$workflow does not expose exactly one independent build archive digest"
[ "$(grep -Fc 'RELEASE_ARCHIVE_SHA256: ${{ needs.build.outputs.archive_sha256 }}' \
  "$workflow")" -eq 1 ] \
  || fail "$workflow does not pass the independent build digest to publication"
grep -Fq 'kuasar-preview-binding' "$ROOT/scripts/publish-release.sh" \
  || fail "Preview publisher does not record its build binding"
for workflow in component-release.yml delete-preview.yml; do
  [ "$(grep -Fc 'group: component-mutation-${{ github.repository }}-${{ inputs.version }}' \
    "$ROOT/.github/workflows/$workflow")" -eq 1 ] \
    || fail "$workflow does not hold exactly one full-workflow mutation lock"
done
grep -Fq 'kuasar-release-source' "$ROOT/scripts/publish-release.sh" \
  || fail "publisher does not record Stable source provenance"
grep -Fq 'reconcile_main_latest' "$ROOT/scripts/publish-release.sh" \
  || fail "publisher does not reconcile component main Latest by source commit"
RECONCILE_WORKFLOW="$ROOT/.github/workflows/reconcile-latest.yml"
grep -Fq 'group: component-latest-reconciliation-${{ github.repository }}' \
  "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation is not serialized across component versions"
grep -Fq 'workflow_run:' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation is not triggered after release completion"
grep -Fq 'schedule:' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation has no automatic recovery schedule"
grep -Fq 'publish-release.sh reconcile' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation does not use the idempotent entrypoint"
if grep -R -Fq 'queue: max' "$ROOT/.github/workflows"; then
  fail "workflows use the unsupported concurrency queue key"
fi
for input in accelerator_version connector_version sandboxer_version; do
  grep -Fq "      $input:" "$WORKFLOW" \
    || fail "release workflow is missing required $input input"
  [ "$(grep -Fc "ref: \${{ needs.preflight.outputs.$input }}" "$WORKFLOW")" -eq 2 ] \
    || fail "release workflow does not pin both $input checkouts"
done
grep -Fq "repos/kuasar-sandbox/\$repository/releases/tags/\$version" "$WORKFLOW" \
  || fail "release workflow does not verify dependency releases"

for entrypoint in test/e2e/e2e_cluster_real.sh test/e2e/e2e_cluster_stub.sh \
  test/e2e/e2e_density.sh test/e2e/e2e_execute.sh \
  test/e2e/e2e_orchestrator.sh test/e2e/e2e_orchestrator_proxy.sh \
  test/e2e/e2e_run_builder.sh test/e2e/e2e_runtask.sh \
  test/e2e/e2e_sandbox_cold_target.sh test/e2e/run_all.sh; do
  [ "$(git -C "$ROOT" ls-files -s -- "$entrypoint" | awk '{print $1}')" = 100755 ] \
    || fail "$entrypoint is not executable in the Git index"
done

PROXY_E2E="$ROOT/test/e2e/e2e_orchestrator_proxy.sh"
grep -Fq 'CUSTOM_PROXY_BIN="-"' "$PROXY_E2E" \
  || fail "exact-assets Proxy E2E does not select the built-in App"
if grep -Fq 'CUSTOM_PROXY_BIN="$BIN/node-ctl"' "$PROXY_E2E"; then
  fail "exact-assets Proxy E2E configures node-ctl as its own custom executable"
fi
# shellcheck source=test/e2e/lib/proxy.sh
. "$ROOT/test/e2e/lib/proxy.sh"
write_proxy_config "$TMP/built-in-proxy.yaml" \
  /run/test/node-ctl.socket /run/test 127.0.0.1:3443 \
  - /run/test/proxy-stats.sock /run/test/proxy-routes.shm \
  16 1 enforce 30s - - - -
if grep -Fq 'proxy_executable:' "$TMP/built-in-proxy.yaml"; then
  fail "built-in Proxy sentinel emitted paths.proxy_executable"
fi

mkdir -p "$TMP/bin" "$TMP/src" "$TMP/accelerator" "$TMP/connector" "$TMP/sandboxer"
printf 'fixture accelerator license\n' > "$TMP/accelerator/LICENSE"
printf 'fixture connector license\n' > "$TMP/connector/LICENSE"
printf 'fixture sandboxer license\n' > "$TMP/sandboxer/LICENSE"
accelerator_sha="$(init_fixture_repo "$TMP/accelerator" LICENSE)"
connector_sha="$(init_fixture_repo "$TMP/connector" LICENSE)"
sandboxer_sha="$(init_fixture_repo "$TMP/sandboxer" LICENSE)"
git -C "$TMP/accelerator" tag v0.1.3 "$accelerator_sha"
git -C "$TMP/connector" tag v0.1.2 "$connector_sha"
git -C "$TMP/sandboxer" tag v0.1.3 "$sandboxer_sha"
fixture_root="$TMP/project"
mkdir -p "$fixture_root/scripts"
install -m 0644 "$ROOT/LICENSE" "$fixture_root/LICENSE"
printf '/bin/\n/build/\n' > "$fixture_root/.gitignore"
install -m 0755 "$ROOT/scripts/release.sh" "$fixture_root/scripts/release.sh"
install -m 0755 "$ROOT/scripts/publish-release.sh" "$fixture_root/scripts/publish-release.sh"
install -m 0755 "$ROOT/scripts/release-materials.sh" "$fixture_root/scripts/release-materials.sh"
install -m 0644 "$ROOT/scripts/release-go-toolchain.go" "$fixture_root/scripts/release-go-toolchain.go"
cat >> "$fixture_root/scripts/release-materials.sh" <<'EOF'
release_materials_download_go_toolchain() {
  GOMODCACHE="${FIXTURE_GO_DISTRIBUTION_CACHE:?}" _release_materials_download_go_toolchain "$@"
}
EOF
printf 'module github.com/kuasar-sandbox/orchestrator\n\ngo 1.24\n' > "$fixture_root/go.mod"
for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
  mkdir -p "$fixture_root/cmd/$binary"
  printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/$binary/main.go"
done
mkdir -p "$fixture_root/."
cp -a "$ROOT/deploy" "$fixture_root/deploy"
printf 'accelerator_commit := %s\nconnector_commit := %s\nsandboxer_commit := %s\n' \
  "$accelerator_sha" "$connector_sha" "$sandboxer_sha" > "$fixture_root/Makefile"
cat >> "$fixture_root/Makefile" <<'EOF'
.PHONY: build
build:
	test "$$GOWORK" = off && test "$$GOFLAGS" = -mod=readonly
	test "$$GOSUMDB" = sum.golang.google.cn && test "$$GOTOOLCHAIN" = local
	test -z "$${GH_TOKEN:-}" && test -z "$${AWS_SECRET_ACCESS_KEY:-}"
	test ! -e ignored-release-input.go
	test ! -e ../accelerator/ignored-release-input.go
	test ! -e ../connector/ignored-release-input.go
	test ! -e ../sandboxer/ignored-release-input.go
	test "$$(git -C ../accelerator rev-parse HEAD)" = "$(accelerator_commit)"
	test "$$(git -C ../connector rev-parse HEAD)" = "$(connector_commit)"
	test "$$(git -C ../sandboxer rev-parse HEAD)" = "$(sandboxer_commit)"
	mkdir -p bin/x86_64
	CGO_ENABLED=0 go build -trimpath -buildvcs=true -o bin/x86_64/node-ctl ./cmd/node-ctl
	CGO_ENABLED=0 go build -trimpath -buildvcs=true -o bin/x86_64/cluster-ctl ./cmd/cluster-ctl
	CGO_ENABLED=0 go build -trimpath -buildvcs=true -o bin/x86_64/node-stub-ctl ./cmd/node-stub-ctl
	CGO_ENABLED=0 go build -trimpath -buildvcs=true -o bin/x86_64/e2b-key-ctl ./cmd/e2b-key-ctl
EOF
fixture_project_sha="$(init_fixture_repo "$fixture_root" LICENSE .gitignore scripts go.mod cmd deploy Makefile)"
(cd "$fixture_root" && GOWORK=off go build -buildvcs=true -o "$TMP/go-fixture" ./cmd/node-ctl)
release_materials_require_go_revision "$TMP/go-fixture" "$fixture_project_sha"
printf '// dirty fixture\n' >> "$fixture_root/cmd/node-ctl/main.go"
(cd "$fixture_root" && GOWORK=off go build -buildvcs=true -o "$TMP/dirty-go-fixture" ./cmd/node-ctl)
if (release_materials_require_go_revision "$TMP/dirty-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary built from dirty source"
fi
printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/node-ctl/main.go"
if (release_materials_require_go_revision "$TMP/go-fixture" \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "release accepted a binary built from another commit"
fi
GO111MODULE=off go build -o "$TMP/unstamped-go-fixture" "$fixture_root/cmd/node-ctl/main.go"
if (release_materials_require_go_revision "$TMP/unstamped-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary without source stamping"
fi
for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
  install -m 0755 "$TMP/go-fixture" "$TMP/bin/$binary"
done
for source in "$fixture_root" "$TMP/accelerator" "$TMP/connector" "$TMP/sandboxer"; do
  printf 'ignored-release-input.go\n' >> "$source/.git/info/exclude"
  printf 'ignored invalid Go input must not enter the release build\n' > "$source/ignored-release-input.go"
done
if RELEASE_BIN_DIR="$TMP/bin" "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 \
  "$TMP/prebuilt-override" > "$TMP/prebuilt-override.log" 2>&1; then
  fail "packager accepted a prebuilt payload override"
fi
grep -Fq 'RELEASE_BIN_DIR is not supported' "$TMP/prebuilt-override.log" \
  || fail "prebuilt override failed for an unrelated reason"
SOURCE_DATE_EPOCH=1700000000 GH_TOKEN=fixture-private AWS_SECRET_ACCESS_KEY=fixture-private \
  RELEASE_ACCELERATOR_SOURCE_DIR="$TMP/accelerator" \
  RELEASE_ACCELERATOR_SOURCE_SHA="$accelerator_sha" \
  RELEASE_ACCELERATOR_VERSION=v0.1.3 \
  RELEASE_CONNECTOR_SOURCE_DIR="$TMP/connector" \
  RELEASE_CONNECTOR_SOURCE_SHA="$connector_sha" \
  RELEASE_CONNECTOR_VERSION=v0.1.2 \
  RELEASE_SANDBOXER_SOURCE_DIR="$TMP/sandboxer" \
  RELEASE_SANDBOXER_SOURCE_SHA="$sandboxer_sha" \
  RELEASE_SANDBOXER_VERSION=v0.1.3 \
  "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/bundle"
"$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/orchestrator v1.2.3 \
  "$fixture_project_sha" main
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/orchestrator v1.2.3 \
  "$fixture_project_sha" release/v1.2.x

archive="$TMP/bundle/assets/orchestrator-v1.2.3-linux-x86_64.tar.gz"
RELEASE_DEPENDENCIES=accelerator=v0.1.3,connector=v0.1.2,sandboxer=v0.1.3 \
  "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
for binding in accelerator=v9.0.0,connector=v0.1.2,sandboxer=v0.1.3 \
  accelerator=v0.1.3,connector=v9.0.0,sandboxer=v0.1.3 \
  accelerator=v0.1.3,connector=v0.1.2,sandboxer=v9.0.0 \
  accelerator=v0.1.3,connector=v0.1.2 \
  accelerator=v0.1.3,accelerator=v0.1.3,sandboxer=v0.1.3 \
  accelerator=v0.1.3,connector=v0.1.2,unexpected=v0.1.3 \
  'accelerator=v0.1.3,connector=v0.1.2,sandboxer=v0.1.3,'; do
  if RELEASE_DEPENDENCIES="$binding" "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 \
    "$TMP/bundle" > "$TMP/dependency-binding.log" 2>&1; then
    fail "validator accepted a conflicting or malformed dependency release request"
  fi
  grep -Eq 'source record|release binding|dependency|dependencies' "$TMP/dependency-binding.log" \
    || fail "dependency binding was rejected for an unrelated reason"
done
go_toolchain="$(go version | awk '{print $3}')"
for path in ./bin/node-ctl ./bin/cluster-ctl ./bin/node-stub-ctl \
  ./bin/e2b-key-ctl ./deploy/node-ctl.service \
  ./share/licenses/orchestrator/accelerator/LICENSE \
  ./share/licenses/orchestrator/connector/LICENSE \
  ./share/licenses/orchestrator/project/LICENSE \
  ./share/licenses/orchestrator/sandboxer/LICENSE \
  ./share/licenses/orchestrator/go-toolchain/"$go_toolchain"/LICENSE \
  ./share/sources/orchestrator/SOURCES.tsv \
  ./share/sources/orchestrator/GO-BUILD-INFO.tsv \
  ./share/sources/orchestrator/GO-MODULES.tsv \
  ./share/sources/orchestrator/MATERIALS.sha256; do
  tar -tzf "$archive" | grep -Fx "$path" >/dev/null || fail "archive is missing $path"
done
tar -xOf "$archive" ./share/sources/orchestrator/SOURCES.tsv \
  | grep -Fq $'\tGo toolchain\t'"$go_toolchain"$'\t' \
  || fail "archive does not associate its Go toolchain with license material"
if tar -tzf "$archive" | grep -E '^\./(docs|test/e2e|test/orchestrator)(/|$)' >/dev/null; then
  fail "component archive contains documentation or E2E sources"
fi
if tar -tzf "$archive" | grep -E '(^|/)release\.json$|(^|/)release/[^/]+\.json$' >/dev/null; then
  fail "archive contains release metadata JSON"
fi

SOURCE_DATE_EPOCH=1700000000 \
  RELEASE_ACCELERATOR_SOURCE_DIR="$TMP/accelerator" \
  RELEASE_ACCELERATOR_SOURCE_SHA="$accelerator_sha" \
  RELEASE_ACCELERATOR_VERSION=v0.1.3 \
  RELEASE_CONNECTOR_SOURCE_DIR="$TMP/connector" \
  RELEASE_CONNECTOR_SOURCE_SHA="$connector_sha" \
  RELEASE_CONNECTOR_VERSION=v0.1.2 \
  RELEASE_SANDBOXER_SOURCE_DIR="$TMP/sandboxer" \
  RELEASE_SANDBOXER_SOURCE_SHA="$sandboxer_sha" \
  RELEASE_SANDBOXER_VERSION=v0.1.3 \
  "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/reproducible"
cmp -s "$archive" "$TMP/reproducible/assets/orchestrator-v1.2.3-linux-x86_64.tar.gz" \
  || fail "identical inputs did not produce an identical archive"

# Refresh actual binary metadata and both local inventories, so rejections below
# prove payload/source identity rather than relying on stale bundle checksums.
repack_candidate() (
  local candidate="$1" binary
  release_materials_init "$candidate/metadata-stage" "$candidate/materials" orchestrator
  for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
    release_materials_add_go_binary "$candidate/root/bin/$binary" "bin/$binary"
  done
  [ ! -s "$candidate/materials/go-modules" ] || fail "unexpected fixture dependency"
  {
    printf 'payload\trecord\tname\tversion_or_value\tchecksum\n'
    LC_ALL=C sort -u "$candidate/materials/go-build-info"
  } > "$candidate/root/share/sources/orchestrator/GO-BUILD-INFO.tsv"
  release_materials_hash_tree "$candidate/root" orchestrator \
    "$candidate/root/share/sources/orchestrator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
)

git clone --quiet --no-local "$fixture_root" "$TMP/target-source"
for target in darwin/amd64 linux/arm64; do
  (cd "$TMP/target-source" && GOWORK=off CGO_ENABLED=0 GOOS="${target%/*}" GOARCH="${target#*/}" \
    go build -trimpath -buildvcs=true -o "$TMP/target-${target//\//-}" ./cmd/node-ctl)
  release_materials_require_go_revision "$TMP/target-${target//\//-}" "$fixture_project_sha"
  candidate="$TMP/wrong-target-${target//\//-}"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  install -m 0755 "$TMP/target-${target//\//-}" "$candidate/root/bin/node-ctl"
  repack_candidate "$candidate"
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted a $target payload with regenerated metadata and checksums"
  fi
  grep -Fq 'must target linux/amd64' "$candidate/result.log" || fail "$target failed for an unrelated reason"
done

for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl command-line-arguments; do
  candidate="$TMP/wrong-main-$binary"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$binary" in
    node-ctl) other=cluster-ctl ;;
    cluster-ctl) other=node-stub-ctl ;;
    node-stub-ctl) other=e2b-key-ctl ;;
    e2b-key-ctl) other=node-ctl ;;
    command-line-arguments)
      (cd "$TMP/target-source" && GOWORK=off CGO_ENABLED=0 \
        go build -o "$TMP/command-line-tool" ./cmd/node-ctl/main.go)
      install -m 0755 "$TMP/command-line-tool" "$candidate/root/bin/node-ctl"
      binary=node-ctl
      other=''
      ;;
  esac
  if [ -n "$other" ]; then
    release_materials_require_go_revision "$candidate/root/bin/$other" "$fixture_project_sha"
    cp "$candidate/root/bin/$other" "$candidate/root/bin/$binary"
  fi
  repack_candidate "$candidate"
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted a swapped main package with regenerated metadata and checksums"
  fi
  grep -Fq "must be the $binary main package" "$candidate/result.log" \
    || fail "wrong main package failed for an unrelated reason"
done

for copied_file in deploy/node-ctl.service deploy/node-proxy.service \
  deploy/cluster-registry.service deploy/cluster-router.service \
  deploy/cluster-placer.service deploy/conductor.example.yaml \
  deploy/proxy.example.yaml deploy/registry.example.yaml \
  deploy/router.example.yaml deploy/placer.example.yaml; do
  candidate="$TMP/changed-source-${copied_file//\//-}"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  printf '\n# fixture modified after source selection\n' >> "$candidate/root/$copied_file"
  repack_candidate "$candidate"
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted changed deployment bytes with regenerated checksums: $copied_file"
  fi
  grep -Fq 'release deployment bytes differ from selected source' "$candidate/result.log" \
    || fail "changed deployment failed for an unrelated reason: $copied_file"
done
printf 'test-release: 4 swapped CLIs, command-line main, 2 targets and 10 deployment mutations rejected\n'

# The archive name is the requested release target; an untagged source record
# identifies the actual commit and does not pretend that target tag exists.
tar -xOf "$archive" ./share/sources/orchestrator/SOURCES.tsv | \
  awk -F '\t' -v sha="$fixture_project_sha" \
    '$2 == "orchestrator" && $3 == "git:" sha {found=1} END {exit !found}' \
  || fail "pre-tag project source was recorded as an existing release"
for column in 3 4 5; do
  candidate="$TMP/project-source-$column"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  inventory="$candidate/root/share/sources/orchestrator/SOURCES.tsv"
  awk -F '\t' -v OFS='\t' -v column="$column" \
    '$2 == "orchestrator" {$column="not-the-selected-source"} {print}' \
    "$inventory" > "$candidate/changed.tsv"
  mv "$candidate/changed.tsv" "$inventory"
  release_materials_hash_tree "$candidate/root" orchestrator \
    "$candidate/root/share/sources/orchestrator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" >/dev/null 2>&1; then
    fail "validator accepted project provenance column $column with regenerated checksums"
  fi
done

cp -a "$TMP/bundle" "$TMP/tampered"
printf 'tampered\n' >> "$TMP/tampered/assets/orchestrator-v1.2.3-linux-x86_64.tar.gz"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/tampered" >/dev/null 2>&1; then
  fail "validator accepted a tampered archive"
fi

cp -a "$TMP/bundle" "$TMP/extra"
touch "$TMP/extra/assets/release.json"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/extra" >/dev/null 2>&1; then
  fail "validator accepted an extra asset"
fi

if RELEASE_BIN_DIR="$TMP/bin" "$fixture_root/scripts/release.sh" package 01.2.3 x86_64 \
  "$TMP/invalid-version" >/dev/null 2>&1; then
  fail "packager accepted an invalid version"
fi
if RELEASE_BIN_DIR="$TMP/bin" "$fixture_root/scripts/release.sh" package v1.2.3 aarch64 \
  "$TMP/invalid-arch" >/dev/null 2>&1; then
  fail "packager accepted an unvalidated release architecture"
fi

for mutation in setuid setgid writable-directory writable-binary; do
  candidate="$TMP/unsafe-mode-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$mutation" in
    setuid) chmod 4755 "$candidate/root/bin/node-ctl" ;;
    setgid) chmod 2755 "$candidate/root/bin/node-ctl" ;;
    writable-directory) chmod 0777 "$candidate/root/bin" ;;
    writable-binary) chmod 0777 "$candidate/root/bin/node-ctl" ;;
  esac
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated checksums"
  fi
  grep -Fq 'unsafe type, mode or ownership' "$candidate/result.log" \
    || fail "$mutation was rejected for an unrelated reason"
done

cp -a "$TMP/bundle" "$TMP/nonroot-owner"
mkdir "$TMP/nonroot-owner/root"
tar -xzf "$archive" -C "$TMP/nonroot-owner/root"
tar --sort=name --owner=1234 --group=0 --numeric-owner --mtime=@1700000000 \
  -czf "$TMP/nonroot-owner/assets/$(basename "$archive")" -C "$TMP/nonroot-owner/root" .
(cd "$TMP/nonroot-owner/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/nonroot-owner" >/dev/null 2>&1; then
  fail "validator accepted non-root numeric ownership with regenerated checksums"
fi

echo "test-release: PASS"
