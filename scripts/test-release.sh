#!/usr/bin/env bash

set -euo pipefail

# This suite compiles private synthetic modules; product source gates select
# their own verified workspace outside these standalone packaging fixtures.
export GOWORK=off

# Archive fixtures target x86_64 even when this gate runs on an ARM host.
# Validators run natively; payload fixtures are inspected, never executed.
# Archive fixtures prescribe 0755 directories regardless of the caller's umask.
# mktemp still creates the enclosing workspace with mode 0700.
umask 022

# Keep this offline fixture's checksum routing and local-only Go isolation.
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

bash "$ROOT/scripts/test-release-materials.sh"
bash "$ROOT/scripts/test-release-go-notices.sh"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-selected-go-materials.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-go-environment.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-validator-environment.py"
bash "$ROOT/scripts/test-release-license-traversal.sh"
GOWORK=off go test -race "$ROOT/scripts/release-archive-validator.go" "$ROOT/scripts/release-archive-validator_test.go"

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

mkdir -p "$TMP/git-source"
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
# Package validation checks actual paths, permissions, notices and payloads.
material_root="$TMP/material-validation"
material_unit=validation
mkdir -p "$material_root/share/licenses/$material_unit/project" "$TMP/material-validation-work"
printf 'fixture license\n' > "$material_root/share/licenses/$material_unit/project/LICENSE"
find "$material_root/share" -type d -exec chmod 0755 {} +
find "$material_root/share" -type f -exec chmod 0644 {} +
WORK="$TMP/material-validation-work" release_materials_validate "$material_root" "$material_unit"
for mutation in parent-mode file-mode link fifo empty missing unsafe-path source-record; do
  altered="$TMP/material-$mutation"
  cp -a "$material_root" "$altered"
  license="$altered/share/licenses/$material_unit/project/LICENSE"
  case "$mutation" in
    parent-mode) chmod 0777 "$altered/share" ;;
    file-mode) chmod 0666 "$license" ;;
    link) rm "$license"; ln -s /etc/passwd "$license" ;;
    fifo) rm "$license"; mkfifo "$license" ;;
    empty) : > "$license" ;;
    missing) rm "$license" ;;
    unsafe-path) mv "$license" "${license} unsafe" ;;
    source-record) mkdir -p "$altered/share/sources/$material_unit"; printf 'retired\n' > "$altered/share/sources/$material_unit/SOURCES.tsv" ;;
  esac
  if (WORK="$TMP/material-validation-work" release_materials_validate "$altered" "$material_unit" >/dev/null 2>&1); then
    fail "license validator accepted $mutation"
  fi
done
if (WORK="$TMP/material-validation-work" release_materials_validate_go "$material_root" "$material_unit" bin/missing >/dev/null 2>&1); then
  fail "validator accepted missing Go binary"
fi

bash "$ROOT/scripts/test-preview-line.sh"
bash "$ROOT/scripts/test-delete-preview.sh"

# The component passes every input unchanged to the fixed trusted helper.
mkdir -p "$TMP/source-entry/trusted/platform/release"
cat > "$TMP/source-entry/trusted/platform/release/producer-inputs.py" <<'PYENTRY'
import os, sys
from pathlib import Path
unit = os.environ['ENTRY_UNIT']
prefix = unit + '-' if unit in ('runtime', 'vmlinux') else ''
assert sys.argv[1:] == ['admit', unit, 'release/v1.2.x', '1' * 40,
                       prefix + 'v1.2.3', os.environ['GITHUB_REPOSITORY'], 'producer-source.tar']
if os.environ.get('ENTRY_REJECT'): raise SystemExit(23)
Path('producer-source.tar').write_text('admitted fixture')
PYENTRY
(
  cd "$TMP/source-entry"
  export GITHUB_REPOSITORY=kuasar-sandbox/orchestrator ENTRY_UNIT=orchestrator
  bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 orchestrator
  [ -f producer-source.tar ] || fail "source admission was not preserved"
  rm producer-source.tar
  status=0
  ENTRY_REJECT=1 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 orchestrator || status=$?
  [ "$status" -eq 23 ] || fail "source admission failure was not preserved"
  [ ! -e producer-source.tar ] || fail "rejected admission produced an artifact"
)
rm "$TMP/source-entry/trusted/platform/release/producer-inputs.py"
if (cd "$TMP/source-entry"; bash "$ROOT/scripts/validate-release-source.sh" main 1111111111111111111111111111111111111111 v1.2.3 connector) >/dev/null 2>&1; then
  fail "source validator accepted a missing trusted helper"
fi
bash -n "$ROOT/scripts/delete-preview.sh" "$ROOT/scripts/validate-release-source.sh"

WORKFLOW="$ROOT/.github/workflows/component-release.yml"
grep -Fqx 'run-name: Release ${{ inputs.version }} @${{ inputs.source_sha }} [accelerator=${{ inputs.accelerator_version }},connector=${{ inputs.connector_version }},sandboxer=${{ inputs.sandboxer_version }}]' \
  "$WORKFLOW" || fail "release run identity does not pin source and dependencies"
workflow="$ROOT/.github/workflows/component-release.yml"
for workflow in component-release.yml delete-preview.yml; do
  [ "$(grep -Fc 'group: component-mutation-${{ github.repository }}-${{ inputs.version }}' \
    "$ROOT/.github/workflows/$workflow")" -eq 1 ] \
    || fail "$workflow does not hold exactly one full-workflow mutation lock"
done
[ ! -e "$ROOT/.github/workflows/reconcile-latest.yml" ] || fail "retired Latest writer remains"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-publisher.py"
for input in accelerator_version connector_version sandboxer_version; do
  grep -Fq "      $input:" "$WORKFLOW" \
    || fail "release workflow is missing required $input input"
  grep -Fq -- "--dependency ${input%_version} \"\${{ needs.preflight.outputs.$input }}\" \"\${{ needs.preflight.outputs.${input%_version}_sha }}\"" "$WORKFLOW" \
    || fail "release restore does not bind $input tag and SHA to trusted preflight outputs"
  if grep -Fq "ref: \${{ needs.preflight.outputs.${input%_version}_sha }}" "$WORKFLOW"; then
    fail "release workflow refetches a dependency by bare SHA"
  fi
done
grep -Fq "repos/kuasar-sandbox/\$repository/releases/tags/\$version" "$WORKFLOW" \
  || fail "release workflow does not verify dependency releases"

for entrypoint in \
  test/e2e/cases/basic.orchestrator-cli.sh \
  test/e2e/cases/builder.api.sh \
  test/e2e/cases/builder.context.sh \
  test/e2e/cases/builder.failure.sh \
  test/e2e/cases/builder.image.sh \
  test/e2e/cases/builder.publish.sh \
  test/e2e/cases/builder.sandbox.sh \
  test/e2e/cases/builder.steps.sh \
  test/e2e/cases/orchestrator.api.sh \
  test/e2e/cases/orchestrator.cluster-lifecycle.sh \
  test/e2e/cases/orchestrator.cluster-recovery.sh \
  test/e2e/cases/orchestrator.exec.sh \
  test/e2e/cases/orchestrator.lifecycle.sh \
  test/e2e/cases/orchestrator.mmds-recovery.sh \
  test/e2e/cases/orchestrator.mmds.sh \
  test/e2e/cases/orchestrator.pause-wake.sh \
  test/e2e/cases/orchestrator.proxy-auth.sh \
  test/e2e/cases/orchestrator.proxy-restart.sh \
  test/e2e/cases/orchestrator.proxy-wake.sh \
  test/e2e/cases/orchestrator.proxy.sh \
  test/e2e/cases/orchestrator.resource-admission.sh \
  test/e2e/cases/orchestrator.resource-control.sh \
  test/e2e/cases/orchestrator.resource-recovery.sh \
  test/e2e/cases/orchestrator.resource-reservation.sh \
  test/e2e/cases/orchestrator.resource-startup.sh \
  test/e2e/cases/orchestrator.runtask.sh \
  test/e2e/cases/orchestrator.snapshot.sh \
  test/e2e/cases/telemetry.backends.sh \
  test/e2e/cases/telemetry.guest.sh \
  test/e2e/cases/telemetry.proxy.sh; do
  git -C "$ROOT" ls-files --error-unmatch "$entrypoint" >/dev/null 2>&1 \
    || fail "rewritten product case is not tracked: $entrypoint"
done

for legacy in test/e2e/e2e_capture_cli.sh test/e2e/e2e_cluster_stub.sh \
  test/e2e/e2e_builder_unit_upgrade.sh test/e2e/e2e_journal_contract.sh \
  test/e2e/runtask_privilege_test.sh test/e2e/vmm_cgroup_test.sh \
  test/e2e/e2e_sandbox_cold_target.sh test/e2e/e2e_cluster_real.sh \
  test/e2e/e2e_density.sh test/e2e/e2e_orchestrator_proxy.sh \
  test/e2e/e2e_mmds_routes.sh test/e2e/e2e_mmds_routes_proxy_restart.sh \
  test/e2e/run_all.sh test/e2e/e2e_execute.sh test/e2e/e2e_run_builder.sh \
  test/e2e/lib/build_actions.py test/e2e/lib/execute_pause_contract.sh \
  test/e2e/cases/builder.pipeline.sh \
  test/e2e/cases/orchestrator.cold-target.sh \
  test/e2e/cases/orchestrator.execute.sh \
  test/e2e/cases/orchestrator.proxy-lifecycle.sh; do
  if git -C "$ROOT" ls-files --error-unmatch "$legacy" >/dev/null 2>&1; then
    fail "migrated legacy E2E entrypoint remains tracked: $legacy"
  fi
done

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
for dependency in accelerator connector sandboxer; do
  mkdir "$TMP/$dependency/LICENSES"
  printf 'fixture nested notice for %s\n' "$dependency" > "$TMP/$dependency/LICENSES/NOTICE.txt"
  printf 'fixture attribution for %s\n' "$dependency" > "$TMP/$dependency/NOTICE"
done
accelerator_sha="$(init_fixture_repo "$TMP/accelerator" LICENSE LICENSES NOTICE)"
connector_sha="$(init_fixture_repo "$TMP/connector" LICENSE LICENSES NOTICE)"
sandboxer_sha="$(init_fixture_repo "$TMP/sandboxer" LICENSE LICENSES NOTICE)"
git -C "$TMP/accelerator" tag v0.1.3 "$accelerator_sha"
git -C "$TMP/connector" tag v0.1.2 "$connector_sha"
git -C "$TMP/sandboxer" tag v0.1.3 "$sandboxer_sha"
fixture_root="$TMP/project"
mkdir -p "$fixture_root/scripts" "$fixture_root/LICENSES"
install -m 0644 "$ROOT/LICENSE" "$fixture_root/LICENSE"
printf 'fixture nested project notice\n' > "$fixture_root/LICENSES/NOTICE.txt"
printf 'fixture project attribution\n' > "$fixture_root/NOTICE"
printf '/bin/\n/build/\n' > "$fixture_root/.gitignore"
install -m 0755 "$ROOT/scripts/release.sh" "$fixture_root/scripts/release.sh"
install -m 0755 "$ROOT/scripts/publish-release.sh" "$fixture_root/scripts/publish-release.sh"
install -m 0644 "$ROOT/scripts/release-publisher.py" "$fixture_root/scripts/release-publisher.py"
install -m 0755 "$ROOT/scripts/release-materials.sh" "$fixture_root/scripts/release-materials.sh"
install -m 0644 "$ROOT/scripts/release-archive-validator.go" "$fixture_root/scripts/release-archive-validator.go"
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
	test "$$(git -C ../accelerator rev-parse HEAD)" = "$(accelerator_commit)"
	test "$$(git -C ../connector rev-parse HEAD)" = "$(connector_commit)"
	test "$$(git -C ../sandboxer rev-parse HEAD)" = "$(sandboxer_commit)"
	mkdir -p bin/x86_64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/node-ctl ./cmd/node-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/cluster-ctl ./cmd/cluster-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/node-stub-ctl ./cmd/node-stub-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/e2b-key-ctl ./cmd/e2b-key-ctl
EOF
fixture_project_sha="$(init_fixture_repo "$fixture_root" LICENSE LICENSES NOTICE .gitignore scripts go.mod cmd deploy Makefile)"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/go-fixture" ./cmd/node-ctl)
release_materials_require_go_revision "$TMP/go-fixture" "$fixture_project_sha"
printf '// dirty fixture\n' >> "$fixture_root/cmd/node-ctl/main.go"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/dirty-go-fixture" ./cmd/node-ctl)
if (release_materials_require_go_revision "$TMP/dirty-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary built from dirty source"
fi
printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/node-ctl/main.go"
if (release_materials_require_go_revision "$TMP/go-fixture" \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "release accepted a binary built from another commit"
fi
GOWORK=off GO111MODULE=off go build -o "$TMP/unstamped-go-fixture" "$fixture_root/cmd/node-ctl/main.go"
if (release_materials_require_go_revision "$TMP/unstamped-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary without source stamping"
fi
GOWORK=off make --no-print-directory -C "$fixture_root" build
for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
  install -m 0755 "$fixture_root/bin/x86_64/$binary" "$TMP/bin/$binary"
done
SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/bin" \
  RELEASE_ACCELERATOR_SOURCE_DIR="$TMP/accelerator" \
  RELEASE_ACCELERATOR_SOURCE_SHA="$accelerator_sha" \
  RELEASE_ACCELERATOR_VERSION=v0.1.3 \
  RELEASE_CONNECTOR_SOURCE_DIR="$TMP/connector" \
  RELEASE_CONNECTOR_SOURCE_SHA="$connector_sha" \
  RELEASE_CONNECTOR_VERSION=v0.1.2 \
  RELEASE_SANDBOXER_SOURCE_DIR="$TMP/sandboxer" \
  RELEASE_SANDBOXER_SOURCE_SHA="$sandboxer_sha" \
  RELEASE_SANDBOXER_VERSION=v0.1.3 \
  RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/bundle"
"$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
# A current publication may bind its own binaries to its selected commit;
# standalone validation above does not require or query historical sources.
if SOURCE_SHA=0000000000000000000000000000000000000000 "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle" > "$TMP/publication-source.log" 2>&1; then
  fail "publication accepted an owner binary built at another commit"
fi
grep -Fq 'Go payload must be built from the clean selected commit' "$TMP/publication-source.log" \
  || fail "publication source mismatch failed for an unrelated reason"

"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/orchestrator v1.2.3 \
  "$fixture_project_sha" main
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/orchestrator v1.2.3 \
  "$fixture_project_sha" release/v1.2.x

archive="$TMP/bundle/assets/orchestrator-v1.2.3-linux-x86_64.tar.gz"
RELEASE_DEPENDENCIES=accelerator=v0.1.3,connector=v0.1.2,sandboxer=v0.1.3 \
  "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
go_toolchain="$(go version | awk '{print $3}')"
for path in ./bin/node-ctl ./bin/cluster-ctl ./bin/node-stub-ctl \
  ./bin/e2b-key-ctl ./deploy/node-ctl.service \
  ./deploy/node-telemetry.service ./deploy/telemetry.example.yaml \
  ./deploy/telemetry-fanout.example.yaml \
  ./deploy/telemetry-prometheus.example.yaml \
  ./deploy/telemetry-clickhouse.example.yaml \
  ./deploy/telemetry-query-only.example.yaml \
  ./share/licenses/orchestrator/accelerator/LICENSE \
  ./share/licenses/orchestrator/connector/LICENSE \
  ./share/licenses/orchestrator/project/LICENSE \
  ./share/licenses/orchestrator/sandboxer/LICENSE \
  ./share/licenses/orchestrator/go-toolchain/"$go_toolchain"/LICENSE; do
  tar -tzf "$archive" | grep -Fx "$path" >/dev/null || fail "archive is missing $path"
done
if tar -tzf "$archive" | grep -E '(^|/)sources/|SOURCES[.]tsv|GO-BUILD-INFO[.]tsv|GO-MODULES[.]tsv|MATERIALS[.]sha256|CLOUD-HYPERVISOR-Cargo.lock' >/dev/null; then
  fail "new archive includes historical source or build records"
fi
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
  RELEASE_SOURCE_ROOT="$fixture_root" "$ROOT/scripts/release.sh" package v1.2.3 x86_64 "$TMP/reproducible"
cmp -s "$archive" "$TMP/reproducible/assets/orchestrator-v1.2.3-linux-x86_64.tar.gz" \
  || fail "identical inputs did not produce an identical archive"

# Repack altered payloads and validate their actual identity and architecture.
repack_candidate() (
  local candidate="$1" binary
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
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


for binary in node-ctl cluster-ctl node-stub-ctl e2b-key-ctl; do
  (cd "$TMP/target-source" && GOWORK=off CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -trimpath -buildvcs=true -o "$TMP/cgo-$binary" "./cmd/$binary")
  release_materials_require_go_revision "$TMP/cgo-$binary" "$fixture_project_sha"
  go version -m "$TMP/cgo-$binary" | grep -Fq 'CGO_ENABLED=1' \
    || fail "CGO fixture did not preserve its actual build setting"
  candidate="$TMP/wrong-cgo-$binary"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  install -m 0755 "$TMP/cgo-$binary" "$candidate/root/bin/$binary"
  repack_candidate "$candidate"
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted a CGO_ENABLED=1 payload with regenerated metadata and checksums: $binary"
  fi
  grep -Fq 'with CGO_ENABLED=0' "$candidate/result.log" || fail "CGO fixture failed for an unrelated reason"
done

for extra in bin/unexpected-tool deploy/unexpected.service bin/extra/; do
  candidate="$TMP/extra-entry-${extra//\//-}"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$extra" in
    */) mkdir -m 0755 "$candidate/root/$extra" ;;
    *) printf 'fixture extra payload\n' > "$candidate/root/$extra"
       case "$extra" in bin/*) chmod 0755 "$candidate/root/$extra" ;; *) chmod 0644 "$candidate/root/$extra" ;; esac ;;
  esac
  repack_candidate "$candidate"
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted an uncontracted archive entry: $extra"
  fi
  grep -Fq 'unexpected member' "$candidate/result.log" || fail "extra entry failed for an unrelated reason"
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
      (cd "$TMP/target-source" && GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
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

if RELEASE_BIN_DIR="$TMP/bin" RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package 01.2.3 x86_64 \
  "$TMP/invalid-version" >/dev/null 2>&1; then
  fail "packager accepted an invalid version"
fi
if RELEASE_BIN_DIR="$TMP/bin" RELEASE_SOURCE_ROOT="$fixture_root" "$fixture_root/scripts/release.sh" package v1.2.3 aarch64 \
  "$TMP/invalid-arch" >/dev/null 2>&1; then
  fail "packager accepted x86_64 payloads as aarch64"
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
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/nonroot-owner" >/dev/null 2>&1; then
  fail "validator accepted non-root numeric ownership with regenerated checksums"
fi

# Standalone validation must not need source checkouts, module downloads or a build.
mkdir -p "$TMP/standalone-tools" "$TMP/standalone-bin"
cp -a "$fixture_root/scripts" "$TMP/standalone-tools/scripts"
for command in git curl wget cargo make gcc; do
  printf '#!/bin/sh\nexit 97\n' > "$TMP/standalone-bin/$command"
  chmod 0755 "$TMP/standalone-bin/$command"
done
env PATH="$TMP/standalone-bin:$PATH" GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  SOURCE_SHA="$fixture_project_sha" "$TMP/standalone-tools/scripts/release.sh" validate \
  v1.2.3 x86_64 "$TMP/bundle"
echo "test-release: standalone validation without checkouts/downloads/build PASS"

echo "test-release: PASS"
