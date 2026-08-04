#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() {
  echo "release-suite: $*" >&2
  exit 1
}

release_exists() {
  local repository="$1" version="$2"
  local state="$TMP/release.json"
  if gh api "repos/$repository/releases/tags/$version" > "$state" 2> "$TMP/api-error"; then
    local prerelease=false
    [[ "$version" != *-preview.* ]] || prerelease=true
    jq -e --arg version "$version" --argjson prerelease "$prerelease" '
      .tag_name == $version and .draft == false and .prerelease == $prerelease
    ' "$state" >/dev/null || fail "$repository $version exists but is not a completed release"
    return 0
  fi
  if grep -q '(HTTP 404)' "$TMP/api-error"; then
    return 1
  fi
  cat "$TMP/api-error" >&2
  exit 1
}

wait_for_dispatch() {
  local repository="$1" workflow="$2" title="$3" started_epoch="$4"
  local run_id="" runs="$TMP/runs.json"
  local attempt=0
  while [ "$attempt" -lt 30 ]; do
    attempt=$((attempt + 1))
    gh run list --repo "$repository" --workflow "$workflow" --event workflow_dispatch \
      --limit 30 --json databaseId,displayTitle,status,conclusion,createdAt,url > "$runs"
    run_id="$(jq -r \
      --arg title "$title" \
      --argjson started "$((started_epoch - 5))" '
        [.[]
          | select(.displayTitle == $title)
          | select((.createdAt | fromdateiso8601) >= $started)
        ]
        | sort_by(.createdAt)
        | last
        | .databaseId // empty
      ' "$runs")"
    [ -z "$run_id" ] || break
    sleep 2
  done
  [ -n "$run_id" ] || fail "cannot identify dispatched run: $repository $workflow $title"
  gh run watch "$run_id" --repo "$repository" --exit-status
}

publish_component() {
  local name="$1" repository="$2" workflow="$3" version="$4" title="$5"
  shift 5
  if release_exists "$repository" "$version"; then
    echo "==> reuse $repository $version"
    return 0
  fi
  echo "==> dispatch $name $version"
  local started
  started="$(date +%s)"
  gh workflow run "$workflow" --repo "$repository" --ref main -f "version=$version" "$@"
  wait_for_dispatch "$repository" "$workflow" "$title" "$started"
  release_exists "$repository" "$version" \
    || fail "$repository $version workflow succeeded without publishing the release"
}

release_suite() {
  [ "$#" -eq 1 ] || fail "usage: release-suite.sh <mapping.json>"
  local mapping="$1"
  "$SCRIPT_DIR/aggregate-release.sh" validate-map "$mapping"

  local aggregate accelerator connector sandboxer orchestrator runtime vmlinux mapping_commit
  aggregate="$(jq -er '.version' "$mapping")"
  accelerator="$(jq -er '.components.accelerator' "$mapping")"
  connector="$(jq -er '.components.connector' "$mapping")"
  sandboxer="$(jq -er '.components.sandboxer' "$mapping")"
  orchestrator="$(jq -er '.components.orchestrator' "$mapping")"
  runtime="$(jq -er '.components.runtime' "$mapping")"
  vmlinux="$(jq -er '.components.vmlinux' "$mapping")"

  mapping_commit="$("$SCRIPT_DIR/release-map.sh" put "$mapping")"
  echo "==> release mapping: $mapping_commit"

  publish_component accelerator kuasar-sandbox/accelerator release.yml "$accelerator" \
    "Release $accelerator"
  publish_component connector kuasar-sandbox/connector release.yml "$connector" \
    "Release $connector"
  publish_component sandboxer kuasar-sandbox/sandboxer release.yml "$sandboxer" \
    "Release $sandboxer"
  publish_component orchestrator kuasar-sandbox/orchestrator component-release.yml "$orchestrator" \
    "Release orchestrator $orchestrator"
  publish_component runtime kuasar-sandbox/guest-runtime release-runtime.yml "$runtime" \
    "Release $runtime with sandboxer $sandboxer" -f "sandboxer_version=$sandboxer"
  publish_component vmlinux kuasar-sandbox/guest-runtime release-vmlinux.yml "$vmlinux" \
    "Release $vmlinux"

  if release_exists kuasar-sandbox/orchestrator "$aggregate"; then
    echo "==> reuse kuasar-sandbox/orchestrator $aggregate"
    return 0
  fi
  echo "==> dispatch aggregate $aggregate"
  local started
  started="$(date +%s)"
  gh workflow run aggregate-release.yml --repo kuasar-sandbox/orchestrator --ref main \
    -f "version=$aggregate"
  wait_for_dispatch kuasar-sandbox/orchestrator aggregate-release.yml \
    "Aggregate $aggregate" "$started"
  release_exists kuasar-sandbox/orchestrator "$aggregate" \
    || fail "$aggregate workflow succeeded without publishing the aggregate release"
  echo "==> completed $aggregate"
}

for command in gh jq; do
  command -v "$command" >/dev/null || fail "$command is required"
done
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

release_suite "$@"
