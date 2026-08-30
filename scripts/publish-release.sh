#!/usr/bin/env bash

set -euo pipefail

REPOSITORY="${GH_REPO:-${GITHUB_REPOSITORY:-}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "publish-release: $*" >&2
  exit 1
}

release_cli() {
  local command="$1"
  shift
  if [ -n "${RELEASE_KIND:-}" ]; then
    "$SCRIPT_DIR/release.sh" "$command" "$RELEASE_KIND" "$@"
  else
    "$SCRIPT_DIR/release.sh" "$command" "$@"
  fi
}

api_optional() {
  local endpoint="$1" output="$2"
  if gh api "$endpoint" > "$output" 2> "$TMP/api-error"; then return 0; fi
  if grep -q '(HTTP 404)' "$TMP/api-error"; then : > "$output"; return 4; fi
  cat "$TMP/api-error" >&2
  return 1
}

find_draft_release() {
  local tag="$1" output="$2"
  gh api --paginate --slurp "repos/$REPOSITORY/releases?per_page=100" \
    | jq --arg tag "$tag" '[.[][] | select(.tag_name == $tag and .draft == true)]' > "$output"
  [ "$(jq 'length' "$output")" -le 1 ] || fail "multiple draft releases use tag $tag"
}

wait_for_draft_release() {
  local tag="$1" output="$2" attempt
  for attempt in {1..15}; do
    find_draft_release "$tag" "$output"
    [ "$(jq 'length' "$output")" -ne 1 ] || return 0
    [ "$attempt" -eq 15 ] || sleep 1
  done
  fail "cannot locate newly created draft for $tag"
}

check_release() {
  [ "$#" -eq 2 ] || fail "usage: publish-release.sh check <tag> <arch>"
  local tag="$1" arch="$2"
  release_cli archive-name "$tag" "$arch" >/dev/null
  if api_optional "repos/$REPOSITORY/releases/tags/$tag" "$TMP/release"; then
    fail "GitHub release already exists: $tag"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi
}

verify_uploaded_assets() {
  local state="$1" bundle="$2" expected="$TMP/expected-assets" actual="$TMP/actual-assets" file
  : > "$expected"
  while IFS= read -r file; do
    printf '%s\tsha256:%s\t%s\tuploaded\n' "$(basename "$file")" \
      "$(sha256sum "$file" | awk '{print $1}')" "$(stat -c '%s' "$file")" >> "$expected"
  done < <(find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -print | LC_ALL=C sort)
  LC_ALL=C sort -o "$expected" "$expected"
  jq -r '.assets[] | [.name, .digest, (.size | tostring), .state] | @tsv' "$state" \
    | LC_ALL=C sort > "$actual"
  cmp -s "$expected" "$actual" \
    || { diff -u "$expected" "$actual" >&2 || true; fail "uploaded asset set does not match the bundle"; }
}

release_unit() {
  local tag="$1" unit="${REPOSITORY##*/}"
  if [ "$unit" = guest-runtime ]; then
    case "$tag" in
      runtime-*) unit=runtime ;;
      vmlinux-*) unit=vmlinux ;;
      *) fail "cannot derive guest-runtime release unit from $tag" ;;
    esac
  fi
  printf '%s\n' "$unit"
}

revalidate_preview_line() {
  local tag="$1" unit
  [[ "$tag" == *-preview.* ]] || return 0
  unit="$(release_unit "$tag")"
  "$SCRIPT_DIR/validate-preview-line.sh" "$unit" "$tag" \
    "${AGGREGATE_VERSION:?AGGREGATE_VERSION is required for a Preview}" \
    "${AGGREGATE_SHA:?AGGREGATE_SHA is required for a Preview}"
}

release_notes_file() {
  local tag="$1" commit="$2" bundle="$3" source_ref="$4"
  local notes="$TMP/release-notes.md"
  cp "$bundle/release-notes.md" "$notes"
  if [[ "$tag" == *-preview.* ]]; then
    local unit dependencies binding
    unit="$(release_unit "$tag")"
    dependencies="${RELEASE_DEPENDENCIES:-}"
    if [ -n "$dependencies" ] \
      && ! [[ "$dependencies" =~ ^[a-z][a-z0-9-]*=v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?(,[a-z][a-z0-9-]*=v[0-9]+\.[0-9]+\.[0-9]+(-preview\.[0-9]{8})?)*$ ]]; then
      fail "invalid RELEASE_DEPENDENCIES"
    fi
    binding="$(jq -cn \
      --arg aggregate_sha "${AGGREGATE_SHA:?AGGREGATE_SHA is required for a Preview}" \
      --arg aggregate_version "${AGGREGATE_VERSION:?AGGREGATE_VERSION is required for a Preview}" \
      --arg dependencies "$dependencies" --arg source_ref "$source_ref" \
      --arg source_sha "$commit" --arg unit "$unit" \
      '{aggregate_sha: $aggregate_sha, aggregate_version: $aggregate_version,
        dependencies: $dependencies, source_ref: $source_ref,
        source_sha: $source_sha, unit: $unit}')"
    printf '\n<!-- kuasar-preview-binding %s -->\n' "$binding" >> "$notes"
  fi
  printf '%s\n' "$notes"
}

publish_bundle() {
  [ "$#" -eq 5 ] || fail "usage: publish-release.sh publish <tag> <arch> <commit> <bundle-dir> <source-ref>"
  local tag="$1" arch="$2" commit="$3" bundle="$4" source_ref="$5"
  [[ "$commit" =~ ^[0-9a-f]{40}$ ]] || fail "commit must be a full lowercase SHA"
  [[ "$source_ref" = main || "$source_ref" =~ ^release/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.x$ ]] \
    || fail "source-ref must be main or release/vMAJOR.MINOR.x"
  release_cli validate "$tag" "$arch" "$bundle"

  local tag_state="$TMP/tag"
  if api_optional "repos/$REPOSITORY/git/ref/tags/$tag" "$tag_state"; then
    [ "$(jq -er '.object.sha' "$tag_state")" = "$commit" ] || fail "$tag already points to another commit"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    jq -n --arg ref "refs/tags/$tag" --arg sha "$commit" '{ref: $ref, sha: $sha}' \
      | gh api --method POST "repos/$REPOSITORY/git/refs" --input - >/dev/null
  fi
  if api_optional "repos/$REPOSITORY/releases/tags/$tag" "$TMP/release"; then
    fail "$tag is already published; refusing to replace it"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi

  local drafts="$TMP/drafts"
  find_draft_release "$tag" "$drafts"
  if [ "$(jq 'length' "$drafts")" -eq 1 ]; then
    gh api --method DELETE "repos/$REPOSITORY/releases/$(jq -er '.[0].id' "$drafts")" >/dev/null
  fi
  local files=() file notes
  while IFS= read -r file; do files+=("$file"); done \
    < <(find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -print | LC_ALL=C sort)
  notes="$(release_notes_file "$tag" "$commit" "$bundle" "$source_ref")"
  gh release create "$tag" "${files[@]}" --repo "$REPOSITORY" --draft --verify-tag \
    --target "$commit" --title "$tag" --notes-file "$notes" >/dev/null
  wait_for_draft_release "$tag" "$drafts"
  jq '.[0]' "$drafts" > "$TMP/release"
  verify_uploaded_assets "$TMP/release" "$bundle"
  local prerelease=false make_latest=false
  [[ "$tag" != *-preview.* ]] || prerelease=true
  [ "$prerelease" = true ] || [ "$source_ref" != main ] || make_latest=true
  revalidate_preview_line "$tag"
  jq -n --argjson prerelease "$prerelease" --argjson make_latest "$make_latest" \
    '{draft: false, prerelease: $prerelease, make_latest: ($make_latest | tostring)}' \
    | gh api --method PATCH "repos/$REPOSITORY/releases/$(jq -er '.id' "$TMP/release")" --input - >/dev/null
  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$TMP/release"
  jq -e --arg tag "$tag" --arg commit "$commit" --argjson prerelease "$prerelease" '
      .tag_name == $tag and .target_commitish == $commit and .draft == false and .prerelease == $prerelease
    ' "$TMP/release" >/dev/null || fail "$tag was not published as requested"
  echo "==> published $REPOSITORY $tag from $commit"
}

[ -n "$REPOSITORY" ] || fail "GH_REPO or GITHUB_REPOSITORY is required"
[[ "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "invalid repository"
command -v gh >/dev/null || fail "gh is required"
command -v jq >/dev/null || fail "jq is required"
case "${1:-}" in
  check) shift; check_release "$@" ;;
  publish) shift; publish_bundle "$@" ;;
  *) fail "usage: publish-release.sh <check|publish> ..." ;;
esac
