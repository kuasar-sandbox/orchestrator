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
  local files=() file
  while IFS= read -r file; do files+=("$file"); done \
    < <(find "$bundle/assets" -mindepth 1 -maxdepth 1 -type f -print | LC_ALL=C sort)
  gh release create "$tag" "${files[@]}" --repo "$REPOSITORY" --draft --verify-tag \
    --target "$commit" --title "$tag" --notes-file "$bundle/release-notes.md" >/dev/null
  wait_for_draft_release "$tag" "$drafts"
  jq '.[0]' "$drafts" > "$TMP/release"
  verify_uploaded_assets "$TMP/release" "$bundle"
  local prerelease=false make_latest=false
  [[ "$tag" != *-preview.* ]] || prerelease=true
  [ "$prerelease" = true ] || [ "$source_ref" != main ] || make_latest=true
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
