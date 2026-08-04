#!/usr/bin/env bash

set -euo pipefail

REPOSITORY="${GH_REPO:-${GITHUB_REPOSITORY:-}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() {
  echo "publish-release: $*" >&2
  exit 1
}

api_optional() {
  local endpoint="$1"
  local output="$2"
  if gh api "$endpoint" > "$output" 2> "$TMP/api-error"; then
    return 0
  fi
  if grep -q '(HTTP 404)' "$TMP/api-error"; then
    : > "$output"
    return 4
  fi
  cat "$TMP/api-error" >&2
  return 1
}

validate_tag() {
  [[ "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
    || fail "tag must match vX.Y.Z or vX.Y.Z-preview.YYYYMMDD without leading zeroes"
}

find_draft_release() {
  local tag="$1"
  local output="$2"
  gh api --paginate --slurp "repos/$REPOSITORY/releases?per_page=100" \
    | jq --arg tag "$tag" '[.[][] | select(.tag_name == $tag and .draft == true)]' \
    > "$output"
  [ "$(jq 'length' "$output")" -le 1 ] \
    || fail "multiple draft releases use tag $tag"
}

wait_for_draft_release() {
  local tag="$1"
  local output="$2"
  local attempt
  for attempt in {1..15}; do
    find_draft_release "$tag" "$output"
    [ "$(jq 'length' "$output")" -ne 1 ] || return 0
    [ "$attempt" -eq 15 ] || sleep 1
  done
  fail "cannot locate newly created draft for $tag"
}

check_release() {
  [ "$#" -eq 1 ] || fail "usage: publish-component-release.sh check <tag>"
  local tag="$1"
  validate_tag "$tag"
  if api_optional "repos/$REPOSITORY/releases/tags/$tag" "$TMP/release"; then
    fail "GitHub release already exists: $tag"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi
}

verify_uploaded_assets() {
  local state="$1"
  local bundle="$2"
  local expected="$TMP/expected-assets"
  local actual="$TMP/actual-assets"
  jq -r '.artifacts[] | [.name, ("sha256:" + .sha256), (.size | tostring), "uploaded"] | @tsv' \
    "$bundle/release.json" > "$expected"
  printf 'release.json\tsha256:%s\t%s\tuploaded\n' \
    "$(sha256sum "$bundle/release.json" | awk '{print $1}')" \
    "$(stat -c '%s' "$bundle/release.json")" >> "$expected"
  LC_ALL=C sort -o "$expected" "$expected"
  jq -r '.assets[] | [.name, .digest, (.size | tostring), .state] | @tsv' "$state" \
    | LC_ALL=C sort > "$actual"
  if ! cmp -s "$expected" "$actual"; then
    echo "publish-release: uploaded asset set does not match the bundle" >&2
    diff -u "$expected" "$actual" >&2 || true
    exit 1
  fi
}

publish_bundle() {
  [ "$#" -eq 1 ] || fail "usage: publish-component-release.sh publish <bundle-dir>"
  local bundle="$1"
  "$SCRIPT_DIR/component-release.sh" validate "$bundle"
  local manifest="$bundle/release.json"
  local tag commit repository
  tag="$(jq -er '.tag' "$manifest")"
  commit="$(jq -er '.commit' "$manifest")"
  repository="$(jq -er '.repository' "$manifest")"
  [ "$repository" = "$REPOSITORY" ] || fail "bundle belongs to $repository, not $REPOSITORY"
  validate_tag "$tag"

  local tag_state="$TMP/tag"
  if api_optional "repos/$REPOSITORY/git/ref/tags/$tag" "$tag_state"; then
    [ "$(jq -er '.object.sha' "$tag_state")" = "$commit" ] \
      || fail "$tag already points to another commit"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    jq -n --arg ref "refs/tags/$tag" --arg sha "$commit" '{ref: $ref, sha: $sha}' \
      | gh api --method POST "repos/$REPOSITORY/git/refs" --input - >/dev/null
  fi

  local release_state="$TMP/release"
  if api_optional "repos/$REPOSITORY/releases/tags/$tag" "$release_state"; then
    fail "$tag is already published; refusing to replace it"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi

  local drafts="$TMP/drafts"
  find_draft_release "$tag" "$drafts"
  if [ "$(jq 'length' "$drafts")" -eq 1 ]; then
    local stale_id
    stale_id="$(jq -er '.[0].id' "$drafts")"
    gh api --method DELETE "repos/$REPOSITORY/releases/$stale_id" >/dev/null
  fi

  local files=()
  while IFS= read -r name; do
    files+=("$bundle/assets/$name")
  done < <(jq -r '.artifacts[].name' "$manifest")
  files+=("$manifest")
  gh release create "$tag" "${files[@]}" --repo "$REPOSITORY" --draft --verify-tag \
    --target "$commit" --title "$tag" --notes-file "$bundle/release-notes.md" >/dev/null

  wait_for_draft_release "$tag" "$drafts"
  jq '.[0]' "$drafts" > "$release_state"
  verify_uploaded_assets "$release_state" "$bundle"
  local release_id
  release_id="$(jq -er '.id' "$release_state")"
  local prerelease=false
  [[ "$tag" != *-preview.* ]] || prerelease=true
  jq -n --argjson prerelease "$prerelease" '
      {draft: false, prerelease: $prerelease,
       make_latest: (if $prerelease then "false" else "true" end)}
    ' \
    | gh api --method PATCH "repos/$REPOSITORY/releases/$release_id" --input - >/dev/null
  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$release_state"
  jq -e --argjson prerelease "$prerelease" '
      .draft == false and .prerelease == $prerelease
    ' "$release_state" >/dev/null \
    || fail "$tag was not published"
  echo "==> published $REPOSITORY $tag from $commit"
}

[ -n "$REPOSITORY" ] || fail "GH_REPO or GITHUB_REPOSITORY is required"
[[ "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "invalid repository"
command -v gh >/dev/null || fail "gh is required"
command -v jq >/dev/null || fail "jq is required"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "${1:-}" in
  check) shift; check_release "$@" ;;
  publish) shift; publish_bundle "$@" ;;
  *) fail "usage: publish-component-release.sh <check <tag>|publish <bundle-dir>>" ;;
esac
