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

check_release() {
  [ "$#" -eq 1 ] || fail "usage: publish-component-release.sh check <tag>"
  local tag="$1"
  [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] \
    || fail "tag must match vX.Y.Z without leading zeroes"
  if api_optional "repos/$REPOSITORY/git/ref/tags/$tag" "$TMP/tag"; then
    fail "Git tag already exists: $tag"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi
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
    jq -e '.draft == true' "$release_state" >/dev/null \
      || fail "$tag is already published; refusing to replace it"
    gh release edit "$tag" --repo "$REPOSITORY" --draft --target "$commit" \
      --title "$tag" --notes-file "$bundle/release-notes.md" >/dev/null
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    gh release create "$tag" --repo "$REPOSITORY" --draft --verify-tag --target "$commit" \
      --title "$tag" --notes-file "$bundle/release-notes.md" >/dev/null
  fi

  local files=()
  while IFS= read -r name; do
    files+=("$bundle/assets/$name")
  done < <(jq -r '.artifacts[].name' "$manifest")
  files+=("$manifest")
  gh release upload "$tag" --repo "$REPOSITORY" --clobber "${files[@]}"

  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$release_state"
  verify_uploaded_assets "$release_state" "$bundle"
  local release_id
  release_id="$(jq -er '.id' "$release_state")"
  jq -n '{draft: false, prerelease: false}' \
    | gh api --method PATCH "repos/$REPOSITORY/releases/$release_id" --input - >/dev/null
  gh api "repos/$REPOSITORY/releases/tags/$tag" > "$release_state"
  jq -e '.draft == false and .prerelease == false' "$release_state" >/dev/null \
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
