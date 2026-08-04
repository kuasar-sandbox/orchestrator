#!/usr/bin/env bash

set -euo pipefail

REPOSITORY="${GH_REPO:-${GITHUB_REPOSITORY:-}}"
RELEASE_BRANCH=release
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() {
  echo "publish-release: $*" >&2
  exit 1
}

validate_version() {
  [[ "$1" =~ ^release-v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] \
    || fail "version must match release-vX.Y.Z without leading zeroes"
}

api_optional() {
  local endpoint="$1" output="$2"
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
  [ "$#" -eq 1 ] || fail "usage: publish-release.sh check <release-vX.Y.Z>"
  local version="$1"
  validate_version "$version"
  if api_optional "repos/$REPOSITORY/git/ref/tags/$version" "$TMP/tag"; then
    fail "Git tag already exists: $version"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi
  if api_optional "repos/$REPOSITORY/releases/tags/$version" "$TMP/release"; then
    fail "GitHub release already exists: $version"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
  fi
}

verify_mapping_commit() {
  local manifest="$1"
  local commit path expected_sha version branch_state branch_sha compare_state mapping
  commit="$(jq -er '.mapping.commit' "$manifest")"
  path="$(jq -er '.mapping.path' "$manifest")"
  expected_sha="$(jq -er '.mapping.sha256' "$manifest")"
  version="$(jq -er '.version' "$manifest")"

  branch_state="$TMP/release-branch.json"
  gh api "repos/$REPOSITORY/git/ref/heads/$RELEASE_BRANCH" > "$branch_state"
  branch_sha="$(jq -er '.object.sha' "$branch_state")"
  compare_state="$TMP/release-compare.json"
  gh api "repos/$REPOSITORY/compare/$commit...$branch_sha" > "$compare_state"
  jq -e '.status == "ahead" or .status == "identical"' "$compare_state" >/dev/null \
    || fail "mapping commit is not on the current $RELEASE_BRANCH history"

  mapping="$TMP/remote-mapping.json"
  gh api -H 'Accept: application/vnd.github.raw+json' \
    "repos/$REPOSITORY/contents/$path?ref=$commit" > "$mapping"
  [ "$(sha256sum "$mapping" | awk '{print $1}')" = "$expected_sha" ] \
    || fail "release mapping bytes differ from the BMS-validated mapping"
  "$SCRIPT_DIR/aggregate-release.sh" validate-map "$mapping"
  [ "$(jq -er '.version' "$mapping")" = "$version" ] \
    || fail "release mapping version differs from aggregate release.json"
  jq -S . "$mapping" > "$TMP/remote-mapping-canonical.json"
  jq -S '.mapping.spec' "$manifest" > "$TMP/manifest-mapping-canonical.json"
  cmp -s "$TMP/remote-mapping-canonical.json" "$TMP/manifest-mapping-canonical.json" \
    || fail "release mapping content differs from aggregate release.json"
}

verify_uploaded_assets() {
  local state="$1" bundle="$2"
  local expected="$TMP/expected-assets" actual="$TMP/actual-assets"
  jq -r '.artifacts[] | [.name, ("sha256:" + .sha256), (.size | tostring), "uploaded"] | @tsv' \
    "$bundle/release.json" > "$expected"
  printf 'release.json\tsha256:%s\t%s\tuploaded\n' \
    "$(sha256sum "$bundle/release.json" | awk '{print $1}')" \
    "$(stat -c '%s' "$bundle/release.json")" >> "$expected"
  LC_ALL=C sort -o "$expected" "$expected"
  jq -r '.assets[] | [.name, .digest, (.size | tostring), .state] | @tsv' "$state" \
    | LC_ALL=C sort > "$actual"
  if ! cmp -s "$expected" "$actual"; then
    echo "publish-release: uploaded asset set does not match the aggregate bundle" >&2
    diff -u "$expected" "$actual" >&2 || true
    exit 1
  fi
}

publish_bundle() {
  [ "$#" -eq 1 ] || fail "usage: publish-release.sh publish <bundle-dir>"
  local bundle="$1" manifest="$1/release.json"
  "$SCRIPT_DIR/aggregate-release.sh" validate-bundle "$bundle"
  local version commit repository
  version="$(jq -er '.version' "$manifest")"
  commit="$(jq -er '.mapping.commit' "$manifest")"
  repository="$(jq -er '.mapping.repository' "$manifest")"
  validate_version "$version"
  [ "$repository" = "$REPOSITORY" ] || fail "bundle belongs to $repository, not $REPOSITORY"
  verify_mapping_commit "$manifest"

  local tag_state="$TMP/tag.json"
  if api_optional "repos/$REPOSITORY/git/ref/tags/$version" "$tag_state"; then
    [ "$(jq -er '.object.sha' "$tag_state")" = "$commit" ] \
      || fail "$version already points to another release mapping"
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    jq -n --arg ref "refs/tags/$version" --arg sha "$commit" '{ref: $ref, sha: $sha}' \
      | gh api --method POST "repos/$REPOSITORY/git/refs" --input - >/dev/null
  fi

  local release_state="$TMP/release.json"
  if api_optional "repos/$REPOSITORY/releases/tags/$version" "$release_state"; then
    jq -e '.draft == true' "$release_state" >/dev/null \
      || fail "$version is already published; refusing to replace it"
    gh release edit "$version" --repo "$REPOSITORY" --draft --target "$commit" \
      --title "Kuasar Sandbox $version" --notes-file "$bundle/release-notes.md" >/dev/null
  else
    local rc=$?
    [ "$rc" -eq 4 ] || exit "$rc"
    gh release create "$version" --repo "$REPOSITORY" --draft --verify-tag --target "$commit" \
      --title "Kuasar Sandbox $version" --notes-file "$bundle/release-notes.md" >/dev/null
  fi

  local files=()
  while IFS= read -r name; do
    files+=("$bundle/assets/$name")
  done < <(jq -r '.artifacts[].name' "$manifest")
  files+=("$manifest")
  gh release upload "$version" --repo "$REPOSITORY" --clobber "${files[@]}"

  gh api "repos/$REPOSITORY/releases/tags/$version" > "$release_state"
  verify_uploaded_assets "$release_state" "$bundle"
  local release_id
  release_id="$(jq -er '.id' "$release_state")"
  jq -n '{draft: false, prerelease: false, make_latest: "true"}' \
    | gh api --method PATCH "repos/$REPOSITORY/releases/$release_id" --input - >/dev/null
  gh api "repos/$REPOSITORY/releases/tags/$version" > "$release_state"
  jq -e '.draft == false and .prerelease == false' "$release_state" >/dev/null \
    || fail "$version was not published"
  [ "$(gh api "repos/$REPOSITORY/git/ref/tags/$version" --jq '.object.sha')" = "$commit" ] \
    || fail "published aggregate tag moved from its release mapping commit"
  echo "==> published $version from release mapping $commit"
}

[ -n "$REPOSITORY" ] || fail "GH_REPO or GITHUB_REPOSITORY is required"
[[ "$REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "invalid repository"
for command in gh jq sha256sum; do
  command -v "$command" >/dev/null || fail "$command is required"
done
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "${1:-}" in
  check) shift; check_release "$@" ;;
  publish) shift; publish_bundle "$@" ;;
  *) fail "usage: publish-release.sh <check <release-vX.Y.Z>|publish <bundle-dir>>" ;;
esac
