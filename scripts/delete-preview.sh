#!/usr/bin/env bash

set -euo pipefail

[ "$#" -eq 4 ] || {
  echo "usage: delete-preview.sh <unit> <tag> <source-sha> <incomplete|gc>" >&2
  exit 2
}

UNIT="$1"
TAG="$2"
SOURCE_SHA="$3"
MODE="$4"
REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "delete-preview: $*" >&2
  exit 1
}

[[ "$SOURCE_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "source-sha must be a full lowercase SHA"
[ "$MODE" = incomplete ] || [ "$MODE" = gc ] || fail "mode must be incomplete or gc"

case "$UNIT" in
  accelerator|connector|sandboxer|orchestrator)
    [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}(\.[1-9][0-9]*)?$ ]]       || fail "invalid $UNIT preview tag: $TAG"
    ;;
  runtime)
    [[ "$TAG" =~ ^runtime-v[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}(\.[1-9][0-9]*)?$ ]]       || fail "invalid runtime preview tag: $TAG"
    export RELEASE_KIND=runtime
    ;;
  vmlinux)
    [[ "$TAG" =~ ^vmlinux-v[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}(\.[1-9][0-9]*)?$ ]]       || fail "invalid vmlinux preview tag: $TAG"
    export RELEASE_KIND=vmlinux
    ;;
  *) fail "unknown release unit: $UNIT" ;;
esac

if [ -n "${RELEASE_KIND:-}" ]; then
  ARCHIVE="$("$SCRIPT_DIR/release.sh" archive-name "$RELEASE_KIND" "$TAG" x86_64)"
  ARM_ARCHIVE="$("$SCRIPT_DIR/release.sh" archive-name "$RELEASE_KIND" "$TAG" aarch64)"
else
  ARCHIVE="$("$SCRIPT_DIR/release.sh" archive-name "$TAG" x86_64)"
  ARM_ARCHIVE="$("$SCRIPT_DIR/release.sh" archive-name "$TAG" aarch64)"
fi

gh api --paginate --slurp "repos/$REPOSITORY/releases?per_page=100"   | jq --arg tag "$TAG" '[.[][] | select(.tag_name == $tag)]' > "$TMP/releases"
[ "$(jq 'length' "$TMP/releases")" -le 1 ] || fail "multiple releases use $TAG"

if [ "$(jq 'length' "$TMP/releases")" -eq 1 ]; then
  jq '.[0]' "$TMP/releases" > "$TMP/release"
  jq -e '.prerelease == true or .draft == true' "$TMP/release" >/dev/null     || fail "refusing to delete a non-preview release"
  complete=false
  if jq -e --arg archive "$ARCHIVE" --arg arm "$ARM_ARCHIVE" '
      .draft == false
      and .prerelease == true
      and (([.assets[].name] | sort) as $names |
        $names == (["SHA256SUMS", $archive] | sort) or
        $names == (["SHA256SUMS", $archive, $arm] | sort))
      and all(.assets[]; .state == "uploaded")
    ' "$TMP/release" >/dev/null; then
    complete=true
  fi
  if [ "$MODE" = incomplete ] && [ "$complete" = true ]; then
    fail "refusing incomplete recovery for a complete preview"
  fi
fi

if [ "$(jq 'length' "$TMP/releases")" -eq 1 ]; then
  jq -e --arg source_sha "$SOURCE_SHA" \
    '.[0].target_commitish == $source_sha' "$TMP/releases" >/dev/null \
    || fail "Release target_commitish does not match the Preview Tag"
fi

if gh api "repos/$REPOSITORY/git/ref/tags/$TAG" > "$TMP/ref" 2> "$TMP/ref-error"; then
  [ "$(jq -er '.object.type' "$TMP/ref")" = commit ]     || fail "refusing to delete a non-lightweight tag"
  [ "$(jq -er '.object.sha' "$TMP/ref")" = "$SOURCE_SHA" ]     || fail "$TAG does not point to the expected source commit"
elif grep -q '(HTTP 404)' "$TMP/ref-error"; then
  : > "$TMP/ref"
else
  cat "$TMP/ref-error" >&2
  exit 1
fi

if [ -s "$TMP/releases" ] && [ "$(jq 'length' "$TMP/releases")" -eq 1 ]; then
  gh api --method DELETE     "repos/$REPOSITORY/releases/$(jq -er '.[0].id' "$TMP/releases")" --silent
fi
if [ -s "$TMP/ref" ]; then
  gh api --method DELETE "repos/$REPOSITORY/git/refs/tags/$TAG" --silent
fi

echo "==> deleted $REPOSITORY $TAG release/assets/tag ($MODE)"
