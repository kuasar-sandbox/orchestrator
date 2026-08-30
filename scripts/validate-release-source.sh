#!/usr/bin/env bash

set -euo pipefail

[ "$#" -eq 4 ] || {
  echo "usage: validate-release-source.sh <source-ref> <source-sha> <tag> <unit>" >&2
  exit 2
}

SOURCE_REF="$1"
SOURCE_SHA="$2"
TAG="$3"
UNIT="$4"
REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

fail() {
  echo "release-source: $*" >&2
  exit 1
}

[[ "$SOURCE_REF" = main || "$SOURCE_REF" =~ ^release/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.x$ ]] \
  || fail "source-ref must be main or release/vMAJOR.MINOR.x"
[[ "$SOURCE_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "source-sha must be a full lowercase SHA"

case "$UNIT" in
  accelerator|connector|sandboxer|orchestrator)
    [[ "$TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
      || fail "invalid $UNIT tag: $TAG"
    TAG_MAJOR="${BASH_REMATCH[1]}"
    TAG_MINOR="${BASH_REMATCH[2]}"
    ;;
  runtime)
    [[ "$TAG" =~ ^runtime-v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
      || fail "invalid runtime tag: $TAG"
    TAG_MAJOR="${BASH_REMATCH[1]}"
    TAG_MINOR="${BASH_REMATCH[2]}"
    ;;
  vmlinux)
    [[ "$TAG" =~ ^vmlinux-v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-preview\.[0-9]{8})?$ ]] \
      || fail "invalid vmlinux tag: $TAG"
    TAG_MAJOR="${BASH_REMATCH[1]}"
    TAG_MINOR="${BASH_REMATCH[2]}"
    ;;
  *) fail "unknown release unit: $UNIT" ;;
esac

if [[ "$SOURCE_REF" =~ ^release/v([0-9]+)\.([0-9]+)\.x$ ]]; then
  [ "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}" = "$TAG_MAJOR.$TAG_MINOR" ] \
    || fail "$TAG does not belong to $SOURCE_REF"
fi

REF_SHA="$(gh api "repos/$REPOSITORY/git/ref/heads/$SOURCE_REF" \
  --jq 'select(.object.type == "commit") | .object.sha')"
[ "$REF_SHA" = "$SOURCE_SHA" ] \
  || fail "$SOURCE_REF moved: expected $SOURCE_SHA, found $REF_SHA"

printf '%s\t%s\n' "$SOURCE_REF" "$SOURCE_SHA"
