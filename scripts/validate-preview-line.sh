#!/usr/bin/env bash

set -euo pipefail

[ "$#" -eq 4 ] || {
  echo "usage: validate-preview-line.sh <unit> <component-tag> <aggregate-version> <aggregate-sha>" >&2
  exit 2
}

UNIT="$1"
TAG="$2"
AGGREGATE="$3"
AGGREGATE_SHA="$4"
if [[ "$TAG" != *-preview.* ]]; then
  exit 0
fi

[[ "$AGGREGATE" =~ ^release-v[0-9]+\.[0-9]+\.[0-9]+-preview\.([0-9]{8})$ ]] || {
  echo "preview releases require aggregate_version=release-vX.Y.Z-preview.YYYYMMDD" >&2
  exit 1
}
AGGREGATE_DATE="${BASH_REMATCH[1]}"
[[ "$AGGREGATE_SHA" =~ ^[0-9a-f]{40}$ ]] || {
  echo "aggregate-sha must be a full lowercase SHA" >&2
  exit 1
}
[[ "$TAG" =~ -preview\.([0-9]{8})$ ]] || {
  echo "component Preview tag must end in -preview.YYYYMMDD" >&2
  exit 1
}
[ "${BASH_REMATCH[1]}" = "$AGGREGATE_DATE" ] || {
  echo "component and aggregate Preview dates must match" >&2
  exit 1
}
STABLE="${AGGREGATE%-preview.*}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
gh api \
  "repos/kuasar-sandbox/kuasar-sandbox/contents/releases/daily-preview.yaml?ref=$AGGREGATE_SHA" \
  --jq .content | tr -d '\n' | base64 -d > "$TMP/daily-preview.yaml"
MANIFEST_BASE="$(awk '/^version:[[:space:]]+/ {print $2}' "$TMP/daily-preview.yaml")"
MANIFEST_PREVIEW="$(awk '/^preview_version:[[:space:]]+/ {print $2}' "$TMP/daily-preview.yaml")"
MANIFEST_COMPONENT="$(awk -v key="$UNIT:" '
  /^components:[[:space:]]*$/ {inside = 1; next}
  /^[^[:space:]]/ {inside = 0}
  inside && substr($0, 1, 2) == "  " &&
    substr($0, 3, 1) !~ /[[:space:]]/ && $1 == key {print $2}
' "$TMP/daily-preview.yaml")"
[ "$(awk '/^version:[[:space:]]+/ {count++} END {print count + 0}' "$TMP/daily-preview.yaml")" -eq 1 ]
[ "$(awk '/^preview_version:[[:space:]]+/ {count++} END {print count + 0}' "$TMP/daily-preview.yaml")" -eq 1 ]
[ "$(awk '/^components:[[:space:]]*$/ {count++} END {print count + 0}' \
  "$TMP/daily-preview.yaml")" -eq 1 ]
[ "$(awk -v key="$UNIT:" '
  /^components:[[:space:]]*$/ {inside = 1; next}
  /^[^[:space:]]/ {inside = 0}
  inside && substr($0, 1, 2) == "  " &&
    substr($0, 3, 1) !~ /[[:space:]]/ && $1 == key {count++}
  END {print count + 0}
' "$TMP/daily-preview.yaml")" -eq 1 ]
[ "$MANIFEST_BASE-$MANIFEST_PREVIEW" = "$AGGREGATE" ] || {
  echo "$AGGREGATE is not selected by platform commit $AGGREGATE_SHA" >&2
  exit 1
}
[ "$MANIFEST_COMPONENT" = "$TAG" ] || {
  echo "$TAG is not selected as $UNIT by platform commit $AGGREGATE_SHA" >&2
  exit 1
}

if gh api "repos/kuasar-sandbox/kuasar-sandbox/releases/tags/$STABLE" \
  > "$TMP/stable" 2> "$TMP/stable.error"; then
  echo "$STABLE is closed; refusing to recreate a component Preview" >&2
  exit 1
fi
if ! grep -q '(HTTP 404)' "$TMP/stable.error"; then
  cat "$TMP/stable.error" >&2
  exit 1
fi
