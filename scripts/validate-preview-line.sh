#!/usr/bin/env bash

set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: validate-preview-line.sh <component-tag> <aggregate-version>" >&2
  exit 2
}

TAG="$1"
AGGREGATE="$2"
if [[ "$TAG" != *-preview.* ]]; then
  exit 0
fi

[[ "$AGGREGATE" =~ ^release-v[0-9]+\.[0-9]+\.[0-9]+-preview\.([0-9]{8})$ ]] || {
  echo "preview releases require aggregate_version=release-vX.Y.Z-preview.YYYYMMDD" >&2
  exit 1
}
AGGREGATE_DATE="${BASH_REMATCH[1]}"
[[ "$TAG" =~ -preview\.([0-9]{8})$ ]] && [ "${BASH_REMATCH[1]}" = "$AGGREGATE_DATE" ] || {
  echo "component and aggregate Preview dates must match" >&2
  exit 1
}
STABLE="${AGGREGATE%-preview.*}"
TMP="$(mktemp)"
if gh api "repos/kuasar-sandbox/kuasar-sandbox/releases/tags/$STABLE" > "$TMP" 2> "$TMP.error"; then
  rm -f "$TMP" "$TMP.error"
  echo "$STABLE is closed; refusing to recreate a component Preview" >&2
  exit 1
fi
if ! grep -q '(HTTP 404)' "$TMP.error"; then
  cat "$TMP.error" >&2
  rm -f "$TMP" "$TMP.error"
  exit 1
fi
rm -f "$TMP" "$TMP.error"
