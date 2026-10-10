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

[[ "$AGGREGATE" =~ ^release-v[0-9]+\.[0-9]+\.[0-9]+-preview\.([0-9]{8}(\.[1-9][0-9]*)?)$ ]] || {
  echo "preview releases require aggregate_version=release-vX.Y.Z-preview.YYYYMMDD[.N]" >&2
  exit 1
}
AGGREGATE_DATE="${BASH_REMATCH[1]}"
[[ "$AGGREGATE_SHA" =~ ^[0-9a-f]{40}$ ]] || {
  echo "aggregate-sha must be a full lowercase SHA" >&2
  exit 1
}
[[ "$TAG" =~ -preview\.([0-9]{8}(\.[1-9][0-9]*)?)$ ]] || {
  echo "component Preview tag must end in -preview.YYYYMMDD[.N]" >&2
  exit 1
}
[ "${BASH_REMATCH[1]}" = "$AGGREGATE_DATE" ] || {
  echo "component and aggregate Preview dates and revisions must match" >&2
  exit 1
}
STABLE="${AGGREGATE%-preview.*}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
# Publication consumes the preflight evidence whose digest is a trusted job output.
if [ -n "${PREVIEW_EVIDENCE_FILE:-}" ]; then
  python3 -B - "$PREVIEW_EVIDENCE_FILE" "${PREVIEW_EVIDENCE_DIGEST:?}" "$UNIT" "$TAG" "$AGGREGATE" "$AGGREGATE_SHA" "$TMP/daily-preview.yaml" <<'PYCODE'
import hashlib, json, sys
from pathlib import Path
path, digest, unit, tag, aggregate, sha, output = sys.argv[1:]
data = Path(path).read_bytes()
if hashlib.sha256(data).hexdigest() != digest:
    raise SystemExit('admitted Preview evidence digest mismatch')
record = json.loads(data)
if {key: record.get(key) for key in ('unit', 'tag', 'aggregate', 'sha')} != dict(unit=unit, tag=tag, aggregate=aggregate, sha=sha):
    raise SystemExit('admitted Preview evidence identity mismatch')
Path(output).write_text(record['manifest'])
PYCODE
else
# The commit is observed evidence, never a Contents retrieval key. Admit a
# trusted branch by identity and reject movement around the manifest read.
aggregate_line="${AGGREGATE#release-v}"
aggregate_line="${aggregate_line%%-preview.*}"
aggregate_line="${aggregate_line%.*}"
manifest_ref=
for branch in main "release/v$aggregate_line.x"; do
  if observed=$(gh api "repos/kuasar-sandbox/kuasar-sandbox/git/ref/heads/$branch" \
      --jq 'select(.object.type == "commit") | .object.sha' 2> "$TMP/ref.error"); then
    if [ "$observed" = "$AGGREGATE_SHA" ]; then manifest_ref=$branch; break; fi
  elif ! grep -q '(HTTP 404)' "$TMP/ref.error"; then
    cat "$TMP/ref.error" >&2; exit 1
  fi
done
[ -n "$manifest_ref" ] || { echo 'aggregate identity is not an admitted branch HEAD' >&2; exit 1; }
gh api \
  "repos/kuasar-sandbox/kuasar-sandbox/contents/releases/daily-preview.yaml?ref=$manifest_ref" \
  --jq .content | tr -d '\n' | base64 -d > "$TMP/daily-preview.yaml"
[ "$(gh api "repos/kuasar-sandbox/kuasar-sandbox/git/ref/heads/$manifest_ref" \
    --jq 'select(.object.type == "commit") | .object.sha')" = "$AGGREGATE_SHA" ] \
  || { echo 'aggregate branch moved during manifest admission' >&2; exit 1; }
fi
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

if [ -n "${PREVIEW_EVIDENCE_OUTPUT:-}" ]; then
  python3 -B - "$TMP/daily-preview.yaml" "$PREVIEW_EVIDENCE_OUTPUT" "$UNIT" "$TAG" "$AGGREGATE" "$AGGREGATE_SHA" "${GITHUB_OUTPUT:?}" <<'PYCODE'
import hashlib, json, sys
from pathlib import Path
manifest, output, unit, tag, aggregate, sha, github_output = sys.argv[1:]
data = (json.dumps(dict(unit=unit, tag=tag, aggregate=aggregate, sha=sha,
                       manifest=Path(manifest).read_text()), sort_keys=True, separators=(',', ':')) + '\n').encode()
with Path(output).open('xb') as stream: stream.write(data)
with Path(github_output).open('a') as stream:
    stream.write('preview_manifest_digest=' + hashlib.sha256(data).hexdigest() + '\n')
PYCODE
fi
