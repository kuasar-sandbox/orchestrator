#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${2:-}" == repos/kuasar-sandbox/kuasar-sandbox/git/ref/heads/* ]]; then
  if [[ -n "${FAKE_READ_MARKER:-}" && -f "$FAKE_READ_MARKER" ]]; then
    printf '%s\n' "${FAKE_AFTER_READ_SHA:-1111111111111111111111111111111111111111}"
  else
    printf '%s\n' "${FAKE_BRANCH_SHA:-1111111111111111111111111111111111111111}"
  fi
  exit 0
fi
if [[ "${2:-}" == repos/kuasar-sandbox/kuasar-sandbox/contents/* ]]; then
  [[ "$2" == *'?ref=main' || "$2" == *'?ref=release/v'* ]] || { echo 'raw SHA source lookup rejected' >&2; exit 1; }
  if [[ -n "${FAKE_READ_MARKER:-}" ]]; then touch "$FAKE_READ_MARKER"; fi
  printf '%s\n' "${FAKE_MANIFEST:?}"
  exit 0
fi
if [ "${FAKE_STABLE_EXISTS:-0}" = 1 ]; then
  printf '{}\n'
  exit 0
fi
echo 'gh: Not Found (HTTP 404)' >&2
exit 1
EOF
chmod +x "$TMP/bin/gh"

AGGREGATE_SHA=1111111111111111111111111111111111111111
FAKE_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.7' \
  'preview_version: preview.20260831' \
  'components:' \
  '    version: ignored-nested-value' \
  '  orchestrator: v1.2.3-preview.20260831' | base64 -w0)"

PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA"
PATH="$TMP/bin:$PATH" bash "$SCRIPT_DIR/validate-preview-line.sh" \
  orchestrator v1.2.3 "" ""

if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260830 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted mismatched dates" >&2
  exit 1
fi
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  FAKE_STABLE_EXISTS=1 bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted a Preview for a closed Stable line" >&2
  exit 1
fi
BAD_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.8' \
  'preview_version: preview.20260831' \
  'components:' \
  '  orchestrator: v1.2.3-preview.20260831' | base64 -w0)"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$BAD_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted a fabricated aggregate Preview" >&2
  exit 1
fi
BAD_COMPONENT_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.7' \
  'preview_version: preview.20260831' \
  'components:' \
  '  orchestrator: v1.2.4-preview.20260831' | base64 -w0)"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$BAD_COMPONENT_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted an unselected component Preview" >&2
  exit 1
fi
OUTSIDE_COMPONENT_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.7' \
  'preview_version: preview.20260831' \
  'metadata:' \
  '  orchestrator: v1.2.3-preview.20260831' \
  'components:' \
  '  connector: v1.2.3-preview.20260831' | base64 -w0)"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$OUTSIDE_COMPONENT_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted a unit outside components" >&2
  exit 1
fi

REVISION_MANIFEST="$(printf '%s' "$FAKE_MANIFEST" | base64 -d \
  | sed 's/preview.20260831/preview.20260831.1/g' | base64 -w0)"
PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$REVISION_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831.1 \
    release-v9.8.7-preview.20260831.1 "$AGGREGATE_SHA"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$REVISION_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    orchestrator v1.2.3-preview.20260831.2 \
    release-v9.8.7-preview.20260831.1 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted mismatched revisions" >&2
  exit 1
fi

if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" FAKE_BRANCH_SHA=2222222222222222222222222222222222222222 \
  bash "$SCRIPT_DIR/validate-preview-line.sh" orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" > "$TMP/moved-before.log" 2>&1; then
  echo 'test-preview-line: accepted unmatched aggregate identity' >&2; exit 1
fi
grep -Fq 'not an admitted branch HEAD' "$TMP/moved-before.log"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" FAKE_READ_MARKER="$TMP/read-marker" \
  FAKE_AFTER_READ_SHA=2222222222222222222222222222222222222222 \
  bash "$SCRIPT_DIR/validate-preview-line.sh" orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" > "$TMP/moved-after.log" 2>&1; then
  echo 'test-preview-line: accepted movement during manifest read' >&2; exit 1
fi
grep -Fq 'moved during manifest admission' "$TMP/moved-after.log"
FAKE_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.7' \
  'preview_version: preview.20260831' \
  'components:' \
  '    version: ignored-nested-value' \
  '  orchestrator: v1.2.3-preview.20260831' | base64 -w0)"
PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  PREVIEW_EVIDENCE_OUTPUT="$TMP/admitted.json" GITHUB_OUTPUT="$TMP/outputs" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" orchestrator v1.2.3-preview.20260831 \
    release-v9.8.7-preview.20260831 "$AGGREGATE_SHA"
FROZEN_DIGEST="$(sed -n 's/^preview_manifest_digest=//p' "$TMP/outputs")"
frozen_env=(PATH="$TMP/bin:$PATH" PREVIEW_EVIDENCE_FILE="$TMP/admitted.json"
  PREVIEW_EVIDENCE_DIGEST="$FROZEN_DIGEST" FAKE_BRANCH_SHA=2222222222222222222222222222222222222222)
# Both branch and manifest have changed; publishing uses the admitted bytes.
env "${frozen_env[@]}" FAKE_MANIFEST=invalid bash "$SCRIPT_DIR/validate-preview-line.sh" \
  orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA"
if env "${frozen_env[@]}" FAKE_STABLE_EXISTS=1 bash "$SCRIPT_DIR/validate-preview-line.sh" \
  orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" >/dev/null 2>&1; then
  echo 'frozen evidence bypassed live Stable closure' >&2; exit 1
fi
if env "${frozen_env[@]}" bash "$SCRIPT_DIR/validate-preview-line.sh" \
  orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 2222222222222222222222222222222222222222 >/dev/null 2>&1; then
  echo 'frozen evidence accepted wrong admitted identity' >&2; exit 1
fi
printf ' ' >> "$TMP/admitted.json"
if env "${frozen_env[@]}" bash "$SCRIPT_DIR/validate-preview-line.sh" \
  orchestrator v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" >/dev/null 2>&1; then
  echo 'frozen evidence accepted tampered manifest' >&2; exit 1
fi
echo "test-preview-line: PASS"
