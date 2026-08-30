#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${2:-}" == repos/kuasar-sandbox/kuasar-sandbox/contents/* ]]; then
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
  'components:' | base64 -w0)"

PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA"
PATH="$TMP/bin:$PATH" bash "$SCRIPT_DIR/validate-preview-line.sh" v1.2.3 "" ""

if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    v1.2.3-preview.20260830 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted mismatched dates" >&2
  exit 1
fi
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$FAKE_MANIFEST" \
  FAKE_STABLE_EXISTS=1 bash "$SCRIPT_DIR/validate-preview-line.sh" \
    v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted a Preview for a closed Stable line" >&2
  exit 1
fi
BAD_MANIFEST="$(printf '%s\n' \
  'version: release-v9.8.8' \
  'preview_version: preview.20260831' \
  'components:' | base64 -w0)"
if PATH="$TMP/bin:$PATH" FAKE_MANIFEST="$BAD_MANIFEST" \
  bash "$SCRIPT_DIR/validate-preview-line.sh" \
    v1.2.3-preview.20260831 release-v9.8.7-preview.20260831 "$AGGREGATE_SHA" \
    >/dev/null 2>&1; then
  echo "test-preview-line: accepted a fabricated aggregate Preview" >&2
  exit 1
fi

echo "test-preview-line: PASS"
