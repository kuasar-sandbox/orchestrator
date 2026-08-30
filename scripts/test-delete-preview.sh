#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
cat > "$TMP/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"releases?per_page=100"* ]]; then
  jq -cn --arg tag "${FAKE_TAG:?}" --arg target "${FAKE_TARGET:?}"     '[[{id: 17, tag_name: $tag, target_commitish: $target,
        draft: true, prerelease: false, assets: []}]]'
  exit 0
fi
if [[ "$*" == *"/git/ref/tags/"* ]]; then
  echo 'gh: Not Found (HTTP 404)' >&2
  exit 1
fi
if [[ "$*" == *"--method DELETE"* ]]; then
  printf '%s\n' "$*" >> "${FAKE_DELETE_LOG:?}"
  exit 0
fi
echo "unexpected gh invocation: $*" >&2
exit 1
EOF
chmod +x "$TMP/bin/gh"

TAG="v9.8.7-preview.20260831"
SOURCE_SHA=1111111111111111111111111111111111111111
DELETE_LOG="$TMP/deletes"

if PATH="$TMP/bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/orchestrator   FAKE_TAG="$TAG" FAKE_TARGET=2222222222222222222222222222222222222222   FAKE_DELETE_LOG="$DELETE_LOG"   bash "$SCRIPT_DIR/delete-preview.sh" orchestrator "$TAG" "$SOURCE_SHA" incomplete   >/dev/null 2>&1; then
  echo "test-delete-preview: deleted a Release without source ownership" >&2
  exit 1
fi
[ ! -e "$DELETE_LOG" ]

PATH="$TMP/bin:$PATH" GITHUB_REPOSITORY=kuasar-sandbox/orchestrator   FAKE_TAG="$TAG" FAKE_TARGET="$SOURCE_SHA" FAKE_DELETE_LOG="$DELETE_LOG"   bash "$SCRIPT_DIR/delete-preview.sh" orchestrator "$TAG" "$SOURCE_SHA" incomplete   >/dev/null

grep -q 'releases/17' "$DELETE_LOG"
if grep -q 'git/refs/tags' "$DELETE_LOG"; then
  echo "test-delete-preview: attempted to delete an absent tag" >&2
  exit 1
fi

echo "test-delete-preview: PASS"
