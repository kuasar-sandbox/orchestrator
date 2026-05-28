#!/usr/bin/env bash
# release.sh — package kuasar-sandbox/bin/$(TARGET_ARCH)/ into a
# download-and-run tarball:
#   kuasar-sandbox-<version>-linux-<arch>.tar.gz
#
# `make build` has already assembled bin/<arch>/ (single source of truth at
# scripts/artifacts.list); this script just copies + tars. The canonical
# invocation is `make release` (depends on `build`).

set -euo pipefail

VERSION="${1:-v0.1.0}"
ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$ARCH" in amd64) ARCH=x86_64 ;; arm64) ARCH=aarch64 ;; esac

UMBRELLA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_BIN="$UMBRELLA_DIR/bin/$ARCH"
NAME="kuasar-sandbox-$VERSION-linux-$ARCH"
OUT="$UMBRELLA_DIR/dist/$NAME"

[ -d "$SRC_BIN" ] && [ -n "$(ls -A "$SRC_BIN" 2>/dev/null || true)" ] || {
  echo "$SRC_BIN is missing or empty — run \`make build\` first." >&2
  exit 1
}

rm -rf "$OUT"
mkdir -p "$OUT/bin"
cp -f "$SRC_BIN"/* "$OUT/bin/"
cp "$UMBRELLA_DIR/README.md" "$OUT/" 2>/dev/null || true

( cd "$UMBRELLA_DIR/dist" && tar czf "$NAME.tar.gz" "$NAME" )
echo "==> $UMBRELLA_DIR/dist/$NAME.tar.gz"
echo "    contents:"; ls -1 "$OUT/bin" | sed 's/^/      /'
