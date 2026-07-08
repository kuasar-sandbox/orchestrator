#!/usr/bin/env bash
#
# Ensure a versitygw binary exists for source-tree e2e/demo runs.
# versitygw is a host-side local S3 test service for builder.files_storage; it
# is not a release artifact.

set -euo pipefail

die() { echo "ensure-versitygw: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
umbrella_dir="$(cd "$script_dir/.." && pwd)"
org_dir="$(cd "$umbrella_dir/../.." && pwd)"

: "${TARGET_ARCH:=$(uname -m)}"
case "$TARGET_ARCH" in
    amd64) TARGET_ARCH=x86_64 ;;
    arm64) TARGET_ARCH=aarch64 ;;
esac
case "$TARGET_ARCH" in
    x86_64) GO_ARCH=amd64 ;;
    aarch64) GO_ARCH=arm64 ;;
    *) die "unsupported TARGET_ARCH=$TARGET_ARCH" ;;
esac

: "${BINDIR:=$umbrella_dir/build/e2e-tools/$TARGET_ARCH}"
: "${BUILD_DIR:=$umbrella_dir/build/e2e-tools-build/$TARGET_ARCH}"
: "${TARBALL_CACHE:=$umbrella_dir/build/tarball}"
: "${VERSITYGW_SRC:=$umbrella_dir/build/src/versitygw}"

out_bin="$BINDIR/versitygw"
if [ -x "$out_bin" ]; then
    log "already available: $out_bin"
    exit 0
fi

mkdir -p "$BINDIR"
if host_vgw="$(command -v versitygw 2>/dev/null)" && [ -x "$host_vgw" ]; then
    log "copying host versitygw: $host_vgw -> $out_bin"
    cp -f "$host_vgw" "$out_bin"
    chmod 0755 "$out_bin"
    exit 0
fi

log "building versitygw for local e2e tools"
TARGET_ARCH="$TARGET_ARCH" \
GO_ARCH="$GO_ARCH" \
BUILD_DIR="$BUILD_DIR" \
BINDIR="$BINDIR" \
TARBALL_CACHE="$TARBALL_CACHE" \
VERSITYGW_SRC="$VERSITYGW_SRC" \
    bash "$org_dir/guest-runtime/native-deps/deps/build-versitygw.sh"
