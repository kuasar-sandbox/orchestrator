#!/usr/bin/env bash
#
# Ensure a zot OCI registry binary exists for release-builder e2e/demo runs.
# zot is a host-side test tool, not a guest-runtime artifact.
#
# Inputs:
#   BINDIR            Destination directory, usually release-builder/bin/<arch>.
#   TARGET_ARCH       x86_64 | aarch64. Default: uname -m.
#   ZOT_VERSION       Release tag. Default: v2.1.17.
#   ZOT_URL           Optional explicit binary URL.
#   ZOT_SHA256_URL    Optional explicit checksums URL.
#   TARBALL_CACHE     Optional download cache.

set -euo pipefail

log() { echo "==> $*" >&2; }
die() { echo "ensure-zot: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 not found"; }

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

: "${ZOT_VERSION:=v2.1.17}"
: "${BINDIR:=$(pwd)/bin/$TARGET_ARCH}"
: "${TARBALL_CACHE:=$(pwd)/build/tarball}"

asset="zot-linux-${GO_ARCH}-minimal"
: "${ZOT_URL:=https://github.com/project-zot/zot/releases/download/${ZOT_VERSION}/${asset}}"
: "${ZOT_SHA256_URL:=https://github.com/project-zot/zot/releases/download/${ZOT_VERSION}/checksums.sha256.txt}"

out_bin="$BINDIR/zot"
if [ -x "$out_bin" ]; then
    log "already available: $out_bin"
    exit 0
fi

mkdir -p "$BINDIR"
if host_zot="$(command -v zot 2>/dev/null)" && [ -x "$host_zot" ]; then
    log "copying host zot: $host_zot -> $out_bin"
    cp -f "$host_zot" "$out_bin"
    chmod 0755 "$out_bin"
    exit 0
fi

need curl
need sha256sum
mkdir -p "$TARBALL_CACHE"

cache="$TARBALL_CACHE/$asset"
checksums="$TARBALL_CACHE/zot-${ZOT_VERSION#v}-checksums.sha256.txt"

if [ -f "$cache" ]; then
    log "zot binary cache hit: $cache"
else
    log "downloading $ZOT_URL -> $cache"
    curl -fL --retry 3 -o "$cache.tmp" "$ZOT_URL"
    mv "$cache.tmp" "$cache"
fi

if [ -f "$checksums" ]; then
    log "zot checksum cache hit: $checksums"
else
    log "downloading $ZOT_SHA256_URL -> $checksums"
    curl -fL --retry 3 -o "$checksums.tmp" "$ZOT_SHA256_URL"
    mv "$checksums.tmp" "$checksums"
fi

expected="$(awk -v name="$asset" '{entry=$2; sub(/^\*/, "", entry); if (entry == name) print $1}' "$checksums")"
[ -n "$expected" ] || die "checksum for $asset not found in $checksums"
actual="$(sha256sum "$cache" | awk '{print $1}')"
[ "$actual" = "$expected" ] || die "sha256 mismatch for $asset"

cp -f "$cache" "$out_bin"
chmod 0755 "$out_bin"
log "installed $out_bin"
