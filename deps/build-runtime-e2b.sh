#!/usr/bin/env bash
#
# build-runtime-e2b.sh — assemble sandbox-runtime-e2b.erofs: extract a base
# sandbox-runtime.erofs, inject envd at /opt/sandbox-runtime/bin/envd (which
# sandbox-init auto-bind-mounts into the guest at the same path), and repack
# deterministically. Pure shell over fsck.erofs + mkfs.erofs — no binary needed.
#
#   build-runtime-e2b.sh --base <erofs> --envd <bin> [--out <path>]
#                        [--fsck-erofs <bin>] [--mkfs-erofs <bin>]
set -euo pipefail

BASE="" ENVD="" OUT="sandbox-runtime-e2b.erofs" FSCK="fsck.erofs" MKFS="mkfs.erofs"
while [ $# -gt 0 ]; do
  case "$1" in
    --base)        BASE="$2";  shift 2 ;;
    --envd)        ENVD="$2";  shift 2 ;;
    --out)         OUT="$2";   shift 2 ;;
    --fsck-erofs)  FSCK="$2";  shift 2 ;;
    --mkfs-erofs)  MKFS="$2";  shift 2 ;;
    *) echo "build-runtime-e2b: unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ -n "$BASE" ] && [ -n "$ENVD" ] || {
  echo "usage: build-runtime-e2b.sh --base <erofs> --envd <bin> [--out <path>] [--fsck-erofs <bin>] [--mkfs-erofs <bin>]" >&2
  exit 2
}

tmp="$(mktemp -d /tmp/runtime-e2b-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT

# run quietly: fsck/mkfs emit benign chatter on stderr (non-root chown warnings;
# the no-compression mkfs notes -Ededupe falls back to chunk dedup). Capture it
# and surface only on a real (non-zero) failure.
run() {
  local log; log="$(mktemp)"
  if ! "$@" 2>"$log"; then cat "$log" >&2; rm -f "$log"; exit 1; fi
  rm -f "$log"
}

run "$FSCK" --extract="$tmp" --force --overwrite --preserve-owner --preserve-perms "$BASE"

mkdir -p "$tmp/opt/sandbox-runtime/bin"
cp "$ENVD" "$tmp/opt/sandbox-runtime/bin/envd"
chmod 0755 "$tmp/opt/sandbox-runtime/bin/envd"

# Deterministic flags match flatten-ctl / sandbox-runtime's mkfs invocation.
run "$MKFS" -Ededupe --chunksize=4096 --all-root -T0 -b4096 -x-1 \
  -U 00000000-0000-0000-0000-000000000000 --quiet "$OUT" "$tmp"

# virtio-pmem requires a 2 MiB-aligned backing file (cloud-hypervisor rejects
# PmemSizeNotAligned). EROFS self-describes its extent in the superblock, so the
# sparse tail padding is invisible to the guest mount. Matches sandbox-runtime's
# base erofs build.
actual="$(stat -c %s "$OUT")"
aligned="$(( (actual + 2097151) / 2097152 * 2097152 ))"
[ "$aligned" = "$actual" ] || truncate -s "$aligned" "$OUT"

echo "$OUT"
