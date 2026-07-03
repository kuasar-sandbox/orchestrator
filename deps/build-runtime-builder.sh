#!/usr/bin/env bash
#
# build-runtime-builder.sh — assemble sandbox-runtime-builder.erofs: the
# build-sandbox guest runtime flavor. Extends the e2b flavor's recipe by
# injecting the build toolchain next to envd under /opt/sandbox-runtime/bin/
# (which sandbox-init auto-bind-mounts into the guest app root at the same
# path, and which export --skip-mounts therefore excludes from any image the
# guest builds):
#
#   envd          e2b data plane (run-builder drives RUN steps through it)
#   flatten-ctl   in-guest image import/export (+ mountpoint, tar tooling)
#   mkfs.erofs    flatten's mkfs backend (static build from guest-runtime/native-deps)
#
#   build-runtime-builder.sh --base <erofs> --envd <bin> --flatten-ctl <bin>
#                            --mkfs-binary <bin> [--out <path>]
#                            [--fsck-erofs <bin>] [--mkfs-erofs <bin>]
set -euo pipefail

BASE="" ENVD="" FLATTEN="" MKFSBIN="" OUT="sandbox-runtime-builder.erofs" FSCK="fsck.erofs" MKFS="mkfs.erofs"
while [ $# -gt 0 ]; do
  case "$1" in
    --base)         BASE="$2";    shift 2 ;;
    --envd)         ENVD="$2";    shift 2 ;;
    --flatten-ctl)  FLATTEN="$2"; shift 2 ;;
    --mkfs-binary)  MKFSBIN="$2"; shift 2 ;;
    --out)          OUT="$2";     shift 2 ;;
    --fsck-erofs)   FSCK="$2";    shift 2 ;;
    --mkfs-erofs)   MKFS="$2";    shift 2 ;;
    *) echo "build-runtime-builder: unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ -n "$BASE" ] && [ -n "$ENVD" ] && [ -n "$FLATTEN" ] && [ -n "$MKFSBIN" ] || {
  echo "usage: build-runtime-builder.sh --base <erofs> --envd <bin> --flatten-ctl <bin> --mkfs-binary <bin> [--out <path>] [--fsck-erofs <bin>] [--mkfs-erofs <bin>]" >&2
  exit 2
}

tmp="$(mktemp -d /tmp/runtime-builder-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT

run() {
  local log; log="$(mktemp)"
  if ! "$@" 2>"$log"; then cat "$log" >&2; rm -f "$log"; exit 1; fi
  rm -f "$log"
}

run "$FSCK" --extract="$tmp" --force --overwrite --preserve-owner --preserve-perms "$BASE"

mkdir -p "$tmp/opt/sandbox-runtime/bin"
cp "$ENVD"    "$tmp/opt/sandbox-runtime/bin/envd"
cp "$FLATTEN" "$tmp/opt/sandbox-runtime/bin/flatten-ctl"
cp "$MKFSBIN" "$tmp/opt/sandbox-runtime/bin/mkfs.erofs"
chmod 0755 "$tmp/opt/sandbox-runtime/bin/envd" \
           "$tmp/opt/sandbox-runtime/bin/flatten-ctl" \
           "$tmp/opt/sandbox-runtime/bin/mkfs.erofs"

# Deterministic flags match flatten-ctl / sandbox-runtime's mkfs invocation.
run "$MKFS" -Ededupe --chunksize=4096 --all-root -T0 -b4096 -x-1 \
  -U 00000000-0000-0000-0000-000000000000 --quiet "$OUT" "$tmp"

# virtio-pmem requires a 2 MiB-aligned backing file (cloud-hypervisor rejects
# PmemSizeNotAligned). EROFS self-describes its extent in the superblock, so
# the sparse tail padding is invisible to the guest mount.
actual="$(stat -c %s "$OUT")"
aligned="$(( (actual + 2097151) / 2097152 * 2097152 ))"
[ "$aligned" = "$actual" ] || truncate -s "$aligned" "$OUT"

echo "$OUT"
