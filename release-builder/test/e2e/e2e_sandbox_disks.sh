#!/usr/bin/env bash
#
# e2e_sandbox_disks.sh — boot.disks[] data disks (cold + snapshot/restore).
#
# Boots a sandbox with two data disks beyond the root — one single-disk
# (writable ext4) and one overlay (ro erofs base + rw ext4 upper) — mounted via
# mounts[].type=disk, then verifies:
#   1. device order: root (vda erofs + vdb upper) then data disks (vdc, vdd+vde)
#   2. both data disks mounted at their targets
#   3. the overlay disk's ro erofs base content is readable
#   4. both data disks are writable
#   5. snapshot (--output, destroy) captures one .overlay per writable disk
#   6. restore preserves data written to BOTH data disks (single + overlay)
#   7. a deterministic guest page-cache warm-up followed by a second capture
#      with --drop-caches=false --merge-ref=false keeps the local memory parent
#      as W.from_refs while root and both data-disk parents are still merged
#   8. a missing local memory lower fails before cloud-hypervisor is executed
#   9. restoring W preserves every disk and mincore reports the warm-up file
#      resident before the test reads its contents
#
# Prerequisites (checked; missing → skip, exit 0; REQUIRE_KVM=1 to fail hard):
#   /dev/kvm rw · bin/{cloud-hypervisor,sandbox-ctl,sandbox-init,
#   sandbox-runtime.bundle,flatten-ctl,mkfs.erofs} · $VMLINUX · docker (or
#   BLK0_IMAGE=) · mkfs.ext4 · python3 · root (tap/cgroup/vsock). The default
#   Python guest supplies the test-only mincore probe; a custom IMAGE must also
#   provide /bin/sh, python3, sha256sum, yes, and head.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"
TAP_NAME="${TAP_NAME:-sb-tap0}"

skip() {
    echo
    echo "==> e2e_sandbox_disks: skipping ($*)"
    [ "${REQUIRE_KVM:-0}" = "1" ] && { echo "REQUIRE_KVM=1 set; failing" >&2; exit 1; }
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl mkfs.erofs; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH (apt install e2fsprogs)"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

WORK=$(mktemp -d /tmp/e2e-disks-XXXXXX)
RR=$WORK/runtime; mkdir -p "$RR"
OUT=$WORK/out; mkdir -p "$OUT"
P1=""; P2=""; P3=""; TAP_CREATED=0; HIDDEN_PARENT=""; PARENT_SIBLING=""
cleanup() {
    set +e
    for P in "$P1" "$P2" "$P3"; do [ -n "$P" ] && kill -0 "$P" 2>/dev/null && kill -KILL "$P" 2>/dev/null; done
    pkill -f "cloud-hypervisor.*dk-" 2>/dev/null
    if [ -n "$HIDDEN_PARENT" ] && [ -e "$HIDDEN_PARENT" ] && [ -n "$PARENT_SIBLING" ] && [ ! -e "$PARENT_SIBLING" ]; then
        mv "$HIDDEN_PARENT" "$PARENT_SIBLING"
    fi
    [ "$TAP_CREATED" = 1 ] && ip link del "$TAP_NAME" 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED=1
fi

# ---- artifacts: root base + overlay upper, single data disk, overlay data disk ----
echo "==> building disk artifacts"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; set BLK0_IMAGE="
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE" >/dev/null
    BLK0_IMAGE="$WORK/root.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
truncate -s 512M "$WORK/root-up.ext4"; mkfs.ext4 -q -F "$WORK/root-up.ext4"
truncate -s 256M "$WORK/scratch.ext4"; mkfs.ext4 -q -F "$WORK/scratch.ext4"
mkdir -p "$WORK/ds"; echo "DATASET-OK" > "$WORK/ds/DATASET-OK"
# disk artifacts are tarstream envelopes: build via flatten-ctl (dir source)
MKFS_EROFS_PATH="$BIN/mkfs.erofs" "$BIN/flatten-ctl" export --no-progress \
    --tmpdir "$WORK/tmp" --output "$WORK/dataset.img" "$WORK/ds"
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"
DATASET_REF="$(plaintext_tarstream_ref "$WORK/dataset.img")"
truncate -s 256M "$WORK/dataset-up.ext4"; mkfs.ext4 -q -F "$WORK/dataset-up.ext4"

cat > "$WORK/cold.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-disks }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay: { diff: file://$WORK/root-up.ext4, size: 512MiB }
  disks:
    - { name: scratch, diff_template: file://$WORK/scratch.ext4, diff_size: 256MiB }
    - { name: dataset, base: $DATASET_REF, overlay: { diff_template: file://$WORK/dataset-up.ext4, diff_size: 256MiB } }
mounts:
  - { target: /scratch, type: disk, source: scratch }
  - { target: /data,    type: disk, source: dataset }
launch: { exec: /bin/sleep, args: ["3600"] }
EOF

SID1=dk-1
echo "==> [cold] boot multi-disk sandbox"
timeout -k 10s 120 "$BIN/sandbox-ctl" run --config "$WORK/cold.yaml" --sandbox-id "$SID1" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RR" > "$WORK/run1.log" 2>&1 &
P1=$!
ready() { # $1=sid
    for _ in $(seq 1 90); do
        if timeout -k 2s 6 "$BIN/sandbox-ctl" exec --sandbox-id "$1" --run-root "$RR" -- /bin/sh -c 'echo R' >"$WORK/r.out" 2>/dev/null && grep -q R "$WORK/r.out"; then return 0; fi
        sleep 1
    done
    return 1
}
ready "$SID1" || { echo "FAIL: cold boot not ready"; tail -60 "$WORK/run1.log"; exit 1; }
ex1() { "$BIN/sandbox-ctl" exec --sandbox-id "$SID1" --run-root "$RR" "$@"; }
echo "==> PASS: booted"

# Python's mmap exposes the virtual address without touching the mapped file.
# mincore(2) can therefore inspect guest page-cache residency before the test
# performs its first content read after restore.
MINCORE_PROBE=$(cat <<'PY'
import ctypes
import mmap
import os
import sys

path = sys.argv[1]
with open(path, "rb", buffering=0) as source:
    size = os.fstat(source.fileno()).st_size
    if size <= 0:
        raise SystemExit(f"empty mincore target: {path}")
    mapping = mmap.mmap(source.fileno(), size, access=mmap.ACCESS_COPY)
    try:
        view = (ctypes.c_char * size).from_buffer(mapping)
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            pages = (size + page_size - 1) // page_size
            residency = (ctypes.c_ubyte * pages)()
            libc = ctypes.CDLL(None, use_errno=True)
            mincore = libc.mincore
            mincore.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte))
            mincore.restype = ctypes.c_int
            if mincore(ctypes.addressof(view), size, residency) != 0:
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error), path)
            resident = sum(value & 1 for value in residency)
            print(f"mincore resident={resident}/{pages} path={path}")
            if resident != pages:
                raise SystemExit(f"warm-up file is not fully resident: {resident}/{pages} pages")
        finally:
            del view
    finally:
        mapping.close()
PY
)

guest_mincore_all() { # $1=sid, $2=path
    "$BIN/sandbox-ctl" exec --sandbox-id "$1" --run-root "$RR" -- \
        python3 -c "$MINCORE_PROBE" "$2"
}

echo "==> [1] device list (vda..vde)"
ex1 -- /bin/sh -c 'ls -1 /dev/vd*' > "$WORK/dev.out" 2>&1 || true; sed 's/^/    /' "$WORK/dev.out"
for d in vda vdb vdc vdd vde; do grep -q "/dev/$d" "$WORK/dev.out" || { echo "FAIL: missing /dev/$d"; exit 1; }; done
echo "==> PASS: 5 devices (root 2 + data 3)"

echo "==> [2] mounts + [3] dataset base + [4] writability"
WARM_PATH=/scratch/working-set-cache.bin
WARM_BYTES=$((16 * 1024 * 1024))
ex1 -- /bin/sh -c 'command -v python3 >/dev/null && command -v sha256sum >/dev/null && command -v yes >/dev/null && command -v head >/dev/null' \
    || { echo "FAIL: guest image must provide python3, sha256sum, yes, and head"; exit 1; }
ex1 -- /bin/sh -c 'grep -qE " /scratch | /data " /proc/mounts && echo MOUNTS-OK; cat /data/DATASET-OK; echo S-OK > /scratch/persist && cat /scratch/persist; echo D-OK > /data/persist && cat /data/persist' > "$WORK/chk.out" 2>&1 || true
sed 's/^/    /' "$WORK/chk.out"
for m in MOUNTS-OK DATASET-OK S-OK D-OK; do grep -q "$m" "$WORK/chk.out" || { echo "FAIL: missing $m"; exit 1; }; done
ex1 -- /bin/sh -c 'yes KUASAR-WORKING-SET | head -c "$1" > "$2"; sync; sha256sum "$2"' sh "$WARM_BYTES" "$WARM_PATH" \
    >"$WORK/warm-create.out" 2>&1 || { cat "$WORK/warm-create.out"; echo "FAIL: create deterministic warm-up file"; exit 1; }
WARM_SHA=$(awk 'NF >= 2 {print $1; exit}' "$WORK/warm-create.out")
[[ "$WARM_SHA" =~ ^[0-9a-f]{64}$ ]] || { cat "$WORK/warm-create.out"; echo "FAIL: invalid warm-up checksum"; exit 1; }
echo "==> PASS: mounted, dataset base readable, both data disks writable"

echo "==> [5] snapshot (--output, destroys sandbox)"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID1" --output "$OUT" --run-root "$RR" 2>&1 | sed 's/^/    /'
wait "$P1" 2>/dev/null || true; P1=""
SNAP="$OUT/$SID1.snapshot"
[ -f "$SNAP" ] || { echo "FAIL: no snapshot bundle"; ls -la "$OUT"; exit 1; }
NOVL=$(ls -1 "$OUT"/*.overlay 2>/dev/null | wc -l)
[ "$NOVL" = 3 ] || { echo "FAIL: expected 3 overlay artifacts (root upper + scratch + dataset upper), got $NOVL"; exit 1; }
echo "==> PASS: 3 writable disks captured as separate overlays"

echo "==> [6] restore + verify persistence"
truncate -s 512M "$WORK/root-r.ext4"; mkfs.ext4 -q -F "$WORK/root-r.ext4"
truncate -s 256M "$WORK/dataset-r.ext4"; mkfs.ext4 -q -F "$WORK/dataset-r.ext4"
cat > "$WORK/restore.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-disks-r }
boot:
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay: { diff: file://$WORK/root-r.ext4, size: 512MiB }
  disks:
    - { name: scratch }
    - { name: dataset, base: $DATASET_REF, overlay: { diff: file://$WORK/dataset-r.ext4, size: 256MiB } }
EOF
SID2=dk-2
timeout -k 10s 120 "$BIN/sandbox-ctl" run --restore "$SNAP" --config "$WORK/restore.yaml" --sandbox-id "$SID2" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RR" > "$WORK/run2.log" 2>&1 &
P2=$!
ready "$SID2" || { echo "FAIL: restore not ready"; tail -60 "$WORK/run2.log"; exit 1; }
"$BIN/sandbox-ctl" exec --sandbox-id "$SID2" --run-root "$RR" -- /bin/sh -c 'cat /scratch/persist /data/persist /data/DATASET-OK' > "$WORK/post.out" 2>&1 || true
sed 's/^/    /' "$WORK/post.out"
grep -q S-OK "$WORK/post.out" || { echo "FAIL: single-disk data lost across snapshot/restore"; tail -60 "$WORK/run2.log"; exit 1; }
grep -q D-OK "$WORK/post.out" || { echo "FAIL: overlay-disk data lost across snapshot/restore"; tail -60 "$WORK/run2.log"; exit 1; }
grep -q DATASET-OK "$WORK/post.out" || { echo "FAIL: dataset erofs base lost"; exit 1; }
echo "==> PASS: both data disks + dataset base survived snapshot→restore"

# Establish the working set from a known cold guest page cache. The first
# mincore assertion proves the warm-up itself succeeded before W is captured.
"$BIN/sandbox-ctl" exec --sandbox-id "$SID2" --run-root "$RR" -- /bin/sh -c \
    'sync && echo 3 > /proc/sys/vm/drop_caches && cat "$1" >/dev/null' sh "$WARM_PATH" \
    >"$WORK/warm-read.out" 2>&1 || { cat "$WORK/warm-read.out"; echo "FAIL: deterministic warm-up"; exit 1; }
guest_mincore_all "$SID2" "$WARM_PATH" >"$WORK/warm-before-snapshot.out" 2>&1 \
    || { cat "$WORK/warm-before-snapshot.out"; echo "FAIL: warm-up file was not resident before W capture"; exit 1; }
sed 's/^/    /' "$WORK/warm-before-snapshot.out"
echo "==> PASS: deterministic warm-up populated the guest page cache"

# ---- [7] working-set capture: memory stacks; every local disk merges ------
# Give each restored writable disk a W-only delta. Without this, a successful
# content-addressed merge can legitimately reproduce the parent's top digest,
# making top-identity checks unable to distinguish merge from stacking.
"$BIN/sandbox-ctl" exec --sandbox-id "$SID2" --run-root "$RR" -- /bin/sh -c \
    'echo ROOT-W-OK > /working-set-root; echo SCRATCH-W-OK > /scratch/working-set; echo DATA-W-OK > /data/working-set; cat /working-set-root /scratch/working-set /data/working-set' \
    >"$WORK/w-write.out" 2>&1 || true
for marker in ROOT-W-OK SCRATCH-W-OK DATA-W-OK; do
    grep -q "$marker" "$WORK/w-write.out" \
        || { echo "FAIL: could not write $marker before W capture"; cat "$WORK/w-write.out"; exit 1; }
done
WOUT="$WORK/working-set"; mkdir -p "$WOUT"
# A non-merged local memory parent is an explicit sibling dependency. Place it
# beside W as the sandboxer local-artifact contract requires; the product does
# not copy or publish artifacts implicitly.
PARENT_MEMORY_ARTIFACT="$(readlink -f "$SNAP")"
[ -f "$PARENT_MEMORY_ARTIFACT" ] || { echo "FAIL: parent snapshot target is missing: $PARENT_MEMORY_ARTIFACT"; exit 1; }
PARENT_MEMORY_BASENAME="$(basename "$PARENT_MEMORY_ARTIFACT")"
ln "$PARENT_MEMORY_ARTIFACT" "$WOUT/$PARENT_MEMORY_BASENAME"
"$BIN/sandbox-ctl" info --json "$SNAP" >"$WORK/s1-info.json"
echo "==> [7] snapshot restored sandbox with --drop-caches=false --merge-ref=false"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID2" --output "$WOUT" --run-root "$RR" \
    --drop-caches=false --merge-ref=false 2>&1 | sed 's/^/    /'
wait "$P2" 2>/dev/null || true; P2=""
grep -Fq 'quiesce: guest acked (drop_caches=skipped' "$WORK/run2.log" \
    || { tail -60 "$WORK/run2.log"; echo "FAIL: W capture did not report drop_caches=skipped"; exit 1; }
W="$WOUT/$SID2.snapshot"
[ -f "$W" ] || { echo "FAIL: no working-set snapshot $W"; exit 1; }
"$BIN/sandbox-ctl" info --json "$W" >"$WORK/w-info.json"
python3 - "$WORK/s1-info.json" "$WORK/w-info.json" "$PARENT_MEMORY_BASENAME" <<'PY'
import json, os, sys

with open(sys.argv[1], encoding="utf-8") as source:
    parent = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    working = json.load(source)

refs = working.get("FromRefs") or []
if len(refs) != 1 or os.path.basename(refs[0].split("@", 1)[0]) != sys.argv[3]:
    raise SystemExit(f"working-set memory from_refs={refs!r}, want one local parent {sys.argv[3]!r}")

def disk_nodes(doc):
    boot = doc["Boot"]
    return [boot["Root"], *(boot.get("Disks") or [])]

def top(node):
    overlay = node.get("Overlay")
    return overlay["Base"] if overlay else node["Base"]

def chain(node):
    overlay = node.get("Overlay")
    return (overlay.get("BaseFromRefs") if overlay else node.get("BaseFromRefs")) or []

parent_nodes = disk_nodes(parent)
working_nodes = disk_nodes(working)
if len(parent_nodes) != 3 or len(working_nodes) != 3:
    raise SystemExit(f"disk node counts parent={len(parent_nodes)} working={len(working_nodes)}, want root+2 data")
for index, (parent_node, working_node) in enumerate(zip(parent_nodes, working_nodes)):
    parent_top = top(parent_node)
    if top(working_node) == parent_top or parent_top in chain(working_node):
        raise SystemExit(f"disk {index} retained local parent {parent_top!r}; local disks must merge even when memory stacks")
PY
echo "==> PASS: W memory self is independent (one local from_ref); root + two data-disk parents were merged"

# Restore W with fresh writable uppers. Its memory parent is already a sibling
# in WOUT; disk state must come entirely from W's merged disk artifacts.
truncate -s 512M "$WORK/root-w.ext4"; mkfs.ext4 -q -F "$WORK/root-w.ext4"
truncate -s 256M "$WORK/dataset-w.ext4"; mkfs.ext4 -q -F "$WORK/dataset-w.ext4"
cat > "$WORK/restore-w.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-disks-w }
boot:
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay: { diff: file://$WORK/root-w.ext4, size: 512MiB }
  disks:
    - { name: scratch }
    - { name: dataset, base: $DATASET_REF, overlay: { diff: file://$WORK/dataset-w.ext4, size: 256MiB } }
EOF

# The local parent is a mandatory sibling artifact. Hide it recoverably and
# pass a marker wrapper as --ch-binary: a clear preflight error plus an absent
# marker proves restore rejected the graph before starting the VMM.
PARENT_SIBLING="$WOUT/$PARENT_MEMORY_BASENAME"
HIDDEN_PARENT="$WORK/$PARENT_MEMORY_BASENAME.hidden"
mv "$PARENT_SIBLING" "$HIDDEN_PARENT"
VMM_MARKER="$WORK/missing-lower-vmm-started"
CH_WRAPPER="$WORK/cloud-hypervisor-marker"
cat >"$CH_WRAPPER" <<EOF
#!/bin/sh
: > "$VMM_MARKER"
exec "$BIN/cloud-hypervisor" "\$@"
EOF
chmod +x "$CH_WRAPPER"
set +e
timeout -k 5s 30 "$BIN/sandbox-ctl" run --restore "$W" --config "$WORK/restore-w.yaml" \
    --sandbox-id dk-missing-lower --ch-binary "$CH_WRAPPER" --run-root "$RR" \
    >"$WORK/missing-lower.log" 2>&1
MISSING_LOWER_RC=$?
set -e
[ "$MISSING_LOWER_RC" -ne 0 ] || { cat "$WORK/missing-lower.log"; echo "FAIL: restore unexpectedly accepted a missing memory lower"; exit 1; }
grep -Fq 'snapshot memory layer 0' "$WORK/missing-lower.log" \
    || { cat "$WORK/missing-lower.log"; echo "FAIL: missing-lower error did not identify snapshot memory layer 0"; exit 1; }
grep -Fq "$PARENT_MEMORY_BASENAME" "$WORK/missing-lower.log" \
    || { cat "$WORK/missing-lower.log"; echo "FAIL: missing-lower error did not identify $PARENT_MEMORY_BASENAME"; exit 1; }
[ ! -e "$VMM_MARKER" ] \
    || { cat "$WORK/missing-lower.log"; echo "FAIL: cloud-hypervisor started before missing memory lower rejection"; exit 1; }
mv "$HIDDEN_PARENT" "$PARENT_SIBLING"
HIDDEN_PARENT=""
echo "==> PASS: missing local memory lower failed clearly before VMM start"

SID3=dk-3
timeout -k 10s 120 "$BIN/sandbox-ctl" run --restore "$W" --config "$WORK/restore-w.yaml" --sandbox-id "$SID3" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RR" > "$WORK/run3.log" 2>&1 &
P3=$!
ready "$SID3" || { echo "FAIL: working-set restore not ready"; tail -60 "$WORK/run3.log"; exit 1; }
guest_mincore_all "$SID3" "$WARM_PATH" >"$WORK/warm-after-restore.out" 2>&1 \
    || { cat "$WORK/warm-after-restore.out"; tail -60 "$WORK/run3.log"; echo "FAIL: W did not restore the guest page cache"; exit 1; }
sed 's/^/    /' "$WORK/warm-after-restore.out"
# Only after mincore has passed may the test read and verify the file contents.
"$BIN/sandbox-ctl" exec --sandbox-id "$SID3" --run-root "$RR" -- sha256sum "$WARM_PATH" \
    >"$WORK/warm-hash-after-restore.out" 2>&1 || true
grep -Fq "$WARM_SHA  $WARM_PATH" "$WORK/warm-hash-after-restore.out" \
    || { cat "$WORK/warm-hash-after-restore.out"; echo "FAIL: restored warm-up file checksum mismatch"; exit 1; }
echo "==> PASS: W restored the warm-up file resident before its first content read"
"$BIN/sandbox-ctl" exec --sandbox-id "$SID3" --run-root "$RR" -- /bin/sh -c \
    'cat /scratch/persist /data/persist /data/DATASET-OK /working-set-root /scratch/working-set /data/working-set' \
    > "$WORK/w-post.out" 2>&1 || true
sed 's/^/    /' "$WORK/w-post.out"
for marker in S-OK D-OK DATASET-OK ROOT-W-OK SCRATCH-W-OK DATA-W-OK; do
    grep -q "$marker" "$WORK/w-post.out" || { echo "FAIL: W restore lost $marker"; tail -60 "$WORK/run3.log"; exit 1; }
done
echo "==> PASS: local working-set W restored memory, root, and both data disks"

kill -TERM "$P3" 2>/dev/null || true; wait "$P3" 2>/dev/null || true; P3=""
echo
echo "==> e2e_sandbox_disks: OK (working-set cache resident; memory layers; all disks merge and restore)"
