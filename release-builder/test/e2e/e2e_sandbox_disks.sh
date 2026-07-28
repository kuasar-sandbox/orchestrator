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
#
# Prerequisites (checked; missing → skip, exit 0; REQUIRE_KVM=1 to fail hard):
#   /dev/kvm rw · bin/{cloud-hypervisor,sandbox-ctl,sandbox-init,
#   sandbox-runtime.bundle,flatten-ctl,mkfs.erofs} · $VMLINUX · docker (or
#   BLK0_IMAGE=) · mkfs.ext4 · root (tap/cgroup/vsock). Any /bin/sh rootfs works
#   (default base busybox).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-busybox:latest}"
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

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

WORK=$(mktemp -d /tmp/e2e-disks-XXXXXX)
RR=$WORK/runtime; mkdir -p "$RR"
OUT=$WORK/out; mkdir -p "$OUT"
P1=""; P2=""; TAP_CREATED=0
cleanup() {
    set +e
    for P in "$P1" "$P2"; do [ -n "$P" ] && kill -0 "$P" 2>/dev/null && kill -KILL "$P" 2>/dev/null; done
    pkill -f "cloud-hypervisor.*dk-" 2>/dev/null
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
truncate -s 256M "$WORK/dataset-up.ext4"; mkfs.ext4 -q -F "$WORK/dataset-up.ext4"

cat > "$WORK/cold.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-disks }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: file://$BLK0_IMAGE
    overlay: { diff: file://$WORK/root-up.ext4, size: 512MiB }
  disks:
    - { name: scratch, diff_template: file://$WORK/scratch.ext4, diff_size: 256MiB }
    - { name: dataset, base: file://$WORK/dataset.img, overlay: { diff_template: file://$WORK/dataset-up.ext4, diff_size: 256MiB } }
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

echo "==> [1] device list (vda..vde)"
ex1 -- /bin/sh -c 'ls -1 /dev/vd*' > "$WORK/dev.out" 2>&1 || true; sed 's/^/    /' "$WORK/dev.out"
for d in vda vdb vdc vdd vde; do grep -q "/dev/$d" "$WORK/dev.out" || { echo "FAIL: missing /dev/$d"; exit 1; }; done
echo "==> PASS: 5 devices (root 2 + data 3)"

echo "==> [2] mounts + [3] dataset base + [4] writability"
ex1 -- /bin/sh -c 'grep -qE " /scratch | /data " /proc/mounts && echo MOUNTS-OK; cat /data/DATASET-OK; echo S-OK > /scratch/persist && cat /scratch/persist; echo D-OK > /data/persist && cat /data/persist' > "$WORK/chk.out" 2>&1 || true
sed 's/^/    /' "$WORK/chk.out"
for m in MOUNTS-OK DATASET-OK S-OK D-OK; do grep -q "$m" "$WORK/chk.out" || { echo "FAIL: missing $m"; exit 1; }; done
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
    base: file://$BLK0_IMAGE
    overlay: { diff: file://$WORK/root-r.ext4, size: 512MiB }
  disks:
    - { name: scratch }
    - { name: dataset, base: file://$WORK/dataset.img, overlay: { diff: file://$WORK/dataset-r.ext4, size: 256MiB } }
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

kill -TERM "$P2" 2>/dev/null || true; wait "$P2" 2>/dev/null || true; P2=""
echo
echo "==> e2e_sandbox_disks: OK"
