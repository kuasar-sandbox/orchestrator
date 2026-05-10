#!/usr/bin/env bash
#
# e2e_sandbox_snapshot.sh — start a long-running sandbox, snapshot it,
# verify the snapshot bundle is well-formed.
#
# Approach:
#   1. Run python progress counter (writes /tmp/progress every 100ms)
#      under sandbox-ctl run (background)
#   2. Wait until guest is past phase 2 (sees a marker line in log)
#   3. Run sandbox-ctl snapshot --sandbox-id <sid> --output <out>
#   4. Verify <out>/<sid>.snapshot exists, is sparse, contains
#      memory + ZIP at end with config.json/state.json/snapshot.cfg
#   5. Verify <out>/<sha256>.overlay exists and is sparse
#   6. Tear down sandbox

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"

skip() {
    echo
    echo "==> e2e_sandbox_snapshot: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.erofs flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build' and 'make cloud-hypervisor'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    [ "$(id -u)" -eq 0 ] || { echo "$0: must run as root to create $TAP_NAME" >&2; exit 1; }
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

# Need root to make /run/<sid>/ctl.sock + uffd usable.
if [ "$(id -u)" -ne 0 ]; then
    skip "must run as root (cgroup + uffd)"
fi

WORK="$(mktemp -d /tmp/e2e-snapshot-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

IMAGE="${IMAGE:-python:3.12-slim}"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing; provide BLK0_IMAGE"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.erofs"
    docker save "$IMAGE" | "$BIN/flatten-ctl" --output "$BLK0_IMAGE" --no-progress
fi

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
mkfs.ext4 -q -F "$DIFF_FILE"

# Long-running app: print a counter every 250ms; first marker
# "PYBOOT-OK" within ~1s confirms app is up.
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-snap
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: file://$BLK0_IMAGE
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  args: ["-c", "import sys,time\nprint('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor, flush=True)\ni=0\nwhile True:\n    print('TICK', i, flush=True)\n    i+=1\n    time.sleep(0.25)"]
  restart: never
EOF

LOG="$WORK/run.log"
SID="snap-$$"
RUNTIME_ROOT="$WORK/runtime"
mkdir -p "$RUNTIME_ROOT/$SID"
"$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-dir "$RUNTIME_ROOT" \
    --sandbox-id "$SID" \
    > "$LOG" 2>&1 &
SBPID=$!

# Wait for first PYBOOT-OK + a few TICKs (~3s).
echo "==> waiting for app to start..."
for i in $(seq 1 600); do
    if grep -q "TICK 5" "$LOG" 2>/dev/null; then break; fi
    if ! kill -0 "$SBPID" 2>/dev/null; then
        echo "==> sandbox-ctl exited early"; tail -30 "$LOG"; exit 1
    fi
    sleep 0.05
done
if ! grep -q "TICK 5" "$LOG"; then
    echo "==> timeout waiting for guest app"; tail -40 "$LOG"; kill -TERM "$SBPID" 2>/dev/null; exit 1
fi
echo "==> guest app running, taking snapshot"

OUT="$WORK/snap-out"
mkdir -p "$OUT"
"$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID" \
    --output "$OUT" \
    --run-dir "$RUNTIME_ROOT" \
    --resume 2>&1 | tee "$WORK/snap.log"

# Tear down sandbox
kill -TERM "$SBPID" 2>/dev/null || true
wait "$SBPID" 2>/dev/null || true

# Validate outputs
echo "==> validating snapshot bundle"
SNAP_FILE="$OUT/$SID.snapshot"
[ -f "$SNAP_FILE" ] || { echo "FAIL: no $SID.snapshot"; ls -la "$OUT"; exit 1; }
OVERLAY_FILE=$(ls "$OUT"/*.overlay 2>/dev/null | head -1)
[ -n "$OVERLAY_FILE" ] && [ -f "$OVERLAY_FILE" ] || { echo "FAIL: no <sha256>.overlay"; ls -la "$OUT"; exit 1; }

# Sizes
SNAP_LOGICAL=$(stat -c%s "$SNAP_FILE")
SNAP_PHYS=$(($(stat -c%b "$SNAP_FILE") * 512))
DISK_LOGICAL=$(stat -c%s "$OVERLAY_FILE")
DISK_PHYS=$(($(stat -c%b "$OVERLAY_FILE") * 512))

echo "    $SID.snapshot:   logical=$(numfmt --to=iec $SNAP_LOGICAL) physical=$(numfmt --to=iec $SNAP_PHYS)"
echo "    $(basename $OVERLAY_FILE): logical=$(numfmt --to=iec $DISK_LOGICAL) physical=$(numfmt --to=iec $DISK_PHYS)"

# <sid>.snapshot logical = ramSize (512 MiB) + ZIP (~tens of KB)
# physical should be much smaller (only resident pages + ZIP)
if [ "$SNAP_PHYS" -ge "$SNAP_LOGICAL" ]; then
    echo "==> WARN: snapshot not sparse (phys >= logical) — fs may not preserve sparseness"
fi
if [ "$SNAP_PHYS" -lt $((1 << 20)) ]; then
    echo "==> FAIL: snapshot has < 1 MiB content (snapshot probably empty)"; exit 1
fi

# Validate ZIP at end (memory section + ZIP). unzip warns about the
# memory prefix and returns non-zero, but stdout is correct — capture
# once and grep without piping unzip's exit code into the conditional.
ZIP_LIST=$(unzip -l "$SNAP_FILE" 2>&1 || true)
for need in "config.json" "state.json" "snapshot.cfg"; do
    if ! grep -qF "$need" <<<"$ZIP_LIST"; then
        echo "==> FAIL: trailing ZIP missing $need"
        echo "$ZIP_LIST" | head -20
        exit 1
    fi
done
echo "==> PASS: trailing ZIP contains config.json, state.json, snapshot.cfg"

# Validate <sha256>.overlay basename matches its content sha256
OVERLAY_BASENAME=$(basename "$OVERLAY_FILE" .overlay)
ACTUAL_SHA=$(sha256sum "$OVERLAY_FILE" | awk '{print $1}')
if [ "$OVERLAY_BASENAME" != "$ACTUAL_SHA" ]; then
    echo "==> FAIL: overlay basename '$OVERLAY_BASENAME' != sha256 '$ACTUAL_SHA'"
    exit 1
fi
echo "==> PASS: overlay basename matches content sha256"

# Validate overlay is recognizable as ext4
if command -v file >/dev/null && file "$OVERLAY_FILE" 2>&1 | grep -qiE "ext4|ext.* filesystem"; then
    echo "==> PASS: overlay is ext4 filesystem"
else
    echo "==> WARN: overlay not recognized as ext4 by file(1)"
fi

echo "==> e2e_sandbox_snapshot: OK"
