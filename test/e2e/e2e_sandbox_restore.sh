#!/usr/bin/env bash
#
# e2e_sandbox_restore.sh — chained:
#   1. Cold-start sandbox running a python TICK counter
#   2. Wait until counter reaches a known value (e.g. TICK 10)
#   3. snapshot the sandbox to <out>/sandbox.snapshot + disk.ext4
#   4. Tear down the sandbox
#   5. restore from the snapshot — vCPU should resume the counter
#   6. Confirm the restored process keeps counting from where it
#      stopped (TICK 10+, increases over time)
#
# This validates that vCPU + memory + disk all restored correctly.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$REPO_ROOT/bin"

skip() {
    echo
    echo "==> e2e_sandbox_restore: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.erofs flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
TAP_NAME="${TAP_NAME:-sb-tap0}"
ip link show "$TAP_NAME" >/dev/null 2>&1 || skip "TAP $TAP_NAME missing"
if [ "$(id -u)" -ne 0 ]; then skip "must run as root"; fi

WORK="$(mktemp -d /tmp/e2e-restore-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"' EXIT

IMAGE="${IMAGE:-python:3.12-slim}"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.erofs"
    docker save "$IMAGE" | "$BIN/flatten-ctl" --output "$BLK0_IMAGE" --no-progress
fi

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
mkfs.ext4 -q -F "$DIFF_FILE"

# Counter that prints TICK i on stdout — restored sandbox should
# continue from the snapshotted i value.
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore
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
  args: ["-c", "import sys,time\nprint('PYBOOT-OK', flush=True)\ni=0\nwhile True:\n    print('TICK', i, flush=True)\n    i+=1\n    time.sleep(0.25)"]
  restart: never
EOF

LOG1="$WORK/run1.log"
SID1="r1-$$"
RUNTIME_ROOT="$WORK/runtime"
mkdir -p "$RUNTIME_ROOT/$SID1"
"$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --runtime-root "$RUNTIME_ROOT" \
    --sandbox-id "$SID1" \
    > "$LOG1" 2>&1 &
SBPID1=$!

echo "==> waiting for TICK 10 in run1..."
for i in $(seq 1 600); do
    if grep -qE "^TICK 10[[:space:]]*$" "$LOG1" 2>/dev/null; then break; fi
    if ! kill -0 "$SBPID1" 2>/dev/null; then
        echo "==> sandbox-ctl run1 exited early"; tail -30 "$LOG1"; exit 1
    fi
    sleep 0.05
done
if ! grep -qE "^TICK 10[[:space:]]*$" "$LOG1"; then
    echo "==> timeout waiting for TICK 10"; tail -40 "$LOG1"
    kill -TERM "$SBPID1" 2>/dev/null; exit 1
fi
PRE_SNAP_TICK=$(grep -oE "^TICK [0-9]+" "$LOG1" | tail -1 | awk '{print $2}')
echo "==> guest at TICK $PRE_SNAP_TICK; taking snapshot"

OUT="$WORK/snap-out"
"$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID1" \
    --output "$OUT" \
    --runtime-root "$RUNTIME_ROOT" \
    --resume=false 2>&1 | tee "$WORK/snap.log"

# Tear down run1.
kill -TERM "$SBPID1" 2>/dev/null || true
wait "$SBPID1" 2>/dev/null || true

[ -f "$OUT/sandbox.snapshot" ] || { echo "FAIL: no snapshot file"; exit 1; }

# Restore — needs a fresh blk1.diff (the snapshotted disk goes in as
# overlay base; new run gets a clean diff).
DIFF_RESTORE="$WORK/runtime/blk1-restore.diff"
truncate -s 1G "$DIFF_RESTORE"
mkfs.ext4 -q -F "$DIFF_RESTORE"

cat > "$WORK/host.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: file://$BLK0_IMAGE
    overlay:
      diff: file://$DIFF_RESTORE
      size: 1GiB
launch:
  args: ["-c", "import time; time.sleep(60)"]
  restart: never
EOF

LOG2="$WORK/run2.log"
SID2="r2-$$"
mkdir -p "$RUNTIME_ROOT/$SID2"
"$BIN/sandbox-ctl" restore \
    --snapshot "$OUT/sandbox.snapshot" \
    --config "$WORK/host.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --runtime-root "$RUNTIME_ROOT" \
    --sandbox-id "$SID2" \
    > "$LOG2" 2>&1 &
SBPID2=$!

echo "==> waiting for restored TICK > $PRE_SNAP_TICK..."
WANT_TICK=$((PRE_SNAP_TICK + 3))
for i in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK[[:space:]]*$" "$LOG2" 2>/dev/null; then break; fi
    if ! kill -0 "$SBPID2" 2>/dev/null; then
        echo "==> sandbox-ctl restore exited early"; tail -50 "$LOG2"; exit 1
    fi
    sleep 0.05
done

# Tear down run2.
kill -TERM "$SBPID2" 2>/dev/null || true
wait "$SBPID2" 2>/dev/null || true

if grep -qE "^TICK $WANT_TICK[[:space:]]*$" "$LOG2"; then
    echo "==> PASS: restored sandbox continued counting (saw TICK $WANT_TICK)"
    echo "==> e2e_sandbox_restore: OK"
else
    echo "==> FAIL: restored sandbox did not reach TICK $WANT_TICK"
    tail -40 "$LOG2"
    exit 1
fi
