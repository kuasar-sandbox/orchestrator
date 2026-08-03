#!/usr/bin/env bash
#
# e2e_sandbox_restore_files.sh — verify per-instance file injection at
# restore reaches the resumed app:
#   1. cold-start an app that prints, each tick, the content of
#      /etc/instance-id (or "none" if absent)
#   2. snapshot it (no per-instance file → app prints ID=none)
#   3. restore with a host.yaml carrying files:[/etc/instance-id="clone-<n>"]
#   4. assert the resumed app starts printing ID=clone-<n>
#
# This proves the restore-window file injection (docs/sandbox-init.md
# §4.3) is visible to the already-running app.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"

skip() {
    echo
    echo "==> e2e_sandbox_restore_files: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

WORK="$(mktemp -d /tmp/e2e-restorefiles-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

IMAGE="${IMAGE:-python:3.12-slim}"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing"
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE" >/dev/null
    BLK0_IMAGE="$WORK/blk0.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"; mkfs.ext4 -q -F "$DIFF_FILE"

# App re-reads /etc/instance-id every tick so a file injected at restore
# becomes visible in subsequent ticks.
PYCODE='import time
print("PYBOOT-OK", flush=True)
i=0
while True:
    try:
        v=open("/etc/instance-id").read().strip()
    except Exception:
        v="none"
    print("TICK", i, "ID="+v, flush=True)
    i+=1
    time.sleep(0.25)'

cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-rf }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay: { diff: file://$DIFF_FILE, size: 1GiB }
launch:
  args: ["-c", $(python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' <<<"$PYCODE")]
  restart: never
EOF

LOG1="$WORK/run1.log"; SID1="rf1-$$"; RUNTIME_ROOT="$WORK/runtime"
mkdir -p "$RUNTIME_ROOT/$SID1"
"$BIN/sandbox-ctl" run --config "$WORK/sandbox.yaml" --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$RUNTIME_ROOT" --sandbox-id "$SID1" > "$LOG1" 2>&1 &
SBPID1=$!

echo "==> waiting for TICK 10 (pre-snapshot, expect ID=none)..."
for i in $(seq 1 600); do
    grep -qE "^TICK 10 ID=" "$LOG1" 2>/dev/null && break
    kill -0 "$SBPID1" 2>/dev/null || { echo "run1 exited early"; tail -30 "$LOG1"; exit 1; }
    sleep 0.05
done
grep -qE "^TICK 10 ID=none$" "$LOG1" || { echo "FAIL: expected ID=none pre-injection"; grep -E "^TICK" "$LOG1" | tail -3; exit 1; }
PRE=$(grep -oE "^TICK [0-9]+" "$LOG1" | tail -1 | awk '{print $2}')
echo "==> at TICK $PRE ID=none; snapshotting"

OUT="$WORK/snap-out"; mkdir -p "$OUT"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID1" --output "$OUT" --run-root "$RUNTIME_ROOT" 2>&1 | tee "$WORK/snap.log"
wait "$SBPID1" 2>/dev/null || true
SNAP_FILE="$OUT/$SID1.snapshot"
[ -f "$SNAP_FILE" ] || { echo "FAIL: no snapshot"; exit 1; }

DIFF_R="$WORK/runtime/blk1-restore.diff"; truncate -s 1G "$DIFF_R"; mkfs.ext4 -q -F "$DIFF_R"

# Restore host.yaml carries the per-instance file.
cat > "$WORK/host.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-rf }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay: { diff: file://$DIFF_R, size: 1GiB }
files:
  - path: /etc/instance-id
    content: "clone-42"
EOF

LOG2="$WORK/run2.log"; SID2="rf2-$$"; mkdir -p "$RUNTIME_ROOT/$SID2"
"$BIN/sandbox-ctl" run --restore "$SNAP_FILE" --config "$WORK/host.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUNTIME_ROOT" --sandbox-id "$SID2" > "$LOG2" 2>&1 &
SBPID2=$!

echo "==> waiting for restored app to print ID=clone-42..."
SEEN=0
for i in $(seq 1 400); do
    if grep -qE "^TICK [0-9]+ ID=clone-42$" "$LOG2" 2>/dev/null; then SEEN=1; break; fi
    kill -0 "$SBPID2" 2>/dev/null || break
    sleep 0.05
done
kill -TERM "$SBPID2" 2>/dev/null || true
wait "$SBPID2" 2>/dev/null || true

echo "==> restored app tick samples:"; grep -oE "^TICK [0-9]+ ID=[^ ]+" "$LOG2" | tail -5 | sed 's/^/    /'
if [ "$SEEN" = "1" ]; then
    echo "==> PASS: restored app saw injected per-instance file (ID=clone-42)"
    echo "==> e2e_sandbox_restore_files: OK"
else
    echo "==> FAIL: restored app never saw ID=clone-42 (injection did not reach the app)"
    exit 1
fi
