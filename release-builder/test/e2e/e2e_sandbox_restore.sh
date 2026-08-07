#!/usr/bin/env bash
#
# e2e_sandbox_restore.sh — chained:
#   1. Cold-start sandbox running a python TICK counter
#   2. Wait until counter reaches a known value (e.g. TICK 10)
#   3. snapshot the sandbox to <out>/<sid>.snapshot + <sha256>.overlay
#      (--resume=false default destroys sandbox after dump)
#   4. Run --restore=<file> to resume; vCPU should resume the counter
#   5. Confirm the restored process keeps counting from where it
#      stopped (TICK 10+, increases over time)
#
# This validates that vCPU + memory + disk all restored correctly via
# the unified `sandbox-ctl run --restore=` path.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
. "$REPO_ROOT/test/lib/uffd_performance_gate.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
source "$REPO_ROOT/test/e2e/readiness_helpers.sh"

skip() {
    echo
    echo "==> e2e_sandbox_restore: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH (needed to probe ctl.sock)"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
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
if [ "$(id -u)" -ne 0 ]; then skip "must run as root"; fi

WORK="$(mktemp -d /tmp/e2e-restore-XXXXXX)"
SBPID1=""
SBPID2=""
cleanup() {
    set +e
    for pid in "$SBPID1" "$SBPID2"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    sleep 1
    for pid in "$SBPID1" "$SBPID2"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null
    done
    [ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null
}
trap cleanup EXIT

IMAGE="${IMAGE:-python:3.12-slim}"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

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
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
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
readiness_begin_capture "$WORK/cold.ready"
COLD_READER_PID=$READY_READER_PID
COLD_T0_NS=$(date +%s%N)
"$BIN/sandbox-ctl" run \
    --ready-fd="$READY_WRITE_FD" \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$RUNTIME_ROOT" \
    --sandbox-id "$SID1" \
    --stats-json "$WORK/cold-stats.json" \
    > "$LOG1" 2>&1 &
SBPID1=$!
readiness_close_parent_writer

readiness_wait_event "$WORK/cold.ready" 1 control_ready "$SBPID1" \
    || { tail -60 "$LOG1"; exit 1; }
readiness_connect_ctl "$RUNTIME_ROOT/$SID1/ctl.sock" \
    || { echo "==> FAIL: cold ctl.sock not connectable at control_ready"; exit 1; }
readiness_wait_event "$WORK/cold.ready" 2 ready "$SBPID1" \
    || { tail -60 "$LOG1"; exit 1; }
COLD_READY_MS=$(( ($(date +%s%N) - COLD_T0_NS) / 1000000 ))
"$BIN/sandbox-ctl" exec --sandbox-id "$SID1" --run-root "$RUNTIME_ROOT" -- /bin/true \
    || { echo "==> FAIL: cold immediate exec after ready failed"; exit 1; }
readiness_assert_wire "$WORK/cold.ready" "$COLD_READER_PID" $'control_ready\nready\n' \
    || { echo "==> FAIL: cold readiness wire was not exact"; exit 1; }
echo "==> PASS: cold exact readiness wire; ctl.sock and immediate exec succeeded"

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
mkdir -p "$OUT"
"$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID1" \
    --output "$OUT" \
    --run-root "$RUNTIME_ROOT" 2>&1 | tee "$WORK/snap.log"

# --resume=false (default) shuts CH down via /vm.shutdown; sandbox-ctl
# run1 returns naturally. wait() not kill().
wait "$SBPID1" 2>/dev/null || true
SBPID1=""
uffd_performance_gate "cold-zero-ready" "$COLD_READY_MS" 2000 \
    "$WORK/cold-stats.json" zero

SNAP_FILE="$OUT/$SID1.snapshot"
[ -f "$SNAP_FILE" ] || { echo "FAIL: no $SID1.snapshot"; ls -la "$OUT"; exit 1; }

# Restore — needs a fresh blk1.diff (the snapshotted disk goes in as
# overlay base; new run gets a clean diff).
DIFF_RESTORE="$WORK/runtime/blk1-restore.diff"
truncate -s 1G "$DIFF_RESTORE"
mkfs.ext4 -q -F "$DIFF_RESTORE"

# Host yaml for restore: capacity/runtime/base must match snapshot.cfg
# per docs §11.0 — declare them explicitly (same as the cold yaml).
# overlay.base is always taken from snapshot.cfg (host value ignored).
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
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_RESTORE
      size: 1GiB
EOF

LOG2="$WORK/run2.log"
SID2="r2-$$"
mkdir -p "$RUNTIME_ROOT/$SID2"
readiness_begin_capture "$WORK/restore.ready"
RESTORE_READER_PID=$READY_READER_PID
RESTORE_T0_NS=$(date +%s%N)
"$BIN/sandbox-ctl" run \
    --ready-fd="$READY_WRITE_FD" \
    --restore "$SNAP_FILE" \
    --config "$WORK/host.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$RUNTIME_ROOT" \
    --sandbox-id "$SID2" \
    --stats-json "$WORK/restore-stats.json" \
    > "$LOG2" 2>&1 &
SBPID2=$!
readiness_close_parent_writer

readiness_wait_event "$WORK/restore.ready" 1 control_ready "$SBPID2" \
    || { tail -60 "$LOG2"; exit 1; }
readiness_connect_ctl "$RUNTIME_ROOT/$SID2/ctl.sock" \
    || { echo "==> FAIL: restore ctl.sock not connectable at control_ready"; exit 1; }
readiness_wait_event "$WORK/restore.ready" 2 ready "$SBPID2" \
    || { tail -60 "$LOG2"; exit 1; }
RESTORE_READY_MS=$(( ($(date +%s%N) - RESTORE_T0_NS) / 1000000 ))
"$BIN/sandbox-ctl" exec --sandbox-id "$SID2" --run-root "$RUNTIME_ROOT" -- /bin/true \
    || { echo "==> FAIL: restore immediate exec after ready failed"; exit 1; }
readiness_assert_wire "$WORK/restore.ready" "$RESTORE_READER_PID" $'control_ready\nready\n' \
    || { echo "==> FAIL: restore readiness wire was not exact"; exit 1; }
echo "==> PASS: restore exact readiness wire; ctl.sock and immediate exec succeeded"

echo "==> waiting for restored TICK > $PRE_SNAP_TICK..."
WANT_TICK=$((PRE_SNAP_TICK + 3))
for i in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK[[:space:]]*$" "$LOG2" 2>/dev/null; then break; fi
    if ! kill -0 "$SBPID2" 2>/dev/null; then
        echo "==> sandbox-ctl run --restore exited early"; tail -50 "$LOG2"; exit 1
    fi
    sleep 0.05
done

# Tear down run2.
kill -TERM "$SBPID2" 2>/dev/null || true
wait "$SBPID2" 2>/dev/null || true
SBPID2=""
uffd_performance_gate "file-restore-ready" "$RESTORE_READY_MS" 1500 \
    "$WORK/restore-stats.json" deferred

if grep -qE "^TICK $WANT_TICK[[:space:]]*$" "$LOG2"; then
    echo "==> PASS: restored sandbox continued counting (saw TICK $WANT_TICK)"
    echo "==> e2e_sandbox_restore: OK"
else
    echo "==> FAIL: restored sandbox did not reach TICK $WANT_TICK"
    tail -40 "$LOG2"
    exit 1
fi
