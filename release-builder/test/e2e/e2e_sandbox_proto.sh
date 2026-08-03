#!/usr/bin/env bash
#
# e2e_sandbox_proto.sh — exercise the bidirectional launch protocol on a
# real sandbox VM. Boots python:3.12-slim, runs a sleep that keeps the
# guest alive for ~4s (long enough for ≥3 host→guest ping ticks at the
# 1s default interval), then asserts:
#
#   1. sandbox-ctl logs show the host receiving app_started{pid} from the
#      guest (LaunchServer.OnAppStarted callback).
#   2. sandbox-ctl logs show the host receiving app_exited{code} from
#      the guest (LaunchServer.OnAppExited callback).
#   3. stats-json includes a populated "ping" object: attempts > 0,
#      success > 0, RTT samples non-zero. This proves both directions of
#      the channel work end-to-end (host dial → CH proxy → guest listener
#      → guest reply → host RoundTrip return).
#
# Skip semantics match e2e_sandbox_cold.sh.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_proto: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"

for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
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

WORK="$(mktemp -d /tmp/e2e-proto-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    echo "==> docker save $IMAGE | flatten-ctl export --output $BLK0_IMAGE"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
mkfs.ext4 -q -F "$DIFF_FILE"

# Keep the guest alive ~4s so the 1Hz ping ticker fires several times.
# A short python sleep+print is enough; restart=never means the VM
# powers off cleanly when python returns.
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:
    cpu: 1
    memory: 512MiB
  allocatable:
    cpu: 1
    memory: 512MiB
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-proto
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
  args: ["-c", "import time, sys; print('PROTO-BOOT-OK'); sys.stdout.flush(); time.sleep(4); print('PROTO-DONE')"]
  restart: never
EOF

echo "==> launching sandbox-ctl run (timeout 30s)"
LOG="$WORK/run.log"
STATS_JSON="$WORK/stats.json"
T0_NS=$(date +%s%N)
set +e
timeout -k 10s 30 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --stats-json "$STATS_JSON" \
    > "$LOG" 2>&1
EXIT=$?
T_END_NS=$(date +%s%N)
set -e

ms_delta() { awk "BEGIN{printf \"%.1f\", ($2 - $1) / 1000000.0}"; }
DUR_MS=$(ms_delta "$T0_NS" "$T_END_NS")
echo "==> sandbox-ctl exit=$EXIT, total ${DUR_MS} ms"
echo "==> last 40 lines:"
tail -40 "$LOG"

if [ "$EXIT" != "0" ]; then
    if [ "$EXIT" = "124" ]; then
        echo "==> FAIL: sandbox run timed out at 30s — guest did not reach quit"
        exit 1
    fi
    echo "==> FAIL: sandbox-ctl exit=$EXIT"
    exit 1
fi

# Required guest output.
grep -q "PROTO-BOOT-OK" "$LOG" || { echo "==> FAIL: PROTO-BOOT-OK marker missing"; exit 1; }
grep -q "PROTO-DONE"    "$LOG" || { echo "==> FAIL: PROTO-DONE marker missing (guest didn't run full 4s)"; exit 1; }

# Bidirectional protocol assertions.
echo
echo "==> protocol log assertions:"

if grep -qE "guest reports user app pid=[0-9]+" "$LOG"; then
    PID_LINE=$(grep -oE "guest reports user app pid=[0-9]+" "$LOG" | head -1)
    echo "    PASS: app_started ($PID_LINE)"
else
    echo "==> FAIL: no app_started log — guest did not notify host"
    exit 1
fi

if grep -qE "guest reports user app exited code=[0-9]+" "$LOG"; then
    EXIT_LINE=$(grep -oE "guest reports user app exited code=[0-9]+" "$LOG" | head -1)
    echo "    PASS: app_exited ($EXIT_LINE)"
else
    echo "==> FAIL: no app_exited log — guest did not notify host before reboot"
    exit 1
fi

# stats.json must carry a populated ping object.
if [ ! -s "$STATS_JSON" ]; then
    echo "==> FAIL: stats-json $STATS_JSON missing or empty"
    exit 1
fi

if command -v python3 >/dev/null 2>&1; then
    python3 - <<PY
import json, sys
with open("$STATS_JSON") as f:
    rep = json.load(f)
ping = rep.get("ping")
assert ping is not None, "stats-json missing 'ping' object"
attempts = ping.get("attempts", 0)
success  = ping.get("success", 0)
timeout  = ping.get("timeout", 0)
dialerr  = ping.get("dial_error", 0)
rtt_avg  = ping.get("rtt_avg_ns", 0)
rtt_max  = ping.get("rtt_max_ns", 0)
rtt_p99  = ping.get("rtt_p99_ns", 0)
assert attempts >= 2, f"attempts={attempts} < 2 (expected several over ~4s)"
assert success  >= 1, f"success={success} < 1 (no pong received)"
# Healthy guest: timeouts / dial errors should be a small fraction.
assert success >= attempts // 2, f"success {success} < half of attempts {attempts}"
assert rtt_avg > 0, f"rtt_avg_ns={rtt_avg}"
assert rtt_max >= rtt_avg, f"rtt_max {rtt_max} < rtt_avg {rtt_avg}"
print(f"==> PASS: ping attempts={attempts} success={success} timeout={timeout} dial_err={dialerr}")
print(f"          rtt avg={rtt_avg/1000:.1f}us p99={rtt_p99/1000:.1f}us max={rtt_max/1000:.1f}us")
PY
else
    echo "==> FAIL: python3 missing for stats-json validation; size=$(stat -c%s "$STATS_JSON") bytes"
    exit 1
fi

echo "==> e2e_sandbox_proto: OK"
