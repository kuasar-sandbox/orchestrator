#!/usr/bin/env bash
#
# e2e_sandbox_cold_target.sh — cold-start a single sandbox VM at the
# product's target spec:
#
#   capacity:    2 vCPU, 8 GiB     (what guest OS sees)
#   allocatable: 0.1 vCPU, 128 MiB (steady-state floor)
#   startup:     512 MiB           (controller-granted cold-start budget)
#
# Same workload as e2e_sandbox_cold.sh (python:3.12-slim → PYBOOT-OK)
# but exercises the production-shaped resources block:
#  - CH boots with --cpus boot=2 / --memory-zone size=8GiB (capacity)
#  - node-ctl resource controller admits a startup budget above the 128MiB floor
#  - CH initial balloon uses that startup budget; Heartbeat/reclaim can later
#    converge the sandbox back to the floor
#
# This is the "Agent app at idle in warm pool" footprint: very small
# allocatable so a node can pack thousands; capacity is the burst ceiling
# the app can reach (post-resize / via balloon deflate, future).
#
# Same skip semantics as cold.sh (KVM / TAP / docker missing → exit 0;
# REQUIRE_KVM=1 fails hard).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_cold_target: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible to current user"

for b in cloud-hypervisor sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done

VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX env var)"

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

WORK="$(mktemp -d /tmp/e2e-target-XXXXXX)"
DAEMON_PID=""
SBPID=""
cleanup_target() {
    if [ -n "$SBPID" ] && kill -0 "$SBPID" 2>/dev/null; then
        kill -TERM "$SBPID" 2>/dev/null || true
        sleep 1
        kill -KILL "$SBPID" 2>/dev/null || true
    fi
    if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill -TERM "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    pkill -KILL -f "cloud-hypervisor.*$WORK/runtime" 2>/dev/null || true
    [ -d "${SB_CGROUP:-}" ] && rmdir "$SB_CGROUP" 2>/dev/null || true
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null || true
}
trap cleanup_target EXIT

start_resource_controller() {
    cat > "$WORK/node-ctl.yaml" <<EOF
api: { domain: cold-target.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { mode: internal, auth: enforce }
sandbox:
  boot:
    kernel: $VMLINUX
    runtime: $BIN/sandbox-runtime.bundle
paths:
  run_root: $WORK/node-run
  base_root: $WORK/node-lib
  config_socket: $WORK/node-ctl.socket
  db_path: $WORK/node-ctl.db
units: { dir: $WORK/units, install: false }
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/state.json
  audit_path: $WORK/audit.log
  cgroup_scan_paths:
    - $CGROUP_ROOT
  resources:
    physical_memory: 4GiB
    physical_cpu: 4
    host_reserved:
      memory: 512MiB
      cpu: 1
  watermarks:
    operational_margin_factor: 0.10
    high_factor: 0.85
    low_factor: 0.70
    emergency_factor: 0.05
    startup_factor: 0.50
  rate_limits:
    memory_grant_per_sec_factor: 0.20
  admission:
    rate: 50
    burst: 50
    startup_ttl: 120s
    queue_ttl: 30s
    queue_max_depth: 256
  dampening:
    recover_duration: 30s
    cooldown_periods: 5
  log_level: info
EOF
    "$BIN/node-ctl" conductor serve --config "$WORK/node-ctl.yaml" >"$WORK/node-ctl.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 80); do
        [ -S "$WORK/sandbox-resource.sock" ] && return 0
        if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
            cat "$WORK/node-ctl.log" >&2
            return 1
        fi
        sleep 0.25
    done
    cat "$WORK/node-ctl.log" >&2
    return 1
}

BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE=path"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    echo "==> docker save $IMAGE | flatten-ctl export --output $BLK0_IMAGE"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
    echo "==> blk0 erofs ready ($(du -h "$BLK0_IMAGE" | cut -f1))"
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
mkfs.ext4 -q -F "$DIFF_FILE"

# Spec semantics:
#   capacity    = guest-visible vCPU + RAM (passed to CH --cpus / --memory-zone)
#   allocatable = idle footprint target — drives the boot-time balloon size
#                 (capacity - allocatable inflated, free_page_reporting on)
#                 plus cgroup cpu.weight on the host CH process.
#
# Defaults track the product target: 2 vCPU / 8 GiB capacity, 0.1 vCPU /
# 128 MiB allocatable. The uffd handler async EVENT_REMOVE flusher
# (handler.go:runRemoveFlusher) coalesces guest free_page_reporting
# events into batched fallocate(PUNCH_HOLE) calls, so the trickle storm
# during boot no longer wedges the handler.
#
# capacity.memory > 3 GiB triggers CH's PCI-hole split into two memory
# regions (low [0,3GiB) at memfd offset 0 + high [4GiB,...) at memfd
# offset 3 GiB); the uffd handler accepts both region's va_reports and
# attaches each region's uffd to the same epoll set. AddressMap routes
# fault VAs from either region to the correct memfd offset.
CAP_MEM="${CAP_MEM:-8GiB}"
ALLOC_MEM="${ALLOC_MEM:-128MiB}"
STARTUP_MEM="${STARTUP_MEM:-512MiB}"
CAP_CPU="${CAP_CPU:-2}"
ALLOC_CPU="${ALLOC_CPU:-0.1}"

# Fractional allocatable.cpu (0.1) requires a cgroup_path for cpu.weight;
# config validator rejects it otherwise. Provision a fresh leaf cgroup under
# /sys/fs/cgroup/sandboxes so the embedded resource controller can scan the
# same root it is configured with.
CGROUP_ROOT="/sys/fs/cgroup/sandboxes"
mkdir -p "$CGROUP_ROOT" 2>/dev/null || skip "cannot create cgroup root at $CGROUP_ROOT (need root + cgroup v2)"
echo "+memory +cpu" > "$CGROUP_ROOT/cgroup.subtree_control" 2>/dev/null || true
SB_CGROUP="$CGROUP_ROOT/sb-target-$$"
mkdir -p "$SB_CGROUP" 2>/dev/null || skip "cannot create cgroup at $SB_CGROUP (need root + cgroup v2)"

echo "==> starting node-ctl resource controller"
start_resource_controller || skip "node-ctl resource controller did not start"

cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:
    cpu: $CAP_CPU
    memory: $CAP_MEM
  allocatable:
    cpu: $ALLOC_CPU
    memory: $ALLOC_MEM
  control:
    cgroup_path: $SB_CGROUP
    controller: $WORK/sandbox-resource.sock
  startup:
    memory: $STARTUP_MEM
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-target
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
  args: ["-c", "import sys; print('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor)"]
  restart: never
EOF

echo "==> sandbox.yaml (target spec):"
sed 's/^/    /' "$WORK/sandbox.yaml"

# Cold-start budget: at allocatable 0.1 vCPU, host scheduler can throttle
# the CH process under contention. The 60s budget from cold.sh is
# generally still enough on an idle host (no contention → cgroup weight
# doesn't bite); raise to 120s for safety on busy CI hosts.
TIMEOUT_S="${E2E_TIMEOUT:-120}"
echo "==> launching sandbox-ctl run (timeout ${TIMEOUT_S}s)"
LOG="$WORK/run.log"
STATS_JSON="${PERF_STATS_JSON:-$WORK/stats.json}"

T0_NS=$(date +%s%N)
set +e
timeout -k 10s "$TIMEOUT_S" "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --stats-json "$STATS_JSON" \
    > "$LOG" 2>&1 &
SBPID=$!

T_APP_NS=""
while kill -0 "$SBPID" 2>/dev/null; do
    if grep -qE "^PYBOOT-OK [0-9]+$" "$LOG" 2>/dev/null; then
        T_APP_NS=$(date +%s%N)
        break
    fi
    sleep 0.005
done
wait "$SBPID"
EXIT=$?
T_END_NS=$(date +%s%N)
set -e

if [ -z "$T_APP_NS" ] && grep -q "PYBOOT-OK" "$LOG" 2>/dev/null; then
    T_APP_NS=$T_END_NS
fi

ms_delta() { awk "BEGIN{printf \"%.1f\", ($2 - $1) / 1000000.0}"; }

echo "==> sandbox-ctl exit code: $EXIT"
echo "==> last 60 lines of log:"
tail -60 "$LOG"

echo
echo "==> cold-start timing (wallclock, target spec 2c8G / 0.1c128M floor / startup $STARTUP_MEM):"
if [ -n "$T_APP_NS" ]; then
    APP_MS=$(ms_delta "$T0_NS" "$T_APP_NS")
    echo "    T0 → app first stdout (PYBOOT-OK):  ${APP_MS} ms"
fi
END_MS=$(ms_delta "$T0_NS" "$T_END_NS")
echo "    T0 → sandbox-ctl exit:               ${END_MS} ms"

echo
echo "==> guest-side milestones:"
grep -oE '\[sandbox-init\]\[T\+[0-9]+ms\][^"]*' "$LOG" | head -10 | sed 's/^/    /' || true

# Confirm CH was actually given the capacity-shape vCPU/RAM:
echo
echo "==> CH command-line (from log; must reflect capacity, not allocatable):"
grep -oE -- '--cpus [^ ]+' "$LOG" | head -1 | sed 's/^/    /' || true
grep -oE -- '--memory [^ ]+' "$LOG" | head -1 | sed 's/^/    /' || true

if [ "$EXIT" = "124" ] && ! grep -q "PYBOOT-OK" "$LOG"; then
    echo "==> FAIL: sandbox run timed out at ${TIMEOUT_S}s — guest didn't reach app. Inspect $LOG"
    exit 1
fi

if grep -qE "PYBOOT-OK [0-9]+" "$LOG"; then
    marker=$(grep -oE "PYBOOT-OK [0-9]+" "$LOG" | head -1)
    echo "==> PASS: python actually executed in sandbox: $marker"
else
    echo "==> FAIL: sandbox-ctl exit=$EXIT and PYBOOT-OK marker not seen"
    exit 1
fi

# Validate CH boot params reflected capacity (not allocatable). The
# CHCommand assembler should pass --cpus boot=2 and --memory size=8G
# regardless of allocatable. If a regression starts using allocatable
# for CH params, the guest would see 0.1 vCPU and 128 MiB and probably
# OOM during python boot.
if grep -qE -- '--cpus boot=2' "$LOG"; then
    echo "==> PASS: CH given --cpus boot=2 (capacity)"
else
    echo "==> FAIL: CH --cpus did not reflect capacity=2"
    exit 1
fi
cap_mb=$(awk -v c="$CAP_MEM" 'BEGIN{
    sub(/GiB/,"",c); if(c+0>0) print c*1024; else { sub(/MiB/,"",c); print c+0 }
}')
# CH puts memory-zone size= on the same memory-zone token spelled
# "--memory-zone id=ram0,size=2048M,...". A simple substring match
# against the size= clause is enough to validate the wiring.
if grep -q "memory-zone.*size=${cap_mb}M" "$LOG"; then
    echo "==> PASS: CH given memory-zone size=${cap_mb}M (capacity=$CAP_MEM)"
else
    echo "==> FAIL: CH --memory-zone did not reflect capacity=$CAP_MEM"
    exit 1
fi

startup_bytes=$(awk -v s="$STARTUP_MEM" 'BEGIN{
    if (s ~ /GiB$/) { sub(/GiB/,"",s); printf "%.0f", s*1024*1024*1024; exit }
    if (s ~ /MiB$/) { sub(/MiB/,"",s); printf "%.0f", s*1024*1024; exit }
    print s+0
}')
cap_bytes=$(awk -v c="$CAP_MEM" 'BEGIN{
    if (c ~ /GiB$/) { sub(/GiB/,"",c); printf "%.0f", c*1024*1024*1024; exit }
    if (c ~ /MiB$/) { sub(/MiB/,"",c); printf "%.0f", c*1024*1024; exit }
    print c+0
}')
want_balloon=$((cap_bytes - startup_bytes))
if grep -q -- "--balloon size=${want_balloon}" "$LOG"; then
    echo "==> PASS: CH initial balloon reflects startup allocatable ($STARTUP_MEM)"
else
    echo "==> FAIL: CH initial balloon did not reflect startup allocatable ($STARTUP_MEM)"
    exit 1
fi

grep -q "controller admit ok, initial allocatable=${startup_bytes}" "$LOG" || {
    echo "==> FAIL: sandbox-ctl did not log controller startup admit for $STARTUP_MEM"
    exit 1
}
grep -q "settled .* sid=" "$WORK/node-ctl.log" 2>/dev/null || {
    echo "==> FAIL: resource controller did not record settled transition"
    exit 1
}

echo "==> e2e_sandbox_cold_target: OK"
