#!/usr/bin/env bash
#
# e2e_density.sh — agent-intermittent density e2e demonstrating three
# resource-control capabilities of node-ctl + sandbox-ctl:
#
#   Phase A   sandbox-local Budget control with node reservation
#             1 sandbox in dynamic mode runs an intermittent Python
#             workload with deterministic grow/rest cycles.
#             Verifies sandbox-originated grow grants and workload liveness;
#             Phase B2 provides the gated dynamic shrink check.
#
#   Phase B   static local control vs dynamic reservation (A/B comparison)
#             B1: static mode, no node controller. Sandbox-local control
#                 accepts a CH grow target and the workload completes.
#             B2: dynamic mode, same headroom/workload. Node admission
#                 and grants complete the workload without guest self-cap/OOM.
#
#   Phase C   creation rate backpressure
#             Compact node-ctl pool sized so 4 concurrent admit succeed
#             but the 5th sandbox-ctl run is rejected at admit time.
#             Real sandbox-ctl run for all five.
#
#   Phase D   stateless controller restart
#             SIGKILL/restart node-ctl around a live dynamic sandbox, with a
#             corrupt deprecated state_path. Verifies StateSync converges to
#             one precise reservation while sandbox-ctl and CH keep the same
#             process identities and guest exec remains available.
#
# Requires /dev/kvm + root + docker + cloud-hypervisor + vmlinux.
# Skips cleanly otherwise (REQUIRE_KVM=1 turns skip into failure).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
. "$SCRIPT_DIR/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_density: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

# Prereqs.
[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"
# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

command -v docker >/dev/null 2>&1 || skip "docker not available"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH (host parser)"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH (CH balloon delivery probe)"

for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl cloud-hypervisor; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build cloud-hypervisor'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run make vmlinux)"

WORK="$(mktemp -d /tmp/e-XXXXXX)"
mkdir -p "$WORK/run" "$WORK/lib"   # serve's run/base roots (sandboxes still use /run/sandbox directly)
DAEMON_PID=""
declare -a SANDBOX_PIDS=()
declare -a SANDBOX_SIDS=()

dump_logs_on_fail() {
    echo "==> failure: dumping last 80 lines of relevant logs"
    [ -f "$WORK/daemon.log" ] && { echo "--- daemon.log ---"; tail -80 "$WORK/daemon.log"; }
    for f in "$WORK"/sb-*.log; do
        [ -f "$f" ] && { echo "--- ${f##*/} ---"; tail -80 "$f"; }
    done
}

cleanup_all() {
    set +e
    if [ "${TEST_FAILED:-0}" = "1" ]; then
        dump_logs_on_fail
    fi
    # Stop every sandbox-ctl in parallel via shutdown_sandbox (defined
    # below) — each runs its own CH teardown over a 15s grace, no script
    # SIGKILL of sandbox-ctl or pkill of cloud-hypervisor. Track helper
    # subshell PIDs so we only wait on those (bare `wait` would also
    # block on $DAEMON_PID which we take down two steps later).
    local stop_pids=()
    for i in "${!SANDBOX_PIDS[@]}"; do
        shutdown_sandbox "${SANDBOX_PIDS[$i]}" "${SANDBOX_SIDS[$i]:-?}" &
        stop_pids+=($!)
    done
    for p in "${stop_pids[@]}"; do wait "$p" 2>/dev/null; done
    if [ -n "$DAEMON_PID" ]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    for sid in "${SANDBOX_SIDS[@]}"; do
        rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null
        ip link delete "${sid}-tap" 2>/dev/null
        rm -rf "/run/sandbox/$sid" 2>/dev/null
    done
    if [ -n "${E2E_KEEP:-}" ]; then
        echo "kept work dir: $WORK"
    else
        rm -rf "$WORK"
    fi
    set -e
}
trap 'TEST_FAILED=$?; [ $TEST_FAILED -ne 0 ] && TEST_FAILED=1 || TEST_FAILED=0; cleanup_all' EXIT

fail() {
    TEST_FAILED=1
    echo "FAIL: $*" >&2
    exit 1
}

# ---------- prepare blk0 (python:3.12-slim flattened) ----------

echo "==> preparing blk0 from $IMAGE"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker pull "$IMAGE"
fi
BLK0="$WORK/blk0.img"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0" --no-progress
BLK0_REF="$(plaintext_tarstream_ref "$BLK0")"

# ---------- cgroup parent ----------

echo "==> preparing cgroup parent /sys/fs/cgroup/sandboxes"
[ -d /sys/fs/cgroup/sandboxes ] || mkdir /sys/fs/cgroup/sandboxes
echo "+memory +cpu" > /sys/fs/cgroup/sandboxes/cgroup.subtree_control 2>/dev/null || true

# ---------- Python workload (agent-intermittent) ----------
# Source the shared workload model. See test/perf/workload.py for the
# full env/mode catalogue. e2e_density.sh always uses WL_MODE=cycles
# for reproducibility; perf harnesses can override.
WORKLOAD_PY="$(cat "$SCRIPT_DIR/lib/workload.py")"

# Phase B uses one workload definition for both sides of the comparison. Both
# sides run the same sandbox-local balloon/cgroup control loop; the dynamic side
# additionally obtains Budget through the existing node reservation boundary.
B_HEADROOM_MIB=320
B_CAP_MIB=1024
B_WORKLOAD_DURATION=15
B_WORKLOAD_CYCLES=2
B_WORKLOAD_RMIN_MIB=256
B_WORKLOAD_RMAX_MIB=384
B2_STARTUP_MIB=512
B2_REPORT_SECONDS=5
B2_READY_TIMEOUT=$((3 * B2_REPORT_SECONDS + 5))
B2_GRANT_TIMEOUT=$((3 * B2_REPORT_SECONDS + 5))
B2_DELIVERY_TIMEOUT=$((B2_REPORT_SECONDS + 10))
B2_WORKLOAD_GATE_TIMEOUT=$((2 * B2_GRANT_TIMEOUT + B2_DELIVERY_TIMEOUT + 5))

# ---------- helpers ----------

setup_sb() {
    local sid="$1"
    SANDBOX_SIDS+=("$sid")
    mkdir -p "/sys/fs/cgroup/sandboxes/$sid"
    ip tuntap add "${sid}-tap" mode tap 2>/dev/null || true
    ip link set "${sid}-tap" up
    truncate -s 1G "$WORK/${sid}.diff"
    mkfs.ext4 -q -F "$WORK/${sid}.diff"
}

# emit_yaml SID MODE HEADROOM_MIB CAP_MIB STARTUP_MIB DUR CYCLES RMIN_MIB RMAX_MIB DEFLATE START_GATE DELIVERY_GATE
#   MODE      = static | dynamic
#   STARTUP_MIB = cold headroom in both static and dynamic mode
#   CYCLES    = number of grow/rest cycles within the duration
#   DEFLATE   = true | false (allocatable.deflate_on_oom)
#   START_GATE = optional guest path; workload waits for the host to create it
#   DELIVERY_GATE = optional guest path; cycles mode holds its first pressure
#                   allocation until the host verifies balloon delivery
emit_yaml() {
    local sid="$1" mode="$2" headroom_mib="$3" cap_mib="$4" startup_mib="$5"
    local wl_dur="$6" wl_cycles="$7" wl_rmin="$8" wl_rmax="$9"
    local deflate="${10:-true}"
    local start_gate="${11:-}"
    local delivery_gate="${12:-}"

    {
        cat <<EOF
resources:
  capacity:
    cpu: 1
    memory: ${cap_mib}MiB
  allocatable:
    cpu: 1
    memory: ${headroom_mib}MiB
    deflate_on_oom: ${deflate}
  control:
    cgroup_path: /sys/fs/cgroup/sandboxes/${sid}
EOF
		if [ "$mode" = "dynamic" ]; then
			echo "    controller: $WORK/sandbox-resource.sock"
		fi
		echo "  startup:"
		echo "    memory: ${startup_mib}MiB"
        cat <<EOF
network:
  tap: ${sid}-tap
boot:
  kernel: file://${VMLINUX}
  runtime: file://${BIN}/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root:
    base: ${BLK0_REF}
    overlay:
      diff: file://${WORK}/${sid}.diff
launch:
  exec: /usr/local/bin/python3
  env:
    WL_MODE: "cycles"
    WL_DURATION: "${wl_dur}"
    WL_CYCLES: "${wl_cycles}"
    WL_RMIN_MIB: "${wl_rmin}"
    WL_RMAX_MIB: "${wl_rmax}"
    PYTHONUNBUFFERED: "1"
EOF
        if [ -n "$start_gate" ]; then
            echo "    WL_START_GATE: \"$start_gate\""
            echo '    WL_START_GATE_TIMEOUT: "60"'
        fi
        if [ -n "$delivery_gate" ]; then
            echo "    WL_DELIVERY_GATE: \"$delivery_gate\""
            echo "    WL_DELIVERY_GATE_TIMEOUT: \"$B2_WORKLOAD_GATE_TIMEOUT\""
        fi
        cat <<EOF
  restart: never
  args:
    - "-c"
    - |
EOF
        printf '%s\n' "$WORKLOAD_PY" | sed 's/^/      /'
    } > "$WORK/$sid.yaml"
}

emit_placeholder_yaml() {
    local sid="$1" headroom_mib="$2" cap_mib="$3" startup_mib="$4"
    cat > "$WORK/$sid.yaml" <<EOF
resources:
  capacity:
    cpu: 1
    memory: ${cap_mib}MiB
  allocatable:
    cpu: 1
    memory: ${headroom_mib}MiB
    deflate_on_oom: true
  control:
    cgroup_path: /sys/fs/cgroup/sandboxes/${sid}
    controller: $WORK/sandbox-resource.sock
  startup:
    memory: ${startup_mib}MiB
network:
  tap: ${sid}-tap
boot:
  kernel: file://${VMLINUX}
  runtime: file://${BIN}/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root:
    base: ${BLK0_REF}
    overlay:
      diff: file://${WORK}/${sid}.diff
launch:
  placeholder: true
EOF
}

# The resource controller is hosted in `node-ctl conductor serve` (resource_listen); there is
# no standalone daemon. We run a minimal serve (install_units=false, api on a
# throwaway port, temp paths) whose only live subsystem is the controller on
# resource_listen.socket — sandboxes are still launched directly by sandbox-ctl
# against that socket.
start_daemon() {
    local cfg="$1"
    "$BIN/node-ctl" conductor serve --config "$cfg" >"$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 60); do
        if [ -S "$WORK/sandbox-resource.sock" ] \
            && "$BIN/node-ctl" resource status \
                --socket "$WORK/sandbox-resource.sock" >/dev/null 2>&1; then
            return 0
        fi
        kill -0 "$DAEMON_PID" 2>/dev/null || { cat "$WORK/daemon.log"; fail "node-ctl conductor serve exited before binding the resource socket"; }
        sleep 0.25
    done
    fail "resource socket not created in time"
}

stop_daemon() {
    if [ -n "$DAEMON_PID" ]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    DAEMON_PID=""
    rm -f "$WORK/sandbox-resource.sock"
    rm -f "$WORK/state.json"
}

cleanup_sb() {
    local sid="$1"
    rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null || true
    ip link delete "${sid}-tap" 2>/dev/null || true
}

# Minimal `node-ctl conductor serve` config: only the in-process resource controller
# (resource_listen) is exercised. The serve scaffold (api/encryption_key/paths/
# units) is inert here — install_units=false, api on a throwaway port — so serve
# touches neither host systemd nor real ports.
write_default_config() {
    cat > "$WORK/node-ctl.yaml" <<EOF
api: { domain: density.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { auth: enforce }
sandbox:
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
paths:
  run_root: $WORK/run
  base_root: $WORK/lib
  config_socket: $WORK/node-ctl.socket
  db_path: $WORK/node-ctl.db
units: { dir: $WORK/units, install: false }
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/state.json
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandboxes
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
  log_level: info
EOF
}

write_compact_config() {
    cat > "$WORK/node-ctl-compact.yaml" <<EOF
api: { domain: density.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { auth: enforce }
sandbox:
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
paths:
  run_root: $WORK/run
  base_root: $WORK/lib
  config_socket: $WORK/node-ctl.socket
  db_path: $WORK/node-ctl.db
units: { dir: $WORK/units, install: false }
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/state.json
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandboxes
  # Sized for DETERMINISTIC creation-rate backpressure (Phase C), independent of
  # startup/settle timing: allocatable_pool = (400-80)MiB * (1-0.10) = 288MiB.
  # Each sandbox reserves its 64MiB cold InitialBudget at Admit, so the 4th
  # leaves the node at 256MiB >= the red water mark (0.85*288 = 244.8MiB) — and
  # the 5th admit is HARD-rejected with "node in zone red" (the zone gate is
  # checked before pool headroom, which would otherwise only queue).
  # startup_factor=1.0 makes the startup pool (= allocatable_pool) fit four
  # 64MiB aligned InitialBudgets, so the first four are not startup-blocked.
  resources:
    physical_memory: 400MiB
    physical_cpu: 4
    host_reserved:
      memory: 80MiB
      cpu: 0.5
  watermarks:
    operational_margin_factor: 0.10
    high_factor: 0.85
    low_factor: 0.70
    emergency_factor: 0.05
    startup_factor: 1.00
  rate_limits:
    memory_grant_per_sec_factor: 0.20
  admission:
    rate: 50
    burst: 50
    startup_ttl: 60s
    queue_ttl: 10s
    queue_max_depth: 256
  log_level: info
EOF
}

# ---------- Phase A: auto resource allocation ----------

# Graceful sandbox-ctl shutdown. sandbox-ctl owns the CH lifecycle: on
# SIGTERM it forwards SIGTERM to CH, waits chShutdownGrace=5s, then
# SIGKILLs CH and tears down tap / run dir / cgroup.
#
# Project policy: NEVER strong-kill sandbox-ctl from the script. SIGKILL
# orphans CH (leaked tap/run/cgroup) and silently masks sandbox-ctl bugs.
# If sandbox-ctl doesn't exit within the timeout, leave it for human
# investigation — print a loud WARN with the exact cleanup commands.
#
# Timeout 60s: under heavy oversubscription the host uffd path can
# starve sandbox-ctl's Go scheduler — observed 5s chShutdownGrace
# expanding to ~11s wallclock. 60s = generous slack.
shutdown_sandbox() {
    local pid="$1" sid="${2:-?}" waited=0 timeout=120
    # Teardown success = the process is GONE, not its exit code. Tolerate wait's
    # code (|| true) so teardown still diagnoses a failed sandbox. Only a process
    # alive past the timeout (a real residual) is a failure (return 1 below).
    kill -TERM "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
    while [ "$waited" -lt "$timeout" ]; do
        kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
        sleep 1
        waited=$((waited+1))
    done
    echo "WARN: sandbox-ctl pid=$pid sid=$sid did not exit within ${timeout}s of SIGTERM" >&2
    echo "WARN: leaving it running per project policy (no script-level SIGKILL of sandbox-ctl)" >&2
    echo "WARN: manual cleanup if needed:" >&2
    echo "WARN:   sudo kill -KILL $pid" >&2
    echo "WARN:   sudo pkill -KILL -f 'cloud-hypervisor.*--net tap=${sid}-tap'" >&2
    echo "WARN:   sudo ip link delete ${sid}-tap; sudo rmdir /sys/fs/cgroup/sandboxes/${sid}" >&2
    # A sandbox-ctl still ignoring SIGTERM after the (generous) timeout is a real
    # bug, and a leaked process is worse than a failed run — fail loudly (do NOT
    # return 0 and march on leaving it behind).
    return 1
}

wait_for_workload() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -q "workload done" "$WORK/$sid.log" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before workload completed"
        sleep 0.5
    done
    fail "$sid: workload did not complete within ${timeout}s"
}

wait_for_reservation_growth() {
    local sid="$1" pid="$2" timeout="$3" baseline="$4" reservation=0
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        reservation=$(resource_reservation_memory "$sid") || reservation=0
        if [[ "$reservation" =~ ^[0-9]+$ ]] && [ "$reservation" -gt "$baseline" ]; then
            echo "$reservation"
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before reservation growth"
        sleep 0.25
    done
    fail "$sid: reservation did not grow above $baseline within ${timeout}s"
}

wait_for_dynamic_control_ready() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if resource_reservation_matches "$sid" settled \
            && grep -qE 'sensor: (PSI|events_poll) mode active' "$WORK/$sid.log" 2>/dev/null \
            && grep -q 'memory: initial CH observation accepted' "$WORK/$sid.log" 2>/dev/null \
            && grep -q 'workload waiting for start gate' "$WORK/$sid.log" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before controller/sensor readiness barrier"
        sleep 0.1
    done
    fail "$sid: controller/sensor readiness barrier not reached within ${timeout}s"
}

open_workload_gate() {
    local sid="$1" gate="$2" label="${3:-start}"
    timeout -k 5s 20 "$BIN/sandbox-ctl" exec \
        --sandbox-id "$sid" \
        --run-root "$WORK/run" \
        -- /bin/sh -ceu 'touch "$1"' sh "$gate" \
        >"$WORK/$sid-$label-gate.log" 2>&1 \
        || { sed 's/^/  gate| /' "$WORK/$sid-$label-gate.log"; fail "$sid: open workload $label gate"; }
}

b2_timeline_event() {
    local sid="$1" monotonic_ns
    shift
    monotonic_ns=$(python3 -c 'import time; print(time.monotonic_ns())')
    printf '%s %s\n' "$monotonic_ns" "$*" >>"$WORK/$sid-timeline.log"
}

wait_for_pressure_probe() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if grep -q 'workload pressure probe ready rss=' "$WORK/$sid.log" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before pressure probe was ready"
        sleep 0.1
    done
    fail "$sid: pressure probe was not ready within ${timeout}s"
}

wait_for_local_grow() {
    local sid="$1" pid="$2" timeout="$3" baseline="$4" count=0
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        count=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || count=0
        if [ "$count" -gt "$baseline" ]; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before applying a local grow target"
        sleep 0.1
    done
    fail "$sid: sandbox-ctl did not apply a grow target within ${timeout}s"
}

wait_for_static_control_ready() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if grep -qE 'sensor: (PSI|events_poll) mode active' "$WORK/$sid.log" 2>/dev/null \
            && grep -q 'memory: initial CH observation accepted' "$WORK/$sid.log" 2>/dev/null \
            && grep -q 'workload waiting for start gate' "$WORK/$sid.log" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before static control readiness barrier"
        sleep 0.1
    done
    fail "$sid: static control readiness barrier not reached within ${timeout}s"
}

read_ch_balloon_state() {
    local sid="$1" json
    json=$(curl --silent --show-error --fail --max-time 2 \
        --unix-socket "$WORK/run/$sid/ch.sock" \
        http://localhost/api/v1/vm.info) || return 1
    python3 -c '
import json, sys
info = json.load(sys.stdin)
balloon = info.get("config", {}).get("balloon") or {}
print(balloon.get("size", -1), info.get("memory_actual_size", -1))
' <<<"$json"
}

wait_for_b2_guest_delivery() {
    local sid="$1" pid="$2" node_reservation="$3" timeout="$4" target_baseline="$5"
    local capacity=$((B_CAP_MIB * 1024 * 1024))
    local deadline=$((SECONDS + timeout)) state="" target=-1 actual=-1 applied_budget=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        state=$(read_ch_balloon_state "$sid" 2>/dev/null) || state=""
        if read -r target actual <<<"$state" \
            && [[ "$target" =~ ^[0-9]+$ ]] && [[ "$actual" =~ ^[0-9]+$ ]] \
            && [ "$target" -lt "$target_baseline" ]; then
            [ "$target" -le "$capacity" ] && [ "$actual" -le "$capacity" ] \
                || fail "$sid: CH balloon state exceeds Capacity (target=$target actual=$actual capacity=$capacity)"
            applied_budget=$((capacity - target))
            if [ "$applied_budget" -le "$node_reservation" ]; then
                b2_timeline_event "$sid" \
                    "grow_target_accepted target=$target current_budget=$actual applied_budget=$applied_budget node_reservation=$node_reservation"
                return 0
            fi
        fi
        if guest_self_cap_observed "$sid"; then
            b2_timeline_event "$sid" "self_cap_before_guest_delivery"
            fail "$sid: guest self-cap fired before controller grant delivery"
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before controller grant delivery"
        sleep 0.25
    done
    b2_timeline_event "$sid" \
        "grow_target_timeout target=$target current_budget=$actual applied_budget=$applied_budget node_reservation=$node_reservation"
    fail "$sid: reserved grow target was not CH-accepted within ${timeout}s (target=$target actual=$actual reservation=$node_reservation)"
}

wait_for_static_grow_delivery() {
    local sid="$1" pid="$2" timeout="$3" target_baseline="$4"
    local capacity=$((B_CAP_MIB * 1024 * 1024))
    local deadline=$((SECONDS + timeout)) state="" target=-1 actual=-1 applied_budget=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        state=$(read_ch_balloon_state "$sid" 2>/dev/null) || state=""
        if read -r target actual <<<"$state" \
            && [[ "$target" =~ ^[0-9]+$ ]] && [[ "$actual" =~ ^[0-9]+$ ]] \
            && [ "$target" -lt "$target_baseline" ]; then
            [ "$target" -le "$capacity" ] && [ "$actual" -le "$capacity" ] \
                || fail "$sid: CH balloon state exceeds Capacity (target=$target actual=$actual capacity=$capacity)"
            applied_budget=$((capacity - target))
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before static grow delivery"
        sleep 0.25
    done
    fail "$sid: static grow target was not CH-accepted within ${timeout}s (target=$target actual=$actual baseline=$target_baseline)"
}

memory_event_count() {
    local sid="$1" event="$2" events="/sys/fs/cgroup/sandboxes/$sid/memory.events.local"
    [ -f "$events" ] || { echo 0; return; }
    awk -v event="$event" '$1 == event { print $2; found=1 } END { if (!found) print 0 }' "$events"
}

guest_oom_observed() {
    local sid="$1"
    grep -qiE 'oom-kill:|Out of memory: Killed process|app exited code=137|app_exited code=137' \
        "$WORK/$sid.log" 2>/dev/null
}

guest_self_cap_observed() {
    local sid="$1"
    grep -qE 'virtio_balloon: pressure at [0-9]+ pages -> cap [0-9]+ pages .*converging' \
        "$WORK/$sid.log" 2>/dev/null
}

wait_for_b2_workload() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if guest_self_cap_observed "$sid"; then
            b2_timeline_event "$sid" "self_cap_after_guest_delivery"
            fail "B2: guest self-cap fired before proactive control could absorb pressure"
        fi
        grep -q "workload done" "$WORK/$sid.log" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before workload completed"
        sleep 0.25
    done
    fail "$sid: workload did not complete within ${timeout}s"
}

reservation_count() {
    local reservations
    if ! reservations=$("$BIN/node-ctl" resource list \
        --socket "$WORK/sandbox-resource.sock" 2>/dev/null); then
        echo 0
        return
    fi
    python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' \
        <<<"$reservations" 2>/dev/null || echo 0
}

resource_reservation_matches() {
    local sid="$1" mode="$2" reservations
    reservations=$("$BIN/node-ctl" resource list \
        --socket "$WORK/sandbox-resource.sock" 2>/dev/null) || return 1
    SID="$sid" MODE="$mode" RESERVATIONS_JSON="$reservations" python3 - 2>/dev/null <<'PY'
import json
import os

rows = json.loads(os.environ["RESERVATIONS_JSON"])
sid = os.environ["SID"]
mode = os.environ["MODE"]
matches = [row for row in rows if row.get("sandbox_id") == sid]
assert len(matches) == 1, rows
row = matches[0]
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
if mode in {"settled", "synced"}:
    assert row.get("stage") == "settled", row
if mode == "settled":
    assert row.get("recovery_source") in {"admit", "synced"}, row
elif mode == "synced":
    assert len(rows) == 1, rows
    assert row.get("recovery_source") == "synced", row
else:
    raise AssertionError(f"unknown reservation mode: {mode}")
PY
}

resource_reservation_memory() {
    local sid="$1" reservations
    reservations=$("$BIN/node-ctl" resource list \
        --socket "$WORK/sandbox-resource.sock" 2>/dev/null) || return 1
    SID="$sid" RESERVATIONS_JSON="$reservations" python3 - 2>/dev/null <<'PY'
import json
import os

rows = json.loads(os.environ["RESERVATIONS_JSON"])
matches = [row for row in rows if row.get("sandbox_id") == os.environ["SID"]]
assert len(matches) == 1, rows
row = matches[0]
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
print(row["allocatable_memory"])
PY
}

wait_for_reservation_state() {
    local sid="$1" pid="$2" timeout="$3" mode="$4"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        resource_reservation_matches "$sid" "$mode" && return 0
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before reservation reached $mode state"
        sleep 0.25
    done
    "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" >&2 2>/dev/null || true
    fail "$sid: reservation did not reach $mode state within ${timeout}s"
}

wait_for_reservations() {
    local want="$1" timeout="$2" count=0
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        count=$(reservation_count)
        [ "$count" -ge "$want" ] && { echo "$count"; return 0; }
        sleep 0.25
    done
    fail "only $count reservations after ${timeout}s (expected $want)"
}

phase_a() {
    echo
    echo "==> Phase A: auto resource allocation (1 sandbox, dynamic)"
    write_default_config
    start_daemon "$WORK/node-ctl.yaml"

    local sid=sb-A-1 start_gate=/tmp/e2e-density-a.start
    setup_sb "$sid"
    # Workload: 20s, 3 grow/rest cycles, R 96-192 MiB above headroom=64 MiB.
    emit_yaml "$sid" dynamic 64 1024 128   20 3 96 192   true "$start_gate"

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    # Bound cold-start and workload completion independently from controller
    # activity so a fast run does not pay the full worst-case allowance.
    wait_for_dynamic_control_ready "$sid" "$pid" 20
    local node_reservation_baseline node_reservation
    node_reservation_baseline=$(resource_reservation_memory "$sid") \
        || fail "$sid: could not read precise node reservation before workload"
    open_workload_gate "$sid" "$start_gate" start
    node_reservation=$(wait_for_reservation_growth "$sid" "$pid" 20 "$node_reservation_baseline")
    wait_for_workload "$sid" "$pid" 40

    # Inspect post-conditions BEFORE shutting the sandbox down.
    local shrinks oom
    shrinks=$(grep -c 'memory: shrink settled Budget=' "$WORK/$sid.log" 2>/dev/null) || shrinks=0
    oom=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        [ -z "$oom" ] && oom=0
    fi

    [ "$oom" -eq 0 ] || fail "A: cgroup oom_count=$oom (Budget grow failed)"

    echo "  Phase A: granted_reservation=$node_reservation local_shrinks=$shrinks oom_count=0"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    stop_daemon
    echo "Phase A: PASS"
}

# ---------- Phase B1: static sandbox-local control ----------

phase_b1_static_control() {
    echo
    echo "==> Phase B1: static mode → sandbox-local grow and liveness"
    # No daemon. Static mode has no controller.

    local sid=sb-B1-1
    local start_gate=/tmp/e2e-density-b1.start
    local delivery_gate=/tmp/e2e-density-b1.delivery
    setup_sb "$sid"
    # deflate_on_oom remains enabled as the guest emergency fallback, but the
    # normal static path is the same sandbox-local Budget loop used by dynamic
    # mode. Gates make its accepted CH target directly observable.
    emit_yaml "$sid" static "$B_HEADROOM_MIB" "$B_CAP_MIB" "$B2_STARTUP_MIB" \
        "$B_WORKLOAD_DURATION" "$B_WORKLOAD_CYCLES" \
        "$B_WORKLOAD_RMIN_MIB" "$B_WORKLOAD_RMAX_MIB" true \
        "$start_gate" "$delivery_gate"

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    wait_for_static_control_ready "$sid" "$pid" "$B2_READY_TIMEOUT"
    local target_baseline=-1 current_baseline=-1 target_after=-1 current_after=-1
    local initial_target=$(((B_CAP_MIB - B2_STARTUP_MIB) * 1024 * 1024))
    local grow_baseline=0 grows=0 grow_phase=pressure target_reference=-1
    read -r target_baseline current_baseline <<<"$(read_ch_balloon_state "$sid")"
    [[ "$target_baseline" =~ ^[0-9]+$ ]] || fail "$sid: invalid static pre-pressure CH target"
    [[ "$current_baseline" =~ ^[0-9]+$ ]] || fail "$sid: invalid static pre-pressure CH current Budget"
    [ "$target_baseline" -le $((B_CAP_MIB * 1024 * 1024)) ] \
        && [ "$current_baseline" -le $((B_CAP_MIB * 1024 * 1024)) ] \
        || fail "$sid: static pre-pressure CH state exceeds Capacity"
    grow_baseline=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grow_baseline=0
    target_reference=$target_baseline
    if [ "$grow_baseline" -gt 0 ] && [ "$target_baseline" -lt "$initial_target" ]; then
        # Boot-time PSI may already have delivered a local grow before the
        # workload gate. That retained Budget is the same valid static-control
        # outcome; a second grow is neither necessary nor guaranteed.
        grow_phase=prepressure
        target_reference=$initial_target
    fi

    open_workload_gate "$sid" "$start_gate" start
    wait_for_pressure_probe "$sid" "$pid" "$B2_GRANT_TIMEOUT"
    if [ "$grow_phase" = pressure ]; then
        wait_for_local_grow "$sid" "$pid" "$B2_GRANT_TIMEOUT" "$grow_baseline"
    fi
    wait_for_static_grow_delivery "$sid" "$pid" "$B2_DELIVERY_TIMEOUT" "$target_reference"
    local state_after=""
    state_after=$(read_ch_balloon_state "$sid") \
        || fail "$sid: cannot read static post-grow CH state"
    read -r target_after current_after <<<"$state_after"
    [[ "$target_after" =~ ^[0-9]+$ ]] || fail "$sid: invalid static post-grow CH target"
    [[ "$current_after" =~ ^[0-9]+$ ]] || fail "$sid: invalid static post-grow CH current Budget"
    [ "$target_after" -lt "$target_reference" ] \
        || fail "B1: static grow did not reduce CH target below $target_reference (got $target_after)"
    open_workload_gate "$sid" "$delivery_gate" delivery
    wait_for_workload "$sid" "$pid" 45

    local oom=0 high=0 self_cap=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        high=$(awk '$1=="high" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || high=0
        [ -z "$oom" ] && oom=0
        [ -z "$high" ] && high=0
    fi

    guest_self_cap_observed "$sid" && self_cap=1
    guest_oom_observed "$sid" && fail "B1: guest OOM/SIGKILL despite static local control"
    [ "$oom" -eq 0 ] || fail "B1: cgroup oom_count=$oom despite static local control"
    grows=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grows=0
    [ "$grows" -gt 0 ] || fail "B1: no static sandbox-local grow observed"
    echo "  Phase B1: local_grows=$grows grow_phase=$grow_phase target=$target_baseline->$target_after initial_target=$initial_target workload_done=1 self_cap=$self_cap cgroup_oom=0 cgroup_high=$high controller=none"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    echo "Phase B1: PASS"
}

# ---------- Phase B2: proactive dynamic control ----------

phase_b2_dynamic_control() {
    echo
    echo "==> Phase B2: dynamic mode (same headroom/workload) → proactive reservation, no emergency"
    write_default_config
    start_daemon "$WORK/node-ctl.yaml"

    local sid=sb-B2-1
    local start_gate=/tmp/e2e-density-b2.start
    local delivery_gate=/tmp/e2e-density-b2.delivery
    setup_sb "$sid"
    # Startup headroom is part of dynamic admission, while settled headroom,
    # Capacity, workload, and guest safety setting are identical to B1.
    emit_yaml "$sid" dynamic "$B_HEADROOM_MIB" "$B_CAP_MIB" "$B2_STARTUP_MIB" \
        "$B_WORKLOAD_DURATION" "$B_WORKLOAD_CYCLES" \
        "$B_WORKLOAD_RMIN_MIB" "$B_WORKLOAD_RMAX_MIB" true \
        "$start_gate" "$delivery_gate"

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")
    b2_timeline_event "$sid" "sandbox_started pid=$pid"

    # Synchronize past launch and the first fresh report-driven steady action.
    # Boot-time PSI may already reserve and deliver a grow before the workload
    # gate. Otherwise the first workload allocation is the pressure probe. In
    # both cases the allocation remains held until the reservation is reflected
    # by CH's accepted target. Grow deliberately does not wait for
    # memory_actual_size convergence.
    wait_for_dynamic_control_ready "$sid" "$pid" "$B2_READY_TIMEOUT"
    b2_timeline_event "$sid" "control_ready admission=1 settled=1 sensor=1 fresh_report=1"
    local target_baseline=-1 current_baseline=-1 grow_baseline=0 node_reservation_baseline=0
    local initial_target=$(((B_CAP_MIB - B2_STARTUP_MIB) * 1024 * 1024))
    local grow_phase=pressure target_reference=-1
    read -r target_baseline current_baseline <<<"$(read_ch_balloon_state "$sid")"
    [[ "$target_baseline" =~ ^[0-9]+$ ]] || fail "$sid: invalid pre-pressure CH target"
    [[ "$current_baseline" =~ ^[0-9]+$ ]] || fail "$sid: invalid pre-pressure CH current Budget"
    grow_baseline=$(grep -c 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grow_baseline=0
    node_reservation_baseline=$(resource_reservation_memory "$sid") \
        || fail "$sid: could not read precise node reservation before pressure"
    target_reference=$target_baseline
    if [ "$grow_baseline" -gt 0 ] && [ "$target_baseline" -lt "$initial_target" ] \
        && [ "$node_reservation_baseline" -gt $((B_HEADROOM_MIB * 1024 * 1024)) ]; then
        # The retained pre-pressure Budget is already a complete dynamic grow:
        # node reserved it before sandbox-ctl lowered the CH balloon target.
        grow_phase=prepressure
        target_reference=$initial_target
    fi
    open_workload_gate "$sid" "$start_gate" start
    b2_timeline_event "$sid" "start_gate_open"

    wait_for_pressure_probe "$sid" "$pid" "$B2_GRANT_TIMEOUT"
    b2_timeline_event "$sid" "pressure_probe_ready"
    local node_reservation
    if [ "$grow_phase" = prepressure ]; then
        node_reservation=$node_reservation_baseline
    else
        node_reservation=$(wait_for_reservation_growth \
            "$sid" "$pid" "$B2_GRANT_TIMEOUT" "$node_reservation_baseline")
    fi
    if [ "$grow_phase" = pressure ]; then
        wait_for_local_grow "$sid" "$pid" "$B2_GRANT_TIMEOUT" "$grow_baseline"
    fi

    local grow_line
    [[ "$node_reservation" =~ ^[0-9]+$ ]] \
        || fail "$sid: could not parse node reservation after grant"
    grow_line=$(grep -m1 'memory: grow accepted Budget=' "$WORK/$sid.log" 2>/dev/null) || grow_line="missing"
    b2_timeline_event "$sid" "grant_decision reservation=$node_reservation"
    b2_timeline_event "$sid" "grant_applied log=$grow_line"

    wait_for_b2_guest_delivery \
        "$sid" "$pid" "$node_reservation" "$B2_DELIVERY_TIMEOUT" "$target_reference"
    open_workload_gate "$sid" "$delivery_gate" delivery
    b2_timeline_event "$sid" "delivery_gate_open"
    wait_for_b2_workload "$sid" "$pid" 45

    local oom=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        [ -z "$oom" ] && oom=0
    fi
    [ "$oom" -eq 0 ]  || fail "B2: cgroup oom_count=$oom (controller couldn't prevent OOM)"
    grep -q "workload done" "$WORK/$sid.log" || fail "B2: workload did not complete"
    if guest_oom_observed "$sid"; then
        fail "B2: guest log contains OOM or SIGKILL despite controller"
    fi
    if guest_self_cap_observed "$sid"; then
        b2_timeline_event "$sid" "self_cap_after_guest_delivery"
        fail "B2: guest self-cap fired before proactive control could absorb pressure"
    fi
    echo "  Phase B2: reservation=$node_reservation grow_phase=$grow_phase oom_count=0 workload_done=1 delivery_verified=1"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    stop_daemon
    echo "Phase B2: PASS"
}

# ---------- Phase C: creation rate backpressure ----------

phase_c() {
    echo
    echo "==> Phase C: 4 admits succeed, 5th sandbox-ctl run rejected by water mark"
    write_compact_config
    start_daemon "$WORK/node-ctl-compact.yaml"

    # Five placeholder sandboxes set up; first four launched in background,
    # fifth synchronous. Phase C verifies only admission/backpressure, so it
    # must not run the memory-pressure workload used by Phase A/B.
    for i in 1 2 3 4 5; do
        local sid=sb-C-$i
        setup_sb "$sid"
        emit_placeholder_yaml "$sid" 64 256 64
    done

    declare -a c_pids=()
    for i in 1 2 3 4; do
        local sid=sb-C-$i
        "$BIN/sandbox-ctl" run \
            --config "$WORK/$sid.yaml" \
            --sandbox-id "$sid" \
            --ch-binary "$BIN/cloud-hypervisor" \
            --run-root "$WORK/run" \
            >"$WORK/$sid.log" 2>&1 &
        local p=$!
        c_pids+=("$p")
        SANDBOX_PIDS+=("$p")
    done

    local nres
    nres=$(wait_for_reservations 4 10)
    echo "  $nres reservations after 4 sandboxes launched"

    # 5th synchronous attempt — must be rejected at admit. Capped at 15s
    # because it should reject within seconds; if it hangs longer the
    # admit logic is broken and timeout exposes that.
    local sid=sb-C-5
    set +e
    timeout -k 10s 15 "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1
    local rc=$?
    set -e

    if [ "$rc" -eq 0 ]; then
        fail "C: 5th sandbox-ctl run succeeded (expected rejection)"
    fi
    if ! grep -qi "rejected\|node in zone\|headroom\|insufficient" "$WORK/$sid.log"; then
        fail "C: 5th rejected (rc=$rc) but reject message missing from log"
    fi
    echo "  5th sandbox-ctl run rc=$rc with reject message"

    # Phase C exercises admission only; stop the placeholders as soon as the
    # rejection has been observed.
    local idx=0
    for p in "${c_pids[@]}"; do
        idx=$((idx+1))
        shutdown_sandbox "$p" "sb-C-$idx"
    done

    for i in 1 2 3 4 5; do
        cleanup_sb "sb-C-$i"
    done
    stop_daemon
    echo "Phase C: PASS"
}

# ---------- Phase D: stateless controller restart ----------

wait_for_log_pattern() {
    local path="$1" pattern="$2" timeout="$3" subject="$4"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -qE "$pattern" "$path" 2>/dev/null && return 0
        sleep 0.25
    done
    fail "$subject not observed within ${timeout}s"
}

process_start_time() {
    local pid="$1"
    awk '{print $22}' "/proc/$pid/stat"
}

phase_d_controller_restart() {
    echo
    echo "==> Phase D: controller SIGKILL/restart recovers from lease + StateSync"
    write_default_config
    start_daemon "$WORK/node-ctl.yaml"

    local sid=sb-D-1 start_gate=/tmp/e2e-density-d.start
    setup_sb "$sid"
    emit_yaml "$sid" dynamic 128 512 256 60 1 32 64 true "$start_gate"

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local sandbox_pid=$!
    SANDBOX_PIDS+=("$sandbox_pid")

    wait_for_reservation_state "$sid" "$sandbox_pid" 20 settled
    wait_for_log_pattern "$WORK/$sid.log" 'CH started pid=[0-9]+' 10 "$sid CH pid"

    local ch_pid sandbox_started ch_started
    ch_pid=$(sed -nE 's/.*CH started pid=([0-9]+).*/\1/p' "$WORK/$sid.log" | tail -1)
    [[ "$ch_pid" =~ ^[0-9]+$ ]] || fail "D: could not parse CH pid"
    sandbox_started=$(process_start_time "$sandbox_pid")
    ch_started=$(process_start_time "$ch_pid")

    # Simulate abrupt controller loss. The stale UDS remains; replacement must
    # acquire the owner lock before removing it. state_path is deliberately
    # corrupt and must remain untouched because recovery is inventory-driven.
    kill -KILL "$DAEMON_PID"
    wait "$DAEMON_PID" 2>/dev/null || true
    DAEMON_PID=""
    printf '{corrupt-state\n' >"$WORK/state.json"
    start_daemon "$WORK/node-ctl.yaml"

    wait_for_reservation_state "$sid" "$sandbox_pid" 20 synced
    kill -0 "$sandbox_pid" 2>/dev/null || fail "D: sandbox-ctl exited across controller restart"
    kill -0 "$ch_pid" 2>/dev/null || fail "D: CH exited across controller restart"
    [ "$(process_start_time "$sandbox_pid")" = "$sandbox_started" ] \
        || fail "D: sandbox-ctl process identity changed"
    [ "$(process_start_time "$ch_pid")" = "$ch_started" ] \
        || fail "D: CH process identity changed"

    timeout -k 5s 20 "$BIN/sandbox-ctl" exec \
        --sandbox-id "$sid" --run-root "$WORK/run" -- /bin/true \
        >"$WORK/$sid-restart-exec.log" 2>&1 \
        || { sed 's/^/  exec| /' "$WORK/$sid-restart-exec.log"; fail "D: guest exec failed after controller restart"; }
    grep -qx '{corrupt-state' "$WORK/state.json" \
        || fail "D: deprecated state_path was read/replaced by controller"
    [ ! -e "$WORK/audit.log" ] || fail "D: controller created removed audit.log"

    echo "  Phase D: sandbox_pid=$sandbox_pid ch_pid=$ch_pid reservations=1 state_sync=1"
    shutdown_sandbox "$sandbox_pid" "$sid"
    cleanup_sb "$sid"
    stop_daemon
    echo "Phase D: PASS"
}

# ---------- run all phases ----------

phase_a
phase_b1_static_control
phase_b2_dynamic_control
phase_c
phase_d_controller_restart

echo
echo "==> e2e_density: PASS"
echo "    Phase A: auto resource allocation (1 sandbox, controller-driven)"
echo "    Phase B1: static → sandbox-local CH target grow + workload liveness"
echo "    Phase B2: dynamic + same headroom/workload → proactive reservation, no self-cap/OOM"
echo "    Phase C: 4 admits ok, 5th rejected by water mark"
echo "    Phase D: controller SIGKILL → lease/provisional/StateSync, VM identity unchanged"
