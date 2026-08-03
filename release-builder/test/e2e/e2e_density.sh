#!/usr/bin/env bash
#
# e2e_density.sh — agent-intermittent density e2e demonstrating three
# resource-control capabilities of node-ctl + sandbox-ctl:
#
#   Phase A   auto resource allocation
#             1 sandbox in dynamic mode runs an intermittent Python
#             workload (active phases linearly grow RSS to R_max,
#             Pareto-distributed durations, exponential idle gaps).
#             Verifies controller burst-grants on rising demand and
#             reclaimer shrinks back during idle.
#
#   Phase B   emergency convergence vs proactive control (A/B comparison)
#             B1: static mode, no node controller. The production guest
#                 self-cap detects an infeasible balloon target, returns
#                 memory, and the workload eventually completes.
#             B2: dynamic mode, same floor/workload. Controller admission
#                 and grants complete the workload without guest self-cap/OOM.
#
#   Phase C   creation rate backpressure
#             Compact node-ctl pool sized so 4 concurrent admit succeed
#             but the 5th sandbox-ctl run is rejected at admit time.
#             Real sandbox-ctl run for all five.
#
# Requires /dev/kvm + root + docker + cloud-hypervisor + vmlinux.
# Skips cleanly otherwise (REQUIRE_KVM=1 turns skip into failure).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
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

for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl cloud-hypervisor; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build cloud-hypervisor'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run make vmlinux)"

WORK="$(mktemp -d /tmp/e2e-density-XXXXXX)"
mkdir -p "$WORK/run" "$WORK/lib"   # serve's run/base roots (sandboxes still use /run/sandbox directly)
DAEMON_PID=""
declare -a SANDBOX_PIDS=()
declare -a SANDBOX_SIDS=()

dump_logs_on_fail() {
    echo "==> failure: dumping last 80 lines of relevant logs"
    [ -f "$WORK/daemon.log" ] && { echo "--- daemon.log ---"; tail -80 "$WORK/daemon.log"; }
    [ -f "$WORK/audit.log" ]  && { echo "--- audit.log ---";  cat "$WORK/audit.log"; }
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
WORKLOAD_PY="$(cat "$REPO_ROOT/test/perf/workload.py")"

# Phase B uses one workload definition for both sides of the comparison. The
# dynamic side additionally receives a controller-managed startup budget.
B_FLOOR_MIB=320
B_CAP_MIB=1024
B_WORKLOAD_DURATION=15
B_WORKLOAD_CYCLES=2
B_WORKLOAD_RMIN_MIB=256
B_WORKLOAD_RMAX_MIB=384
B2_STARTUP_MIB=512

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

# emit_yaml SID MODE FLOOR_MIB CAP_MIB BURST_MIB DUR CYCLES RMIN_MIB RMAX_MIB DEFLATE
#   MODE      = static | dynamic
#   BURST_MIB = ignored when MODE=static (no startup section emitted)
#   CYCLES    = number of grow/rest cycles within the duration
#   DEFLATE   = true | false (allocatable.deflate_on_oom)
emit_yaml() {
    local sid="$1" mode="$2" floor_mib="$3" cap_mib="$4" burst_mib="$5"
    local wl_dur="$6" wl_cycles="$7" wl_rmin="$8" wl_rmax="$9"
    local deflate="${10:-true}"

    {
        cat <<EOF
resources:
  capacity:
    cpu: 1
    memory: ${cap_mib}MiB
  allocatable:
    cpu: 1
    memory: ${floor_mib}MiB
    deflate_on_oom: ${deflate}
  control:
    cgroup_path: /sys/fs/cgroup/sandboxes/${sid}
EOF
        if [ "$mode" = "dynamic" ]; then
            echo "    controller: $WORK/sandbox-resource.sock"
            echo "  startup:"
            echo "    memory: ${burst_mib}MiB"
        fi
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
  restart: never
  args:
    - "-c"
    - |
EOF
        printf '%s\n' "$WORKLOAD_PY" | sed 's/^/      /'
    } > "$WORK/$sid.yaml"
}

emit_placeholder_yaml() {
    local sid="$1" floor_mib="$2" cap_mib="$3" startup_mib="$4"
    cat > "$WORK/$sid.yaml" <<EOF
resources:
  capacity:
    cpu: 1
    memory: ${cap_mib}MiB
  allocatable:
    cpu: 1
    memory: ${floor_mib}MiB
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
        [ -S "$WORK/sandbox-resource.sock" ] && return 0
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
proxy: { mode: internal, auth: enforce }
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
  audit_path: $WORK/audit.log
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
  dampening:
    recover_duration: 30s
    cooldown_periods: 5
  log_level: info
EOF
}

write_compact_config() {
    cat > "$WORK/node-ctl-compact.yaml" <<EOF
api: { domain: density.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { mode: internal, auth: enforce }
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
  audit_path: $WORK/audit.log
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandboxes
  # Sized for DETERMINISTIC creation-rate backpressure (Phase C), independent of
  # startup/settle timing: allocatable_pool = (400-80)MiB * (1-0.10) = 288MiB.
  # Each sandbox commits its 64MiB floor to NodeAllocated at admit, so the 4th
  # leaves the node at 256MiB >= the red water mark (0.85*288 = 244.8MiB) — and
  # the 5th admit is HARD-rejected with "node in zone red" (the zone gate is
  # checked before pool headroom, which would otherwise only queue).
  # startup_factor=1.0 makes the startup pool (= allocatable_pool) fit four
  # 64MiB startup budgets, so the first four are not startup-blocked.
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
  dampening:
    recover_duration: 30s
    cooldown_periods: 5
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

wait_for_controller_activity() {
    local sid="$1" timeout="$2"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if grep -q " grant .* sid=$sid " "$WORK/daemon.log" 2>/dev/null \
            || grep -q "reclaim sid=$sid" "$WORK/audit.log" 2>/dev/null; then
            return 0
        fi
        sleep 0.5
    done
    fail "$sid: controller recorded neither a grant nor a reclaim within ${timeout}s"
}

wait_for_controller_admit() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -q "admit token=.* sid=$sid" "$WORK/audit.log" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before controller admission"
        sleep 0.25
    done
    fail "$sid: controller did not admit the sandbox within ${timeout}s"
}

wait_for_controller_grant() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -q " grant .* sid=$sid " "$WORK/daemon.log" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || fail "$sid: sandbox exited before a controller grant"
        sleep 0.25
    done
    fail "$sid: controller did not grant memory within ${timeout}s"
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

wait_for_guest_self_cap() {
    local sid="$1" pid="$2" timeout="$3"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        guest_self_cap_observed "$sid" && return 0
        if ! kill -0 "$pid" 2>/dev/null; then
            guest_self_cap_observed "$sid" && return 0
            fail "$sid: sandbox exited without guest self-cap evidence"
        fi
        sleep 0.25
    done
    fail "$sid: no guest self-cap evidence within ${timeout}s"
}

reservation_count() {
    python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1])).get("reservations", {})))' \
        "$WORK/state.json" 2>/dev/null || echo 0
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

    local sid=sb-A-1
    setup_sb "$sid"
    # Workload: 20s, 3 grow/rest cycles, R 96-192 MiB above floor=64 MiB.
    emit_yaml "$sid" dynamic 64 1024 128   20 3 96 192   true

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
    wait_for_workload "$sid" "$pid" 40
    wait_for_controller_activity "$sid" 10

    # Inspect post-conditions BEFORE shutting the sandbox down.
    grep -q "admit token=.* sid=$sid" "$WORK/audit.log" || fail "A: no admit in audit"

    local grants reclaims oom
    grants=$(grep -c " grant .* sid=$sid " "$WORK/daemon.log" 2>/dev/null) || grants=0
    reclaims=$(grep -c "reclaim sid=$sid" "$WORK/audit.log" 2>/dev/null) || reclaims=0
    oom=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        [ -z "$oom" ] && oom=0
    fi

    # Reclaim only sweeps StageSettled. If the workload's first cycle
    # produces a grant within the 10s sweep interval, the sandbox enters
    # StageBurst and the reclaimer skips it for the rest of the run
    # (recover_duration default 30s exceeds our 45s budget). We accept
    # either grants or reclaims as evidence the controller is alive.
    [ "$oom" -eq 0 ] || fail "A: cgroup oom_count=$oom (controller failed to grow allocatable)"
    if [ "$grants" -eq 0 ] && [ "$reclaims" -eq 0 ]; then
        fail "A: neither grants nor reclaims observed (controller idle)"
    fi

    echo "  Phase A: grants=$grants reclaims=$reclaims oom_count=0"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    stop_daemon
    echo "Phase A: PASS"
}

# ---------- Phase B1: static guest emergency convergence ----------

phase_b1_static_self_cap() {
    echo
    echo "==> Phase B1: static mode → guest self-cap convergence and liveness"
    # No daemon. Static mode has no controller.

    local sid=sb-B1-1
    setup_sb "$sid"
    # The infeasible static target must trigger the production guest's sticky
    # balloon self-cap. deflate_on_oom remains at its production default; the
    # direct self-cap log, not an OOM race or host pressure, is authoritative.
    emit_yaml "$sid" static "$B_FLOOR_MIB" "$B_CAP_MIB" 0 \
        "$B_WORKLOAD_DURATION" "$B_WORKLOAD_CYCLES" \
        "$B_WORKLOAD_RMIN_MIB" "$B_WORKLOAD_RMAX_MIB" true

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    # Observe the guest emergency mechanism before asserting terminal liveness.
    wait_for_guest_self_cap "$sid" "$pid" 35
    wait_for_workload "$sid" "$pid" 45

    # Host cgroup pressure remains diagnostics, not a substitute for the direct
    # guest self-cap evidence above.
    local oom=0 high=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        high=$(awk '$1=="high" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || high=0
        [ -z "$oom" ] && oom=0
        [ -z "$high" ] && high=0
    fi

    guest_self_cap_observed "$sid" || fail "B1: guest self-cap evidence disappeared"
    guest_oom_observed "$sid" && fail "B1: guest OOM/SIGKILL despite self-cap convergence"
    [ "$oom" -eq 0 ] || fail "B1: cgroup oom_count=$oom despite self-cap convergence"
    if grep -q "sid=$sid" "$WORK/audit.log" "$WORK/daemon.log" 2>/dev/null; then
        fail "B1: node-controller activity observed for static sandbox"
    fi
    echo "  Phase B1: self_cap=1 workload_done=1 cgroup_oom=0 cgroup_high=$high controller=none"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    echo "Phase B1: PASS"
}

# ---------- Phase B2: proactive dynamic control ----------

phase_b2_dynamic_control() {
    echo
    echo "==> Phase B2: dynamic mode (same floor/workload) → proactive grant, no emergency"
    write_default_config
    start_daemon "$WORK/node-ctl.yaml"

    local sid=sb-B2-1
    setup_sb "$sid"
    # The startup budget is part of dynamic admission, while the steady-state
    # floor, capacity, workload, and guest safety setting are identical to B1.
    emit_yaml "$sid" dynamic "$B_FLOOR_MIB" "$B_CAP_MIB" "$B2_STARTUP_MIB" \
        "$B_WORKLOAD_DURATION" "$B_WORKLOAD_CYCLES" \
        "$B_WORKLOAD_RMIN_MIB" "$B_WORKLOAD_RMAX_MIB" true

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/run" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    # Prove the proactive control path before checking terminal completion.
    wait_for_controller_admit "$sid" "$pid" 15
    wait_for_controller_grant "$sid" "$pid" 30
    wait_for_workload "$sid" "$pid" 45

    local oom=0 grants=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        [ -z "$oom" ] && oom=0
    fi
    grants=$(grep -c " grant .* sid=$sid " "$WORK/daemon.log" 2>/dev/null) || grants=0

    [ "$oom" -eq 0 ]  || fail "B2: cgroup oom_count=$oom (controller couldn't prevent OOM)"
    [ "$grants" -gt 0 ] || fail "B2: workload completed without a controller grant"
    grep -q "workload done" "$WORK/$sid.log" || fail "B2: workload did not complete"
    if guest_oom_observed "$sid"; then
        fail "B2: guest log contains OOM or SIGKILL despite controller"
    fi
    if guest_self_cap_observed "$sid"; then
        fail "B2: guest self-cap fired before proactive control could absorb pressure"
    fi
    grep -q "admit token=.* sid=$sid" "$WORK/audit.log" || fail "B2: no admit in audit"
    echo "  Phase B2: grants=$grants oom_count=0 workload_done=1"

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

# ---------- run all phases ----------

phase_a
phase_b1_static_self_cap
phase_b2_dynamic_control
phase_c

echo
echo "==> e2e_density: PASS"
echo "    Phase A: auto resource allocation (1 sandbox, controller-driven)"
echo "    Phase B1: static → guest self-cap convergence + workload liveness"
echo "    Phase B2: dynamic + same floor/workload → proactive grant, no self-cap/OOM"
echo "    Phase C: 4 admits ok, 5th rejected by water mark"
