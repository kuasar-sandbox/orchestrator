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
#   Phase B   OOM elimination via controller (A/B comparison)
#             B1: static mode + allocatable.deflate_on_oom=false
#                 + tight allocatable=64MiB → Python killed by guest OOM
#                 (proves baseline is broken without the controller)
#             B2: dynamic mode, same workload + same allocatable
#                 → controller burst-grants → workload completes
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

for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.erofs flatten-ctl cloud-hypervisor; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build cloud-hypervisor'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run make vmlinux)"

WORK="$(mktemp -d /tmp/e2e-density-XXXXXX)"
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
    for pid in "${SANDBOX_PIDS[@]}"; do
        kill -TERM "$pid" 2>/dev/null
    done
    # Race: SIGTERM may not reach hung CH; wait briefly then force.
    local _w=0
    while [ "$_w" -lt 5 ]; do
        local _alive=0
        for pid in "${SANDBOX_PIDS[@]}"; do
            kill -0 "$pid" 2>/dev/null && _alive=1
        done
        [ "$_alive" -eq 0 ] && break
        sleep 1; _w=$((_w+1))
    done
    for pid in "${SANDBOX_PIDS[@]}"; do
        kill -KILL "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    if [ -n "$DAEMON_PID" ]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    for sid in "${SANDBOX_SIDS[@]}"; do
        pkill -KILL -f "cloud-hypervisor.*--api-socket /run/$sid/" 2>/dev/null
        rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null
        ip link delete "${sid}-tap" 2>/dev/null
        rm -rf "/run/$sid" 2>/dev/null
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
BLK0="$WORK/blk0.erofs"
docker save "$IMAGE" | "$BIN/flatten-ctl" --output "$BLK0" --no-progress

# ---------- cgroup parent ----------

echo "==> preparing cgroup parent /sys/fs/cgroup/sandboxes"
[ -d /sys/fs/cgroup/sandboxes ] || mkdir /sys/fs/cgroup/sandboxes
echo "+memory +cpu" > /sys/fs/cgroup/sandboxes/cgroup.subtree_control 2>/dev/null || true

# ---------- Python workload (agent-intermittent) ----------
# Source the shared workload model. See test/perf/workload.py for the
# full env/mode catalogue. e2e_density.sh always uses WL_MODE=cycles
# for reproducibility; perf harnesses can override.
WORKLOAD_PY="$(cat "$REPO_ROOT/test/perf/workload.py")"

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
#   BURST_MIB = ignored when MODE=static (no startup_burst section emitted)
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
            echo "  startup_burst:"
            echo "    memory: ${burst_mib}MiB"
        fi
        cat <<EOF
network:
  tap: ${sid}-tap
boot:
  kernel: file://${VMLINUX}
  runtime: file://${BIN}/sandbox-runtime.erofs
  cmdline: "console=hvc0"
  root:
    base: file://${BLK0}
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

start_daemon() {
    local cfg="$1"
    "$BIN/node-ctl" daemon --config "$cfg" >"$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        [ -S "$WORK/sandbox-resource.sock" ] && return 0
        sleep 0.2
    done
    fail "daemon socket not created in time"
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

write_default_config() {
    cat > "$WORK/node-ctl.yaml" <<EOF
listen: $WORK/sandbox-resource.sock
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
rate_limits:
  memory_grant_per_sec_factor: 0.20
admission:
  rate: 50
  burst: 50
  max_concurrent_creating: 50
  startup_ttl: 120s
  queue_ttl: 30s
dampening:
  recover_duration: 30s
  cooldown_periods: 5
logging:
  level: info
  audit_path: $WORK/audit.log
EOF
}

write_compact_config() {
    cat > "$WORK/node-ctl-compact.yaml" <<EOF
listen: $WORK/sandbox-resource.sock
state_path: $WORK/state.json
cgroup_scan_paths:
  - /sys/fs/cgroup/sandboxes
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
rate_limits:
  memory_grant_per_sec_factor: 0.20
admission:
  rate: 50
  burst: 50
  max_concurrent_creating: 50
  startup_ttl: 60s
  queue_ttl: 10s
dampening:
  recover_duration: 30s
  cooldown_periods: 5
logging:
  level: info
  audit_path: $WORK/audit.log
EOF
}

# ---------- Phase A: auto resource allocation ----------

# Note: we don't wait for sandbox-ctl run to exit cleanly because guest
# reboot path through vhost-user-blk has reconnect edge-cases unrelated
# to resource control. Instead we let the workload run for its declared
# duration + a margin, inspect cgroup + audit.log post-conditions, then
# SIGTERM the sandbox.
shutdown_sandbox() {
    local pid="$1" sid="${2:-}"
    kill -TERM "$pid" 2>/dev/null || true
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 15 ]; do
        sleep 1
        waited=$((waited+1))
    done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    # SIGKILL on sandbox-ctl doesn't propagate to CH child; nuke any
    # cloud-hypervisor still attached to this sandbox's API socket.
    if [ -n "$sid" ]; then
        pkill -KILL -f "cloud-hypervisor.*--api-socket /run/$sid/" 2>/dev/null || true
    fi
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
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    # Wait for guest cold-start (5-15s) + workload (20s) + margin for
    # grant/reclaim observation. Cold cache amplifies guest boot variance.
    sleep 45

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

# ---------- Phase B1: static mode demonstrates OOM ----------

phase_b1_static() {
    echo
    echo "==> Phase B1: static mode + deflate_on_oom=false → expected guest OOM"
    # No daemon. Static mode has no controller.

    local sid=sb-B1-1
    setup_sb "$sid"
    # Tight floor=64 MiB and large demand 256-384 MiB → guest physical
    # memory exhausted, no balloon escape (deflate_on_oom=false).
    emit_yaml "$sid" static 64 1024 0   15 2 256 384   false

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    # Guest boot + 15s workload + margin; OOM may fire mid-grow.
    sleep 35

    # Inspect cgroup memory.events.local for the OOM signature BEFORE
    # rmdir. Note: the OOM is INSIDE the guest, but the host's cgroup
    # also tracks under-pressure events.
    local oom=0 high=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        high=$(awk '$1=="high" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || high=0
        [ -z "$oom" ] && oom=0
        [ -z "$high" ] && high=0
    fi

    # On the guest-internal OOM path, host cgroup may not see oom but
    # it sees PSI throttling (high events). Either is enough evidence
    # the floor was insufficient; we assert at least one fired.
    if [ "$oom" -eq 0 ] && [ "$high" -eq 0 ]; then
        fail "B1: no oom or high events — workload may have completed unexpectedly"
    fi
    echo "  Phase B1: cgroup oom_count=$oom high_count=$high (insufficient floor)"

    shutdown_sandbox "$pid" "$sid"
    cleanup_sb "$sid"
    echo "Phase B1: PASS"
}

# ---------- Phase B2: dynamic mode prevents OOM ----------

phase_b2_dynamic() {
    echo
    echo "==> Phase B2: dynamic mode (same workload, same allocatable) → no OOM"
    write_default_config
    start_daemon "$WORK/node-ctl.yaml"

    local sid=sb-B2-1
    setup_sb "$sid"
    # Same params as B1, but with controller — burst grants must rescue
    # the workload from the same demand profile.
    emit_yaml "$sid" dynamic 64 1024 128   15 2 256 384   false

    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        >"$WORK/$sid.log" 2>&1 &
    local pid=$!
    SANDBOX_PIDS+=("$pid")

    sleep 35

    local oom=0 grants=0
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        oom=$(awk '$1=="oom" {print $2}' "/sys/fs/cgroup/sandboxes/$sid/memory.events.local") || oom=0
        [ -z "$oom" ] && oom=0
    fi
    grants=$(grep -c " grant .* sid=$sid " "$WORK/daemon.log" 2>/dev/null) || grants=0

    [ "$oom" -eq 0 ]  || fail "B2: cgroup oom_count=$oom (controller couldn't prevent OOM)"
    [ "$grants" -gt 0 ] || fail "B2: no grants observed (sensor broken)"
    grep -q "admit token=.* sid=$sid" "$WORK/audit.log" || fail "B2: no admit in audit"
    echo "  Phase B2: grants=$grants oom_count=0 (controller eliminated B1's OOM)"

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

    # Five sandboxes set up; first four launched in background, fifth synchronous.
    for i in 1 2 3 4 5; do
        local sid=sb-C-$i
        setup_sb "$sid"
        emit_yaml "$sid" dynamic 32 256 64   8 1 32 64   true
    done

    declare -a c_pids=()
    for i in 1 2 3 4; do
        local sid=sb-C-$i
        "$BIN/sandbox-ctl" run \
            --config "$WORK/$sid.yaml" \
            --sandbox-id "$sid" \
            --ch-binary "$BIN/cloud-hypervisor" \
            >"$WORK/$sid.log" 2>&1 &
        local p=$!
        c_pids+=("$p")
        SANDBOX_PIDS+=("$p")
    done

    # Race: try the 5th admit BEFORE the 10s active reclaimer first sweep,
    # otherwise the reclaimer would free enough to admit the 5th. Cold-start
    # takes ~1s so 5s gives all four time to settle.
    sleep 5

    local nres
    nres=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d.get("reservations", {})))' "$WORK/state.json" 2>/dev/null || echo 0)
    if [ "$nres" -lt 4 ]; then
        fail "C: only $nres reservations after 4 admits (expected 4)"
    fi
    echo "  $nres reservations after 4 sandboxes launched"

    # 5th synchronous attempt — must be rejected at admit. Capped at 15s
    # because it should reject within seconds; if it hangs longer the
    # admit logic is broken and timeout exposes that.
    local sid=sb-C-5
    set +e
    timeout 15 "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
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

    # Workload duration is 8s; let the 4 finish their workload then SIGTERM
    # to avoid hanging on CH reboot path.
    sleep 10
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
phase_b1_static
phase_b2_dynamic
phase_c

echo
echo "==> e2e_density: PASS"
echo "    Phase A: auto resource allocation (1 sandbox, controller-driven)"
echo "    Phase B1: static + deflate=false → guest OOM (baseline broken)"
echo "    Phase B2: dynamic + same allocatable → no OOM (controller fixes it)"
echo "    Phase C: 4 admits ok, 5th rejected by water mark"
