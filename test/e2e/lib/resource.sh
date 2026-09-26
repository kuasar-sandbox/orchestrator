#!/usr/bin/env bash
# Shared process/setup helpers for focused orchestrator resource product cases.
# Product cases consume prepared binaries and a locally prepared base image; this
# helper never downloads inputs or compiles products/helpers.
set -euo pipefail

resource_fail() { echo "FAIL: $*" >&2; exit 1; }

resource_init() {
    : "${BIN:?BIN must point to prepared products}"
    : "${WORK:?WORK must be provided by the E2E runner}"
    : "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
    . "$E2E_LIB/orchestrator/tarstream.sh"
    for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl cloud-hypervisor vmlinux; do
        [ -e "$BIN/$b" ] || resource_fail "missing prepared product $BIN/$b"
    done
    for tool in docker mkfs.ext4 python3 ip; do
        command -v "$tool" >/dev/null || resource_fail "missing prerequisite $tool"
    done
    [ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || resource_fail "/dev/kvm is required"
    [ "$(id -u)" -eq 0 ] || resource_fail "root is required"
    RESOURCE_IMAGE="${ORCHESTRATOR_BASE_IMAGE:-${E2E_IMAGE:-python:3.12-slim}}"
    docker image inspect "$RESOURCE_IMAGE" >/dev/null 2>&1         || resource_fail "prepared base image is missing: $RESOURCE_IMAGE"
    mkdir -p "$WORK/run" "$WORK/lib" "$WORK/units"
    RESOURCE_DAEMON_PID=""
    RESOURCE_SANDBOX_PIDS=()
    RESOURCE_SANDBOX_IDS=()
    [ -d /sys/fs/cgroup/sandboxes ] || mkdir /sys/fs/cgroup/sandboxes
    echo "+memory +cpu" >/sys/fs/cgroup/sandboxes/cgroup.subtree_control 2>/dev/null || true
    local blk="$WORK/base.img"
    docker save "$RESOURCE_IMAGE" | "$BIN/flatten-ctl" export --output "$blk" --no-progress
    RESOURCE_BASE_REF="$(plaintext_tarstream_ref "$blk")"
}

resource_setup_sandbox() {
    local sid="$1"
    RESOURCE_SANDBOX_IDS+=("$sid")
    mkdir -p "/sys/fs/cgroup/sandboxes/$sid"
    ip tuntap add "${sid}-tap" mode tap 2>/dev/null || true
    ip link set "${sid}-tap" up
    truncate -s 1G "$WORK/${sid}.diff"
    mkfs.ext4 -q -F -O ^has_journal "$WORK/${sid}.diff"
}

resource_write_sandbox() {
    local sid="$1" headroom="$2" capacity="$3" startup="$4" placeholder="${5:-false}"
    cat >"$WORK/$sid.yaml" <<EOF
resources:
  capacity: { cpu: 1, memory: ${capacity}MiB }
  allocatable: { cpu: 1, memory: ${headroom}MiB, deflate_on_oom: true }
  control:
    cgroup_path: /sys/fs/cgroup/sandboxes/$sid
    controller: $WORK/sandbox-resource.sock
  startup: { memory: ${startup}MiB }
network: { tap: ${sid}-tap }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root:
    base: $RESOURCE_BASE_REF
    overlay: { diff: file://$WORK/${sid}.diff }
EOF
    if [ "$placeholder" = true ]; then
        printf 'launch:\n  placeholder: true\n' >>"$WORK/$sid.yaml"
    else
        printf 'launch:\n  exec: /bin/sleep\n  args: ["300"]\n' >>"$WORK/$sid.yaml"
    fi
}

resource_write_controller() {
    local output="$1" compact="${2:-false}"
    local physical_memory=4GiB host_memory=512MiB host_cpu=1 startup_factor=0.50 ttl=120s
    if [ "$compact" = true ]; then
        physical_memory=400MiB; host_memory=80MiB; host_cpu=0.5; startup_factor=1.00; ttl=60s
    fi
    cat >"$output" <<EOF
api: { domain: resource.e2e.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { auth: enforce }
sandbox:
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle }
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
  cgroup_scan_paths: [/sys/fs/cgroup/sandboxes]
  resources:
    physical_memory: $physical_memory
    physical_cpu: 4
    host_reserved: { memory: $host_memory, cpu: $host_cpu }
  watermarks:
    operational_margin_factor: 0.10
    high_factor: 0.85
    low_factor: 0.70
    emergency_factor: 0.05
    startup_factor: $startup_factor
  rate_limits: { memory_grant_per_sec_factor: 0.20 }
  admission:
    rate: 50
    burst: 50
    startup_ttl: $ttl
    queue_ttl: 10s
    queue_max_depth: 256
  log_level: info
EOF
}

resource_start_controller() {
    local config="$1"
    "$BIN/node-ctl" conductor serve --config "$config" >"$WORK/resource-controller.log" 2>&1 &
    RESOURCE_DAEMON_PID=$!
    for _ in $(seq 1 80); do
        if [ -S "$WORK/sandbox-resource.sock" ] &&
           "$BIN/node-ctl" resource status --socket "$WORK/sandbox-resource.sock" >/dev/null 2>&1; then
            return 0
        fi
        kill -0 "$RESOURCE_DAEMON_PID" 2>/dev/null || {
            cat "$WORK/resource-controller.log" >&2
            resource_fail "resource controller exited before readiness"
        }
        sleep 0.25
    done
    resource_fail "resource controller did not become ready"
}

resource_stop_controller() {
    if [ -n "${RESOURCE_DAEMON_PID:-}" ]; then
        kill -TERM "$RESOURCE_DAEMON_PID" 2>/dev/null || true
        wait "$RESOURCE_DAEMON_PID" 2>/dev/null || true
    fi
    RESOURCE_DAEMON_PID=""
    rm -f "$WORK/sandbox-resource.sock"
}

resource_run_sandbox() {
    local sid="$1"
    "$BIN/sandbox-ctl" run --config "$WORK/$sid.yaml" --sandbox-id "$sid"         --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/run"         >"$WORK/$sid.log" 2>&1 &
    RESOURCE_LAST_PID=$!
    RESOURCE_SANDBOX_PIDS+=("$RESOURCE_LAST_PID")
}

resource_reservation_count() {
    "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" 2>/dev/null |
        python3 -c 'import json,sys; print(len(json.load(sys.stdin)))'
}

resource_wait_reservations() {
    local want="$1" timeout="$2" count=0 deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        count="$(resource_reservation_count 2>/dev/null || echo 0)"
        [ "$count" -ge "$want" ] && return 0
        sleep 0.25
    done
    resource_fail "only $count reservations after ${timeout}s; want $want"
}

resource_wait_state() {
    local sid="$1" pid="$2" mode="$3" timeout="$4" deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local rows
        rows="$("$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" 2>/dev/null || true)"
        if SID="$sid" MODE="$mode" ROWS="$rows" python3 - <<'PY' 2>/dev/null
import json, os
rows=json.loads(os.environ["ROWS"])
match=[r for r in rows if r.get("sandbox_id")==os.environ["SID"]]
assert len(match)==1, rows
r=match[0]
assert r.get("connected") is True and r.get("provisional", False) is False, r
assert r.get("stage")=="settled", r
if os.environ["MODE"]=="synced":
    assert len(rows)==1 and r.get("recovery_source")=="synced", (rows,r)
else:
    assert r.get("recovery_source") in {"admit","synced"}, r
PY
        then return 0; fi
        kill -0 "$pid" 2>/dev/null || resource_fail "$sid exited before reservation state $mode"
        sleep 0.25
    done
    resource_fail "$sid did not reach reservation state $mode"
}

resource_shutdown_pid() {
    local pid="$1"
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 120); do
        kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
        sleep 1
    done
    resource_fail "sandbox-ctl pid $pid did not stop after SIGTERM"
}

resource_cleanup() {
    set +e
    for pid in "${RESOURCE_SANDBOX_PIDS[@]:-}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${RESOURCE_SANDBOX_PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
    resource_stop_controller
    for sid in "${RESOURCE_SANDBOX_IDS[@]:-}"; do
        ip link delete "${sid}-tap" 2>/dev/null || true
        rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null || true
    done
}
