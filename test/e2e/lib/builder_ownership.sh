#!/usr/bin/env bash
assert_terminal_build_unowned() { # $1=build id, $2=terminal state
    python3 - "$WORK/lib/node-ctl.db" "$1" "$2" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("""
        select status, run_id, execution_claimed, execution_claimed_unix,
               phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json
          from builds where build_id=?
    """, (sys.argv[2],)).fetchone()
assert row is not None and row[0] == sys.argv[3], row
assert row[1] == "" and row[2] == 0 and row[3] == 0, row
assert all(value == "" for value in row[4:]), row
PY
}
live_build_signature() { # $1=build id; phase-reservations.json must be current
    python3 - "$WORK" "$1" <<'PY'
import hashlib, json, pathlib, sqlite3, sys
work = pathlib.Path(sys.argv[1])
with sqlite3.connect(work / "lib/node-ctl.db", timeout=5) as db:
    db.row_factory = sqlite3.Row
    row = db.execute("select * from builds where build_id=?", (sys.argv[2],)).fetchone()
assert row is not None, "live Build row is missing"
row = dict(row)
assert row["status"] == "building" and row["run_id"], "live Build lost its run-id"
assert row["execution_claimed"] == 1 and row["execution_claimed_unix"] > 0, "live claim is missing"
assert row["phase"] == "b" and row["phase_sandbox_id"], "expected live phase B"
assert row["runtime_prepare_json"] and row["runtime_vswitch_port"], "preparation is missing"
assert not row["execution_result_json"], "Build already accepted a result"
reservations = json.loads((work / "phase-reservations.json").read_text())
matches = [item for item in reservations if item["sandbox_id"] == row["phase_sandbox_id"]]
assert len(matches) == 1, "live phase must have exactly one reservation"
reservation = matches[0]
vmm = pathlib.Path(reservation["cgroup_path"])
vmm_members = [int(pid) for pid in (vmm / "cgroup.procs").read_text().split()]
# The existing VMM membership check accepts kernel tasks outside this PID
# namespace, which cgroup.procs renders as 0. They have no inspectable /proc
# identity. Keep every visible member, including associated KVM workers.
vmm_pids = [pid for pid in vmm_members if pid != 0]
assert vmm_pids, "phase VMM is not populated"
builder_pid = int((work / "run/runners" / (row["run_id"] + ".pid")).read_text())
ctl_pid = reservation["peer_pid"]
pids = {
    builder_pid, ctl_pid, *vmm_pids,
}
assert len(pids) >= 3 and all(pid > 0 for pid in pids), (
    "missing builder/ctl/VMM processes", builder_pid, ctl_pid, vmm_members)
print(f"live ownership: builder={builder_pid} ctl={ctl_pid} vmm_members={vmm_members}", file=sys.stderr)
# Include process start ticks so PID reuse cannot masquerade as live adoption.
processes = {}
for pid in sorted(pids):
    stat = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    assert stat[0] != "Z", f"process {pid} is a zombie"
    processes[str(pid)] = stat[19]
# Compare every durable value, including ciphertext and preparation, without
# writing the values themselves into test output or another plaintext fixture.
raw = json.dumps(row, sort_keys=True, default=lambda value: value.hex()).encode()
print(json.dumps({"run_id": row["run_id"], "row_sha256": hashlib.sha256(raw).hexdigest(),
                  "processes": processes, "vmm_path": str(vmm)}, sort_keys=True))
PY
}
assert_build_processes_gone() { # $1=live signature JSON
    python3 - "$1" <<'PY'
import json, pathlib, sys
signature = json.loads(pathlib.Path(sys.argv[1]).read_text())
for pid, start in signature["processes"].items():
    try:
        stat = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        continue
    assert stat[19] != start, f"Build process {pid} still exists after claim release"
events = pathlib.Path(signature["vmm_path"]) / "cgroup.events"
if events.exists():
    assert "populated 0" in events.read_text().splitlines(), "VMM still populated after claim release"
PY
}
build_trigger_signature() { # $1=build id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute(
        """select kind, from_image, from_template, start_cmd, ready_cmd,
                  steps_json, registry_auth_enc, metadata_json, builder_json,
                  status, reason, run_id
             from builds where build_id=?""",
        (sys.argv[2],),
    ).fetchone()
assert row is not None, "build row missing"
print(json.dumps(row, separators=(",", ":")))
PY
}
assert_build_finalized() { # build id label
    local bid="$1" label="$2"
    [ ! -e "$WORK/run/builds/$bid" ] || fail "$label retained BuildRunDir"
    [ ! -e "$WORK/lib/builds/$bid" ] || fail "$label retained BuildBaseDir"
    if find "$WORK/run" -type f \( -name '*.sandbox' -o -name '*.snapshot' -o -name '*.bundle' \) \
        -print -quit | grep -q .; then
        find "$WORK/run" -type f -print >&2
        fail "$label left a complete E/S/Bundle under run_root"
    fi
}
phase_sandbox_id() { # $1=phase, $2=opaque build id
    python3 - "$1" "$2" <<'PY'
import base64
import hashlib
import sys

digest = hashlib.sha256(sys.argv[2].encode()).digest()[:12]
encoded = base64.b32hexencode(digest).decode().rstrip("=").lower()
print(f"bp-{sys.argv[1]}-{encoded}")
PY
}
wait_phase_reservation() { # $1=phase, $2=build id
    local phase="$1" bid="$2" sid reservations=""
    sid=$(phase_sandbox_id "$phase" "$bid")
    for _ in $(seq 1 240); do
        reservations=$("$BIN/node-ctl" resource list \
            --socket "$WORK/sandbox-resource.sock" 2>/dev/null) || reservations=""
        if SID="$sid" RESERVATIONS_JSON="$reservations" python3 - 2>/dev/null <<'PY'
import json
import os

rows = json.loads(os.environ["RESERVATIONS_JSON"])
matches = [row for row in rows if row.get("sandbox_id") == os.environ["SID"]]
assert len(matches) == 1, rows
row = matches[0]
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
assert row.get("stage") == "settled", row
assert row.get("recovery_source") in {"admit", "synced"}, row
PY
        then
            return 0
        fi
        sleep 0.25
    done
    printf '%s\n' "$reservations" >&2
    return 1
}
wait_resource_reservations_empty() {
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" >"$WORK/phase-reservations-final.json" 2>/dev/null &&
            python3 - "$WORK/phase-reservations-final.json" 2>/dev/null <<'PY'
import json, sys
assert not json.load(open(sys.argv[1]))
PY
        then
            return 0
        fi
        sleep 0.25
    done
    cat "$WORK/phase-reservations-final.json" >&2 2>/dev/null || true
    return 1
}
assert_active_build_accounting() { # $1=tid, $2=bid, $3=expected phase
    local tid="$1" bid="$2" expected_phase="$3" code="" phase="" sid="" run_id="" reservation_json=""
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK" || true)
        if [ "$code" = "200" ]; then
            phase="" sid="" run_id=""
            read -r phase sid run_id < <(EXPECTED_PHASE="$expected_phase" python3 - "$WORK/resp.body" <<'PY'
import json, os, sys
status = json.load(open(sys.argv[1]))
phase = status.get("phase") or {}
if phase.get("name") == os.environ["EXPECTED_PHASE"] and phase.get("sandboxID") and status.get("runID"):
    print(phase["name"], phase["sandboxID"], status["runID"])
PY
) || true
            [ -n "$phase" ] && break
        fi
        sleep 0.25
    done
    [ -n "$phase" ] || fail "build $bid never exposed active phase $expected_phase"

    python3 - "$WORK/resp.body" "$BUILDER_CPU_MILLI" <<'PY' || fail "active Build status admission/phase"
import json, sys
status = json.load(open(sys.argv[1]))
assert status["resources"] == {
    "cpuMilli": int(sys.argv[2]),
    "memoryBytes": 6 << 30,
    "storageBytes": 4 << 30,
}, status
assert status["executionClaimed"] is True, status
assert "systemdEnforcement" not in status, status
assert status["storageEnforcement"] == "admission-only", status
PY

    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" >"$WORK/phase-reservations.json" 2>/dev/null &&
            SID="$sid" BUILD_CPU_MILLI="$BUILDER_CPU_MILLI" python3 - "$WORK/phase-reservations.json" 2>/dev/null <<'PY'
import json, os, sys
rows = json.load(open(sys.argv[1]))
assert len(rows) == 1, rows
row = rows[0]
assert row["sandbox_id"] == os.environ["SID"], row
assert row["capacity"] == {"memory_bytes": 6 << 30, "cpu_milli": int(os.environ["BUILD_CPU_MILLI"])}, row
assert row["floor"] == {"memory_bytes": 6 << 30, "cpu_milli": int(os.environ["BUILD_CPU_MILLI"])}, row
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
assert row["stage"] == "settled", row
# The existing admin-list wire name carries the sandbox's absolute safe
# reservation baseline. A/B resources come from Build.Resources and are
# intentionally independent from the 3GiB target Sandbox configuration.
reservation = row["allocatable_memory"]
assert reservation == 6 << 30, row
assert row["cgroup_path"].endswith("/vmm"), row
assert row["peer_pid"] > 0, row
PY
        then
            reservation_json=$(cat "$WORK/phase-reservations.json")
            break
        fi
        sleep 0.25
    done
    [ -n "$reservation_json" ] || fail "phase $phase/$sid did not become the sole precise nodectl reservation"

    local build_run_dir="$WORK/run/builds/$bid"
    local build_base_dir="$WORK/lib/builds/$bid"
    [ "$sid" != "$phase" ] || fail "phase PathID replaced logical SandboxID ($sid)"
    [ -f "$build_run_dir/builder.pid" ] || fail "BuildRunDir is missing builder.pid"
    [ -S "$build_run_dir/$phase/ctl.sock" ] || fail "phase $phase ctl.sock is outside its PathID RunDir"
    [ -d "$build_base_dir/$phase" ] || fail "phase $phase BaseDir is missing"
    [ -d "$build_base_dir/checkpoint" ] || fail "Build checkpoint directory is missing from BuildBaseDir"
    if find "$build_run_dir" -type f \( -name '*.img' -o -name '*.diff' -o -name '*.sandbox' -o -name '*.snapshot' \) -print -quit | grep -q .; then
        find "$build_run_dir" -type f -print >&2
        fail "BuildRunDir contains a large image/diff/checkpoint artifact"
    fi

    "$BIN/node-ctl" builder status --socket "$WORK/node-ctl.socket" >"$WORK/builder-status.json" \
        || fail "node-ctl builder status"
    python3 - "$WORK/builder-status.json" "$BUILDER_CPU_MILLI" "$BUILDER_EXECUTION_CPU_MILLI" "$BUILDER_REGISTRATION_CPU_MILLI" <<'PY' || fail "durable Builder admission status"
import json, sys
status = json.load(open(sys.argv[1]))
build_cpu, execution_cpu, registration_cpu = map(int, sys.argv[2:])
assert status["registration"]["configured"] == {
    "max_builds": 16,
    "resources": {"cpu": registration_cpu, "memory": 96 << 30, "storage": 128 << 30},
}, status
assert status["execution"]["configured"] == {
    "max_builds": 2,
    "resources": {"cpu": execution_cpu, "memory": 12 << 30, "storage": 16 << 30},
}, status
# The deliberately untriggered negative-test registration and this active
# Build both consume registration admission. Only this Build consumes execution.
assert status["registration"]["used_builds"] == 2, status
assert status["registration"]["used_resources"] == {
    "cpu": 2 * build_cpu, "memory": 12 << 30, "storage": 8 << 30,
}, status
assert status["execution"]["used_builds"] == 1, status
assert status["execution"]["used_resources"] == {
    "cpu": build_cpu, "memory": 6 << 30, "storage": 4 << 30,
}, status
PY

    local build_pid sandbox_ctl_pid vmm_path ctl_path unit_path slice_path memory_high=""
    build_pid=$(cat "$WORK/run/runners/$run_id.pid")
    sandbox_ctl_pid=$(python3 - "$WORK/phase-reservations.json" "$sid" <<'PY'
import json, sys
for row in json.load(open(sys.argv[1])):
    if row["sandbox_id"] == sys.argv[2]:
        print(row["peer_pid"])
        break
PY
)
    vmm_path=$(python3 - "$WORK/phase-reservations.json" "$sid" <<'PY'
import json, sys
for row in json.load(open(sys.argv[1])):
    if row["sandbox_id"] == sys.argv[2]:
        print(row["cgroup_path"])
        break
PY
)
    ctl_path="$(dirname "$vmm_path")/ctl"
    unit_path=$(dirname "$vmm_path")
    slice_path=$(dirname "$unit_path")
    e2e_assert_vmm_cgroup_members /proc "$vmm_path/cgroup.procs" \
        || fail "phase VMM membership does not match Cloud Hypervisor and its KVM workers"
    grep -Eq '/sandbox-builder.slice/.+/ctl$' "/proc/$build_pid/cgroup" \
        || fail "run-builder pid $build_pid is not in its ctl subgroup"
    grep -Eq '/sandbox-builder.slice/.+/ctl$' "/proc/$sandbox_ctl_pid/cgroup" \
        || fail "phase sandbox-ctl pid $sandbox_ctl_pid is not in its ctl subgroup"
    [ ! -s "$unit_path/cgroup.procs" ] || fail "builder service cgroup root has direct processes"
    grep -qw cpu "$unit_path/cgroup.subtree_control" || fail "builder unit did not enable cpu controller"
    grep -qw memory "$unit_path/cgroup.subtree_control" || fail "builder unit did not enable memory controller"
    [ "$(cat "$ctl_path/memory.high")" = "max" ] || fail "builder ctl subgroup inherited a low memory.high"
    # Cold start deliberately leaves VMM memory.high=max through launch ACK and
    # settled.  The first trusted guest report starts the initial shrink, and
    # high becomes finite only after balloon current converges. B2 waits on a
    # guest marker while these checks and live recovery inspect its resources.
    for _ in $(seq 1 60); do
        memory_high=$(cat "$vmm_path/memory.high" 2>/dev/null || true)
        [ "$memory_high" != "max" ] && [ -n "$memory_high" ] && break
        sleep 0.25
    done
    if [ "$memory_high" = "max" ] || [ -z "$memory_high" ]; then
        fail "phase VMM did not leave deferred memory.high after a trusted report (last=${memory_high:-missing})"
    fi
    python3 - "$unit_path" "$slice_path" "$vmm_path" "$ctl_path" "$BUILDER_CPU_MILLI" <<'PY' || fail "parent/ctl isolation and VMM resource limits"
import pathlib, sys

def assert_cpu(path, milli):
    quota, period = (path / "cpu.max").read_text().split()
    assert quota != "max", (path, quota, period)
    assert int(quota) * 1000 == int(period) * milli, (path, quota, period, milli)

unit, pool, vmm, ctl = map(pathlib.Path, sys.argv[1:5])
build_cpu = int(sys.argv[5])
for parent in (unit, pool, ctl):
    assert (parent / "memory.max").read_text().strip() == "max", parent
    assert (parent / "cpu.max").read_text().split()[0] == "max", parent
assert (ctl / "memory.high").read_text().strip() == "max", ctl
# Existing sandbox-ctl policy: capacity plus the node's default 32MiB overhead.
assert int((vmm / "memory.max").read_text()) == (6 << 30) + (32 << 20), vmm
assert 0 < int((vmm / "memory.high").read_text()) <= (6 << 30) + (32 << 20), vmm
assert_cpu(vmm, build_cpu)
assert int((vmm / "cpu.weight").read_text()) == min(10000, max(1, build_cpu // 10)), vmm
print("parent service/slice/ctl: cpu.max=max memory.max=max; VMM capacity/weight/high retained")
PY
    echo "==> PASS: active phase $phase/$sid is the only nodectl reservation; parent limits absent and VMM policy/ctl isolation verified (memory.high=$memory_high)"
}
