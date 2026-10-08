#!/usr/bin/env bash
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
# ---- (5) auto-resume THROUGH the proxy ------------------------------------
echo "==> pause $SID, then reuse the same exec KAT through the Proxy"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
if [ "$code" = "204" ]; then
    exec_argv_denied_through_proxy "$SID" "$EXEC_TOKEN"
    wait_traffic_stats "$SID" paused \
        || { dump_logs; fail "condition-denied Proxy exec changed paused state or traffic"; }
    RESUME_MARK="PROXY_EXEC_RESUME_$RANDOM"
    exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK" 40
    ok=""
    for _ in $(seq 1 40); do
        code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
        { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
        sleep 0.5
    done
    if [ -n "$ok" ]; then echo "==> PASS: same KAT woke the paused sandbox through Proxy (envd code=$code)"
    else dump_logs; fail "auto-resume via proxy did not complete, last code=$code"; fi
else dump_logs; fail "pause returned $code (want 204 before auto-resume check)"; fi
unset EXEC_TOKEN

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
# ---- full node pressure / Snapshot / adoption / Proxy recovery ------------
# Build while the case still uses its ordinary static Builder configuration.
# The pressure phase starts only after that Build has completed and owns just
# these two primary guest workloads; it never changes host services/resources.
build_image_template bare
stop_orchestrator
write_orchestrator_config unset controller
python3 - "$WORK/config.yaml" <<'PY_CONFIG'
from pathlib import Path
import sys
path = Path(sys.argv[1]); text = path.read_text()
# P=1GiB fits either full-capacity Snapshot, while both held workloads plus
# their real headroom cannot run together. No workload size enters node policy.
text = text.replace("physical_memory: auto", "physical_memory: 1792MiB", 1)
text = text.replace("host_reserved: { memory: 1GiB, cpu: 0.5 }", "host_reserved: { memory: 512MiB, cpu: 0.5 }", 1)
needle = "  admission: { rate: 50,"
assert text.count(needle) == 1
text = text.replace(needle, """  watermarks: { low_factor: 0.70, high_factor: 0.85, emergency_factor: 0.05, operational_margin_factor: 0.20, startup_factor: 0.40 }
  # This deliberately small pool must exercise memory shortage, not the
  # independent normal-grow rate limiter's one-second bucket.
  rate_limits: { memory_grant_per_sec_factor: 1.0 }
  pressure: { interval: 2s, failure_interval: 500ms, critical_after_rounds: 3, pause_after_rounds: 3, critical_exit_hold: 5s, red_to_yellow_hold: 5s, yellow_to_green_hold: 5s, minimum_run_time: 2s }
""" + needle, 1)
path.write_text(text)
PY_CONFIG
start_orchestrator "$WORK/orch-pressure.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"

pressure_status() {
    "$BIN/node-ctl" resource pressure --socket "$WORK/node-ctl.socket" > "$WORK/pressure-status.json"
}
pressure_diagnostics() (
    # Best-effort, bounded diagnostics preserve the original assertion/exit.
    # Project explicit nonsecret fields: never print API credentials, full
    # sandbox metadata, resource tokens, or the whole conductor configuration.
    set +e
    echo "==> pressure failure diagnostics"
    timeout 5 "$BIN/node-ctl" resource pressure --socket "$WORK/node-ctl.socket"
    timeout 5 "$BIN/node-ctl" resource status --socket "$WORK/sandbox-resource.sock"
    timeout 5 "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" |
        python3 -c '
import json, pathlib, sys
fields = ("sandbox_id", "peer_pid", "cgroup_path", "capacity", "floor", "allocatable_memory", "effective_startup_bytes", "stage", "last_report_unix", "current_rss", "connected", "provisional", "startup_expired")
for row in json.load(sys.stdin) or []:
    out = {key: row[key] for key in fields if key in row}
    pid = row.get("peer_pid", 0)
    try:
        out["process_status"] = [line for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines() if line.split(":", 1)[0] in ("Name", "State", "Pid", "PPid", "VmRSS", "RssAnon", "VmSwap")]
    except OSError:
        out["process_status"] = "absent"
    cg = pathlib.Path("/sys/fs/cgroup") / row.get("cgroup_path", "").removeprefix("/sys/fs/cgroup/").lstrip("/")
    for name in ("memory.current", "memory.high", "memory.max", "memory.events", "memory.stat", "cgroup.events"):
        try: out[name] = (cg / name).read_text().strip()
        except OSError: out[name] = "absent"
    print(json.dumps(out, sort_keys=True))
'
    python3 - "$WORK" "$EXECUTE_RUN_ROOT" <<'PY_DIAG'
from pathlib import Path
import json, sys, yaml
work = Path(sys.argv[1]); run_root = Path(sys.argv[2]); section = False
for line in (work / "config.yaml").read_text().splitlines():
    if line == "resource_listen:": section = True
    elif line and not line[0].isspace(): section = False
    if section: print(line)
for path in (run_root / "sandboxes").glob("*/*.yaml"):
    config = yaml.safe_load(path.read_text()); resource = config.get("resources", {})
    projection = {key: resource.get(key) for key in ("capacity", "allocatable", "startup", "overhead", "watermark_high")}
    projection["controller"] = resource.get("control", {}).get("controller")
    print("runtime resource config", path.parent.name, json.dumps(projection))
for name in ("pause-barrier-target", "pause-barrier-reached", "pause-barrier-release", "snapshot-argv.jsonl"):
    path = work / name
    print(name, "present" if path.exists() else "absent")
PY_DIAG
    for sid in "${FIRST:-}" "${SECOND:-}" "${PRESSURE_SID:-}"; do
        [ -n "$sid" ] || continue
        timeout 5 python3 - "$SCRIPT_DIR" "$WORK" "$sid" "$RUNNER_PREFIX" <<'PY_TERMINAL'
import json, subprocess, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import runner_lifecycle
record = runner_lifecycle.terminal_evidence(Path(sys.argv[2]), sys.argv[3])
print("durable runner termination " + json.dumps(record, sort_keys=True), flush=True)
for rid in sorted({record["run_id"], record["result_run_id"]} - {""}):
    result = subprocess.run(["systemctl", "show", sys.argv[4] + rid + ".service",
        "--property=ActiveState,SubState,Result,MainPID,ExecMainCode,ExecMainStatus"],
        text=True, capture_output=True, timeout=3, check=False)
    print("runner unit result " + rid + " " + result.stdout[:2048], flush=True)
PY_TERMINAL
        echo "==> guest memory/state sid=$sid"
        timeout 5 "$BIN/sandbox-ctl" exec --run-root "$EXECUTE_RUN_ROOT/sandboxes" --sandbox-id "$sid" -- /bin/cat /proc/meminfo /tmp/pressure-state.json
        timeout 5 curl -fsS --unix-socket "$EXECUTE_RUN_ROOT/sandboxes/$sid/ch.sock" http://localhost/api/v1/vm.info |
            python3 -c 'import json,sys; v=json.load(sys.stdin); print(json.dumps({"memory_actual_size":v.get("memory_actual_size"), "balloon":v.get("config",{}).get("balloon")}))'
        echo "==> guest/controller journal sid=$sid"
        # Keep the beginning of bounded guest panic reports, not just trailing goexit frames.
        timeout 5 journalctl "KUASAR_SANDBOX_ID=$sid" --no-pager -n 800 -o cat |
            grep -E 'memory:|mem_report|sensor:|BudgetAtSnapshot|workload|Cloud Hypervisor|runtime.*(ready|exit)|admit rejected|exec: open guest session:|reverse-channel: (restore|attach|quiesce)|CH exited code=|received (terminated|interrupt)|primary.*(exit|signal)|held-memory|RuntimeError:|fatal error:|panic:|SIG[A-Z]+:|^runtime:|^goroutine [0-9]+|^Kernel panic|^CPU:|^Call Trace:|^RIP:' | tail -120
    done
    echo "==> conductor pressure/settlement events"
    grep -E 'node pressure|resource controller listening|grant .*sid=|settled .*sid=|inventory' "$WORK/orch-pressure.log" |
        sed -E 's/(grant|settled|admit) [[:xdigit:]]+ sid=/\1 [redacted] sid=/g' | tail -100
    [ ! -f "$WORK/pressure-timeline.log" ] || tail -20 "$WORK/pressure-timeline.log"
    return 0
)
fail() {
    pressure_diagnostics > "$WORK/pressure-failure.log" 2>&1 || true
    cat "$WORK/pressure-failure.log" >&2
    echo "==> FAIL: $*" >&2
    exit 1
}
pressure_cleanup() {
    local result=$? sid
    trap - EXIT
    set +e
    if [ "$result" != 0 ]; then
        # Keep the conductor alive for normal detach before the shared helper
        # stops processes. Abrupt failure with live guests otherwise leaves
        # attached ports, correctly retaining the execute recovery record.
        [ ! -e "$PAUSE_BARRIER_TARGET" ] || : > "$PAUSE_BARRIER_RELEASE"
        timeout 2 "$BIN/node-ctl" resource drain --socket "$WORK/sandbox-resource.sock" >/dev/null 2>&1
        for sid in "${FIRST:-}" "${SECOND:-}" "${PRESSURE_SID:-}"; do
            [ -n "$sid" ] || continue
            curl -sS --noproxy '*' --max-time 5 -o /dev/null -X DELETE \
                -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
                "http://127.0.0.1:$PORT/sandboxes/$sid" >/dev/null 2>&1
        done
        python3 - "$WORK/lib/node-ctl.db" "${FIRST:-}" "${SECOND:-}" "${PRESSURE_SID:-}" <<'PY_CLEANUP'
import sqlite3, sys, time
ids = set(sys.argv[2:]) - {""}; end = time.monotonic() + 20
while ids and time.monotonic() < end:
    with sqlite3.connect(sys.argv[1], timeout=1) as db:
        ids.intersection_update(row[0] for row in db.execute("select id from sandboxes"))
    if ids: time.sleep(.2)
if ids: print("==> pressure cleanup still pending; ownership recovery guard will verify remaining resources")
PY_CLEANUP
    fi
    cleanup
    exit "$result"
}
trap pressure_cleanup EXIT
pressure_memory_observation() {
    # Observe real consumers and file cache without faulting Snapshot pages in,
    # reclaiming host caches, or assuming when the other guest will grow.
    python3 - "$BIN/node-ctl" "$WORK" "$FIRST" "$SECOND" "$1" <<'PY_RELEASE'
import ctypes, json, os, subprocess, sys, time
from pathlib import Path
binary, directory, first, second, phase = sys.argv[1:]
work = Path(directory)
rows = json.loads(subprocess.check_output([binary, "resource", "list", "--socket", str(work / "sandbox-resource.sock")], timeout=5)) or []
def start_ticks(pid):
    try: return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
    except FileNotFoundError: return None
def values(path):
    return {key: int(value) for key, value in (line.split() for line in path.read_text().splitlines())}
record = {"phase": phase, "at": time.time(), "guests": []}
record["host_memory_kib"] = {key.rstrip(":"): int(value) for key, value, *_ in
    (line.split() for line in Path("/proc/meminfo").read_text().splitlines())
    if key.rstrip(":") in ("MemAvailable", "MemFree", "Cached", "Dirty", "Writeback")}
for row in rows:
    assert row["sandbox_id"] in (first, second), "unexpected resource consumer"
    cg = Path(row["cgroup_path"])
    vmm = []
    for pid in (cg / "cgroup.procs").read_text().split():
        try:
            if Path(f"/proc/{pid}/exe").resolve().name == "cloud-hypervisor":
                vmm.append({"pid": int(pid), "start_ticks": start_ticks(pid)})
        except FileNotFoundError: pass
    record["guests"].append({"sid": row["sandbox_id"], "reservation": row["allocatable_memory"],
        "stage": row["stage"], "cgroup": str(cg), "vmm": vmm,
        "memory_current": int((cg / "memory.current").read_text()), "memory_stat": values(cg / "memory.stat")})
if phase == "before":
    old = next(row for row in record["guests"] if row["sid"] == first)
    assert old["stage"] == "settled" and old["vmm"] and old["memory_current"] > 0 and old["reservation"] > 0, old
else:
    before = json.loads((work / "pressure-memory-before.json").read_text())
    old = next(row for row in before["guests"] if row["sid"] == first)
    assert all(row["sid"] != first for row in record["guests"]), "old reservation still charged"
    for proc in old["vmm"]:
        assert start_ticks(proc["pid"]) != proc["start_ticks"], "old VMM still alive"
    cg = Path(old["cgroup"])
    assert not cg.exists() or values(cg / "cgroup.events")["populated"] == 0, "old consumer cgroup still populated"
    record["released_vmm_memory_current"] = old["memory_current"]
    # mincore only inspects residency; PROT_NONE mappings do not read/fault data.
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    page = os.sysconf("SC_PAGE_SIZE"); seen = set(); files = []
    for path in (work / "lib/sandboxes" / first / "checkpoint").rglob("*"):
        if not path.is_file(): continue
        stat = path.stat(); identity = (stat.st_dev, stat.st_ino)
        if identity in seen: continue
        seen.add(identity); resident = 0
        if stat.st_size:
            with path.open("rb") as source:
                address = libc.mmap(None, stat.st_size, 0, 1, source.fileno(), 0)
                assert address != ctypes.c_void_p(-1).value, ctypes.get_errno()
                try:
                    vector = (ctypes.c_ubyte * ((stat.st_size + page - 1) // page))()
                    assert libc.mincore(address, stat.st_size, vector) == 0, ctypes.get_errno()
                    resident = sum(value & 1 for value in vector) * page
                finally: assert libc.munmap(address, stat.st_size) == 0
        files.append({"bytes": stat.st_size, "allocated_bytes": stat.st_blocks * 512, "resident_cache_bytes": resident})
    assert files, "no saved Snapshot files"
    record["checkpoint_files"] = files
(work / f"pressure-memory-{phase}.json").write_text(json.dumps(record, sort_keys=True))
print("==> physical memory observation " + json.dumps(record, sort_keys=True))
PY_RELEASE
}
pressure_wait_parking() {
    # These are bare guests: only forward and native exec are registered.
    # The general E2B helper requires envd/code-interpreter service entries.
    local code
    for _ in $(seq 1 100); do
        code=$(req GET "/sandboxes/$FIRST/stats/traffic" "$AK")
        pressure_status
        if [ "$code" = 200 ] && python3 - "$WORK/resp.body" "$WORK/pressure-status.json" "$FIRST" <<'PY_PARKED'
import json, sys
stats, pressure = [json.load(open(path)) for path in sys.argv[1:3]]
row = next(row for row in pressure["sandboxes"] if row["sandbox_id"] == sys.argv[3])
services = stats.get("services", {})
assert set(services) == {"forward", "exec"}, stats
assert set(stats["inflight"]) == {"parking", "connected"}, stats
assert all(set(item) >= {"parking", "connected"} for item in services.values()), stats
ok = (pressure["zone"] == "critical" and row["state"] == "paused" and
      row["pause_reason"] == "explicit" and not row["recovery_obligation"] and
      stats["state"] == "paused" and stats["inflight"]["parking"] >= 1 and
      services["exec"]["parking"] >= 1 and "idleSince" not in stats)
if ok: print("==> ordinary critical Wake remains paused with native exec parked")
raise SystemExit(0 if ok else 1)
PY_PARKED
        then return 0; fi
        sleep .05
    done
    return 1
}
pressure_create() { # primary workload, no dependence on a surviving exec
    local amount="$1" body
    body=$(python3 - "$TEMPLATE" "$SCRIPT_DIR/workload.py" "$amount" <<'PY_CREATE'
import json, sys
from pathlib import Path
program = Path(sys.argv[2]).read_text()
print(json.dumps(dict(templateID=sys.argv[1], timeout=600, metadata={
    "kuasar-sandbox.resource": json.dumps(dict(capacity=dict(cpu=1, memory="1GiB"), allocatable=dict(cpu=1, memory="64MiB"), startup=dict(memory="256MiB"))),
    "kuasar-sandbox.launch": json.dumps(dict(exec="/bin/sh", args=["-c", "exec python3 -u -c \"$1\"", "pressure", program], restart="never", env={"WL_MODE":"hold", "WL_DURATION":"600", "WL_RMAX_MIB":sys.argv[3], "WL_START_GATE":"/tmp/pressure.start", "WL_START_GATE_TIMEOUT":"120"}))
})))
PY_CREATE
    )
    code=$(req POST /sandboxes "$AK" "$body")
    [ "$code" = 201 ] || fail "pressure create=$code"
    PRESSURE_SID=$(json_field "$WORK/resp.body" sandboxID)
    wait_sandbox_state "$PRESSURE_SID" running 600 || fail "pressure primary did not start"
}
pressure_guest_state() {
    "$BIN/sandbox-ctl" exec --run-root "$EXECUTE_RUN_ROOT/sandboxes" --sandbox-id "$1" -- /bin/cat /tmp/pressure-state.json
}
pressure_create 384
FIRST=$PRESSURE_SID
FIRST_KAT=$(issue_exec_session "$FIRST" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")
"$BIN/sandbox-ctl" exec --run-root "$EXECUTE_RUN_ROOT/sandboxes" --sandbox-id "$FIRST" -- /bin/touch /tmp/pressure.start
ready=""
for _ in $(seq 1 300); do
    if pressure_guest_state "$FIRST" > "$WORK/pressure-before.json" 2>/dev/null; then ready=1; break; fi
    sleep 0.1
done
[ -n "$ready" ] || fail "first held-memory workload did not become ready"
# Heap allocation can finish through balloon self-deflation before its node
# Budget catches up. Establish a satisfied, eligible oldest consumer first;
# otherwise it is legitimately the beneficiary and the newer guest is paused.
python3 - "$BIN" "$WORK" "$EXECUTE_RUN_ROOT" "$FIRST" <<'PY_SATISFIED' || fail "first held workload did not obtain its real Budget/headroom"
import json, subprocess, sys, time
from pathlib import Path
binary, directory, runtime, sid = sys.argv[1:]; work = Path(directory); run_root = Path(runtime)
end = time.monotonic() + 40
last = None
while time.monotonic() < end:
    rows = json.loads(subprocess.check_output([binary + "/node-ctl", "resource", "list", "--socket", str(work / "sandbox-resource.sock")], timeout=5)) or []
    row = next(r for r in rows if r["sandbox_id"] == sid)
    info = json.loads(subprocess.check_output(["curl", "-fsS", "--max-time", "2", "--unix-socket", str(run_root / "sandboxes" / sid / "ch.sock"), "http://localhost/api/v1/vm.info"], timeout=3))
    mem = subprocess.check_output([binary + "/sandbox-ctl", "exec", "--run-root", str(run_root / "sandboxes"), "--sandbox-id", sid, "--", "/bin/cat", "/proc/meminfo"], timeout=5).decode()
    available = next(int(line.split()[1]) * 1024 for line in mem.splitlines() if line.startswith("MemAvailable:"))
    last = dict(reservation=row["allocatable_memory"], actual=info["memory_actual_size"], target_budget=(1<<30)-info["config"]["balloon"]["size"], available=available, stage=row["stage"])
    # Actual can remain above target after guest self-deflation. The existing
    # local policy grows for demand + headroom, not actual/target equality.
    required = max(last["target_budget"], last["actual"] - available + 64*(1<<20))
    if last["stage"] == "settled" and last["reservation"] >= required and last["actual"] >= 384*(1<<20) and available >= 64*(1<<20):
        print("==> oldest workload has observed Budget/headroom", json.dumps(last))
        break
    time.sleep(.5)
else:
    raise AssertionError(last)
PY_SATISFIED
# Hold the existing capture boundary to observe Q before any memory is released.
printf '%s\n' "$FIRST" > "$PAUSE_BARRIER_TARGET"
rm -f "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
pressure_create 576
SECOND=$PRESSURE_SID
"$BIN/sandbox-ctl" exec --run-root "$EXECUTE_RUN_ROOT/sandboxes" --sandbox-id "$SECOND" -- /bin/touch /tmp/pressure.start
pressure_deadline=$((SECONDS + 60))
pressure_next_sample=$SECONDS
while [ ! -e "$PAUSE_BARRIER_REACHED" ] && [ "$SECONDS" -lt "$pressure_deadline" ]; do
    if [ "$SECONDS" -ge "$pressure_next_sample" ]; then
        timeout 2 "$BIN/node-ctl" resource pressure --socket "$WORK/node-ctl.socket" |
            python3 -c 'import json,sys,time; print(json.dumps(dict(at=time.time(), pressure=json.load(sys.stdin))))' >> "$WORK/pressure-timeline.log" || true
        pressure_next_sample=$((SECONDS + 2))
    fi
    sleep 0.1
done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail "sustained demand did not select the longest running guest"
pressure_status
python3 - "$WORK/pressure-status.json" <<'PY_CAPTURE' || fail "capture obligation accounting"
import json, sys
p=json.load(open(sys.argv[1]))
assert p["zone"] == "critical" and p["capturing"] == 1 and p["pending_recoveries"] == 1, p
assert p["reserved_memory"] > 0, p
PY_CAPTURE
pressure_memory_observation before || fail "live pressure consumer observation failed"
# Drain only this test controller while capture is already accepted, so slow
# cleanup cannot let automatic recovery win the adoption assertion.
"$BIN/node-ctl" resource drain --socket "$WORK/sandbox-resource.sock" >/dev/null
: > "$PAUSE_BARRIER_RELEASE"
wait_sandbox_state "$FIRST" paused 600 || fail "resource capture did not finish"
# The lifecycle fence serializes adoption with cleanup. Drain postpones only
# background launch acceptance; adoption performs no capture or new RunID.
SNAPSHOT_PAIR=$(checkpoint_pair "$FIRST" snapshot "$WORK/lib/sandboxes/$FIRST/checkpoint/$FIRST.snapshot")
code=$(req POST "/sandboxes/$FIRST/pause" "$AK")
[ "$code" = 204 ] || fail "resource Pause adoption=$code"
pressure_status
python3 - "$WORK/pressure-status.json" "$FIRST" <<'PY_ADOPT' || fail "adoption obligation accounting"
import json, sys
p=json.load(open(sys.argv[1])); row=next(r for r in p["sandboxes"] if r["sandbox_id"]==sys.argv[2])
assert p["zone"]=="critical" and p["pending_recoveries"]==0, p
assert row["state"]=="paused" and row["pause_reason"]=="explicit" and not row["recovery_obligation"], row
PY_ADOPT
wait_paused_cleanup "$FIRST" 600 || fail "resource Pause cleanup incomplete"
pressure_memory_observation after || fail "resource Pause did not release the old physical consumer"
[ "$(checkpoint_pair "$FIRST" snapshot "$WORK/lib/sandboxes/$FIRST/checkpoint/$FIRST.snapshot")" = "$SNAPSHOT_PAIR" ] || fail "adoption changed Snapshot source"
"$BIN/node-ctl" resource drain --disable --socket "$WORK/sandbox-resource.sock" >/dev/null
code=$(req POST "/sandboxes/$FIRST/pause" "$AK")
[ "$code" = 409 ] || fail "ordinary repeated Pause=$code"
# A real request remains parked across the critical policy refusal. It is sent
# once; there is no replay of a delivered operation or a killed exec session.
exec_through_proxy_connect "$FIRST" "$FIRST_KAT" "PRESSURE_RESTORED_$RANDOM" 1 100 &
PRESSURE_EXEC_PID=$!
PIDS+=("$PRESSURE_EXEC_PID")
pressure_wait_parking || fail "ordinary critical request did not park"
# Record the second primary before its explicit Snapshot as well; later
# recovery must preserve its identity, payload and increasing work counter.
second_ready=""
for _ in $(seq 1 200); do
    if pressure_guest_state "$SECOND" > "$WORK/pressure-second-before.json" 2>/dev/null &&
       python3 - "$WORK/pressure-second-before.json" <<'PY_SECOND'
import json, sys
v = json.load(open(sys.argv[1]))
assert v["held"] == 576*(1<<20) and v["nonce"] and v["tick"] >= 0, v
PY_SECOND
    then second_ready=1; break; fi
    sleep .2
done
[ -n "$second_ready" ] || fail "second held primary did not finish allocation"
code=$(req POST "/sandboxes/$SECOND/pause" "$AK")
[ "$code" = 204 ] || fail "second explicit Pause=$code"
wait "$PRESSURE_EXEC_PID" || fail "parked Proxy Wake did not recover adopted Snapshot"
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$PRESSURE_EXEC_PID" ] || PIDS[$i]=""; done
wait_sandbox_state "$FIRST" running 600 || fail "adopted Snapshot did not resume"
restored=""
for _ in $(seq 1 100); do
    pressure_guest_state "$FIRST" > "$WORK/pressure-after.json"
    if python3 - "$WORK/pressure-before.json" "$WORK/pressure-after.json" <<'PY_MEMORY'
import json, sys
before,after=[json.load(open(p)) for p in sys.argv[1:]]
assert before["nonce"]==after["nonce"] and before["held"]==after["held"]==384*(1<<20)
raise SystemExit(0 if after["tick"]>before["tick"] else 1)
PY_MEMORY
    then restored=1; break; fi
    sleep 0.2
done
[ -n "$restored" ] || fail "primary held memory/identity did not survive Snapshot"
# The saved baseline exceeds the ordinary startup pool; runtime admission must
# have restored the full Snapshot budget through the serialized saved lane.
grep -Eq 'restore BudgetAtSnapshot reserved=' "$WORK/orch-pressure.log" ||     journalctl KUASAR_SANDBOX_ID="$FIRST" --no-pager -o cat > "$WORK/pressure-restore.journal"
python3 - "$WORK/orch-pressure.log" "$WORK/pressure-restore.journal" "$WORK/pressure-status.json" <<'PY_BUDGET' || fail "complete Snapshot budget admission"
import re, sys
from pathlib import Path
text="\n".join(Path(p).read_text() for p in sys.argv[1:3] if Path(p).exists())
budgets=[int(n) for n in re.findall(r'restore BudgetAtSnapshot reserved=(\d+)',text)]
pool=__import__("json").load(open(sys.argv[3]))["pool_memory"]
assert any(int(pool*.4)<n<=pool for n in budgets), budgets
PY_BUDGET
# ---- background policy hold followed by actual resource relief ---------
# Re-enable the second saved workload once, then send no more Wake/Connect.
# Background recovery must not recreate raw red pressure merely to rotate
# these incompatible held workloads (#440). The running primary must progress
# while the other Snapshot remains intact; real Delete then permits recovery.
# Read-only native exec probes below cannot launch a paused sandbox.
allowed=""
for _ in $(seq 1 100); do
    pressure_status
    if python3 - "$WORK/pressure-status.json" <<'PY_ORDINARY'
import json, sys
raise SystemExit(0 if json.load(open(sys.argv[1]))["zone"] != "critical" else 1)
PY_ORDINARY
    then allowed=1; break; fi
    sleep .1
done
[ -n "$allowed" ] || fail "ordinary recovery never became eligible after relief"
curl -sS --noproxy '*' --max-time 120 -o "$WORK/pressure-connect.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
    --data '{"timeout":600}' "http://127.0.0.1:$PORT/sandboxes/$SECOND/connect" > "$WORK/pressure-connect.code" &
PRESSURE_CONNECT_PID=$!
PIDS+=("$PRESSURE_CONNECT_PID")
python3 - "$BIN" "$WORK" "$EXECUTE_RUN_ROOT" "$FIRST" "$SECOND" <<'PY_POLICY' || fail "background policy did not preserve a stable primary and saved source"
import json, sqlite3, subprocess, sys, time
from pathlib import Path
binary, directory, runtime, first, second = sys.argv[1:]
work = Path(directory); run_root = Path(runtime); stable_since = None
baseline = json.loads((work / "pressure-second-before.json").read_text())
seen = None; deadline = time.monotonic() + 180
def primary(sid):
    r = subprocess.run([binary + "/sandbox-ctl", "exec", "--run-root", str(run_root / "sandboxes"),
        "--sandbox-id", sid, "--", "/bin/cat", "/tmp/pressure-state.json"], capture_output=True, timeout=3)
    return json.loads(r.stdout) if r.returncode == 0 else None
with (work / "pressure-policy.log").open("w") as output:
    while time.monotonic() < deadline:
        p = json.loads(subprocess.check_output([binary + "/node-ctl", "resource", "pressure",
            "--socket", str(work / "node-ctl.socket")], timeout=5))
        assert p["reserved_memory"] <= p["pool_memory"], p
        with sqlite3.connect((work / "lib/node-ctl.db").resolve().as_uri()+"?mode=ro", uri=True, timeout=2) as db:
            rows = {r[0]: r[1:] for r in db.execute("select id,state,run_id,running_since_ns from sandboxes where id in (?,?)", (first,second))}
        assert len(rows)==2 and all(r[0] in ("running","paused","starting") for r in rows.values()), rows
        output.write(json.dumps(dict(at=time.time(), pressure=p, lifecycle=rows))+"\n"); output.flush()
        by_sid = {r["sandbox_id"]: r for r in p["sandboxes"]}
        ready = (rows[second][0]=="running" and rows[first][0] in ("paused","starting")
                 and by_sid[first].get("recovery_obligation") and p["pending_recoveries"]==1
                 and p["capturing"]==0)
        if not ready:
            assert stable_since is None, ("stable primary was evicted by background recovery", p, rows)
            time.sleep(.5); continue
        try: held = primary(second)
        except (subprocess.TimeoutExpired, json.JSONDecodeError): held = None
        if held is None: time.sleep(.5); continue
        assert held["nonce"]==baseline["nonce"] and held["held"]==baseline["held"]==576*(1<<20), held
        assert held["tick"]>=baseline["tick"], held
        assert not any(d["sandbox_id"]==first for d in p.get("demands",[])), p
        if stable_since is None:
            stable_since=time.monotonic(); seen=(rows[second][1], held["tick"])
        assert rows[second][1]==seen[0], ("background hold replaced the live RunID", rows)
        if time.monotonic()-stable_since>=20 and held["tick"]>seen[1]:
            (work/"pressure-stable-primary.json").write_text(json.dumps(dict(first=rows[first],second=rows[second],held=held)))
            break
        time.sleep(.5)
    else: raise AssertionError(("no stable policy hold", p, rows))
print("==> background policy preserved the saved obligation and progressing primary")
PY_POLICY
wait "$PRESSURE_CONNECT_PID" || fail "second ordinary Connect transport failed"
[ "$(cat "$WORK/pressure-connect.code")" = 200 ] || fail "second ordinary Connect response"
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$PRESSURE_CONNECT_PID" ] || PIDS[$i]=""; done
# Actual release, not a larger pool, a relaxed watermark or a second Wake,
# must unlock the saved primary's unattended recovery.
code=$(req DELETE "/sandboxes/$SECOND" "$AK"); [ "$code" = 204 ] || fail "second pressure delete=$code"
wait_sandbox_state "$SECOND" missing 600 || fail "second pressure delete finalization"
wait_sandbox_state "$FIRST" running 600 || fail "saved primary did not recover after real relief"
recovered=""
for _ in $(seq 1 100); do
    pressure_guest_state "$FIRST" > "$WORK/pressure-final-primary.json"
    pressure_status
    if python3 - "$WORK/pressure-before.json" "$WORK/pressure-final-primary.json" "$WORK/pressure-status.json" <<'PY_RELIEF'
import json,sys
before,after,p=[json.load(open(x)) for x in sys.argv[1:]]
assert before["nonce"]==after["nonce"] and before["held"]==after["held"]==384*(1<<20)
assert p["reserved_memory"]<=p["pool_memory"],p
raise SystemExit(0 if after["tick"]>before["tick"] and p["pending_recoveries"]==0 else 1)
PY_RELIEF
    then recovered=1; break; fi
    sleep .2
done
[ -n "$recovered" ] || fail "automatic recovery lost payload, identity, progress or obligation accounting"
echo "==> real relief restored the saved primary without another Wake"
# Stop new background acceptance while the existing normal Delete path drains
# every owned consumer and obligation. This does not certify ambiguous cleanup.
"$BIN/node-ctl" resource drain --socket "$WORK/sandbox-resource.sock" >/dev/null
for sid in "$FIRST"; do
    code=$(req DELETE "/sandboxes/$sid" "$AK"); [ "$code" = 204 ] || fail "pressure delete=$code"
    wait_sandbox_state "$sid" missing 600 || fail "pressure delete finalization"
done
pressure_status
cat "$WORK/pressure-status.json" > "$WORK/pressure-final.log"
python3 - "$WORK/pressure-status.json" <<'PY_CLEAN'
import json,sys
p=json.load(open(sys.argv[1]))
assert p["reserved_memory"]==0 and p["pending_recoveries"]==0 and p["pending_cleanup"]==0,p
PY_CLEAN
stop_proxy_master
echo "PASS orchestrator.proxy-wake.sh"
