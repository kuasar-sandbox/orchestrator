#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
wait_proxy_traffic_stats() { # $1=sid, $2=parking|idle|paused
    local sid="$1" mode="$2" code=""
    for _ in $(seq 1 240); do
        code="$(req GET "/sandboxes/$sid/stats/traffic" "$AK" || true)"
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$mode" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
mode = sys.argv[2]
if set(stats) - {"state", "maxInflight", "inflight", "idleSince", "services", "platform", "transit", "egress"}:
    raise SystemExit(1)
if stats.get("egress") != {}:
    raise SystemExit(1)
for plane in ("platform", "transit"):
    counters = stats.get(plane)
    if not isinstance(counters, dict) or (counters and set(counters) != {"rxPackets", "rxBytes", "txPackets", "txBytes"}):
        raise SystemExit(1)
    if any(type(value) is not int or value < 0 for value in counters.values()):
        raise SystemExit(1)
max_inflight = stats.get("maxInflight")
if not isinstance(max_inflight, dict) or set(max_inflight) != {"total", "forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
if any(type(value) is not int or value < 0 for value in max_inflight.values()):
    raise SystemExit(1)
inflight = stats.get("inflight", {})
services = stats.get("services", {})
if set(inflight) != {"parking", "connected"}:
    raise SystemExit(1)
if set(services) != {"forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
for item in services.values():
    if set(item) - {"parking", "connected", "idleSince"} or not {"parking", "connected"} <= set(item):
        raise SystemExit(1)
    if (item["parking"] or item["connected"]) and "idleSince" in item:
        raise SystemExit(1)
if mode == "parking":
    ok = inflight["parking"] >= 1 and "idleSince" not in stats
elif mode == "idle":
    ok = stats.get("state") == "running" and inflight == {"parking": 0, "connected": 0} and "idleSince" in stats
elif mode == "paused":
    ok = stats.get("state") == "paused" and inflight == {"parking": 0, "connected": 0} and "idleSince" not in stats
else:
    ok = False
raise SystemExit(0 if ok else 1)
PY
        then
            return 0
        fi
        sleep 0.05
    done
    echo "traffic stats sid=$sid mode=$mode last_status=$code body=$(cat "$WORK/resp.body" 2>/dev/null)" >&2
    return 1
}
wait_resource_stats() { # $1=sid
    local sid="$1" code=""
    for _ in $(seq 1 240); do
        code="$(req GET "/sandboxes/$sid/stats/resource" "$AK" || true)"
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
allowed = {"cpuCapacity", "cpuAllocatable", "memoryCapacity", "memoryHeadroom", "memoryReserved", "memoryUsed", "cpuSeconds", "timestampUnix"}
if not stats or set(stats) - allowed:
    raise SystemExit(1)
required = allowed
if not required <= set(stats):
    raise SystemExit(1)
if any(stats[name] <= 0 for name in required):
    raise SystemExit(1)
PY
        then
            return 0
        fi
        sleep 0.25
    done
    echo "resource stats sid=$sid last_status=$code body=$(cat "$WORK/resp.body" 2>/dev/null)" >&2
    return 1
}
wait_resource_status() { # $1=sid, $2=expected status
    local sid="$1" expected="$2" code=""
    for _ in $(seq 1 240); do
        code="$(req GET "/sandboxes/$sid/stats/resource" "$AK" || true)"
        [ "$code" = "$expected" ] && return 0
        sleep 0.05
    done
    echo "resource stats sid=$sid last_status=$code want=$expected body=$(cat "$WORK/resp.body" 2>/dev/null)" >&2
    return 1
}

assert_resolved_resource_yaml() { # $1=config, $2=capacity, $3=startup, $4=controller|-, $5=headroom(default 256MiB), $6=deflate(default true)|-
    local headroom_memory="${5:-256MiB}"
    local deflate="${6:-true}"
    python3 - "$1" "$2" "$3" "$4" "$headroom_memory" "$deflate" <<'PY'
import re
import sys

path, capacity_memory, startup_memory, controller, headroom_memory, deflate = sys.argv[1:]
values = {}
stack = []
for raw in open(path, encoding="utf-8"):
    line = raw.split("#", 1)[0].rstrip()
    if not line.strip() or line.lstrip().startswith("-") or ":" not in line:
        continue
    indent = len(line) - len(line.lstrip(" "))
    key, value = line.strip().split(":", 1)
    while stack and indent <= stack[-1][0]:
        stack.pop()
    dotted = ".".join([entry[1] for entry in stack] + [key])
    value = value.strip().strip('"').strip("'")
    if value:
        values[dotted] = value
    else:
        stack.append((indent, key))

expected = {
    "resources.capacity.cpu": "2",
    "resources.allocatable.cpu": "2",
}
for key, want in expected.items():
    if values.get(key) != want:
        raise SystemExit(f"{path}: {key}={values.get(key)!r}, want {want!r}; values={values}")

units = {
    "B": 1,
    "KiB": 1 << 10,
    "MiB": 1 << 20,
    "GiB": 1 << 30,
    "TiB": 1 << 40,
}
def size_bytes(field, value):
    match = re.fullmatch(r"([1-9][0-9]*)(B|KiB|MiB|GiB|TiB)", value or "")
    if not match:
        raise SystemExit(f"{path}: {field} has invalid size {value!r}; values={values}")
    return int(match.group(1)) * units[match.group(2)]

memory_expected = {
    "resources.capacity.memory": capacity_memory,
    "resources.allocatable.memory": headroom_memory,
    "resources.overhead.memory": "32MiB",
}
for key, want in memory_expected.items():
    got = values.get(key)
    if size_bytes(key, got) != size_bytes(key, want):
        raise SystemExit(f"{path}: {key}={got!r}, want {want!r}; values={values}")
if deflate == "-":
    if "resources.allocatable.deflate_on_oom" in values:
        raise SystemExit(f"{path}: no-balloon config rendered deflate_on_oom: {values}")
elif values.get("resources.allocatable.deflate_on_oom") != deflate:
    raise SystemExit(f"{path}: deflate_on_oom={values.get('resources.allocatable.deflate_on_oom')!r}, want {deflate!r}")
got = values.get("resources.startup.memory")
if size_bytes("resources.startup.memory", got) != size_bytes("resources.startup.memory", startup_memory):
    raise SystemExit(f"{path}: startup={got!r}, want {startup_memory!r}")
if values.get("resources.watermark_high.ratio") != "0.875":
    raise SystemExit(f"{path}: watermark_high.ratio={values.get('resources.watermark_high.ratio')!r}, want '0.875'")
if controller == "-":
    if "resources.control.controller" in values:
        raise SystemExit(f"{path}: static config rendered controller: {values}")
elif values.get("resources.control.controller") != controller:
    raise SystemExit(f"{path}: controller={values.get('resources.control.controller')!r}, want {controller!r}")
for forbidden in ("resources.control.cgroup_path", "resources.watermark_high.memory",
                  "resources.control.sensor.mode"):
    if forbidden in values:
        raise SystemExit(f"{path}: renderer fixed runtime-owned {forbidden}: {values}")
PY
}

assert_resource_lease() { # $1=sid, $2=capacity bytes, $3=headroom bytes, $4=startup headroom
    python3 - "$WORK/sandbox-resource.sock" "$1" "$2" "$3" "$4" <<'PY'
import hashlib, json, pathlib, sys

socket, sid, capacity, headroom, startup = sys.argv[1:]
lease = pathlib.Path(socket + ".leases") / (hashlib.sha256(sid.encode()).hexdigest() + ".json")
row = json.loads(lease.read_text())
expected = {
    "sandbox_id": sid,
    "controller_socket": socket,
    "capacity_memory": int(capacity),
    "capacity_cpu_milli": 2000,
    "floor_memory": int(headroom),
    "floor_cpu_milli": 2000,
    "startup_memory": int(startup),
}
for key, want in expected.items():
    if row.get(key) != want:
        raise SystemExit(f"lease {lease}: {key}={row.get(key)!r}, want {want!r}; row={row}")
PY
}

capture_mem_report_state() { # $1=unit, $2=output file
    journalctl -u "$1" --no-pager >"$2" 2>/dev/null || true
    awk '
        /mem_report: recovered after [0-9]+ consecutive failures/ { state = "recovered"; next }
        /mem_report:/ { state = "failed" }
        END { print state }
    ' "$2"
}
wait_for_mem_report_progress() { # $1=unit, $2=output file
    local unit="$1" output="$2" state=""

    # sandbox-init reports immediately and every five seconds. Observe more
    # than one full interval so a permanently failing channel cannot pass just
    # because the first successful attempt is silent.
    for _ in $(seq 1 28); do
        state="$(capture_mem_report_state "$unit" "$output")"
        case "$state" in
            failed) break ;;
            recovered)
                echo "==> observed transient mem_report failure followed by recovery; complete unit/CH timeline:"
                sed 's/^/  low-unit| /' "$output"
                return 0
                ;;
        esac
        sleep 0.25
    done
    state="$(capture_mem_report_state "$unit" "$output")"
    [ "$state" = failed ] || return 0

    echo "==> transient mem_report failure observed; complete unit/CH timeline before recovery wait:"
    sed 's/^/  low-unit| /' "$output"
    # The next attempt is due within five seconds. Twelve seconds covers two
    # complete retry intervals under runner pressure without accepting a
    # reporter that has stopped making progress.
    for _ in $(seq 1 48); do
        state="$(capture_mem_report_state "$unit" "$output")"
        [ "$state" = recovered ] && return 0
        sleep 0.25
    done
    capture_mem_report_state "$unit" "$output" >/dev/null
    sed 's/^/  low-unit| /' "$output"
    return 1
}
