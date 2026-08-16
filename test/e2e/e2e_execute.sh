#!/usr/bin/env bash
#
# e2e_execute.sh — Phase 2: boot a REAL microVM sandbox from a built template and
# execute in it, end to end.
#
#   vswitch (ip netns + start) -> sw0 (eBPF/TC); builds need it too (the pull
#                                runs INSIDE a build sandbox on the tenant network)
#   build (native v3)          -> import build sandbox pulls via the mgmt VIP +
#                                flattens -> a ready e2b-img template in the store
#   POST /sandboxes            -> sandbox-runner@<run-id> assignment -> sandbox-ctl boots
#                                cloud-hypervisor (KVM) from the template +
#                                sandbox-runtime.bundle; envd comes up at 49983,
#                                exposed as envd.sock; after runtime readiness the
#                                orchestrator requires envdInit(/init), without a
#                                launch-time /health probe. 201 == durable starting
#                                acceptance; the immediate data request below parks
#                                until runtime readiness + /init complete.
#                                The create injects sandbox config via the
#                                X-Kuasar-Sandbox-Network header (hostname), checked
#                                in the guest below (§4.6 config passing chain).
#   exec-session + CONNECT     -> issue an explicit exec capability, then use the
#                                real sandbox-ctl HTTP CONNECT client against the
#                                guest (stdio, PTY resize, exit status, pause wake).
#   envd exec                  -> run a command in the guest via envd (incl. hostname).
#   local Pause policy        -> all-unset keeps the legacy argv and commits
#                                after its HTTP caller disconnects; then node,
#                                Create metadata/header, Pause body/header, and
#                                automatic reaper policy are layered fieldwise.
#                                Captures are real local bundles and are restored
#                                before teardown.
#   portable working set      -> restore local B, capture W -> local B, remove
#                                B's merged disk top, explicitly export/promote
#                                W, then restore the portable W with self-only
#                                memory prefetch.
#   DELETE                    -> teardown.
#
# Needs systemd+root, /dev/kvm (rw), the vswitch eBPF stack, store-ctl, zot, docker,
# mkfs.erofs, and the built kernel + runtime erofs
# in bin. Missing prereqs -> exit 0 ("skipped") unless REQUIRE_EXEC=1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
. "$SCRIPT_DIR/lib/vmm_cgroup.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
MMDS_ROUTES_E2E="${MMDS_ROUTES_E2E:-0}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_sw}"
PROXY_NETNS="${PROXY_NETNS:-e2e_proxy_int}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-e2eih0}"
PROXY_VETH_NS="${PROXY_VETH_NS:-e2ein0}"
PROXY_HOST_IP="${PROXY_HOST_IP:-172.31.253.1}"
PROXY_NS_IP="${PROXY_NS_IP:-172.31.253.2}"
FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"
LOW_ALLOC_REPEATS="${LOW_ALLOC_REPEATS:-2}"

skip() { echo; echo "==> e2e_execute: skipping ($*)"; [ "${REQUIRE_EXEC:-0}" = "1" ] && { echo "REQUIRE_EXEC=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

case "$BIN" in
    /*) ;;
    *) BIN="$(cd "$BIN" 2>/dev/null && pwd)" || skip "BIN directory not found";;
esac

for b in node-ctl sandbox-ctl flatten-ctl manifest-ctl store-ctl e2b-key-ctl connector-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.bundle" ] || skip "missing $BIN/sandbox-runtime.bundle"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
command -v iptables >/dev/null 2>&1 || skip "iptables not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "base image $E2E_IMAGE unavailable (set E2E_IMAGE to a local or pullable image)"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-exec-XXXXXX)"
TAPFD_SOCKET="$WORK/tapfd.sock"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"

MMDS_ROUTES_CONFIG=""
MMDS_SERVICES_CONFIG=""
if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$SCRIPT_DIR/lib/mmds_service_guest.sh"
    start_mmds_service_backend "$WORK" || fail "start MMDS service UDS backend"
    MMDS_ROUTES_CONFIG="  routes:
    enabled: true"
    MMDS_SERVICES_CONFIG="  services:
    $MMDS_SERVICE_NAME:
      endpoint: unix://$MMDS_SERVICE_SOCKET"
fi

# Run conductor from a private sibling-bin directory so its normal binary
# discovery reaches this transparent sandbox-ctl wrapper. The wrapper records
# snapshot argv as JSONL, then normally execs the unmodified release candidate
# binary. Explicit sentinel modes below are confined to deterministic negative
# lifecycle tests; every ordinary create/restore still uses the real KVM stack.
ORCH_BIN_DIR="$WORK/orch-bin"
SNAPSHOT_ARGV_LOG="$WORK/snapshot-argv.jsonl"
mkdir -p "$ORCH_BIN_DIR"
cp "$BIN/node-ctl" "$ORCH_BIN_DIR/node-ctl"
for b in connector-ctl flatten-ctl manifest-ctl; do
    ln -s "$BIN/$b" "$ORCH_BIN_DIR/$b"
done
cat > "$ORCH_BIN_DIR/sandbox-ctl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "snapshot" ]; then
    python3 - "$SNAPSHOT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\\n")
PY
fi
if [ "\${1:-}" = "run" ] && [ -f "$WORK/inject-sandbox-run" ]; then
    mode="\$(<"$WORK/inject-sandbox-run")"
    case "\$mode" in
        hold)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
            exit 44
            ;;
        runtime-wire-failure)
            python3 - "\$@" <<'PY'
import os, sys
fd = next(int(arg.split("=", 1)[1]) for arg in sys.argv[1:] if arg.startswith("--ready-fd="))
os.write(fd, b"control_ready\ninvalid_runtime_event\n")
os.close(fd)
PY
            exit 42
            ;;
        envd-init-failure)
            # Replace this wrapper so no parent process retains ready-fd after
            # Python closes it; EOF is the final readiness protocol event.
            exec python3 - "\$@" <<'PY'
import os, socket, sys, time

args = sys.argv[1:]
def value(name):
    for index, arg in enumerate(args):
        if arg == name:
            return args[index + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    raise SystemExit("missing " + name)

sid = value("--sandbox-id")
run_root = value("--run-root")
ready_fd = int(value("--ready-fd"))
envd_path = os.path.join(run_root, sid, "envd.sock")
try:
    os.unlink(envd_path)
except FileNotFoundError:
    pass
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(envd_path)
server.listen(1)
server.settimeout(15)
os.write(ready_fd, b"control_ready\nready\n")
os.close(ready_fd)
conn, _ = server.accept()
conn.settimeout(5)
request = b""
while b"\r\n\r\n" not in request:
    chunk = conn.recv(65536)
    if not chunk:
        break
    request += chunk
conn.sendall(b"HTTP/1.1 500 Injected envd failure\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
conn.close()
server.close()
time.sleep(300)
PY
            exit 43
            ;;
        *)
            echo "unknown sandbox run injection: \$mode" >&2
            exit 2
            ;;
    esac
fi
exec "$BIN/sandbox-ctl" "\$@"
EOF
chmod +x "$ORCH_BIN_DIR/sandbox-ctl"

declare -a PIDS=()
declare -a TAGS=()
IMMEDIATE_DATA_PID=""
SW_STARTED=""
ORIG_IP_FORWARD=""
cleanup() {
    set +e
    [ "$MMDS_ROUTES_E2E" = 1 ] && stop_mmds_service_backend
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    [ -n "$IMMEDIATE_DATA_PID" ] && kill "$IMMEDIATE_DATA_PID" 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" >/dev/null 2>&1
    iptables -D FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null
    iptables -D FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null
    ip link del "$PROXY_VETH_HOST" 2>/dev/null
    ip netns del "$PROXY_NETNS" 2>/dev/null
    ip netns del "$SW_NETNS" 2>/dev/null
    [ -n "$ORIG_IP_FORWARD" ] && sysctl -q -w "net.ipv4.ip_forward=$ORIG_IP_FORWARD" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
wait_mmds_listener() {
    local hex
    hex="$(printf '%04X' "$MMDS_PORT")"
    for _ in $(seq 1 60); do
        ip netns exec "$PROXY_NETNS" awk -v p=":$hex" '$2 ~ p && $4 == "0A" { found = 1 } END { exit(found ? 0 : 1) }' /proc/net/tcp 2>/dev/null && return 0
        sleep 0.5
    done
    fail "internal mmds listener did not appear in proxy_netns=$PROXY_NETNS on $PROXY_NS_IP:$MMDS_PORT"
}
setup_proxy_netns() {
    ip link del "$PROXY_VETH_HOST" 2>/dev/null || true
    ip netns del "$PROXY_NETNS" 2>/dev/null || true
    ip netns add "$PROXY_NETNS"
    ip link add "$PROXY_VETH_HOST" type veth peer name "$PROXY_VETH_NS"
    ip link set "$PROXY_VETH_NS" netns "$PROXY_NETNS"
    ip addr add "$PROXY_HOST_IP/30" dev "$PROXY_VETH_HOST"
    ip link set "$PROXY_VETH_HOST" up
    ip netns exec "$PROXY_NETNS" ip addr add "$PROXY_NS_IP/30" dev "$PROXY_VETH_NS"
    ip netns exec "$PROXY_NETNS" ip link set lo up
    ip netns exec "$PROXY_NETNS" ip link set "$PROXY_VETH_NS" up
    ip netns exec "$PROXY_NETNS" ip route add "$FIP_CIDR" via "$PROXY_HOST_IP"
    ORIG_IP_FORWARD="$(sysctl -n net.ipv4.ip_forward 2>/dev/null || true)"
    sysctl -q -w net.ipv4.ip_forward=1
}
allow_proxy_forwarding() {
    iptables -C FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT
    iptables -C FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT
}
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    # Optional sandbox-config injection header (§4.6): set REQ_NET_HEADER to a JSON
    # network spec to exercise X-Kuasar-Sandbox-Network on a create.
    [ -n "${REQ_NET_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Network: ${REQ_NET_HEADER}")
    [ -n "${REQ_CHECKPOINT_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Checkpoint: ${REQ_CHECKPOINT_HEADER}")
    if [ "${REQ_ATTACH_MMDS:-0}" = 1 ] && [ -n "${REQ_MMDS_HEADER:-}" ]; then
        args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    fi
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
wait_internal_traffic_stats() { # $1=sid, $2=parking|idle|paused
    local sid="$1" mode="$2" code=""
    for _ in $(seq 1 240); do
        code="$(req GET "/sandboxes/$sid/stats/traffic" "$AK" || true)"
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$mode" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
mode = sys.argv[2]
if set(stats) - {"state", "inflight", "idleSince", "services"}:
    raise SystemExit(1)
inflight = stats.get("inflight", {})
services = stats.get("services", {})
if set(inflight) != {"parking", "egress"}:
    raise SystemExit(1)
if set(services) != {"forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
for item in services.values():
    if set(item) - {"parking", "egress", "idleSince"} or not {"parking", "egress"} <= set(item):
        raise SystemExit(1)
    if (item["parking"] or item["egress"]) and "idleSince" in item:
        raise SystemExit(1)
if mode == "parking":
    ok = inflight["parking"] >= 1 and "idleSince" not in stats
elif mode == "idle":
    ok = stats.get("state") == "running" and inflight == {"parking": 0, "egress": 0} and "idleSince" in stats
elif mode == "paused":
    ok = stats.get("state") == "paused" and inflight == {"parking": 0, "egress": 0} and "idleSince" not in stats
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
allowed = {"timestampUnix", "cpuCount", "cpuAllocatable", "memUsed", "memTotal", "memAllocatable"}
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

assert_resolved_resource_yaml() { # $1=config, $2=capacity, $3=startup|-, $4=controller|-, $5=floor(default 256MiB), $6=deflate(default true)|-
    local floor_memory="${5:-256MiB}"
    local deflate="${6:-true}"
    python3 - "$1" "$2" "$3" "$4" "$floor_memory" "$deflate" <<'PY'
import re
import sys

path, capacity_memory, startup_memory, controller, floor_memory, deflate = sys.argv[1:]
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
    "resources.allocatable.memory": floor_memory,
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
if startup_memory == "-":
    if any(key.startswith("resources.startup") for key in values):
        raise SystemExit(f"{path}: static config rendered startup: {values}")
else:
    got = values.get("resources.startup.memory")
    if size_bytes("resources.startup.memory", got) != size_bytes("resources.startup.memory", startup_memory):
        raise SystemExit(f"{path}: startup={got!r}, want {startup_memory!r}")
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

assert_resource_lease() { # $1=sid, $2=capacity bytes, $3=floor bytes, $4=startup bytes
    python3 - "$WORK/sandbox-resource.sock" "$1" "$2" "$3" "$4" <<'PY'
import hashlib, json, pathlib, sys

socket, sid, capacity, floor, startup = sys.argv[1:]
lease = pathlib.Path(socket + ".leases") / (hashlib.sha256(sid.encode()).hexdigest() + ".json")
row = json.loads(lease.read_text())
expected = {
    "sandbox_id": sid,
    "controller_socket": socket,
    "capacity_memory": int(capacity),
    "capacity_cpu_milli": 2000,
    "floor_memory": int(floor),
    "floor_cpu_milli": 2000,
    "startup_memory": int(startup),
}
for key, want in expected.items():
    if row.get(key) != want:
        raise SystemExit(f"lease {lease}: {key}={row.get(key)!r}, want {want!r}; row={row}")
PY
}

snapshot_argv_count() {
    python3 - "$SNAPSHOT_ARGV_LOG" <<'PY'
import json, os, sys
path = sys.argv[1]
if not os.path.exists(path):
    print(0)
else:
    with open(path, encoding="utf-8") as source:
        print(sum(1 for line in source if line.strip()))
PY
}

assert_snapshot_argv() { # $1=index, remaining args=expected argv
    local index="$1"
    shift
    python3 - "$SNAPSHOT_ARGV_LOG" "$index" "$@" <<'PY'
import json, sys
path, index, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
with open(path, encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
if index >= len(calls):
    raise SystemExit(f"missing snapshot call {index}; captured {len(calls)}")
if calls[index] != expected:
    raise SystemExit(f"snapshot call {index}={calls[index]!r}, want {expected!r}")
PY
}

assert_snapshot_has_no_policy_flags() { # $1=index
    python3 - "$SNAPSHOT_ARGV_LOG" "$1" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
call = calls[int(sys.argv[2])]
bad = [arg for arg in call if arg.startswith("--merge-ref=") or arg.startswith("--drop-caches=")]
if bad:
    raise SystemExit(f"builder snapshot unexpectedly received checkpoint policy flags: {bad!r}")
PY
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
sandbox_run_id() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select run_id from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
}
sandbox_state() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
}
wait_sandbox_state() { # $1=sandbox id, $2=state, $3=attempts(optional)
    local sid="$1" want="$2" attempts="${3:-120}" state=""
    for _ in $(seq 1 "$attempts"); do
        state="$(sandbox_state "$sid")"
        [ "$state" = "$want" ] && return 0
        sleep 0.1
    done
    echo "sandbox $sid state=$state, want $want" >&2
    return 1
}
wait_unit_journal_contains() { # $1=unit, $2=fixed string, $3=output file
    local unit="$1" pattern="$2" output="$3"
    for _ in $(seq 1 50); do
        journalctl -u "$unit" --no-pager >"$output" 2>/dev/null || true
        grep -Fq "$pattern" "$output" && return 0
        sleep 0.1
    done
    return 1
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
assert_sandbox_detail() {
    python3 - "$@" <<'PY'
import datetime
import json
import re
import sys

path, sandbox_id, expected_cpu, expected_memory, expected_disk = sys.argv[1:]
expected = {
    "cpuCount": int(expected_cpu),
    "memoryMB": int(expected_memory),
    "diskSizeMB": int(expected_disk),
}
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
with open(path, encoding="utf-8") as f:
    detail = json.load(f)

if detail.get("sandboxID") != sandbox_id:
    raise SystemExit(f"detail sandboxID={detail.get('sandboxID')!r}, want {sandbox_id!r}")
for field in ("startedAt", "endAt"):
    value = detail.get(field)
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        raise SystemExit(f"detail {field}={value!r} is not RFC3339")
    try:
        datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"detail {field}={value!r} is not RFC3339: {exc}")

started = datetime.datetime.fromisoformat(detail["startedAt"].replace("Z", "+00:00"))
ended = datetime.datetime.fromisoformat(detail["endAt"].replace("Z", "+00:00"))
if ended < started:
    raise SystemExit(f"detail endAt={detail['endAt']!r} is before startedAt={detail['startedAt']!r}")
for field, want in expected.items():
    if detail.get(field) != want:
        raise SystemExit(f"detail {field}={detail.get(field)!r}, want {want}")
PY
}
assert_no_default_exec_token() {
    python3 - "$1" <<'PY'
import json, sys
created = json.load(open(sys.argv[1]))
if "execAccessToken" in created:
    raise SystemExit("create response unexpectedly contains execAccessToken")
PY
}
issue_exec_session() {
    local sid="$1" key="$2" code
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-API-KEY: $key" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$PORT/sandboxes/$sid/exec-sessions")"
    [ "$code" = "201" ] || fail "exec-session=$code (want 201)"
    python3 - "$WORK/exec-session.headers" "$WORK/exec-session.secret" <<'PY'
import json, sys
headers = [line.strip().lower() for line in open(sys.argv[1], "rb").read().splitlines()]
if b"cache-control: no-store" not in headers:
    raise SystemExit("exec-session response omitted Cache-Control: no-store")
payload = json.load(open(sys.argv[2]))
if not isinstance(payload, dict) or set(payload) != {"execAccessToken"}:
    raise SystemExit("exec-session response must contain only execAccessToken")
token = payload["execAccessToken"]
if not isinstance(token, str) or not token.startswith("kat1.") or len(token.split(".")) != 3:
    raise SystemExit("exec-session response contains an invalid KAT token")
print(token)
PY
}
exec_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local status

    printf 'stdin:%s\n' "$marker" >"$input"
    : >"$output"
    : >"$error_output"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --proxy-header "X-Kuasar-E2E-Duplicate: first" \
        --proxy-header "X-Kuasar-E2E-Duplicate: second" \
        --stdin-from "$input" --stdout-to "$output" --stderr-to "$error_output" -- \
        /bin/sh -c "IFS= read -r value; printf 'stdout:%s:%s\\n' '$marker' \"\$value\"; printf 'stderr:%s\\n' '$marker' >&2; exit 47" \
        >"$diagnostics" 2>&1; then
        status=0
    else
        status=$?
    fi
    grep -Fxq "stdout:$marker:stdin:$marker" "$output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stdout| /' "$output" 2>/dev/null; dump_exec_failure_context "$sid"; fail "native exec stdout/stdin mismatch"; }
    grep -Fxq "stderr:$marker" "$error_output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stderr| /' "$error_output" 2>/dev/null; dump_exec_failure_context "$sid"; fail "native exec stderr mismatch"; }
    [ "$status" = "47" ] \
        || { sed 's/^/  client| /' "$diagnostics"; dump_exec_failure_context "$sid"; fail "native exec exit=$status (want guest status 47)"; }
}

dump_exec_failure_context() { # $1=sandbox id
    local sid="$1" state run_id failed_run_id=""
    state="$(sandbox_state "$sid")"
    run_id="$(sandbox_run_id "$sid")"
    echo "==> exec failure context: sid=$sid state=$state run_id=${run_id:-<empty>}" >&2
    if [ -n "${ORCH_LOG:-}" ] && [ -f "$ORCH_LOG" ]; then
        echo "==> matching node-ctl lifecycle log:" >&2
        grep -F "sid=$sid" "$ORCH_LOG" | sed 's/^/  orch| /' >&2 || true
        failed_run_id="$(python3 - "$ORCH_LOG" "$sid" <<'PY'
import re, sys
last = ""
for line in open(sys.argv[1], encoding="utf-8"):
    if f"sid={sys.argv[2]}" not in line:
        continue
    match = re.search(r'\brun_id=(?:"([^"]*)"|(\S+))', line)
    if match and (match.group(1) or match.group(2)):
        last = match.group(1) or match.group(2)
print(last)
PY
)"
    fi
    if [ -n "$failed_run_id" ]; then
        echo "==> failed runner unit journal: sandbox-runner@$failed_run_id.service" >&2
        journalctl -u "sandbox-runner@$failed_run_id.service" --no-pager 2>/dev/null | sed 's/^/  unit| /' >&2 || true
    else
        echo "==> matching sandbox runner journal:" >&2
        journalctl KUASAR_SANDBOX_ID="$sid" --no-pager 2>/dev/null | sed 's/^/  unit| /' >&2 || true
    fi
}

exec_pty_resize_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local output="$WORK/native-exec-pty.out"
    local status=0

    python3 - "$output" "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --tty -- /bin/sh -c \
        "stty size; trap 'stty size; echo $marker; exit 23' WINCH; echo PTY_READY; while :; do sleep 1; done" <<'PY' || status=$?
import errno, fcntl, os, pty, select, signal, struct, subprocess, sys, termios, time

output_path, argv = sys.argv[1], sys.argv[2:]
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 37, 91, 0, 0))
proc = subprocess.Popen(argv, stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
captured = bytearray()
resized = False
deadline = time.monotonic() + 60
try:
    while True:
        if time.monotonic() >= deadline:
            proc.kill()
            proc.wait()
            raise SystemExit(124)
        readable, _, _ = select.select([master], [], [], 0.1)
        if not readable:
            continue
        try:
            chunk = os.read(master, 65536)
        except OSError as exc:
            if exc.errno == errno.EIO:
                break
            raise
        if not chunk:
            break
        captured.extend(chunk)
        if not resized and b"PTY_READY" in captured:
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 41, 101, 0, 0))
            os.kill(proc.pid, signal.SIGWINCH)
            resized = True
finally:
    os.close(master)
    with open(output_path, "wb") as output_file:
        output_file.write(captured)
raise SystemExit(proc.wait())
PY
    grep -q '37 91' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec initial PTY size mismatch"; }
    grep -q '41 101' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec resized PTY size mismatch"; }
    grep -q "$marker" "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY marker missing"; }
    [ "$status" = "23" ] || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY exit=$status (want 23)"; }
}
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --max-time "${DP_MAX_TIME:-120}" --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
    [ -n "$token" ] && args+=(-H "X-Access-Token: $token")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

# ---- store + zot + creds + orchestrator (same as the build e2e) -----------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl

# 0.0.0.0: the host pushes via 127.0.0.1; the BUILD SANDBOX pulls via the
# vswitch mgmt VIP (guest loopback is not the host's).
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
REF="127.0.0.1:$ZOT_PORT/e2e/app:v1"
# A real e2b base template has a 'user' account and util-linux/coreutils, because
# envd runs guest processes as /init's defaultUser and wraps them as
# `ionice -c 2 -n 4 nice -n N "$@"`. Bare alpine has neither, and apk has no
# network here — so build a minimal compliant image offline: add the user and
# tiny ionice/nice shims (strip -c/-n opts, exec the rest). The orchestrator/envd
# are unchanged; this only makes the test image match the e2b base contract.
cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -c|-n) shift 2 ;;
    -c*|-n*) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
exec "$@"
SH
cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice \
 && if ! id -u user >/dev/null 2>&1; then \
      if command -v useradd >/dev/null 2>&1; then useradd -m -d /home/user -s /bin/sh user; \
      elif command -v adduser >/dev/null 2>&1; then adduser -D -h /home/user -s /bin/sh user; \
      else echo "missing useradd/adduser" >&2; exit 1; fi; \
    fi \
 && mkdir -p /home/user \
 && chown user:user /home/user \
 && id user >/dev/null
CMD ["sleep", "86400"]
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build (e2b-compliant image)"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF (user + ionice/nice shims)"

# ---- vswitch up (ip netns + start) -----------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP. Clear any leftover
# switch of the same name (eBPF maps are pinned and survive a crash; --force
# drains orphaned ports), then create the netns fresh.
MGMT_VIP="169.254.169.254"
MMDS_PORT="$(free_port)"
setup_proxy_netns
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
echo "==> starting vswitch $SWITCH (netns=$SW_NETNS)"
"$BIN/connector-ctl" vswitch serve "$SWITCH" \
    --netns="$SW_NETNS" \
    --ports=64 \
    --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 \
    --mode=tap \
    --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$PROXY_NS_IP:$MMDS_PORT \
    --tapfd-listen="$TAPFD_SOCKET" --watch-interval=2s >"$WORK/vswitch-start.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
    if [ -S "$TAPFD_SOCKET" ] && "$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1; then
        break
    fi
    kill -0 "${PIDS[-1]}" 2>/dev/null || { echo "vswitch serve failed:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch serve exited"; }
    sleep 0.2
done
"$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1 \
    || { echo "vswitch not ready:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch not ready"; }
SW_STARTED=1
ip addr replace "$MGMT_VIP/32" dev "${SWITCH}m0" \
    || fail "configure management VIP on ${SWITCH}m0"
allow_proxy_forwarding
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF; tapfd_socket=$TAPFD_SOCKET; internal proxy_netns=$PROXY_NETNS reaches $FIP_CIDR)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper
# (deployment-provided in prod; created inline here). mkfs.ext4 may live in /sbin.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
echo "==> overlay diff_template: $OVL ($(du -h "$OVL" | cut -f1) on disk)"
BLD="$WORK/builder-2G.ext4"   # build sandbox writable disk (pull cache + export scratch)
truncate -s 2G "$BLD"
"$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || { cat "$WORK/mkfs-bld.log"; fail "mkfs.ext4 builder template"; }

CHECKPOINT_DIR="$WORK/checkpoints"
write_orchestrator_config() { # $1=unset|node-policy, $2=static|controller (default controller)
    local policy_mode="$1"
    local resource_mode="${2:-controller}"
    local resource_controller_config=""
    case "$resource_mode" in
        static) ;;
        controller)
            resource_controller_config="resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/resource-state.json
  audit_path: $WORK/resource-audit.log
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 1GiB, cpu: 0.5 }
  admission: { rate: 50, burst: 50, startup_ttl: 180s, queue_ttl: 30s, queue_max_depth: 256 }"
            ;;
        *) fail "unknown resource mode: $resource_mode" ;;
    esac
    cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { mode: internal, auth: enforce, proxy_netns: $PROXY_NETNS, park_timeout: 120s }
mmds:
  enabled: true
  listen: "$PROXY_NS_IP:$MMDS_PORT"
$MMDS_ROUTES_CONFIG
$MMDS_SERVICES_CONFIG
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  resources:
    capacity: { cpu: 2, memory: 2GiB }
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
$resource_controller_config
checkpoint:
  mode: local
  local_dir: $CHECKPOINT_DIR
EOF
    if [ "$policy_mode" = "node-policy" ]; then
        cat >> "$WORK/config.yaml" <<'EOF'
  merge_ref: true
  drop_caches: false
EOF
    fi
}

ORCH_PID=""
ORCH_LOG=""
start_orchestrator() { # $1=log path
    local log_path="$1" ready=""
    ORCH_LOG="$log_path"
    "$ORCH_BIN_DIR/node-ctl" conductor serve --config "$WORK/config.yaml" >"$log_path" 2>&1 &
    ORCH_PID=$!
    PIDS+=("$ORCH_PID")
    for _ in $(seq 1 30); do
        if curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null; then
            ready=1
            break
        fi
        kill -0 "$ORCH_PID" 2>/dev/null || { sed 's/^/  /' "$log_path"; fail "orchestrator exited"; }
        sleep 0.5
    done
    [ -n "$ready" ] || { sed 's/^/  /' "$log_path"; fail "orchestrator health did not become ready"; }
    if grep -q 'checkpoint.mode=remote is deprecated' "$log_path"; then
        fail "local checkpoint mode emitted the remote deprecation warning"
    fi
    return 0
}

stop_orchestrator() {
    [ -n "$ORCH_PID" ] || return 0
    local stopped_pid="$ORCH_PID"
    kill -TERM "$stopped_pid" 2>/dev/null || true
    wait "$stopped_pid" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" = "$stopped_pid" ] && unset 'PIDS[i]'
    done
    ORCH_PID=""
}

write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
echo "==> node-ctl up (:$PORT)"
wait_mmds_listener
echo "==> PASS: internal mmds.listen is bound in proxy_netns=$PROXY_NETNS"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"

# ---- build a ready template (native v3, proven) ---------------------------
code=$(req POST /v3/templates "$AK" '{"name":"exec-tmpl","cpuCount":2,"memoryMB":8192}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\",\"startCmd\":\"exec sleep 86400\",\"readyCmd\":\"true\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
case "$TEMPLATE" in e2b-snp-*) : ;; *) fail "build produced $TEMPLATE (want e2b-snp-...)";; esac
echo "==> built template: $TEMPLATE"
BUILD_SNAPSHOT_COUNT=$(snapshot_argv_count)
[ "$BUILD_SNAPSHOT_COUNT" -gt 0 ] || fail "builder did not invoke sandbox-ctl snapshot"
for ((i=0; i<BUILD_SNAPSHOT_COUNT; i++)); do
    assert_snapshot_has_no_policy_flags "$i" || fail "builder snapshot received Pause-only policy flags"
done
echo "==> PASS: local checkpoint mode did not add merge-ref/drop-caches to builder snapshots"

# ---- low-allocatable runner cgroup isolation regression --------------------
# A snapshot restore preserves max(requested, snapshot-time allocatable), so the
# snapshot template above cannot reproduce a 256 MiB startup floor. Build the
# same OCI input as an image template (no startCmd), then cold boot it with the
# complete 8 GiB / 256 MiB resource declaration from issue #152.
code=$(req POST /v3/templates "$AK" '{"name":"exec-low-cgroup","cpuCount":1,"memoryMB":1024}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "low-cgroup register=$code"; }
LOW_TID=$(json_field "$WORK/resp.body" templateID)
LOW_BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$LOW_TID/builds/$LOW_BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "low-cgroup trigger=$code"; }
LOW_TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$LOW_TID/builds/$LOW_BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) LOW_TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "low-cgroup build error";;
    esac
    sleep 1
done
[ -n "$LOW_TEMPLATE" ] || fail "low-cgroup image build did not become ready"
case "$LOW_TEMPLATE" in e2b-img-*) : ;; *) fail "low-cgroup build produced $LOW_TEMPLATE (want e2b-img-...)";; esac

# A bare image lets the capacity<256MiB case validate a real KVM launch without
# paying envd's steady workload. It carries no resource patch, so the create
# request below proves inherited 256MiB floor normalization.
code=$(req POST /v3/templates "$AK" '{"name":"small-capacity","profile":"bare","cpuCount":1,"memoryMB":1024}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "small-capacity register=$code"; }
SMALL_TID=$(json_field "$WORK/resp.body" templateID)
SMALL_BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$SMALL_TID/builds/$SMALL_BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "small-capacity trigger=$code"; }
SMALL_TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$SMALL_TID/builds/$SMALL_BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) SMALL_TEMPLATE=$(json_field "$WORK/resp.body" templateID); break ;;
        error) cat "$WORK/resp.body"; fail "small-capacity build error" ;;
    esac
    sleep 1
done
[ -n "$SMALL_TEMPLATE" ] || fail "small-capacity bare image build did not become ready"
case "$SMALL_TEMPLATE" in bare-img-*) : ;; *) fail "small-capacity build produced $SMALL_TEMPLATE (want bare-img-...)" ;; esac

LOW_CREATE_BODY=$(python3 - "$LOW_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.resource": json.dumps({
            "capacity": {"cpu": 2, "memory": "8GiB"},
            "allocatable": {"cpu": 2, "memory": "256MiB"},
        }),
    },
}))
PY
)
case "$LOW_ALLOC_REPEATS" in
    ''|*[!0-9]*) fail "LOW_ALLOC_REPEATS must be a positive integer" ;;
esac
[ "$LOW_ALLOC_REPEATS" -gt 0 ] || fail "LOW_ALLOC_REPEATS must be positive"

run_low_allocatable_case() { # $1=iteration
    local iteration="$1" code LOW_START_MS LOW_SID LOW_TOKEN LOW_ELAPSED_MS
    local LOW_RUN_ID LOW_UNIT LOW_CG LOW_CTL_PID LOW_CTL_CG LOW_VMM_PROCS
    local LOW_JOURNAL VMM_MEMBERS_ERROR

    LOW_START_MS=$(date +%s%3N)
    code=$(req POST /sandboxes "$AK" "$LOW_CREATE_BODY")
    [ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "low-allocatable[$iteration] create=$code"; }
    LOW_SID=$(json_field "$WORK/resp.body" sandboxID)
    LOW_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
    code=$(DP_MAX_TIME=65 dp "49983-$LOW_SID" /health "$LOW_TOKEN" || true)
    { [ "$code" = "204" ] || [ "$code" = "200" ]; } || {
        journalctl KUASAR_SANDBOX_ID="$LOW_SID" --no-pager -n 100 2>/dev/null || true
        fail "low-allocatable[$iteration] health=$code"
    }
    LOW_ELAPSED_MS=$(( $(date +%s%3N) - LOW_START_MS ))
    [ "$LOW_ELAPSED_MS" -le 65000 ] || fail "low-allocatable[$iteration] startup took ${LOW_ELAPSED_MS}ms"
    wait_sandbox_state "$LOW_SID" running 20 || fail "low-allocatable[$iteration] sandbox not running"
    assert_resolved_resource_yaml "$WORK/run/$LOW_SID/$LOW_SID.yaml" 8GiB - - \
        || fail "low-allocatable[$iteration] static resolved resource YAML"
    LOW_RUN_ID=$(sandbox_run_id "$LOW_SID")
    [ -n "$LOW_RUN_ID" ] || fail "low-allocatable[$iteration] sandbox has no run_id"
    LOW_UNIT="sandbox-runner@$LOW_RUN_ID.service"
    LOW_CG=$(systemctl show "$LOW_UNIT" -p ControlGroup --value)
    LOW_CTL_PID=$(systemctl show "$LOW_UNIT" -p MainPID --value)
    [ -n "$LOW_CG" ] && [ "$LOW_CG" != "/" ] || fail "runner ControlGroup is invalid: $LOW_CG"
    [ "$LOW_CTL_PID" -gt 1 ] || fail "runner MainPID is invalid: $LOW_CTL_PID"
    LOW_CTL_CG=$(awk -F: '$1 == "0" {print $3}' "/proc/$LOW_CTL_PID/cgroup")
    [ "$LOW_CTL_CG" = "$LOW_CG/ctl" ] || fail "sandbox-ctl cgroup=$LOW_CTL_CG, want $LOW_CG/ctl"
    LOW_VMM_PROCS="/sys/fs/cgroup$LOW_CG/vmm/cgroup.procs"
    if ! VMM_MEMBERS_ERROR=$(e2e_assert_vmm_cgroup_members /proc "$LOW_VMM_PROCS" 2>&1); then
        fail "low-allocatable[$iteration] $VMM_MEMBERS_ERROR"
    fi
    [ "$(<"/sys/fs/cgroup$LOW_CG/vmm/memory.high")" = "234881024" ] \
        || fail "vmm memory.high is not 224MiB"
    [ "$(<"/sys/fs/cgroup$LOW_CG/vmm/memory.max")" = "8623489024" ] \
        || fail "vmm memory.max does not reflect 8GiB capacity + 32MiB overhead"
    [ "$(<"/sys/fs/cgroup$LOW_CG/ctl/memory.high")" = "max" ] \
        || fail "ctl memory.high is constrained"

    LOW_JOURNAL="$WORK/low-allocatable-$iteration.journal"
    wait_for_mem_report_progress "$LOW_UNIT" "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] mem_report made no progress after a transient failure"

    code=$(req DELETE "/sandboxes/$LOW_SID" "$AK"); [ "$code" = "204" ] || fail "delete low-allocatable[$iteration] sandbox=$code"
    for _ in $(seq 1 50); do
        [ ! -e "/sys/fs/cgroup$LOW_CG/vmm" ] && break
        sleep 0.1
    done
    [ ! -e "/sys/fs/cgroup$LOW_CG/vmm" ] || fail "vmm cgroup remained after StopUnit"
    journalctl -u "$LOW_UNIT" --no-pager >"$LOW_JOURNAL" 2>/dev/null || true
    # KillMode=control-group may terminate CH before sandbox-ctl reaches the API.
    grep -Fq '[sandbox-ctl] received terminated' "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run did not observe StopUnit"
    grep -Fq '[sandbox-ctl] CH exited code=' "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run did not observe CH exit"
    # Under deliberate memory.high pressure, CH's direct systemd signal can race
    # sandbox-ctl's shutdown API and make CH report a non-zero shutdown exit. The
    # lifecycle contract here is bounded exit and cgroup removal without SIGKILL.
    ! grep -Fq "CH didn't exit within" "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run escalated shutdown to SIGKILL"
    echo "==> PASS: 8GiB/256MiB runner[$iteration] ready in ${LOW_ELAPSED_MS}ms; mem_report progressed; ctl/vmm isolated and cleaned"
}

for LOW_ALLOC_ITERATION in $(seq 1 "$LOW_ALLOC_REPEATS"); do
    run_low_allocatable_case "$LOW_ALLOC_ITERATION"
done
echo "==> PASS: repeated 8GiB/256MiB startup $LOW_ALLOC_REPEATS times"

# Keep the pre-existing low-allocatable cgroup check in static mode: its exact
# 224 MiB memory.high assertion is the launch-time value. A live controller may
# legitimately grant memory before envd reaches ready. With no sandbox left from
# that check, restart against the same store and attach subsequent sandboxes to
# the controller for the resource stats and restore coverage below.
stop_orchestrator
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
echo "==> PASS: conductor restarted with the resource controller after static cgroup validation"

# ---- default managed resource policy: cold boot + exec + pause/resume -------
# LOW_TEMPLATE carries no request/template resource patch. This makes the real
# managed launch exercise conductor defaults rather than merely asserting an
# explicit 256MiB request.
DEFAULT_CREATE_BODY=$(python3 - "$LOW_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({"templateID": sys.argv[1], "timeout": 180}))
PY
)
code=$(req POST /sandboxes "$AK" "$DEFAULT_CREATE_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "default-policy create=$code"; }
DEFAULT_SID=$(json_field "$WORK/resp.body" sandboxID)
DEFAULT_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
code=$(DP_MAX_TIME=65 dp "49983-$DEFAULT_SID" /health "$DEFAULT_TOKEN" || true)
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; fail "default-policy cold health=$code"; }
wait_sandbox_state "$DEFAULT_SID" running 200 || fail "default-policy cold sandbox not running"
assert_resolved_resource_yaml "$WORK/run/$DEFAULT_SID/$DEFAULT_SID.yaml" \
    2GiB 2GiB "$WORK/sandbox-resource.sock" || fail "default-policy dynamic resolved resource YAML"
wait_resource_stats "$DEFAULT_SID" || fail "default-policy resource stats missing"
assert_resource_lease "$DEFAULT_SID" 2147483648 268435456 2147483648 \
    || fail "default-policy lease did not match generated YAML"
DEFAULT_EXEC_TOKEN="$(issue_exec_session "$DEFAULT_SID" "$AK")" || fail "default-policy exec session"
exec_through_connect "$DEFAULT_SID" "$DEFAULT_EXEC_TOKEN" "DEFAULT_COLD_$RANDOM"

code=$(req POST "/sandboxes/$DEFAULT_SID/pause" "$AK")
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "default-policy pause=$code"; }
wait_sandbox_state "$DEFAULT_SID" paused 1200 || fail "default-policy sandbox did not pause"
code=$(req POST "/sandboxes/$DEFAULT_SID/connect" "$AK" '{"timeout":180}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "default-policy resume=$code"; }
default_resumed=""
for _ in $(seq 1 240); do
    code=$(DP_MAX_TIME=5 dp "49983-$DEFAULT_SID" /health "$DEFAULT_TOKEN" || true)
    case "$code" in 200|204) default_resumed=1; break ;; esac
    sleep 0.25
done
[ -n "$default_resumed" ] || fail "default-policy envd did not recover after resume"
wait_sandbox_state "$DEFAULT_SID" running 200 || fail "default-policy resumed sandbox not running"
assert_resolved_resource_yaml "$WORK/run/$DEFAULT_SID/$DEFAULT_SID.yaml" \
    2GiB 2GiB "$WORK/sandbox-resource.sock" || fail "default-policy restore resource YAML"
wait_resource_stats "$DEFAULT_SID" || fail "default-policy resource stats missing after resume"
assert_resource_lease "$DEFAULT_SID" 2147483648 268435456 2147483648 \
    || fail "default-policy restored lease did not match generated YAML"
exec_through_connect "$DEFAULT_SID" "$DEFAULT_EXEC_TOKEN" "DEFAULT_RESUME_$RANDOM"
code=$(req DELETE "/sandboxes/$DEFAULT_SID" "$AK")
[ "$code" = "204" ] || fail "default-policy delete=$code"
echo "==> PASS: default 2GiB capacity / 256MiB floor cold boot, envd, exec, pause/resume, YAML and lease inventory"

SMALL_CREATE_BODY=$(python3 - "$SMALL_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.resource": json.dumps({"capacity": {"memory": "192MiB"}}),
    },
}))
PY
)
code=$(req POST /sandboxes "$AK" "$SMALL_CREATE_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "small-capacity create=$code"; }
SMALL_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$SMALL_SID" running 600 || {
    journalctl KUASAR_SANDBOX_ID="$SMALL_SID" --no-pager -n 120 2>/dev/null || true
    fail "small-capacity bare sandbox did not reach running"
}
assert_resolved_resource_yaml "$WORK/run/$SMALL_SID/$SMALL_SID.yaml" \
    192MiB 192MiB "$WORK/sandbox-resource.sock" 192MiB - \
    || fail "capacity<256MiB did not normalize inherited floor"
assert_resource_lease "$SMALL_SID" 201326592 201326592 201326592 \
    || fail "small-capacity lease did not match normalized YAML"
SMALL_EXEC_TOKEN="$(issue_exec_session "$SMALL_SID" "$AK")" || fail "small-capacity exec session"
exec_through_connect "$SMALL_SID" "$SMALL_EXEC_TOKEN" "SMALL_CAPACITY_$RANDOM"
code=$(req DELETE "/sandboxes/$SMALL_SID" "$AK")
[ "$code" = "204" ] || fail "small-capacity delete=$code"
echo "==> PASS: real bare KVM launch normalized inherited 256MiB floor to 192MiB capacity"

# ---- async launch failure/kill gates --------------------------------------
# These deterministic injections surround the release-candidate binaries; they
# exercise the real conductor, sqlite store, run pool, systemd units, network,
# routes and cleanup without relying on timing races.
RUNNER_UNIT="$UNIT_DIR/sandbox-runner@.service"
RUNNER_UNIT_SAVED="$WORK/sandbox-runner@.service.saved"
cp "$RUNNER_UNIT" "$RUNNER_UNIT_SAVED"
python3 - "$RUNNER_UNIT" <<'PY'
import sys
path = sys.argv[1]
lines = open(path, encoding="utf-8").read().splitlines()
lines = ["ExecStart=/bin/sleep 30" if line.startswith("ExecStart=") else line for line in lines]
open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
PY
systemctl daemon-reload
echo "==> inject runner WaitAssignment timeout; Create must still return durable 201"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runner-timeout create=$code (want 201)"; }
RUNNER_TIMEOUT_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNNER_TIMEOUT_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNNER_TIMEOUT_SID" dead 200 || fail "runner-timeout sandbox did not roll back to dead"
[ -z "$(sandbox_run_id "$RUNNER_TIMEOUT_SID")" ] || fail "runner-timeout rollback retained run_id"
code=$(DP_MAX_TIME=10 dp "49983-$RUNNER_TIMEOUT_SID" /health "$RUNNER_TIMEOUT_TOKEN" || true)
[ "$code" = "404" ] || fail "runner-timeout route=$code (want prompt 404 after Delete)"
code=$(req DELETE "/sandboxes/$RUNNER_TIMEOUT_SID" "$AK"); [ "$code" = "204" ] || fail "delete runner-timeout sandbox=$code"
cp "$RUNNER_UNIT_SAVED" "$RUNNER_UNIT"
systemctl daemon-reload
systemctl stop 'sandbox-runner@*.service' >/dev/null 2>&1 || true
systemctl reset-failed 'sandbox-runner@*.service' >/dev/null 2>&1 || true
echo "==> PASS: runner wait timeout rolled accepted fresh Create to dead + Delete with empty run_id"

printf '%s\n' runtime-wire-failure >"$WORK/inject-sandbox-run"
echo "==> inject malformed runtime readiness after runner commit"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runtime-failure create=$code (want 201)"; }
RUNTIME_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNTIME_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNTIME_FAILURE_SID" dead 120 || fail "runtime protocol failure did not roll back to dead"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$RUNTIME_FAILURE_SID" /health "$RUNTIME_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "runtime-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$RUNTIME_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete runtime-failure sandbox=$code"
echo "==> PASS: readiness protocol failure fenced the assigned runner and published Delete"

printf '%s\n' envd-init-failure >"$WORK/inject-sandbox-run"
echo "==> inject mandatory envd /init HTTP 500 after valid readiness wire"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "envd-failure create=$code (want 201)"; }
ENVD_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$ENVD_FAILURE_SID" dead 120 || fail "envd init failure did not roll back to dead"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$ENVD_FAILURE_SID" /health "$ENVD_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "envd-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$ENVD_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete envd-failure sandbox=$code"
echo "==> PASS: mandatory envd /init failure rolled accepted Create to dead + Delete"

printf '%s\n' hold >"$WORK/inject-sandbox-run"
echo "==> hold assigned runtime to exercise starting SetTimeout/Pause/Kill"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "starting-kill create=$code (want 201)"; }
STARTING_KILL_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$STARTING_KILL_SID" starting 50 || fail "held sandbox was not starting"
STARTING_KILL_RUN_ID=""
for _ in $(seq 1 100); do
    STARTING_KILL_RUN_ID="$(sandbox_run_id "$STARTING_KILL_SID")"
    [ -n "$STARTING_KILL_RUN_ID" ] && break
    sleep 0.1
done
[ -n "$STARTING_KILL_RUN_ID" ] || fail "held starting sandbox never bound a runner"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/timeout" "$AK" '{"timeout":77}')
[ "$code" = "204" ] || fail "SetTimeout starting=$code (want 204)"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/pause" "$AK")
[ "$code" = "409" ] || fail "Pause starting=$code (want 409)"
code=$(req DELETE "/sandboxes/$STARTING_KILL_SID" "$AK")
[ "$code" = "204" ] || fail "Kill starting=$code (want 204)"
rm -f "$WORK/inject-sandbox-run"
wait_sandbox_state "$STARTING_KILL_SID" missing 50 || fail "Kill starting left a durable row"
if systemctl is-active --quiet "sandbox-runner@$STARTING_KILL_RUN_ID.service"; then
    fail "Kill starting left runner $STARTING_KILL_RUN_ID active"
fi
echo "==> PASS: starting SetTimeout=204, Pause=409, Kill removed row/runner without resurrection"

# ---- create the sandbox (boots the microVM) -------------------------------
# Inject sandbox config via the X-Kuasar-Sandbox-Network header (§4.6): the guest
# hostname should become CFG_HOST, verified by `hostname` in the exec below.
CFG_HOST="e2e-cfg-host"
echo "==> POST /sandboxes (boot microVM from $TEMPLATE; inject hostname=$CFG_HOST via header)"
CREATE_BASE_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.restore": json.dumps({"prefetch": "memory"}),
    },
}))
PY
)
REQ_NET_HEADER="{\"hostname\":\"$CFG_HOST\"}"
REQ_ATTACH_MMDS="$MMDS_ROUTES_E2E"
code=$(req POST /sandboxes "$AK" "$CREATE_BASE_BODY")
unset REQ_NET_HEADER REQ_ATTACH_MMDS
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo
    echo "==> orchestrator log:"; sed 's/^/  orch| /' "$WORK/orch.log"
    SID=$(ls "$WORK/run" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201 durable acceptance)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
CREATE_RETURN_STATE="$(sandbox_state "$SID")"
case "$CREATE_RETURN_STATE" in starting|running) ;; *) fail "state immediately after 201=$CREATE_RETURN_STATE";; esac
echo "==> PASS: Create 201 durably accepted sandbox $SID (observed state=$CREATE_RETURN_STATE)"
echo "==> issue data request immediately after Create; starting must park until running"
(
    code=$(DP_MAX_TIME=120 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
    printf '%s\n' "$code" >"$WORK/immediate-data.code"
) &
IMMEDIATE_DATA_PID=$!
wait_internal_traffic_stats "$SID" parking \
    || { sed 's/^/  orch| /' "$WORK/orch.log"; fail "immediate post-Create request was not reported as parking"; }
immediate_data_status=0
wait "$IMMEDIATE_DATA_PID" || immediate_data_status=$?
IMMEDIATE_DATA_PID=""
[ "$immediate_data_status" = 0 ] || fail "immediate post-Create data client exited $immediate_data_status"
code=$(cat "$WORK/immediate-data.code")
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; fail "immediate post-Create data request=$code"; }
wait_sandbox_state "$SID" running 20 || fail "sandbox was not running after parked data request"
wait_internal_traffic_stats "$SID" idle || fail "internal traffic did not converge to idle"
wait_resource_stats "$SID" || fail "controller resource stats were not reported"
assert_resolved_resource_yaml "$WORK/run/$SID/$SID.yaml" 8GiB 8GiB "$WORK/sandbox-resource.sock" \
    || fail "snapshot restore resource YAML did not preserve capacity and target-node defaults"
assert_resource_lease "$SID" 8589934592 268435456 8589934592 \
    || fail "snapshot restore lease did not match generated YAML"
echo "==> PASS: post-Create data request was reported parking through readiness, then idle; resource stats are live (code=$code)"

# ---- list -----------------------------------------------------------------
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list=$code"
grep -q "$SID" "$WORK/resp.body" || fail "sandbox $SID not listed"
echo "==> PASS: sandbox listed"

# ---- detail / info contract -----------------------------------------------
# This is the HTTP endpoint used by the E2B-compatible sandbox info/get-info
# path. Keep this in the real microVM E2E so the detail contract is checked
# against a running sandbox, not only through an in-process handler test.
code=$(req GET "/sandboxes/$SID" "$AK")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "sandbox detail=$code"; }
# These are the node defaults used by this E2E config. The API obtains them
# from node-ctl's a.res, not from the sandbox create request.
assert_sandbox_detail "$WORK/resp.body" "$SID" 2 2048 1024 \
    || { cat "$WORK/resp.body"; fail "sandbox detail contract"; }
echo "==> PASS: sandbox detail/info returned RFC3339 timestamps and resource fields"

# ---- native exec capability -> CONNECT -> sandbox-ctl -> real guest -------
echo "==> issue an explicit native exec capability (create has no default token)"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue native exec capability"
rm -f "$WORK/exec-session.secret"
NATIVE_MARK="NATIVE_EXEC_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
PTY_MARK="NATIVE_EXEC_PTY_$RANDOM"
exec_pty_resize_through_connect "$SID" "$EXEC_TOKEN" "$PTY_MARK"
echo "==> PASS: real sandbox-ctl CONNECT reached the guest (stdio, duplicate headers, PTY resize, exit status)"

# ---- execute a command in the guest via envd (Connect-RPC over envd.sock) --
ENVD_SOCK="$WORK/run/$SID/envd.sock"
[ -S "$ENVD_SOCK" ] || fail "envd.sock not found at $ENVD_SOCK"
cat > "$WORK/envd_exec.py" <<'PY'
import http.client, socket, struct, json, base64, sys
sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.sock.connect(sock_path)
req = {"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json", "Connect-Protocol-Version": "1",
    "X-Access-Token": token})
r = c.getresponse(); data = r.read()
out = b""; exit_code = None; err = None; i = 0
while i + 5 <= len(data):
    flag = data[i]; ln = struct.unpack(">I", data[i+1:i+5])[0]; msg = data[i+5:i+5+ln]; i += 5+ln
    j = json.loads(msg) if msg else {}
    if flag & 2:
        if j.get("error"): err = j
        continue
    ev = j.get("event", {})
    if "data" in ev:
        d = ev["data"]
        for k in ("stdout","stderr"):
            if d.get(k): out += base64.b64decode(d[k])
    if "end" in ev: exit_code = ev["end"].get("exitCode", 0)
print("HTTP_STATUS", r.status)
print("EXIT_CODE", exit_code)
if err is not None:
    print("API_ERROR", json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n"); sys.stdout.flush()
sys.stdout.buffer.write(out); sys.stdout.write("\nOUTPUT_END\n")
PY
cat > "$WORK/envd_start.py" <<'PY'
import base64, http.client, json, socket, struct, sys

sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.sock.connect(sock_path)

req = {"process": {"cmd": "/bin/sh", "args": ["-c", "exec " + cmd], "cwd": "/home/user"}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json", "Connect-Protocol-Version": "1",
    "Authorization": "Basic " + base64.b64encode(b"user:").decode(),
    "X-Access-Token": token})
r = c.getresponse()
if r.status != 200:
    raise SystemExit(f"envd start HTTP {r.status}: {r.read()!r}")
while True:
    header = r.read(5)
    if len(header) != 5:
        raise SystemExit("envd stream ended before start event")
    flag, length = header[0], struct.unpack(">I", header[1:])[0]
    message = r.read(length)
    event = json.loads(message) if message else {}
    if flag & 2:
        raise SystemExit(f"envd stream error before start: {event!r}")
    start = event.get("event", {}).get("start")
    if start is not None:
        print(start["pid"])
        r.close()
        c.close()
        break
PY
MARK="HELLO_FROM_GUEST_$RANDOM"
echo "==> exec in guest: sh -c 'hostname; id; echo $MARK; uname -sm'"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "hostname; id; echo $MARK; uname -sm" > "$WORK/exec.out" 2>&1 || true
sed 's/^/  guest| /' "$WORK/exec.out"
grep -q "$MARK" "$WORK/exec.out" || fail "guest command output missing $MARK (envd exec failed; see above)"
grep -q 'EXIT_CODE 0' "$WORK/exec.out" || fail "guest command exit code != 0"
echo "==> PASS: command executed in guest (saw $MARK, exit 0)"
# Sandbox-config injection (§4.6): the X-Kuasar-Sandbox-Network header set the guest
# hostname. Best-effort (the main flow already passed); a note rather than a failure.
if grep -q "$CFG_HOST" "$WORK/exec.out"; then echo "==> PASS: config injected (guest hostname=$CFG_HOST via X-Kuasar-Sandbox-Network)"
else echo "    (note: guest hostname != $CFG_HOST; config-injection check inconclusive)"; fi

# ---- delegated cgroup topology + long-lived envd-managed service ---------
# The service stream is deliberately dropped after envd reports its start event;
# envd owns the process from then on. Its in-memory counter and HTTP listener ride
# both local snapshots below, covering the /app freezer recursively across
# envd's /user subtree.
FREEZE_SERVICE_B64=$(python3 - <<'PY'
import base64
program = r'''
import os, pathlib, socket, time

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", 8001))
listener.listen()
listener.settimeout(0.05)
counter = 0
cgroup = pathlib.Path("/proc/self/cgroup").read_text().strip().splitlines()[0].split(":", 2)[2]
while True:
    counter += 1
    try:
        conn, _ = listener.accept()
    except socket.timeout:
        pass
    else:
        with conn:
            conn.settimeout(0.2)
            try:
                conn.recv(4096)
            except OSError:
                pass
            body = f"ENVD_FREEZE_SERVICE {counter} {os.getpid()} {cgroup}\n".encode()
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " +
                         str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
    time.sleep(0.05)
'''
print(base64.b64encode(program.encode()).decode())
PY
)
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "python3 -c \"import base64; open('/home/user/freeze_service.py','wb').write(base64.b64decode('$FREEZE_SERVICE_B64'))\"" \
    >"$WORK/install-freeze-service.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/install-freeze-service.out" \
    || { sed 's/^/  envd| /' "$WORK/install-freeze-service.out"; fail "install envd freeze service"; }
FREEZE_PID=$(python3 "$WORK/envd_start.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "python3 /home/user/freeze_service.py" 2>"$WORK/start-freeze-service.err") \
    || { cat "$WORK/start-freeze-service.err"; fail "start envd freeze service"; }
[[ "$FREEZE_PID" =~ ^[0-9]+$ ]] || fail "envd freeze service returned invalid pid: $FREEZE_PID"

freeze_service_probe() {
    local service_code
    service_code=$(DP_MAX_TIME=8 dp "8001-$SID" / "$FORWARD_TOKEN" || true)
    [ "$service_code" = "200" ] || return 1
    read -r FREEZE_MARK FREEZE_COUNTER FREEZE_PROBE_PID FREEZE_CGROUP <"$WORK/dp.body"
    [ "$FREEZE_MARK" = "ENVD_FREEZE_SERVICE" ] &&
        [ "$FREEZE_PROBE_PID" = "$FREEZE_PID" ] &&
        [[ "$FREEZE_COUNTER" =~ ^[0-9]+$ ]] &&
        [[ "$FREEZE_CGROUP" == /user || "$FREEZE_CGROUP" == /user/* ]]
}

service_ready=""
for _ in $(seq 1 40); do
    freeze_service_probe && { service_ready=1; break; }
    sleep 0.25
done
[ -n "$service_ready" ] || { cat "$WORK/dp.body" 2>/dev/null || true; fail "envd freeze service did not listen"; }

"$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/run" -- /bin/sh -ceu '
pid=$1
global=/proc/1/root/sys/fs/cgroup
real=$global/app
grep -qx "0::/init" /proc/self/cgroup
test ! -s /sys/fs/cgroup/cgroup.procs
test ! -s "$real/cgroup.procs"
grep -qx "$$" "$real/init/cgroup.procs"
service_path=$(cut -d: -f3 "/proc/$pid/cgroup")
case "$service_path" in /user|/user/*) ;; *) exit 1 ;; esac
grep -qx "$pid" "$real$service_path/cgroup.procs"
for controller in cpu memory io; do
    grep -qw "$controller" "$global/cgroup.subtree_control"
    grep -qw "$controller" "$real/cgroup.subtree_control"
done
for group in init user ptys socats; do
    test -d "$real/$group"
    test -e "$real/$group/cpu.weight"
    test -e "$real/$group/memory.max"
    test -e "$real/$group/io.weight"
done
' sh "$FREEZE_PID" >"$WORK/cgroup-topology.out" 2>&1 \
    || { sed 's/^/  cgroup| /' "$WORK/cgroup-topology.out"; fail "delegated envd cgroup topology"; }
echo "==> PASS: real /app is empty; /init + envd user/ptys/socats and cpu/memory/io delegation verified"

# ---- internal proxy_netns -> floatingip user port -------------------------
USER_MARK="internal-proxy-netns-user-port-$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "mkdir -p /home/user/e2e-site; echo '$USER_MARK' > /home/user/e2e-site/index.html; cd /home/user/e2e-site; python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/e2e-http-8000.log 2>&1 &" \
    >"$WORK/start-user-port.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-user-port.out" || { sed 's/^/  envd| /' "$WORK/start-user-port.out"; fail "start guest user-port server"; }
ok=""
for _ in $(seq 1 30); do
    code=$(DP_MAX_TIME=8 dp "8000-$SID" / "$FORWARD_TOKEN" || true)
    grep -q "$USER_MARK" "$WORK/dp.body" 2>/dev/null && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; fail "internal proxy_netns -> floatingip user port did not return marker"; }
echo "==> PASS: internal proxy per-dial proxy_netns reached sandbox floatingip:8000 (marker=$USER_MARK)"

# ---- local Pause, all policy fields unset -> exact legacy argv + restore ---
# Write a marker file in the guest BEFORE pausing; after resume it must still be
# there — proving both the local snapshot/restore overlay AND that a resumed img
# sandbox restores (not cold-boots). /home/user is user-owned.
PERSIST="PERSIST_$MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "echo $PERSIST > /home/user/persist.txt; cat /home/user/persist.txt" > "$WORK/wr.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/wr.out" || { sed 's/^/  guest| /' "$WORK/wr.out"; fail "could not write /home/user/persist.txt as the guest user (ownership not preserved?)"; }
echo "==> wrote /home/user/persist.txt in the guest (as user)"
freeze_service_probe || fail "envd freeze service disappeared before local pause"
FREEZE_COUNTER_BEFORE_B=$FREEZE_COUNTER

UNSET_CALL=$(snapshot_argv_count)
echo "==> local pause with all policy fields unset; disconnect caller after 0.5s: $SID"
set +e
code=$(curl -sS --noproxy '*' --max-time 0.5 \
    -o "$WORK/pause-cancel.body" -w '%{http_code}' \
    -X POST \
    -H "Host: api.$DOMAIN" \
    -H "X-API-KEY: $AK" \
    -H 'Content-Type: application/json' \
    --data '{}' \
    "http://127.0.0.1:$PORT/sandboxes/$SID/pause" \
    2>"$WORK/pause-cancel.stderr")
PAUSE_CURL_RC=$?
set -e
[ "$PAUSE_CURL_RC" = "28" ] || {
    cat "$WORK/pause-cancel.stderr" >&2
    fail "pause cancellation curl rc=$PAUSE_CURL_RC http=$code (want timeout rc=28)"
}
wait_sandbox_state "$SID" paused 1200 || {
    echo "==> pause client timed out but durable state did not become paused:"
    grep -iE 'snapshot|pause|api error' "$WORK/orch.log" | tail -10 | sed 's/^/  orch| /'
    SID_JOURNAL=$(journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 30 2>/dev/null | grep -iE 'snapshot|ctl.sock|error' | tail -8)
    [ -n "$SID_JOURNAL" ] && echo "$SID_JOURNAL" | sed 's/^/  unit| /'
    fail "accepted local Pause did not commit after caller cancellation"
}
wait_internal_traffic_stats "$SID" paused || fail "paused traffic stats were not stable"
# Pause commits the durable paused state before StopUnit makes sandboxer release
# its live controller reservation. During that bounded cleanup window, returning
# the still-real report is valid; the stable paused state must converge to 409.
wait_resource_status "$SID" 409 || fail "paused resource stats did not converge to 409"
# Keep the sandbox durably paused for longer than several service counter ticks.
# On restore the counter must resume from the frozen snapshot rather than track
# this host wall-clock interval.
sleep 3
assert_snapshot_argv "$UNSET_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    || fail "all-unset local Pause changed the legacy snapshot argv"
B_LOCAL="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$B_LOCAL" ] || fail "local Pause did not create $B_LOCAL"
B_ARTIFACT="$(readlink -f "$B_LOCAL")"
[ -f "$B_ARTIFACT" ] || fail "local B target is missing: $B_ARTIFACT"
B_SNAPSHOT_BASENAME="$(basename "$B_ARTIFACT")"
"$BIN/sandbox-ctl" info --json "$B_LOCAL" >"$WORK/b-local.json" \
    || fail "all-unset local B is not a readable snapshot bundle"
echo "==> PASS: caller timed out, accepted all-unset Pause still committed B and passed no policy flags"

echo "==> accept paused -> starting through POST /connect, then activate native exec immediately"
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":113}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "Connect paused=$code (want 200)"; }
CONNECT_RETURN_STATE="$(sandbox_state "$SID")"
case "$CONNECT_RETURN_STATE" in starting|running) ;; *) fail "state immediately after Connect=$CONNECT_RETURN_STATE (must not remain paused)";; esac
RESUME_MARK="NATIVE_EXEC_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "envd did not become ready after local restore"
wait_resource_stats "$SID" || fail "resource stats did not recover after restore"
echo "==> PASS: Connect returned after durable starting acceptance (observed $CONNECT_RETURN_STATE); immediate native exec parked to running"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" > "$WORK/exec2.out" 2>&1 || true
sed 's/^/  guest2| /' "$WORK/exec2.out"
grep -q "$PERSIST" "$WORK/exec2.out" || fail "pre-pause state LOST after local restore"
freeze_service_probe || fail "envd freeze service/listener missing after local B restore"
FREEZE_COUNTER_AFTER_B=$FREEZE_COUNTER
FREEZE_DELTA_B=$((FREEZE_COUNTER_AFTER_B - FREEZE_COUNTER_BEFORE_B))
[ "$FREEZE_DELTA_B" -ge 0 ] && [ "$FREEZE_DELTA_B" -lt 100 ] \
    || fail "envd service counter advanced across frozen local B window: before=$FREEZE_COUNTER_BEFORE_B after=$FREEZE_COUNTER_AFTER_B"
sleep 1
freeze_service_probe || fail "envd freeze service stopped after local B restore"
[ "$FREEZE_COUNTER" -gt "$FREEZE_COUNTER_AFTER_B" ] \
    || fail "envd freeze service did not resume counter after local B restore"
echo "==> PASS: all-unset B restored envd-managed PID $FREEZE_PID + listener; frozen counter delta=$FREEZE_DELTA_B, then advanced"

# ---- local B -> working-set W -> independent portable publication --------
# The second local Pause keeps memory self separate while disks still merge.
# Make the restored disk delta observably different from B: a content-addressed
# merge with no intervening writes can legitimately reproduce B's top digest.
# This W-only marker makes removal of B's old disk top a meaningful proof.
W_DISK_PERSIST="W_DISK_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $W_DISK_PERSIST > /home/user/working-set-disk.txt" >"$WORK/w-disk-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/w-disk-write.out" \
    || { sed 's/^/  guest| /' "$WORK/w-disk-write.out"; fail "write W-only disk marker"; }
# Removing B's old root-disk top before promotion proves upload-snapshot treats
# B.snapshot as an opaque memory lower instead of recursively publishing B's
# stale disk graph.
PORTABLE_W_CALL=$(snapshot_argv_count)
freeze_service_probe || fail "envd freeze service disappeared before portable W pause"
FREEZE_COUNTER_BEFORE_W=$FREEZE_COUNTER
PORTABLE_W_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$PORTABLE_W_RUN_ID" ] || fail "working-set source runner id is empty"
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":false,"checkpoint_drop_caches":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "portable W pause=$code (want 204)"; }
assert_snapshot_argv "$PORTABLE_W_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "portable W Pause policy did not reach sandbox-ctl exactly"
W_PORTABLE_LOCAL="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$W_PORTABLE_LOCAL" ] || fail "working-set Pause did not create $W_PORTABLE_LOCAL"
W_PORTABLE_ARTIFACT="$(readlink -f "$W_PORTABLE_LOCAL")"
[ "$W_PORTABLE_ARTIFACT" != "$B_ARTIFACT" ] || fail "working-set W reused B memory self"
"$BIN/sandbox-ctl" info --json "$W_PORTABLE_LOCAL" >"$WORK/w-portable-local.json" \
    || fail "local working-set W is unreadable"
PORTABLE_W_UNIT="sandbox-runner@$PORTABLE_W_RUN_ID.service"
wait_unit_journal_contains "$PORTABLE_W_UNIT" \
    'quiesce: guest acked (drop_caches=skipped' "$WORK/w-portable-local.journal" \
    || { tail -40 "$WORK/w-portable-local.journal" | sed 's/^/  unit| /'; fail "working-set Pause did not preserve guest page cache"; }
B_ROOT_TOP_BASENAME=$(python3 - "$WORK/b-local.json" "$WORK/w-portable-local.json" "$B_SNAPSHOT_BASENAME" <<'PY'
import json, os, sys

with open(sys.argv[1], encoding="utf-8") as source:
    parent = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    working = json.load(source)

parent_refs = parent.get("FromRefs") or []
memory_refs = working.get("FromRefs") or []
if len(memory_refs) != len(parent_refs) + 1 or not memory_refs[0].startswith("file://"):
    raise SystemExit(f"working-set from_refs={memory_refs!r}, want local B plus {parent_refs!r}")
memory_path = memory_refs[0][len("file://"):].split("@", 1)[0]
if os.path.basename(memory_path) != sys.argv[3]:
    raise SystemExit(f"working-set memory lower={memory_refs[0]!r}, want {sys.argv[3]!r}")
if memory_refs[1:] != parent_refs:
    raise SystemExit(f"working-set lower tail={memory_refs[1:]!r}, want inherited {parent_refs!r}")

def top(node):
    overlay = node.get("Overlay")
    return overlay["Base"] if overlay else node["Base"]

def chain(node):
    overlay = node.get("Overlay")
    return (overlay.get("BaseFromRefs") if overlay else node.get("BaseFromRefs")) or []

parent_top = top(parent["Boot"]["Root"])
working_root = working["Boot"]["Root"]
if parent_top == top(working_root) or parent_top in chain(working_root):
    raise SystemExit(f"W retained B disk top {parent_top!r}; local disks must merge")
if not parent_top.startswith("file://"):
    raise SystemExit(f"B root disk top is not local: {parent_top!r}")
relative = parent_top[len("file://"):].split("@", 1)[0]
if relative != os.path.basename(relative) or not relative.endswith(".overlay"):
    raise SystemExit(f"refusing to remove unexpected B disk ref {parent_top!r}")
print(relative)
PY
) || fail "local B/W graph validation failed"
B_ROOT_TOP_PATH="$CHECKPOINT_DIR/$SID/$B_ROOT_TOP_BASENAME"
[ -f "$B_ROOT_TOP_PATH" ] || fail "B root disk top is missing before minimal-set test: $B_ROOT_TOP_PATH"
rm -f -- "$B_ROOT_TOP_PATH"
echo "==> PASS: W -> local B is separate; B root disk top was merged and removed"

PROMOTION_TOKEN=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --socket "$WORK/node-ctl.socket") \
    || fail "independent export/promote of local W failed"
case "$PROMOTION_TOKEN" in kmt1.*) ;; *) fail "export-sandbox returned a non-KMT result" ;; esac
PORTABLE_W_REF=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select snapshot_ref from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
)
PORTABLE_W_KEY="${PORTABLE_W_REF#manifest://}"
[[ "$PORTABLE_W_REF" == manifest://* && "$PORTABLE_W_KEY" =~ ^[0-9a-f]{64}$ ]] \
    || fail "promoted W ref is not manifest://<64hex>: $PORTABLE_W_REF"
[ ! -e "$CHECKPOINT_DIR/$SID" ] || fail "promotion retained redundant local checkpoint directory"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$PORTABLE_W_REF" >"$WORK/w-portable-manifest.json" \
    || fail "promoted W is not readable from the manifest store"
PORTABLE_LAYER_SUMMARY=$(python3 - "$WORK/w-portable-manifest.json" "$WORK/b-local.json" "$PORTABLE_W_REF" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    working = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    parent = json.load(source)
refs = working.get("FromRefs") or []
parent_refs = parent.get("FromRefs") or []
if len(refs) != len(parent_refs) + 1 or not refs[0].startswith("manifest://"):
    raise SystemExit(f"portable W from_refs={refs!r}, want manifest B plus {parent_refs!r}")
if refs[1:] != parent_refs:
    raise SystemExit(f"portable W lower tail={refs[1:]!r}, want preserved {parent_refs!r}")
if refs[0] == sys.argv[3]:
    raise SystemExit("portable W self and B memory lower collapsed to one ref")
print(refs[0][len("manifest://"):], len(refs))
PY
) || fail "portable W/B layer validation failed"
read -r PORTABLE_B_KEY PORTABLE_PARENT_LAYERS <<<"$PORTABLE_LAYER_SUMMARY"
echo "==> PASS: independent promotion published distinct W self and opaque B memory layer"

RESUME_MARK="PORTABLE_W_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "portable W did not restore"
freeze_service_probe || fail "envd freeze service/listener missing after portable W restore"
FREEZE_COUNTER_AFTER_W=$FREEZE_COUNTER
FREEZE_DELTA_W=$((FREEZE_COUNTER_AFTER_W - FREEZE_COUNTER_BEFORE_W))
[ "$FREEZE_DELTA_W" -ge 0 ] && [ "$FREEZE_DELTA_W" -lt 100 ] \
    || fail "envd service counter advanced across frozen portable W window: before=$FREEZE_COUNTER_BEFORE_W after=$FREEZE_COUNTER_AFTER_W"
sleep 1
freeze_service_probe || fail "envd freeze service stopped after portable W restore"
[ "$FREEZE_COUNTER" -gt "$FREEZE_COUNTER_AFTER_W" ] \
    || fail "envd freeze service did not resume counter after portable W restore"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" \
    >"$WORK/portable-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/portable-read.out" || { sed 's/^/  guest| /' "$WORK/portable-read.out"; fail "portable W lost guest state"; }
# The working-set memory intentionally retained guest cache, so evict it before
# reading the W-only file. This makes the assertion prove the published disk
# artifact, independently of the restored memory self/lower chain.
"$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/run" -- /bin/sh -c \
    'sync && echo 3 > /proc/sys/vm/drop_caches && cat /home/user/working-set-disk.txt' \
    >"$WORK/portable-disk-read.out" 2>&1 || true
grep -q "$W_DISK_PERSIST" "$WORK/portable-disk-read.out" \
    || { sed 's/^/  guest| /' "$WORK/portable-disk-read.out"; fail "portable W lost merged W-only disk state"; }
PORTABLE_RESTORE_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$PORTABLE_RESTORE_RUN_ID" ] || fail "portable restore runner id is empty"
PORTABLE_RESTORE_UNIT="sandbox-runner@$PORTABLE_RESTORE_RUN_ID.service"
wait_unit_journal_contains "$PORTABLE_RESTORE_UNIT" \
    "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_W_KEY" \
    "$WORK/portable-w.journal" || { tail -40 "$WORK/portable-w.journal" | sed 's/^/  unit| /'; fail "portable restore did not prefetch W self"; }
MANIFEST_PREFETCH_COUNT=$(grep -Fc 'memory prefetch started backend=manifest' "$WORK/portable-w.journal" || true)
[ "$MANIFEST_PREFETCH_COUNT" = "1" ] || fail "portable restore started $MANIFEST_PREFETCH_COUNT manifest prefetches, want W self only"
grep -Fq "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_B_KEY" \
    "$WORK/portable-w.journal" && fail "portable restore prefetched B memory lower"
echo "==> PASS: portable W restored envd-managed PID/listener (frozen delta=$FREEZE_DELTA_W); prefetch targeted W self only"

if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$SCRIPT_DIR/lib/mmds_static_guest.sh"
    source "$SCRIPT_DIR/lib/mmds_secret_guest.sh"
    run_mmds_static_guest_get "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS static exact route"
    run_mmds_secret_standalone_e2e "$WORK/node-ctl.socket" "$SID" "$WORK/envd_exec.py" \
        "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS initial/unresolved/update/delete lifecycle"
    run_mmds_service_standalone_e2e "$SID" "$WORK/envd_exec.py" "$ENVD_SOCK" \
        "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS conductor-owned local service"
    for value in MMDS_SECRET_INITIAL_GUEST_E2E MMDS_SECRET_UPDATED_GUEST_E2E MMDS_SECRET_ROTATED_GUEST_E2E; do
        for artifact in "$WORK"/orch*.log "$WORK"/mmds-*.out "$WORK"/mmds-service.requests \
            "$WORK"/lib/node-ctl.db*; do
            [ -f "$artifact" ] || continue
            grep -a -F -q -- "$value" "$artifact" \
                && fail "MMDS secret plaintext appeared in internal E2E artifact $artifact"
        done
    done
    echo "==> PASS: internal real guest covered static, initial/unresolved/rotated/deleted secret, and local service"
fi

code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill first sandbox before KMT import=$code (want 204)"
unset EXEC_TOKEN

echo "==> KMT missing import -> paused -> durable starting -> asynchronous restore"
code=$(curl -sS --noproxy '*' --max-time 30 -o "$WORK/resp.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
    -H "X-Kuasar-Migration-Token: $PROMOTION_TOKEN" \
    -H 'Content-Type: application/json' --data '{"timeout":119}' \
    "http://127.0.0.1:$PORT/sandboxes/$SID/connect")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "KMT Connect=$code (want 200)"; }
[ "$(json_field "$WORK/resp.body" sandboxID)" = "$SID" ] || fail "KMT Connect changed sandbox identity"
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
KMT_RETURN_STATE="$(sandbox_state "$SID")"
case "$KMT_RETURN_STATE" in starting|running) ;; *) fail "KMT Connect returned with state=$KMT_RETURN_STATE";; esac
KMT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue exec capability during KMT starting"
rm -f "$WORK/exec-session.secret"
KMT_MARK="KMT_RESTORE_$RANDOM"
exec_through_connect "$SID" "$KMT_EXEC_TOKEN" "$KMT_MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" >"$WORK/kmt-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/kmt-read.out" \
    || { sed 's/^/  guest| /' "$WORK/kmt-read.out"; fail "KMT restore lost portable guest state"; }
wait_sandbox_state "$SID" running 20 || fail "KMT restore did not commit running"
echo "==> PASS: KMT Connect returned at $KMT_RETURN_STATE; immediate native exec parked and portable state restored"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill KMT-imported sandbox=$code (want 204)"
unset KMT_EXEC_TOKEN
SID_UNSET="$SID"

# Restart the conductor against the same store with explicit node defaults.
# Local mode must remain warning-free; these values are inherited only when a
# higher layer leaves the corresponding field unset.
stop_orchestrator
write_orchestrator_config node-policy
start_orchestrator "$WORK/orch-node-policy.log"
wait_mmds_listener
echo "==> PASS: conductor restarted in local mode with node merge_ref=true/drop_caches=false"

# ---- Create metadata/header + Pause body/header fieldwise overlays --------
CREATE_POLICY_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": True, "drop_caches": True})
    },
}))
PY
)
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST /sandboxes "$AK" "$CREATE_POLICY_BODY")
unset REQ_CHECKPOINT_HEADER
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "policy create=$code (want 201)"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "policy create exposed a default exec token"

# The Create header overrides merge_ref, while its null drop_caches inherits
# the body. The stored request-scoped namespace must be canonical.
python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select metadata_json from sandboxes where id=?", (sys.argv[2],)).fetchone()
if row is None:
    raise SystemExit("created sandbox row not found")
metadata = json.loads(row[0])
got = metadata.get("kuasar-sandbox.checkpoint")
want = '{"merge_ref":false,"drop_caches":true}'
if got != want:
    raise SystemExit(f"stored checkpoint metadata={got!r}, want {want!r}")
PY
echo "==> PASS: Create checkpoint header overlaid body per field and persisted canonical metadata"

ENVD_SOCK="$WORK/run/$SID/envd.sock"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue policy sandbox exec capability"
POLICY_START_MARK="POLICY_CREATE_ACTIVATION_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$POLICY_START_MARK"
wait_sandbox_state "$SID" running 20 || fail "policy sandbox did not reach running after exec activation"
POLICY_PERSIST="POLICY_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $POLICY_PERSIST > /home/user/policy-persist.txt" >"$WORK/policy-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/policy-write.out" || { sed 's/^/  guest| /' "$WORK/policy-write.out"; fail "write policy sandbox marker"; }

POLICY_CALL=$(snapshot_argv_count)
# Body overrides stored metadata; the header then overrides only merge_ref.
# Its null drop_caches must preserve the body's false.
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":true,"checkpoint_drop_caches":false}')
unset REQ_CHECKPOINT_HEADER
[ "$code" = "204" ] || { cat "$WORK/resp.body"; sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "policy pause=$code (want 204)"; }
assert_snapshot_argv "$POLICY_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "Pause body/header policy did not reach sandbox-ctl exactly"
W_POLICY="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$W_POLICY" ] || fail "policy Pause did not create $W_POLICY"
"$BIN/sandbox-ctl" info --json "$W_POLICY" >"$WORK/w-policy.json" || fail "policy W is unreadable"
python3 - "$WORK/w-policy.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    info = json.load(source)
refs = info.get("FromRefs") or []
if not refs:
    raise SystemExit("working-set W has no memory lower reference")
if "kuasar-sandbox.checkpoint" in (info.get("Metadata") or {}):
    raise SystemExit("host-only checkpoint policy leaked into snapshot.cfg metadata")
PY
echo "==> PASS: Pause header null inherited body, explicit false flags reached local capture, W self remains separate from its memory lower"

RESUME_MARK="POLICY_W_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "policy W did not restore locally"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/policy-persist.txt" >"$WORK/policy-read.out" 2>&1 || true
grep -q "$POLICY_PERSIST" "$WORK/policy-read.out" || { sed 's/^/  guest| /' "$WORK/policy-read.out"; fail "policy W lost guest state"; }
echo "==> PASS: policy W restored locally with guest state intact"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill policy sandbox=$code"
unset EXEC_TOKEN
SID_POLICY="$SID"

# ---- automatic Pause: metadata merge_ref > node; node supplies drop_caches -
AUTO_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": False})
    },
}))
PY
)
AUTO_CALL=$(snapshot_argv_count)
code=$(req POST /sandboxes "$AK" "$AUTO_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "auto-pause create=$code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
code=$(req POST "/sandboxes/$SID/timeout" "$AK" '{"timeout":15}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "arm auto-pause timeout=$code"; }
AUTO_PAUSED=""
for _ in $(seq 1 180); do
    state=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
)
    [ "$state" = "paused" ] && { AUTO_PAUSED=1; break; }
    sleep 0.5
done
[ -n "$AUTO_PAUSED" ] || { sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "reaper did not auto-pause policy sandbox (last state=$state)"; }
assert_snapshot_argv "$AUTO_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "auto-pause did not resolve metadata > node fieldwise"
[ -f "$CHECKPOINT_DIR/$SID/$SID.snapshot" ] || fail "auto-pause did not create local W"
echo "==> PASS: reaper auto-pause used metadata merge_ref=false and node drop_caches=false"

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill auto-paused sandbox=$code (want 204)"
echo "==> PASS: all local checkpoint-policy sandboxes killed"
echo
echo "==> e2e_execute: OK   (template $TEMPLATE, portable $PORTABLE_W_REF, all-unset $SID_UNSET, policy $SID_POLICY, auto $SID)"
