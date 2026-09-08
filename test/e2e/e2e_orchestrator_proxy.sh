#!/usr/bin/env bash
#
# e2e_orchestrator_proxy.sh — exercise the proxy-only data plane end to end with REAL
# components: node-ctl conductor serve (control plane), a separate node-ctl
# proxy master that REGISTERS once on the config-socket plugin plane, syncs routes
# into shared memory, supervises workers, a REAL microVM sandbox with REAL envd, and
# data-plane traffic driven THROUGH the proxy (not the orchestrator):
#
#   conductor serve                                      # control plane on :PORT
#   proxy serve --config <proxy.yaml>                    # data-plane on :PROXY_PORT
#         # one master plugin registration + N workers sharing inherited listeners;
#         # workers run in PROXY_NETNS; conductor policy supplies MMDS listen
#   POST /sandboxes  -> durable starting + proxy route ACK; immediate ordinary and
#                       exec requests park across runner boot and init without retry
#   GET <proxy>/health (Host 49983-<sid>): no token -> 401 (enforce);
#                                          right X-Access-Token -> forwarded to envd
#   data-shaped HTTP/CONNECT sent to <conductor> stays on its API-only handler
#   GET <proxy>/private/... reaches the custom WorkerExtension ingress wrapper
#   CONNECT through the proxy (token on the CONNECT) -> tunnel to envd
#   GET <proxy> for an unknown sandbox -> passive propagation wait, no Wake
#   POST /sandboxes/<sid>/exec-sessions -> KAT; service=exec CONNECT through the
#                                          Proxy -> real guest exec
#   pause -> GET <proxy> -> wake -> auto-resume -> forwarded
#   /metrics on the proxy master reports worker data-plane counters
#
# Setup mirrors e2e_execute.sh (real VM boot). Same heavy prerequisites: systemd+
# root, /dev/kvm, vswitch, zot, docker, store-ctl, mkfs.ext4, the built kernel + e2b
# runtime. Missing prereqs -> exit 0 ("skipped") unless REQUIRE_PROXY=1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
. "$SCRIPT_DIR/lib/proxy.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
MMDS_ROUTES_E2E="${MMDS_ROUTES_E2E:-0}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
PROXY_PORT="${PROXY_PORT:-3443}"
METRICS_PORT="${METRICS_PORT:-3990}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_sw}"
PROXY_NETNS="${PROXY_NETNS:-e2e_proxy}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-e2eph0}"
PROXY_VETH_NS="${PROXY_VETH_NS:-e2epn0}"
PROXY_HOST_IP="${PROXY_HOST_IP:-172.31.254.1}"
PROXY_NS_IP="${PROXY_NS_IP:-172.31.254.2}"
FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"
PROXY_WORKERS=2

skip() { echo; echo "==> e2e_orchestrator_proxy: skipping ($*)"; [ "${REQUIRE_PROXY:-0}" = "1" ] && { echo "REQUIRE_PROXY=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
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

WORK="$(mktemp -d /tmp/e-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"
CUSTOM_PROXY_EXTENSION_E2E=0
if [ -n "${CUSTOM_PROXY_BIN:-}" ]; then
    [ -x "$CUSTOM_PROXY_BIN" ] || skip "CUSTOM_PROXY_BIN is not executable: $CUSTOM_PROXY_BIN"
    CUSTOM_PROXY_EXTENSION_E2E=1
else
    CUSTOM_PROXY_SOURCE_ROOT="$REPO_ROOT"
    if [ ! -f "$CUSTOM_PROXY_SOURCE_ROOT/examples/custom-proxy/main.go" ]; then
        PROXY_PLATFORM_ROOT="$(cd "$BIN/../.." && pwd)"
        if [ -f "$PROXY_PLATFORM_ROOT/../orchestrator/examples/custom-proxy/main.go" ]; then
            CUSTOM_PROXY_SOURCE_ROOT="$(cd "$PROXY_PLATFORM_ROOT/../orchestrator" && pwd)"
        fi
    fi
    if [ -f "$CUSTOM_PROXY_SOURCE_ROOT/examples/custom-proxy/main.go" ]; then
        command -v go >/dev/null 2>&1 || skip "go not on PATH (custom Proxy Extension build)"
        CUSTOM_PROXY_BIN="$WORK/custom-proxy"
        (cd "$CUSTOM_PROXY_SOURCE_ROOT" && GOWORK=off go build -o "$CUSTOM_PROXY_BIN" ./examples/custom-proxy) \
            || skip "failed to build examples/custom-proxy"
        CUSTOM_PROXY_EXTENSION_E2E=1
    else
        # Exact-assets packages contain the E2E suite but no component source.
        # Keep proxy_executable absent so node-ctl runs its built-in Proxy App.
        CUSTOM_PROXY_BIN="-"
    fi
fi
declare -a PIDS=()
declare -a TAGS=()
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
IMMEDIATE_EXEC_PID=""
IMMEDIATE_DATA_PID=""
SW_STARTED=""
ORIG_IP_FORWARD=""
PROXY_TIMELINE_START_MS=""
cleanup() {
    set +e
    [ "$MMDS_ROUTES_E2E" = 1 ] && stop_mmds_service_backend
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    [ -n "$IMMEDIATE_EXEC_PID" ] && kill "$IMMEDIATE_EXEC_PID" 2>/dev/null
    [ -n "$IMMEDIATE_DATA_PID" ] && kill "$IMMEDIATE_DATA_PID" 2>/dev/null
    [ -n "$IMMEDIATE_EXEC_PID" ] && wait "$IMMEDIATE_EXEC_PID" 2>/dev/null
    [ -n "$IMMEDIATE_DATA_PID" ] && wait "$IMMEDIATE_DATA_PID" 2>/dev/null
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        p="${PIDS[$i]}"
        [ -n "$p" ] && kill "$p" 2>/dev/null
    done
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        p="${PIDS[$i]}"
        [ -n "$p" ] && wait "$p" 2>/dev/null
    done
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
monotonic_ms() { awk '{ printf "%.0f\n", $1 * 1000 }' /proc/uptime; }
proxy_timeline() {
    local now elapsed
    now="$(monotonic_ms)"
    elapsed=$((now - ${PROXY_TIMELINE_START_MS:-now}))
    printf '==> proxy-startup +%dms: %s\n' "$elapsed" "$*"
}
process_live() {
    local state
    [ -r "/proc/$1/stat" ] || return 1
    state="$(awk '{ print $3 }' "/proc/$1/stat" 2>/dev/null || true)"
    [ -n "$state" ] && [ "$state" != Z ] && [ "$state" != X ]
}
child_worker_pids() {
    local parent="$1" p stat ppid cmd
    for d in /proc/[0-9]*; do
        p="${d##*/}"
        [ -r "$d/stat" ] || continue
        stat="$(cat "$d/stat" 2>/dev/null || true)"
        ppid="$(printf '%s\n' "$stat" | awk '{print $4}')"
        [ "$ppid" = "$parent" ] || continue
        cmd="$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null || true)"
        case "$cmd" in *" proxy serve --worker "*) printf '%s\n' "$p";; esac
    done
}
dump_proxy_startup_state() {
    local pid workers
    proxy_timeline "capturing failed readiness state"
    workers="$(child_worker_pids "${PROXY_MASTER_PID:-0}" | tr '\n' ' ')"
    echo "==> proxy startup processes:"
    for pid in "${CONDUCTOR_PID:-}" "${PROXY_MASTER_PID:-}" $workers; do
        [ -n "$pid" ] || continue
        ps -o pid=,ppid=,stat=,lstart=,etime=,cmd= -p "$pid" 2>/dev/null || true
    done
    echo "==> network namespaces:"
    ip netns list 2>&1 || true
    echo "==> proxy host link:"
    ip -details link show "$PROXY_VETH_HOST" 2>&1 || true
    echo "==> host data listener:"
    if command -v ss >/dev/null 2>&1; then
        ss -H -lntp "sport = :$PROXY_PORT" 2>&1 || true
    else
        cat /proc/net/tcp 2>&1 || true
    fi
    echo "==> proxy namespace links:"
    ip netns exec "$PROXY_NETNS" ip -br link 2>&1 || true
    echo "==> proxy namespace addresses:"
    ip netns exec "$PROXY_NETNS" ip -br address 2>&1 || true
    echo "==> proxy namespace routes:"
    ip netns exec "$PROXY_NETNS" ip route 2>&1 || true
    echo "==> proxy namespace IPv4 listeners:"
    ip netns exec "$PROXY_NETNS" cat /proc/net/tcp 2>&1 || true
    echo "==> proxy namespace IPv6 listeners:"
    ip netns exec "$PROXY_NETNS" cat /proc/net/tcp6 2>&1 || true
    dump_logs
}
fail_proxy_startup() {
    dump_proxy_startup_state
    fail "$*"
}
wait_proxy_topology_ready() {
    local master="$1" deadline now endpoint target data_ready mmds_ready workers_ready
    local workers worker_count worker_ns wp got state last_state=""
    endpoint="$(python3 - "$PROXY_NS_IP" "$MMDS_PORT" <<'PY'
import socket, sys
print(f"{socket.inet_aton(sys.argv[1])[::-1].hex().upper()}:{int(sys.argv[2]):04X}")
PY
)"
    target="$(stat -Lc '%i' "/var/run/netns/$PROXY_NETNS" 2>/dev/null || true)"
    [ -n "$target" ] || fail_proxy_startup "proxy_netns=$PROXY_NETNS disappeared before readiness"
    deadline=$(($(monotonic_ms) + 30000))
    data_ready=0
    mmds_ready=0
    workers_ready=0
    while :; do
        process_live "$master" || fail_proxy_startup "proxy master pid $master exited before readiness"
        data_ready=0
        (exec 3<>"/dev/tcp/127.0.0.1/$PROXY_PORT") 2>/dev/null && data_ready=1
        mmds_ready=0
        # shellcheck disable=SC2016 # endpoint is supplied to awk through -v.
        ip netns exec "$PROXY_NETNS" awk -v e="$endpoint" \
            '$2 == e && $4 == "0A" { found = 1 } END { exit(found ? 0 : 1) }' \
            /proc/net/tcp 2>/dev/null && mmds_ready=1
        workers="$(child_worker_pids "$master" | tr '\n' ' ')"
        worker_count=0
        worker_ns=1
        for wp in $workers; do
            worker_count=$((worker_count + 1))
            got="$(stat -Lc '%i' "/proc/$wp/ns/net" 2>/dev/null || true)"
            [ "$got" = "$target" ] || worker_ns=0
        done
        workers_ready=0
        [ "$worker_count" -eq "$PROXY_WORKERS" ] && [ "$worker_ns" -eq 1 ] && workers_ready=1
        state="data=$data_ready mmds=$mmds_ready workers=$worker_count/$PROXY_WORKERS workers_netns=$worker_ns"
        if [ "$state" != "$last_state" ]; then
            proxy_timeline "readiness progress: $state"
            last_state="$state"
        fi
        if [ "$data_ready" -eq 1 ] && [ "$mmds_ready" -eq 1 ] && [ "$workers_ready" -eq 1 ]; then
            proxy_timeline "master pid=$master registered; data and MMDS listeners ready; workers=[$workers] in proxy_netns=$PROXY_NETNS"
            return 0
        fi
        now="$(monotonic_ms)"
        [ "$now" -lt "$deadline" ] || break
        sleep 0.1
    done
    fail_proxy_startup "Proxy readiness timed out after 30s (master=$master data=$data_ready mmds=$mmds_ready workers=$worker_count/$PROXY_WORKERS workers_netns=$worker_ns)"
}

stop_proxy_master() {
    local old_pid="$PROXY_MASTER_PID" old_workers worker
    [ -n "$old_pid" ] || return 0
    PROXY_TIMELINE_START_MS="$(monotonic_ms)"
    old_workers="$(child_worker_pids "$old_pid")"
    proxy_timeline "terminating master pid=$old_pid with workers=[$(printf '%s' "$old_workers" | tr '\n' ' ')]"
    stop_proxy "$old_pid"
    PIDS[$PROXY_MASTER_PID_SLOT]=""
    PROXY_MASTER_PID=""
    for worker in $old_workers; do
        process_live "$worker" && fail_proxy_startup "proxy worker $worker survived master pid $old_pid shutdown"
    done
    proxy_timeline "master pid=$old_pid exited and inherited listeners were released"
}

restart_proxy_master_fresh() {
    local log="$1" old_pid="$PROXY_MASTER_PID"
    stop_proxy_master || return 1
    start_proxy "$BIN/node-ctl" "$WORK/proxy.yaml" "$log"
    PROXY_MASTER_PID="$PROXY_HELPER_PID"
    PIDS+=("$PROXY_MASTER_PID")
    PROXY_MASTER_PID_SLOT=$((${#PIDS[@]} - 1))
    proxy_timeline "replacement master pid=$PROXY_MASTER_PID launched after pid=$old_pid exited"
    wait_proxy_topology_ready "$PROXY_MASTER_PID"
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

# control-plane request (api.<domain> on :PORT)
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    if [ "${REQ_ATTACH_MMDS:-0}" = 1 ] && [ -n "${REQ_MMDS_HEADER:-}" ]; then
        args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    fi
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
wait_traffic_stats() { # $1=sid, $2=parking|idle|paused
    local sid="$1" mode="$2" code=""
    for _ in $(seq 1 240); do
        code="$(req GET "/sandboxes/$sid/stats/traffic" "$AK" || true)"
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$mode" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
mode = sys.argv[2]
if set(stats) - {"state", "maxInflight", "inflight", "idleSince", "services"}:
    raise SystemExit(1)
max_inflight = stats.get("maxInflight")
if not isinstance(max_inflight, dict) or set(max_inflight) != {"total", "forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
if any(type(value) is not int or value < 0 for value in max_inflight.values()):
    raise SystemExit(1)
inflight = stats.get("inflight", {})
if set(inflight) != {"parking", "egress"}:
    raise SystemExit(1)
services = stats.get("services", {})
if set(services) != {"forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
for item in services.values():
    if set(item) - {"parking", "egress", "idleSince"} or not {"parking", "egress"} <= set(item):
        raise SystemExit(1)
    busy = item["parking"] or item["egress"]
    if busy and "idleSince" in item:
        raise SystemExit(1)
if mode == "parking":
    ok = inflight["parking"] >= 1 and inflight["egress"] >= 0 and "idleSince" not in stats
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
traffic_idle_since() { # $1=sid
    local code
    code="$(req GET "/sandboxes/$1/stats/traffic" "$AK")"
    [ "$code" = "200" ] || return 1
    python3 - "$WORK/resp.body" <<'PY'
import json, sys
print(json.load(open(sys.argv[1])).get("idleSince", ""))
PY
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
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
    local sid="$1" key="$2" body="${3:-}" code
    [ -n "$body" ] || body='{}'
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-API-KEY: $key" \
        -H 'Content-Type: application/json' \
        --data "$body" \
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
exec_argv_denied_through_proxy() {
    local sid="$1" token="$2" diagnostics="$WORK/native-exec-condition-denied.log"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PROXY_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        -- /bin/true >"$diagnostics" 2>&1; then
        fail "Proxy executed a condition-denied argv"
    fi
    grep -Fq "exec: remote exec rejected" "$diagnostics" \
        || { sed 's/^/  client| /' "$diagnostics"; fail "Proxy denial was not redacted"; }
}
exec_through_proxy_connect() {
    local sid="$1" token="$2" marker="$3"
    local retries="${4:-1}"
    local timeout_seconds="${5:-60}"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local attempt status

    printf 'stdin:%s\n' "$marker" >"$input"
    for attempt in $(seq 1 "$retries"); do
        : >"$output"; : >"$error_output"; : >"$diagnostics"
        if timeout -k 5s "$timeout_seconds" "$BIN/sandbox-ctl" exec \
            --proxy "http://127.0.0.1:$PROXY_PORT" \
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
        if [ "$status" = "47" ] && \
            grep -Fxq "stdout:$marker:stdin:$marker" "$output" 2>/dev/null && \
            grep -Fxq "stderr:$marker" "$error_output" 2>/dev/null; then
            return 0
        fi
        [ "$attempt" = "$retries" ] || sleep 0.5
    done
    sed 's/^/  client| /' "$diagnostics"
    sed 's/^/  stdout| /' "$output" 2>/dev/null
    sed 's/^/  stderr| /' "$error_output" 2>/dev/null
    fail "native exec did not complete after $retries attempt(s), last exit=$status"
}
# data-plane request through the PROXY (:PROXY_PORT), Host <port>-<sid>.<domain>
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --max-time "${DP_MAX_TIME:-120}" --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
    [ -n "$token" ] && args+=(-H "X-Access-Token: $token")
    curl "${args[@]}" "http://127.0.0.1:$PROXY_PORT$path"
}
dump_logs() {
    local log
    for log in "$WORK"/orch*.log "$WORK"/proxy*.log; do
        [ -f "$log" ] || continue
        echo "==> $(basename "$log"):"
        sed 's/^/  /' "$log"
    done
}

# ---- store + zot ----------------------------------------------------------
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

# An e2b-compliant base image: add a 'user' account + tiny ionice/nice shims (envd
# wraps guest processes as `ionice -c.. nice -n.. cmd` and runs them as the default
# user). Same prep as e2e_execute.sh; the orchestrator/envd are unchanged.
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
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF"

# ---- vswitch up ------------------------------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP.
MGMT_VIP="169.254.169.254"
MMDS_PORT="$(free_port)"
PROXY_TIMELINE_START_MS="$(monotonic_ms)"
proxy_timeline "setting up proxy_netns=$PROXY_NETNS"
setup_proxy_netns
proxy_timeline "proxy_netns=$PROXY_NETNS links, address, and route configured"
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true; ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
"$BIN/connector-ctl" vswitch start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap \
    --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$PROXY_NS_IP:$MMDS_PORT >"$WORK/vswitch-start.log" 2>&1 || { sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch start"; }
SW_STARTED=1
ip addr replace "$MGMT_VIP/32" dev "${SWITCH}m0" \
    || fail "configure management VIP on ${SWITCH}m0"
allow_proxy_forwarding
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF; proxy_netns=$PROXY_NETNS reaches $FIP_CIDR via $PROXY_VETH_HOST)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
BLD="$WORK/builder-2G.ext4"   # build sandbox writable disk (pull cache + export scratch)
truncate -s 2G "$BLD"
"$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || { cat "$WORK/mkfs-bld.log"; fail "mkfs.ext4 builder template"; }

# ---- conductor config ------------------------------------------------------
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { auth: enforce, park_timeout: 120s }
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
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
checkpoint: { mode: local }
EOF

# ---- start serve (control plane), then the proxy master -------------------
# The proxy master DIALS serve's config-socket to register, so serve comes up first.
echo "==> node-ctl conductor serve (API control :$PORT)"
"$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
CONDUCTOR_PID="${PIDS[-1]}"
proxy_timeline "conductor pid=$CONDUCTOR_PID launched"
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { dump_logs; skip "orchestrator exited"; }
    sleep 0.5
done
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"
proxy_timeline "conductor health and config socket ready"

echo "==> node-ctl proxy master (single plugin registration, data-plane :$PROXY_PORT, workers=2)"
# The master reads local worker/bootstrap settings from proxy.yaml and receives
# MMDS listen/services only from the conductor registration policy. It then supervises
# workers that inherit listener fds and read the shared route table. h2c here (no tls),
# matching the deployment's plain HTTP ingress. proxy_netns exercises direct mode:
# workers run inside that netns, so floatingip TCP dials need its route table.
write_proxy_config "$WORK/proxy.yaml" \
    "$WORK/node-ctl.socket" "$WORK/run" "127.0.0.1:$PROXY_PORT" \
    "$PROXY_NETNS" "$WORK/run/proxy-stats.sock" "$WORK/run/proxy-routes.shm" \
    1024 "$PROXY_WORKERS" enforce 120s "127.0.0.1:$METRICS_PORT" - - "$CUSTOM_PROXY_BIN"
start_proxy "$BIN/node-ctl" "$WORK/proxy.yaml" "$WORK/proxy.log"
PROXY_MASTER_PID="$PROXY_HELPER_PID"
PIDS+=("$PROXY_MASTER_PID")
PROXY_MASTER_PID_SLOT=$((${#PIDS[@]} - 1))
proxy_timeline "master pid=$PROXY_MASTER_PID launched"
wait_proxy_topology_ready "$PROXY_MASTER_PID"
echo "==> control plane up; proxy master registered on the config-socket plugin plane"
echo "==> PASS: Proxy workers and MMDS listener are in proxy_netns=$PROXY_NETNS"

# The conductor listener is API-only: neither a data-shaped Host nor CONNECT
# reaches the Proxy. Its response is the unmodified API handler's natural miss.
code=$(curl -sS --noproxy '*' \
    -D "$WORK/conductor-data.headers" \
    -o "$WORK/conductor-data.body" \
    -w '%{http_code}' \
    -H "Host: 49983-private-s1.$DOMAIN" \
    -H 'X-Sandbox-Id: private-s1' \
    "http://127.0.0.1:$PORT/private/sandboxes/private-s1/49983/health")
[ "$code" = "404" ] || { cat "$WORK/conductor-data.body"; fail "conductor data Host=$code (want API 404)"; }
! grep -Eiq '^X-Kuasar-Proxy-Error:' "$WORK/conductor-data.headers" \
    || fail "conductor data Host reached Proxy"
code=$(curl -sS --noproxy '*' \
    -D "$WORK/conductor-connect.headers" \
    -o "$WORK/conductor-connect.body" \
    -w '%{http_code}' \
    -X CONNECT \
    -H 'Host: 49983-private-s1.invalid' \
    "http://127.0.0.1:$PORT/private-connect")
case "$code" in 404|405) ;; *) fail "conductor CONNECT=$code (want API handler miss)";; esac
! grep -Eiq '^X-Kuasar-Proxy-Error:' "$WORK/conductor-connect.headers" \
    || fail "conductor CONNECT reached Proxy"
echo "==> PASS: conductor API endpoint did not proxy data Host or CONNECT"

if [ "$CUSTOM_PROXY_EXTENSION_E2E" = 1 ]; then
    # The custom Worker's IngressWrapper is served on data_listen. Its private
    # authentication response differs from the built-in canonical parser.
    code=$(curl -sS --noproxy '*' \
        -o "$WORK/worker-extension.body" \
        -w '%{http_code}' \
        "http://127.0.0.1:$PROXY_PORT/private/sandboxes/private-s1/49983/health")
    [ "$code" = "401" ] || { cat "$WORK/worker-extension.body"; fail "WorkerExtension data ingress=$code (want 401)"; }
    grep -q 'private authentication failed' "$WORK/worker-extension.body" \
        || fail "data listener did not use the WorkerExtension wrapper"
    echo "==> PASS: Data listener serves the WorkerExtension ingress wrapper"
else
    echo "==> SKIP: custom WorkerExtension assertion (set CUSTOM_PROXY_BIN when running without component sources)"
fi

# ---- build a ready e2b template (native v3) --------------------------------
# The 6GiB Build limit is independent of the phase Sandbox's node-default 2GiB
# capacity and leaves room for its VMM plus the build toolchain.
code=$(req POST /v3/templates "$AK" '{"name":"proxy-tmpl","cpuCount":2,"memoryMB":6144}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$GUEST_REF\"}")
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
echo "==> built template: $TEMPLATE"

# Every Create requires the current Proxy registration and its route-applied
# barrier. Removing the master must fail admission before any launch ownership
# or durable sandbox state is retained.
{ find "$WORK/run/sandboxes" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null || true; } \
    | sort >"$WORK/run-dirs.before-unavailable"
"$BIN/connector-ctl" vswitch status "$SWITCH" >"$WORK/vswitch.before-unavailable.json"
python3 - "$WORK/vswitch.before-unavailable.json" <<'PY' >"$WORK/vswitch-ports.before-unavailable"
import json, sys
print(json.load(open(sys.argv[1]))["ports_used"])
PY
systemctl list-units --all --type=service --no-legend --no-pager 'sandbox-runner@*.service' \
    | sort >"$WORK/runners.before-unavailable"
stop_proxy_master
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "503" ] || { cat "$WORK/resp.body"; fail "Create without Proxy=$code (want 503)"; }
code=$(req GET /v2/sandboxes "$AK")
[ "$code" = "200" ] || fail "list after unavailable Create=$code"
python3 - "$WORK/resp.body" <<'PY' || fail "unavailable Create retained durable/cache state"
import json, sys
assert json.load(open(sys.argv[1])) == []
PY
{ find "$WORK/run/sandboxes" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null || true; } \
    | sort >"$WORK/run-dirs.after-unavailable"
cmp -s "$WORK/run-dirs.before-unavailable" "$WORK/run-dirs.after-unavailable" \
    || fail "unavailable Create allocated a sandbox run directory"
"$BIN/connector-ctl" vswitch status "$SWITCH" >"$WORK/vswitch.after-unavailable.json"
python3 - "$WORK/vswitch.after-unavailable.json" <<'PY' >"$WORK/vswitch-ports.after-unavailable"
import json, sys
print(json.load(open(sys.argv[1]))["ports_used"])
PY
cmp -s "$WORK/vswitch-ports.before-unavailable" "$WORK/vswitch-ports.after-unavailable" \
    || fail "unavailable Create attached a network port"
systemctl list-units --all --type=service --no-legend --no-pager 'sandbox-runner@*.service' \
    | sort >"$WORK/runners.after-unavailable"
cmp -s "$WORK/runners.before-unavailable" "$WORK/runners.after-unavailable" \
    || fail "unavailable Create assigned a sandbox runner"
if ps -eo args= | grep -F 'cloud-hypervisor' | grep -F "$WORK/run" >/dev/null; then
    fail "unavailable Create started a VMM"
fi
echo "==> PASS: Proxy absent -> Create 503 with no durable/cache/run-dir/runner/VMM side effect"
restart_proxy_master_fresh "$WORK/proxy-after-unavailable.log"
echo "==> PASS: Proxy re-registered after admission failure"

# ---- create the sandbox (boots the microVM; serve pushes the route) -------
echo "==> POST /sandboxes (boot microVM from $TEMPLATE)"
IDENTITY_ID="identity-$(cat /proc/sys/kernel/random/uuid)"
IDENTITY_STABLE_ID="logical-$IDENTITY_ID"
IDENTITY_CREATE_BODY="$(python3 - "$TEMPLATE" "$IDENTITY_ID" "$IDENTITY_STABLE_ID" <<'PY_IDENTITY'
import json, sys
print(json.dumps({"templateID": sys.argv[1], "timeout": 120, "metadata": {
    "kuasar-sandbox.identity": json.dumps({"id": sys.argv[2], "stable_id": sys.argv[3]})
}}))
PY_IDENTITY
)"
REQ_ATTACH_MMDS="$MMDS_ROUTES_E2E"
code=$(req POST /sandboxes "$AK" "$IDENTITY_CREATE_BODY")
unset REQ_ATTACH_MMDS
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo; dump_logs
    SID=$(ls "$WORK/run/sandboxes" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
[ "$SID" = "$IDENTITY_ID" ] || fail "Create did not use the requested local ID"
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
[ -n "$SID" ] && [ -n "$ENVD_TOKEN" ] && [ -n "$FORWARD_TOKEN" ] \
    || fail "missing sandboxID/envdAccessToken/forwardAccessToken in create response"
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
echo "==> PASS: sandbox $SID durably accepted after Proxy route ACK (tokens captured; no default exec token)"
echo "==> issue native exec capability immediately and park its Proxy CONNECT from starting"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")" \
    || fail "issue conditioned immediate native exec capability"
rm -f "$WORK/exec-session.secret"
IMMEDIATE_NATIVE_MARK="PROXY_IMMEDIATE_NATIVE_EXEC_$RANDOM"
(
    exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$IMMEDIATE_NATIVE_MARK" 1 130
) &
IMMEDIATE_EXEC_PID=$!
echo "==> request envd immediately through Proxy; ACKed starting route must park to running"
(
    code=$(DP_MAX_TIME=120 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
    printf '%s\n' "$code" >"$WORK/immediate-data.code"
    [ "$code" = "204" ] || [ "$code" = "200" ]
) &
IMMEDIATE_DATA_PID=$!
wait_traffic_stats "$SID" parking \
    || { dump_logs; fail "Proxy starting request was not visible as parking"; }
echo "==> PASS: Proxy master cache observed authorized starting ingress as parking"
immediate_data_status=0
wait "$IMMEDIATE_DATA_PID" || immediate_data_status=$?
IMMEDIATE_DATA_PID=""
code="$(<"$WORK/immediate-data.code")"
[ "$immediate_data_status" = "0" ] \
    || { cat "$WORK/dp.body"; dump_logs; fail "immediate Proxy request=$code"; }
echo "==> PASS: Proxy parked post-Create request through starting to running (code=$code)"
immediate_exec_status=0
wait "$IMMEDIATE_EXEC_PID" || immediate_exec_status=$?
IMMEDIATE_EXEC_PID=""
[ "$immediate_exec_status" = "0" ] || { dump_logs; fail "immediate Proxy native exec did not park to running"; }
echo "==> PASS: Proxy native exec parked post-Create CONNECT from ACKed starting to running"

# Conflict must not replace the running object or disclose its credentials.
code=$(req POST /sandboxes "$AK" "$IDENTITY_CREATE_BODY")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "duplicate identity Create=$code (want 409)"; }
code=$(req GET "/sandboxes/$SID" "$AK")
[ "$code" = "200" ] || fail "identity readback=$code"
python3 - "$WORK/resp.body" "$IDENTITY_ID" "$FORWARD_TOKEN" "$IDENTITY_STABLE_ID" <<'PY_IDENTITY'
import base64, json, sys
sandbox = json.load(open(sys.argv[1]))
assert sandbox["sandboxID"] == sys.argv[2]
assert "kuasar-sandbox.identity" not in sandbox.get("metadata", {})
encoded = sys.argv[3].split(".")[1]
claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
assert claims["sid"] == sys.argv[4]
PY_IDENTITY
echo "==> PASS: explicit local/stable identity survived real Proxy exec/envd; duplicate Create is 409"
wait_traffic_stats "$SID" idle || { dump_logs; fail "Proxy traffic did not converge to idle"; }
echo "==> PASS: Proxy master cache converged parking/egress to idle"

code=$(req GET "/sandboxes/$SID/stats/resource" "$AK")
[ "$code" = "501" ] || { cat "$WORK/resp.body"; fail "disabled resource controller stats=$code (want 501)"; }
echo "==> PASS: resource stats reports 501 when the controller is disabled"

ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
for _ in $(seq 1 40); do [ -S "$ENVD_SOCK" ] && break; sleep 0.25; done
[ -S "$ENVD_SOCK" ] || fail "envd.sock not found at $ENVD_SOCK"
cat > "$WORK/envd_exec.py" <<'PY'
import http.client, socket, struct, json, base64, sys
sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.sock.connect(sock_path)
req = {"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json",
    "Connect-Protocol-Version": "1",
    "X-Access-Token": token,
})
r = c.getresponse()
data = r.read()
out = b""
exit_code = None
err = None
i = 0
while i + 5 <= len(data):
    flag = data[i]
    ln = struct.unpack(">I", data[i+1:i+5])[0]
    msg = data[i+5:i+5+ln]
    i += 5 + ln
    j = json.loads(msg) if msg else {}
    if flag & 2:
        if j.get("error"):
            err = j
        continue
    ev = j.get("event", {})
    if "data" in ev:
        d = ev["data"]
        for k in ("stdout", "stderr"):
            if d.get(k):
                out += base64.b64decode(d[k])
    if "end" in ev:
        exit_code = ev["end"].get("exitCode", 0)
print("HTTP_STATUS", r.status)
print("EXIT_CODE", exit_code)
if err is not None:
    print("API_ERROR", json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n")
sys.stdout.flush()
sys.stdout.buffer.write(out)
sys.stdout.write("\nOUTPUT_END\n")
PY

# ---- (1) data plane THROUGH the proxy: route-sync + forward + auth ---------
ok=""
for _ in $(seq 1 20); do
    code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
    { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "envd /health via proxy with token = $code (want 200/204)"; }
echo "==> PASS: route-synced + data plane forwarded through the proxy to real envd (X-Access-Token accepted)"

wait_traffic_stats "$SID" idle || { dump_logs; fail "traffic did not return idle before auth rejection checks"; }
IDLE_BEFORE_INVALID="$(traffic_idle_since "$SID")"
[ -n "$IDLE_BEFORE_INVALID" ] || fail "running idle traffic stats omitted idleSince"
code=$(dp "49983-$SID" /health "")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy WITHOUT token = $code (want 401 enforce)"; }
echo "==> PASS: proxy enforces X-Access-Token (missing -> 401)"

code=$(dp "49983-$SID" /health "wrong-token")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy with WRONG token = $code (want 401)"; }
code=$(curl -sS --noproxy '*' \
    -D "$WORK/typed-error.headers" \
    -o "$WORK/typed-error.body" \
    -w '%{http_code}' \
    -H "Host: 49983-$SID.$DOMAIN" \
    -H 'X-Access-Token: wrong-token' \
    "http://127.0.0.1:$PROXY_PORT/health")
[ "$code" = "401" ] || { cat "$WORK/typed-error.body"; fail "typed unauthorized response=$code"; }
grep -Eiq '^X-Kuasar-Proxy-Error:[[:space:]]*unauthorized' "$WORK/typed-error.headers" \
    || { cat "$WORK/typed-error.headers"; fail "DataEndpoint omitted the typed unauthorized proxy error"; }
echo "==> PASS: Proxy rejects a wrong token with the typed DataEndpoint error"
wait_traffic_stats "$SID" idle || { dump_logs; fail "invalid credentials changed traffic inflight"; }
IDLE_AFTER_INVALID="$(traffic_idle_since "$SID")"
[ "$IDLE_AFTER_INVALID" = "$IDLE_BEFORE_INVALID" ] \
    || fail "invalid credentials changed idleSince ($IDLE_BEFORE_INVALID -> $IDLE_AFTER_INVALID)"
echo "==> PASS: invalid credentials caused no parking/egress and did not refresh idleSince"

USER_MARK="proxy-netns-user-port-$RANDOM"
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
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "user port via proxy_netns -> floatingip did not return marker"; }
echo "==> PASS: proxy_netns worker reached sandbox floatingip:8000 (real user port, marker=$USER_MARK)"

# A signed envd /files request carries no X-Access-Token. Both the Proxy and
# envd verify the same signature; the file bytes still travel only through the
# independent DataEndpoint.
SIGNED_FILE="/tmp/kuasar-proxy-signed-$RANDOM.txt"
SIGNED_MARK="PROXY_SIGNED_FILE_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "printf '%s' '$SIGNED_MARK' > '$SIGNED_FILE'" >"$WORK/write-signed-file.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/write-signed-file.out" \
    || { sed 's/^/  envd| /' "$WORK/write-signed-file.out"; fail "create signed-file fixture"; }
SIGNED_QUERY=$(python3 - "$SIGNED_FILE" "$ENVD_TOKEN" <<'PY'
import base64, hashlib, sys, urllib.parse
path, token = sys.argv[1:]
digest = hashlib.sha256(f"{path}:read::{token}".encode()).digest()
signature = "v1_" + base64.b64encode(digest).decode().rstrip("=")
print(urllib.parse.urlencode({"path": path, "signature": signature}))
PY
)
code=$(curl -sS --noproxy '*' \
    -o "$WORK/signed-file.body" \
    -w '%{http_code}' \
    -H "Host: 49983-$SID.$DOMAIN" \
    "http://127.0.0.1:$PROXY_PORT/files?$SIGNED_QUERY")
[ "$code" = "200" ] && grep -Fq "$SIGNED_MARK" "$WORK/signed-file.body" \
    || { cat "$WORK/signed-file.body"; dump_logs; fail "signed /files through DataEndpoint=$code"; }
echo "==> PASS: signed /files crossed only the independent Proxy DataEndpoint"

# Restart the whole master with a live sandbox. The replacement starts from a
# fresh SHM table and must complete a full route sync before serving the route.
restart_proxy_master_fresh "$WORK/proxy-full-resync.log"
code=$(DP_MAX_TIME=30 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; dump_logs; fail "data request after Proxy full resync=$code"; }
wait_traffic_stats "$SID" idle || { dump_logs; fail "traffic stats missing after Proxy full resync"; }
echo "==> PASS: replacement Proxy master full-synced the live route before ingress"

# ---- (2) initially missing route waits passively without unauthorized Wake -
# The full passive propagation window is 120s. Cancel this probe after two
# seconds: the retired pre-#70 flow emitted Wake immediately, and OnWake's Delete
# made the request return 404 well before this client deadline.
UNKNOWN_SID="deadbeefdeadbeef"
if code=$(DP_MAX_TIME=2 dp "49983-$UNKNOWN_SID" /health "$ENVD_TOKEN" 2>"$WORK/unknown-route.err"); then
    dump_logs
    fail "initially missing route returned $code before its passive propagation window"
else
    curl_status=$?
fi
[ "$curl_status" = "28" ] && [ "$code" = "000" ] \
    || { cat "$WORK/unknown-route.err"; dump_logs; fail "initially missing route probe status=$curl_status code=$code (want client timeout without Wake)"; }
echo "==> PASS: initially missing route waited passively without unauthorized Wake"

# ---- (3) metrics ----------------------------------------------------------
if curl -sS --noproxy '*' "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null | grep -q 'data_requests_total'; then
    echo "==> PASS: proxy master /metrics reports aggregated worker data_requests_total"
else dump_logs; fail "proxy master /metrics did not report data_requests_total"; fi

# A stats EOF/crash must make the aggregate unavailable until the replacement's
# new epoch has completed hello+ready. The dead worker's contribution is removed
# only after the supervisor has reaped it.
mapfile -t CRASH_WORKERS < <(child_worker_pids "$PROXY_MASTER_PID")
CRASH_WORKER="${CRASH_WORKERS[0]:-}"
[ -n "$CRASH_WORKER" ] || fail "no Proxy worker available for crash test"
kill -KILL "$CRASH_WORKER"
SEEN_STATS_503=""
for _ in $(seq 1 120); do
    code="$(req GET "/sandboxes/$SID/stats/traffic" "$AK" || true)"
    [ "$code" = "503" ] && { SEEN_STATS_503=1; break; }
    sleep 0.01
done
[ -n "$SEEN_STATS_503" ] || { dump_logs; fail "worker crash did not create a traffic-stats 503 window"; }
wait_traffic_stats "$SID" idle || { dump_logs; fail "traffic stats did not recover after replacement ready"; }
wait_proxy_topology_ready "$PROXY_MASTER_PID"
echo "==> PASS: worker crash returned 503 until replacement epoch was stats-ready"

# ---- (3b) CONNECT tunnel THROUGH the proxy to envd control -----------------
# Drive CONNECT with raw TCP so the test does not depend on curl proxy-header
# feature variations. The proxy auths the CONNECT and then tunnels a GET /health
# request to the envd control socket.
cc=$(python3 - "$PROXY_PORT" "49983-$SID.$DOMAIN:49983" "$ENVD_TOKEN" <<'PY'
import socket, sys

proxy_port, target, token = int(sys.argv[1]), sys.argv[2], sys.argv[3]
with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as s:
    s.settimeout(10)
    req = (
        f"CONNECT {target} HTTP/1.1\r\n"
        f"Host: {target}\r\n"
        f"X-Access-Token: {token}\r\n"
        "\r\n"
    )
    s.sendall(req.encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    status = data.split(b"\r\n", 1)[0].decode("latin1", "replace")
    if " 200 " not in status and not status.endswith(" 200"):
        print(status or "no-connect-response")
        sys.exit(0)
    s.sendall(f"GET /health HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n".encode())
    data = b""
    while b"\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    print((data.split(b"\r\n", 1)[0] or b"no-health-response").decode("latin1", "replace"))
PY
)
if echo "$cc" | grep -Eq 'HTTP/[0-9.]+ (200|204)'; then
    echo "==> PASS: CONNECT tunnel through the proxy reached envd /health ($cc)"
else
    dump_logs
    fail "CONNECT tunnel through proxy failed: $cc"
fi

# ---- (4) native exec capability THROUGH the Proxy -------------------------
echo "==> reuse the explicit native exec capability after the sandbox is running"
NATIVE_MARK="PROXY_NATIVE_EXEC_$RANDOM"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
echo "==> PASS: real sandbox-ctl CONNECT through the Proxy verified stdin/stdout/stderr and exit status"

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

if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$SCRIPT_DIR/lib/mmds_static_guest.sh"
    source "$SCRIPT_DIR/lib/mmds_secret_guest.sh"
    run_mmds_static_guest_get "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" proxy \
        || { dump_logs; fail "Proxy MMDS static exact route"; }
    mmds_secret_restart_proxy() {
        echo "==> restart Proxy from an empty heap; wait for full route/value/service resync"
        restart_proxy_master_fresh "$WORK/proxy-mmds-restart.log"
    }
    MMDS_SECRET_AFTER_UPDATE_HOOK=mmds_secret_restart_proxy
    run_mmds_secret_standalone_e2e "$WORK/node-ctl.socket" "$SID" "$WORK/envd_exec.py" \
        "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" proxy \
        || { dump_logs; fail "Proxy MMDS initial/unresolved/update/resync/rotation/delete lifecycle"; }
    unset MMDS_SECRET_AFTER_UPDATE_HOOK
    run_mmds_service_standalone_e2e "$SID" "$WORK/envd_exec.py" "$ENVD_SOCK" \
        "$ENVD_TOKEN" "$WORK" proxy \
        || { dump_logs; fail "Proxy MMDS conductor-only service registry"; }
    for value in MMDS_SECRET_INITIAL_GUEST_E2E MMDS_SECRET_UPDATED_GUEST_E2E MMDS_SECRET_ROTATED_GUEST_E2E; do
        for artifact in "$WORK"/orch*.log "$WORK"/proxy*.log "$WORK"/mmds-*.out \
            "$WORK"/mmds-service.requests "$WORK"/lib/node-ctl.db* "$WORK"/run/proxy-routes.shm; do
            [ -f "$artifact" ] || continue
            grep -a -F -q -- "$value" "$artifact" \
                && fail "MMDS secret plaintext appeared in Proxy E2E artifact $artifact"
        done
    done
    echo "==> PASS: real guest covered Proxy restart/full resync, secret rotation/delete, and conductor-only service"
fi

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || { dump_logs; fail "kill=$code (want 204)"; }
echo "==> PASS: sandbox killed"
stop_proxy_master || { dump_logs; fail "proxy master shutdown left a worker alive"; }
echo "==> PASS: proxy master shutdown reaped all worker processes"
echo
echo "==> e2e_orchestrator_proxy: OK   (template $TEMPLATE, sandbox $SID, Proxy on :$PROXY_PORT)"
