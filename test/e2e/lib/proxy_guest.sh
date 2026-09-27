#!/usr/bin/env bash
# Proxy process, readiness and request primitives.
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
    for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" != "$old_pid" ] || PIDS[$i]=""; done
    PROXY_PID=""
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
    PROXY_PID="$PROXY_MASTER_PID"
    PIDS+=("$PROXY_MASTER_PID")
    PROXY_MASTER_PID_SLOT=$((${#PIDS[@]} - 1))
    proxy_timeline "replacement master pid=$PROXY_MASTER_PID launched after pid=$old_pid exited"
    wait_proxy_topology_ready "$PROXY_MASTER_PID"
}
wait_traffic_stats() { # $1=sid, $2=parking|idle|paused
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
if set(inflight) != {"parking", "connected"}:
    raise SystemExit(1)
services = stats.get("services", {})
if set(services) != {"forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
for item in services.values():
    if set(item) - {"parking", "connected", "idleSince"} or not {"parking", "connected"} <= set(item):
        raise SystemExit(1)
    busy = item["parking"] or item["connected"]
    if busy and "idleSince" in item:
        raise SystemExit(1)
if mode == "parking":
    ok = inflight["parking"] >= 1 and inflight["connected"] >= 0 and "idleSince" not in stats
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
traffic_idle_since() { # $1=sid
    local code
    code="$(req GET "/sandboxes/$1/stats/traffic" "$AK")"
    [ "$code" = "200" ] || return 1
    python3 - "$WORK/resp.body" <<'PY'
import json, sys
print(json.load(open(sys.argv[1])).get("idleSince", ""))
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
    for log in "$WORK"/orch*.log "$WORK"/proxy*.log "$WORK"/telemetry.log; do
        [ -f "$log" ] || continue
        echo "==> $(basename "$log"):"
        sed 's/^/  /' "$log"
    done
}

install_prepared_proxy() {
    [ -x "${CUSTOM_PROXY_BIN:-}" ] || fail "prepared CUSTOM_PROXY_BIN is required"
    install -m 0700 "$CUSTOM_PROXY_BIN" "$WORK/custom-proxy"
    CUSTOM_PROXY_BIN="$WORK/custom-proxy"
}
