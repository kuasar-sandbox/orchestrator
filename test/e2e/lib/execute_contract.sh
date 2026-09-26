#!/usr/bin/env bash
# Compiler-free helper contracts shared by focused execute/MMDS product cases.
set -euo pipefail

run_host_serialized() {
    local lock_directory="$1" result=0
    shift
    flock --nonblock --exclusive --close --conflict-exit-code 75         "$lock_directory" env KUASAR_EXECUTE_LOCK_HELD=1 "$@" || result=$?
    [ "$result" -ne 75 ] || { echo "another execute/MMDS case holds the host runtime lock" >&2; return 1; }
    return "$result"
}

argv_log_count() {
    python3 - "$1" <<'PY'
import os,sys
p=sys.argv[1]
print(0 if not os.path.exists(p) else sum(1 for line in open(p,encoding="utf-8") if line.strip()))
PY
}

run_argv_count() { argv_log_count "$RUN_ARGV_LOG"; }

wait_run_argv() {
    local index="$1" attempt
    for ((attempt=0; attempt<120; attempt++)); do
        [ "$(run_argv_count)" -gt "$index" ] && return 0
        sleep 0.05
    done
    echo "timed out waiting for run call $index" >&2
    return 1
}

assert_run_source_mode() {
    python3 - "$RUN_ARGV_LOG" "$1" "$2" "$3" <<'PY'
import json,sys
path,index,sid,want=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
calls=[json.loads(x) for x in open(path,encoding="utf-8") if x.strip()]
assert index < len(calls), calls
call=calls[index]
def option(name):
    for i,arg in enumerate(call):
        if arg==name: return call[i+1] if i+1<len(call) else ""
        if arg.startswith(name+"="): return arg.split("=",1)[1]
    return ""
assert option("--sandbox-id")==sid, call
selected=option("--"+want); other=option("--restore" if want=="from" else "--from")
assert selected and not other, call
if want=="from": assert ".sandbox" in selected or selected.startswith("manifest://") or "@manifest:" in selected, selected
if want=="restore": assert ".snapshot" in selected or selected.startswith("manifest://") or "@manifest:" in selected, selected
PY
}

stop_owned_units() {
    local prefix unit
    for prefix in "$@"; do
        while IFS= read -r unit; do
            case "$unit" in
                "$prefix"*.service)
                    systemctl stop "$unit" >/dev/null 2>&1 || true
                    systemctl reset-failed "$unit" >/dev/null 2>&1 || true ;;
            esac
        done < <(systemctl list-units --all --plain --no-legend --no-pager "$prefix*.service" | awk '{print $1}')
    done
}

cleanup() {
    local forwarding_clean=1
    set +e
    [ -e "${PAUSE_BARRIER_TARGET:-}" ] && : > "$PAUSE_BARRIER_RELEASE"
    [ "$MMDS_ROUTES_E2E" = 1 ] && stop_mmds_service_backend
    stop_owned_units "$RUNNER_PREFIX" "$BUILDER_PREFIX"
    [ -n "$IMMEDIATE_DATA_PID" ] && kill "$IMMEDIATE_DATA_PID" 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" >/dev/null 2>&1
    [ "$FORWARD_TO_SWITCH_OWNED" = 1 ] && iptables -D FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null
    [ "$FORWARD_FROM_SWITCH_OWNED" = 1 ] && iptables -D FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null
    [ "$PROXY_VETH_OWNED" = 1 ] && ip link del "$PROXY_VETH_HOST" 2>/dev/null
    [ "$PROXY_NETNS_OWNED" = 1 ] && ip netns del "$PROXY_NETNS" 2>/dev/null
    [ "$SW_NETNS_OWNED" = 1 ] && ip netns del "$SW_NETNS" 2>/dev/null
    execute_state_restore_forwarding "$ORIG_IP_FORWARD" || forwarding_clean=0
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
    if [ -n "${EXECUTE_STATE_EXPECTED:-}" ] && [ "$forwarding_clean" = 1 ]; then
        execute_state_finish "$BIN" "$EXECUTE_STATE_EXPECTED" || true
    fi
}

setup_proxy_netns() {
    if [ -e "/var/run/netns/$PROXY_NETNS" ] || [ -L "/var/run/netns/$PROXY_NETNS" ]; then
        fail "Proxy netns already exists; refusing foreign resource: $PROXY_NETNS"
    fi
    if ip link show "$PROXY_VETH_HOST" >/dev/null 2>&1 || ip link show "$PROXY_VETH_NS" >/dev/null 2>&1; then
        fail "Proxy veth already exists; refusing foreign resource"
    fi
    ip netns add "$PROXY_NETNS"
    PROXY_NETNS_OWNED=1
    ip link add "$PROXY_VETH_HOST" type veth peer name "$PROXY_VETH_NS"
    PROXY_VETH_OWNED=1
    ip link set "$PROXY_VETH_NS" netns "$PROXY_NETNS"
    ip addr add "$PROXY_HOST_IP/30" dev "$PROXY_VETH_HOST"
    ip link set "$PROXY_VETH_HOST" up
    ip netns exec "$PROXY_NETNS" ip addr add "$PROXY_NS_IP/30" dev "$PROXY_VETH_NS"
    ip netns exec "$PROXY_NETNS" ip link set lo up
    ip netns exec "$PROXY_NETNS" ip link set "$PROXY_VETH_NS" up
    ip netns exec "$PROXY_NETNS" ip route add "$FIP_CIDR" via "$PROXY_HOST_IP"
    ORIG_IP_FORWARD="$(sysctl -n net.ipv4.ip_forward 2>/dev/null || true)"
    case "$ORIG_IP_FORWARD" in 0|1) ;; *) fail "cannot read net.ipv4.ip_forward" ;; esac
    if [ -n "${EXECUTE_STATE_EXPECTED:-}" ]; then
        execute_state_remember_forwarding "$ORIG_IP_FORWARD"
        EXECUTE_STATE_EXPECTED="$(execute_state_record "$RUN_KEY" "$WORK" "$SWITCH" "$SW_NETNS" "$PROXY_NETNS" "$PROXY_VETH_HOST" "$PROXY_VETH_NS" "$ORIG_IP_FORWARD")"
    fi
    sysctl -q -w net.ipv4.ip_forward=1
}
