#!/usr/bin/env bash

# Small lifecycle helpers for the independent node Proxy. Callers continue to
# own their work directory, networking, process list, and cleanup policy.

write_proxy_config() {
    local output="$1" config_socket="$2" run_root="$3" data_listen="$4"
    local proxy_netns="$5" stats_socket="$6" shm_path="$7" route_capacity="$8"
    local workers="$9" auth="${10}" park_timeout="${11}" metrics_listen="${12:--}"
    local tls_cert="${13:--}" tls_key="${14:--}"
    local proxy_executable="${15:--}"

    {
        printf 'config_socket: %s\n' "$config_socket"
        printf 'paths:\n'
        if [ -n "$proxy_executable" ] && [ "$proxy_executable" != "-" ]; then
            printf '  proxy_executable: %s\n' "$proxy_executable"
        fi
        printf '  run_root: %s\n' "$run_root"
        printf 'data_listen: %s\n' "$data_listen"
        if [ -n "$proxy_netns" ] && [ "$proxy_netns" != "-" ]; then
            printf 'proxy_netns: %s\n' "$proxy_netns"
        fi
        printf 'stats_socket: %s\n' "$stats_socket"
        printf 'shm_path: %s\n' "$shm_path"
        printf 'route_capacity: %s\n' "$route_capacity"
        printf 'workers: %s\n' "$workers"
        printf 'auth: %s\n' "$auth"
        printf 'park_timeout: %s\n' "$park_timeout"
        if [ -n "$metrics_listen" ] && [ "$metrics_listen" != "-" ]; then
            printf 'metrics_listen: %s\n' "$metrics_listen"
        fi
        if [ -n "$tls_cert" ] && [ "$tls_cert" != "-" ]; then
            printf 'tls:\n  cert: %s\n  key: %s\n' "$tls_cert" "$tls_key"
        fi
    } >"$output"
}

start_proxy() {
    local node_ctl="$1" config="$2" log="$3"
    "$node_ctl" proxy serve --config "$config" >"$log" 2>&1 &
    PROXY_HELPER_PID=$!
}

wait_proxy_ready() {
    local pid="$1" host="$2" port="$3" stats_socket="$4" log="$5"
    local timeout_seconds="${6:-30}" deadline=$((SECONDS + timeout_seconds))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            collect_proxy_log "$log"
            return 1
        fi
        if [ -S "$stats_socket" ] && timeout 1 bash -c '
            exec 9<>"/dev/tcp/$1/$2"
            printf "GET / HTTP/1.1\r\nHost: readiness.invalid\r\nConnection: close\r\n\r\n" >&9
            IFS= read -r line <&9
            case "$line" in HTTP/*) exit 0;; *) exit 1;; esac
        ' _ "$host" "$port" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    collect_proxy_log "$log"
    return 1
}

stop_proxy() {
    local pid="${1:-${PROXY_HELPER_PID:-}}"
    [ -n "$pid" ] || return 0
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    if [ "${PROXY_HELPER_PID:-}" = "$pid" ]; then
        PROXY_HELPER_PID=""
    fi
}

collect_proxy_log() {
    local log="$1" prefix="${2:-  proxy| }"
    [ -f "$log" ] && sed "s/^/$prefix/" "$log" >&2
}
