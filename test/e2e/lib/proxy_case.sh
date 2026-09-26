#!/usr/bin/env bash
# Focused proxy process/config helpers. Cases own networking, products and cleanup.
set -euo pipefail

proxy_case_free_port() {
    python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'
}

proxy_case_wait_http() {
    local pid="$1" url="$2" host="$3" log="$4" deadline=$((SECONDS+30))
    while [ "$SECONDS" -lt "$deadline" ]; do
        curl -fsS --noproxy '*' -H "Host: $host" "$url" >/dev/null 2>&1 && return 0
        kill -0 "$pid" 2>/dev/null || { cat "$log" >&2; return 1; }
        sleep 0.2
    done
    return 1
}

proxy_case_wait_data() {
    local pid="$1" host="$2" port="$3" stats="$4" log="$5"
    wait_proxy_ready "$pid" "$host" "$port" "$stats" "$log"
}
