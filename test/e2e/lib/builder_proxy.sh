#!/usr/bin/env bash
start_builder_proxy() {
write_proxy_config "$WORK/proxy.yaml" \
    "$WORK/node-ctl.socket" "$WORK/run" "127.0.0.1:$PROXY_PORT" - \
    "$WORK/proxy-stats.sock" "$WORK/proxy-routes.shm" 1024 2 enforce 30s -
start_proxy "$BIN/node-ctl" "$WORK/proxy.yaml" "$WORK/proxy.log"
PIDS+=("$PROXY_HELPER_PID")
wait_proxy_ready "$PROXY_HELPER_PID" 127.0.0.1 "$PROXY_PORT" "$WORK/proxy-stats.sock" "$WORK/proxy.log" \
    || fail "Proxy did not become ready"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"
echo "==> conductor API :$PORT and Proxy data :$PROXY_PORT up; tenant allowlisted"
}
