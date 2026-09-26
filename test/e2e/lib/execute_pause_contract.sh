#!/usr/bin/env bash
# Offline fixture contract for cancellation-safe Pause orchestration.
set -euo pipefail
PAUSE_BARRIER_TARGET="$WORK/pause-barrier-target"
PAUSE_BARRIER_REACHED="$WORK/pause-barrier-reached"
PAUSE_BARRIER_RELEASE="$WORK/pause-barrier-release"

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
trap cleanup EXIT

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
path_id=""
if [ "\${1:-}" = "snapshot" ] && [ -f "$PAUSE_BARRIER_TARGET" ]; then
    previous=""
    for arg in "\$@"; do
        if [ "\$previous" = "--path-id" ]; then path_id="\$arg"; break; fi
        previous="\$arg"
    done
    if [ "\$path_id" = "\$(cat "$PAUSE_BARRIER_TARGET")" ]; then
        : > "$PAUSE_BARRIER_REACHED"
        while [ ! -e "$PAUSE_BARRIER_RELEASE" ]; do sleep 0.02; done
    fi
fi
if [ "\${1:-}" = "export" ]; then
    python3 - "$EXPORT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\n")
PY
fi
if [ "\${1:-}" = "run" ]; then
    python3 - "$RUN_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\n")
PY
fi
if [ "\${1:-}" = "run" ] && [ -f "$WORK/inject-sandbox-run" ]; then
    mode="\$(<"$WORK/inject-sandbox-run")"
    case "\$mode" in
        hold)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
            exit 44
            ;;
        park)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
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
path_id = value("--path-id")
run_root = value("--run-root")
ready_fd = int(value("--ready-fd"))
envd_path = os.path.join(run_root, path_id, "envd.sock")
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


printf '%s\n' "$SID" > "$PAUSE_BARRIER_TARGET"
rm -f "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
curl -sS --noproxy '*' -o "$WORK/pause-cancel.body" -w '%{http_code}' -X POST \
    -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
    --data '{}' "http://127.0.0.1:$PORT/sandboxes/$SID/pause" \
    >"$WORK/pause-cancel.code" 2>"$WORK/pause-cancel.stderr" &
PAUSE_CURL_PID=$!
PIDS+=("$PAUSE_CURL_PID")
for _ in $(seq 1 1500); do
    [ -e "$PAUSE_BARRIER_REACHED" ] && break
    if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
        wait "$PAUSE_CURL_PID" 2>/dev/null || true
        for i in "${!PIDS[@]}"; do
            [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
        done
        fail "Pause HTTP client completed before snapshot barrier"
    fi
    sleep 0.02
done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail "accepted Pause did not reach deterministic snapshot barrier"
if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
    wait "$PAUSE_CURL_PID" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
    done
    fail "Pause HTTP client completed before deterministic cancellation"
fi
if ! kill -TERM "$PAUSE_CURL_PID"; then
    if ! kill -0 "$PAUSE_CURL_PID" 2>/dev/null; then
        wait "$PAUSE_CURL_PID" 2>/dev/null || true
        for i in "${!PIDS[@]}"; do
            [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
        done
    fi
    fail "could not cancel Pause HTTP client"
fi
set +e
wait "$PAUSE_CURL_PID"
PAUSE_CURL_RC=$?
set -e
# The client has been reaped; cleanup must not signal a later user of its PID.
for i in "${!PIDS[@]}"; do
    [ "${PIDS[$i]}" = "$PAUSE_CURL_PID" ] && unset 'PIDS[i]'
done
[ "$PAUSE_CURL_RC" = 143 ] || fail "Pause HTTP client cancellation rc=$PAUSE_CURL_RC (want SIGTERM 143)"
: > "$PAUSE_BARRIER_RELEASE"
rm -f "$PAUSE_BARRIER_TARGET"

