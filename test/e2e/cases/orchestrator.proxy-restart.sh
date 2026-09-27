#!/usr/bin/env bash
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
install_prepared_proxy
PROXY_EXECUTABLE="$CUSTOM_PROXY_BIN"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
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
systemctl list-units --all --type=service --no-legend --no-pager "${RUNNER_PREFIX}*.service" \
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
systemctl list-units --all --type=service --no-legend --no-pager "${RUNNER_PREFIX}*.service" \
    | sort >"$WORK/runners.after-unavailable"
cmp -s "$WORK/runners.before-unavailable" "$WORK/runners.after-unavailable" \
    || fail "unavailable Create assigned a sandbox runner"
if ps -eo args= | grep -F 'cloud-hypervisor' | grep -F "$WORK/run" >/dev/null; then
    fail "unavailable Create started a VMM"
fi
echo "==> PASS: Proxy absent -> Create 503 with no durable/cache/run-dir/runner/VMM side effect"
restart_proxy_master_fresh "$WORK/proxy-after-unavailable.log"
echo "==> PASS: Proxy re-registered after admission failure"

create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
# WebSocket uses the same user-port route and traffic lifecycle as HTTP/CONNECT.
WS_GUEST_COMMAND=$(python3 "$SCRIPT_DIR/websocket_probe.py" guest-command)
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WS_GUEST_COMMAND" \
    >"$WORK/start-websocket.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-websocket.out" \
    || { cat "$WORK/start-websocket.out"; fail "start guest WebSocket fixture"; }
python3 "$SCRIPT_DIR/websocket_probe.py" probe --port "$PROXY_PORT" \
    --authority "8001-$SID.$DOMAIN" --token "$FORWARD_TOKEN" \
    || { dump_logs; fail "canonical WebSocket through Proxy"; }
    python3 "$SCRIPT_DIR/websocket_probe.py" probe --port "$PROXY_PORT" \
        --authority private.example --path "/private/sandboxes/$SID/8001/ws" \
        --header 'X-Private-Authorization: example-only' \
        || { dump_logs; fail "private ForwardAuthorized WebSocket through Proxy"; }
wait_traffic_stats "$SID" idle || { dump_logs; fail "WebSocket traffic did not return to idle"; }

# Restart the whole master with a live sandbox. The replacement starts from a
# fresh SHM table and must complete a full route sync before serving the route.
restart_proxy_master_fresh "$WORK/proxy-full-resync.log"
code=$(DP_MAX_TIME=30 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; dump_logs; fail "data request after Proxy full resync=$code"; }
wait_traffic_stats "$SID" idle || { dump_logs; fail "traffic stats missing after Proxy full resync"; }
echo "==> PASS: replacement Proxy master full-synced the live route before ingress"

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

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
stop_proxy_master
echo "PASS orchestrator.proxy-restart.sh"
