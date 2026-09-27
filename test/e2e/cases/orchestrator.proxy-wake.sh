#!/usr/bin/env bash
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
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

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
stop_proxy_master
echo "PASS orchestrator.proxy-wake.sh"
