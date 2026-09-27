#!/usr/bin/env bash
set -euo pipefail
MMDS_ROUTES_E2E=1
MMDS_SECRET_INITIAL_VALUE=MMDS_SECRET_INITIAL_GUEST_E2E
REQ_MMDS_HEADER='{"secrets":{"e2e_secret":"MMDS_SECRET_INITIAL_GUEST_E2E"},"routes":[{"path":"/e2e/static","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret":"e2e_secret","content_type":"application/x-kuasar-e2e-secret"},{"path":"/e2e/unresolved","type":"secret","secret":"e2e_unresolved"},{"path":"/e2e/service","type":"service","service":"e2e_service"}]}'
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/mmds_static_guest.sh"
. "$SCRIPT_DIR/mmds_secret_guest.sh"
PROXY_WORKERS=2
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
REQ_ATTACH_MMDS=1
create_guest
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
unset REQ_ATTACH_MMDS
mmds_restart_proxy() {
    restart_proxy_master_fresh "$WORK/proxy-mmds-restart.log"
    run_mmds_static_guest_get "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" resynced || fail "static route full resync"
}
MMDS_SECRET_AFTER_UPDATE_HOOK=mmds_restart_proxy
run_mmds_static_guest_get "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" mmds || fail "static MMDS route"
run_mmds_secret_standalone_e2e "$WORK/node-ctl.socket" "$SID" "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" mmds || fail "MMDS secret lifecycle"
run_mmds_service_standalone_e2e "$SID" "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" mmds || fail "conductor service registry"
for value in MMDS_SECRET_INITIAL_GUEST_E2E MMDS_SECRET_UPDATED_GUEST_E2E MMDS_SECRET_ROTATED_GUEST_E2E; do
    for artifact in "$WORK"/orch*.log "$WORK"/proxy*.log "$WORK"/mmds-*.out "$WORK"/mmds-service.requests "$WORK"/lib/node-ctl.db* "$WORK"/proxy-routes.shm; do
        [ -f "$artifact" ] || continue
        ! grep -a -F -q -- "$value" "$artifact" || fail "MMDS plaintext in $artifact"
    done
done
code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
stop_proxy_master
echo "PASS orchestrator.mmds-recovery.sh"
