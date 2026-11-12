#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
PROXY_WORKERS=2
PROXY_PARK_TIMEOUT=10s
MMDS_ROUTES_CONFIG="  routes:
    enabled: true"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
# Keep the standard 2GiB Sandbox capacity and the original 10s park budget.
build_image_template
for IDLE_TIMEOUT in 0 2; do
    if [ "$IDLE_TIMEOUT" = 2 ]; then
        REQ_ATTACH_MMDS=1
        REQ_MMDS_HEADER='{"routes":[{"path":"/work/hold","type":"hold-traffic"}]}'
    fi
    create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":600,\"autoPauseMemory\":false,\"idleTimeout\":$IDLE_TIMEOUT}"
    unset REQ_ATTACH_MMDS REQ_MMDS_HEADER
    # No business call: the initial complete observation must schedule Pause.
    wait_sandbox_state "$SID" paused 1200 || fail "never-used Sandbox did not auto-pause"
    EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
    export IDLE_TIMEOUT SID AK EXEC_TOKEN FORWARD_TOKEN DOMAIN PORT PROXY_PORT BIN WORK CASE_OUT CONDUCTOR_PID PROXY_MASTER_PID EXECUTE_RUN_ROOT PAUSE_BARRIER_TARGET PAUSE_BARRIER_REACHED PAUSE_BARRIER_RELEASE PAUSE_BARRIER_AFTER_CAPTURE
    python3 "$SCRIPT_DIR/idle_timeout.py"
    code=$(req DELETE "/sandboxes/$SID" "$AK")
    [ "$code" = 204 ] || fail "delete=$code"
    wait_sandbox_state "$SID" missing 120 || fail "idle sandbox cleanup"
done
echo 'PASS orchestrator.idle-timeout.sh'
