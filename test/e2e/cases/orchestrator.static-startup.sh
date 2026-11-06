#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
mkdir -p "$CASE_OUT"
BUILDER_CPU=2
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
. "$SCRIPT_DIR/guest_service.sh"
build_image_template e2b
IMAGE_TEMPLATE="$TEMPLATE"
case "$IMAGE_TEMPLATE" in e2b-img-*) ;; *) fail 'expected IMG template';; esac
start_explicit_guest() {
    local template="$1" label="$2"
    create_guest "$(python3 - "$template" <<'PY'
import json, sys
print(json.dumps(dict(templateID=sys.argv[1], timeout=600, metadata={"kuasar-sandbox.resource":json.dumps(dict(startup=dict(memory="1GiB")))})))
PY
)"
    EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
    exec_through_connect "$SID" "$EXEC_TOKEN" "$label"
    wait_sandbox_state "$SID" running 1200 || fail "$label startup"
    assert_resolved_resource_yaml "$EXECUTE_RUN_ROOT/sandboxes/$SID/$SID.yaml" 2GiB 1GiB -
}
delete_static_guest() {
    local code
    code="$(req DELETE "/sandboxes/$SID" "$AK")"
    [ "$code" = 204 ] || fail "delete=$code"
    wait_sandbox_state "$SID" missing 1200 || fail 'delete did not finish'
}
for mode in omitted disabled; do
    if [ "$mode" = disabled ]; then
        old_pid="$ORCH_PID"
        kill -TERM "$old_pid"; wait "$old_pid" || true
        for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" = "$old_pid" ] && unset 'PIDS[i]'; done
        ORCH_PID=""
        printf '\nresource_listen: {enabled: false}\n' >>"$WORK/config.yaml"
        start_conductor "$WORK/orch-disabled.log"
    fi
    [ ! -S "$WORK/sandbox-resource.sock" ] || fail 'static test enabled dynamic controller'
    start_explicit_guest "$IMAGE_TEMPLATE" "STATIC_IMG_$mode"
    SOURCE_SID="$SID"; SOURCE_EXEC="$EXEC_TOKEN"; SOURCE_ENVD="$ENVD_TOKEN"; SOURCE_FORWARD="$FORWARD_TOKEN"
    start_freeze_service
    counter="$FREEZE_COUNTER"
    "$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- /bin/sh -ceu 'echo static-startup >/home/user/startup-marker'
    code="$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":true,"checkpoint_drop_caches":false}')"
    [ "$code" = 204 ] || fail "memory pause=$code"
    wait_paused_cleanup "$SID" 1200 || fail 'memory pause cleanup'
    SNAPSHOT_TEMPLATE="$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --keep-source --to-template --socket "$WORK/node-ctl.socket")"
    case "$SNAPSHOT_TEMPLATE" in e2b-snp-*) ;; *) fail 'expected SNP export';; esac
    code="$(req POST "/sandboxes/$SID/connect" "$AK" '{"memory":true,"timeout":600}')"
    [ "$code" = 200 ] || fail "memory resume=$code"
    wait_sandbox_state "$SID" running 1200 || fail 'memory resume'
    freeze_service_probe || fail 'memory Resume lost guest process'
    [ "$FREEZE_COUNTER" -ge "$counter" ] || fail 'memory counter restarted'
    assert_resolved_resource_yaml "$EXECUTE_RUN_ROOT/sandboxes/$SID/$SID.yaml" 2GiB 1GiB -
    code="$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":true}')"
    [ "$code" = 204 ] || fail "second pause=$code"
    wait_paused_cleanup "$SID" 1200 || fail 'second pause cleanup'
    code="$(req POST "/sandboxes/$SID/connect" "$AK" '{"memory":false,"timeout":600}')"
    [ "$code" = 200 ] || fail "cold resume=$code"
    wait_sandbox_state "$SID" running 1200 || fail 'cold resume'
    exec_through_connect "$SID" "$SOURCE_EXEC" "STATIC_COLD_$mode"
    "$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- /bin/sh -ceu 'test "$(cat /home/user/startup-marker)" = static-startup'
    assert_resolved_resource_yaml "$EXECUTE_RUN_ROOT/sandboxes/$SID/$SID.yaml" 2GiB 1GiB -
    code="$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false}')"
    [ "$code" = 204 ] || fail "cold pause=$code"
    wait_paused_cleanup "$SID" 1200 || fail 'cold pause cleanup'
    SANDBOX_TEMPLATE="$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --keep-source --to-template --socket "$WORK/node-ctl.socket")"
    case "$SANDBOX_TEMPLATE" in e2b-sbx-*) ;; *) fail 'expected SBX export';; esac
    for template in "$SNAPSHOT_TEMPLATE" "$SANDBOX_TEMPLATE"; do
        start_explicit_guest "$template" "STATIC_ARTIFACT_$mode"
        delete_static_guest
    done
    SID="$SOURCE_SID"
    delete_static_guest
    echo "==> PASS: static $mode IMG/startup, memory and cold Resume, fresh SNP and SBX/startup"
done
echo 'PASS orchestrator.static-startup.sh'