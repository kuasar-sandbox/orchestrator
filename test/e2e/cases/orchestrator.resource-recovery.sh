#!/usr/bin/env bash
set -euo pipefail
: "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
. "$E2E_LIB/orchestrator/resource.sh"
resource_init
trap resource_cleanup EXIT

resource_write_controller "$WORK/controller.yaml" false
resource_start_controller "$WORK/controller.yaml"
sid=resource-recovery
resource_setup_sandbox "$sid"
resource_write_sandbox "$sid" 128 512 256 false
resource_run_sandbox "$sid"
sandbox_pid="$RESOURCE_LAST_PID"
resource_wait_state "$sid" "$sandbox_pid" settled 20

for _ in $(seq 1 40); do
    grep -qE 'CH started pid=[0-9]+' "$WORK/$sid.log" && break
    kill -0 "$sandbox_pid" 2>/dev/null || resource_fail "sandbox exited before CH startup"
    sleep 0.25
done
ch_pid="$(sed -nE 's/.*CH started pid=([0-9]+).*/\1/p' "$WORK/$sid.log" | tail -1)"
[[ "$ch_pid" =~ ^[0-9]+$ ]] || resource_fail "could not identify Cloud Hypervisor"
sandbox_started="$(awk '{print $22}' "/proc/$sandbox_pid/stat")"
ch_started="$(awk '{print $22}' "/proc/$ch_pid/stat")"

kill -KILL "$RESOURCE_DAEMON_PID"
wait "$RESOURCE_DAEMON_PID" 2>/dev/null || true
RESOURCE_DAEMON_PID=""
printf '{corrupt-state\n' >"$WORK/state.json"
resource_start_controller "$WORK/controller.yaml"
resource_wait_state "$sid" "$sandbox_pid" synced 20

kill -0 "$sandbox_pid" || resource_fail "sandbox-ctl exited across controller restart"
kill -0 "$ch_pid" || resource_fail "Cloud Hypervisor exited across controller restart"
[ "$(awk '{print $22}' "/proc/$sandbox_pid/stat")" = "$sandbox_started" ]     || resource_fail "sandbox-ctl identity changed across controller restart"
[ "$(awk '{print $22}' "/proc/$ch_pid/stat")" = "$ch_started" ]     || resource_fail "Cloud Hypervisor identity changed across controller restart"

timeout -k 5s 20 "$BIN/sandbox-ctl" exec --sandbox-id "$sid" --run-root "$WORK/run" -- /bin/true     >"$WORK/restart-exec.log" 2>&1     || { cat "$WORK/restart-exec.log" >&2; resource_fail "guest exec failed after controller restart"; }
grep -qx '{corrupt-state' "$WORK/state.json"     || resource_fail "deprecated state_path was consumed or replaced"
[ ! -e "$WORK/audit.log" ] || resource_fail "removed audit.log was recreated"

echo "PASS orchestrator.resource-recovery.sh"
