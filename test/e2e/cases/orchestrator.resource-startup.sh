#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
# ---- low-allocatable runner cgroup isolation regression --------------------
# Restore admission uses BudgetAtSnapshot and intentionally ignores startup
# headroom; image templates are required to validate cold-start policy.
# Build the same OCI input as an image template (no startCmd), then cold boot it
# with the complete 8 GiB / 256 MiB resource declaration from issue #152. These
# are independent Build resources sized for the node-default phase VM and toolchain.
code=$(req POST /v3/templates "$AK" '{"name":"exec-low-cgroup","cpuCount":2,"memoryMB":6144}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "low-cgroup register=$code"; }
LOW_TID=$(json_field "$WORK/resp.body" templateID)
LOW_BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$LOW_TID/builds/$LOW_BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "low-cgroup trigger=$code"; }
LOW_TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$LOW_TID/builds/$LOW_BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) LOW_TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "low-cgroup build error";;
    esac
    sleep 1
done
[ -n "$LOW_TEMPLATE" ] || fail "low-cgroup image build did not become ready"
case "$LOW_TEMPLATE" in e2b-img-*) : ;; *) fail "low-cgroup build produced $LOW_TEMPLATE (want e2b-img-...)";; esac

# A bare image lets the capacity<256MiB case validate a real KVM launch without
# paying envd's steady workload. It carries no resource patch, so the create
# request below proves inherited 256MiB settled-headroom normalization.
code=$(req POST /v3/templates "$AK" '{"name":"small-capacity","profile":"bare","cpuCount":2,"memoryMB":6144}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "small-capacity register=$code"; }
SMALL_TID=$(json_field "$WORK/resp.body" templateID)
SMALL_BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$SMALL_TID/builds/$SMALL_BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "small-capacity trigger=$code"; }
SMALL_TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$SMALL_TID/builds/$SMALL_BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) SMALL_TEMPLATE=$(json_field "$WORK/resp.body" templateID); break ;;
        error) cat "$WORK/resp.body"; fail "small-capacity build error" ;;
    esac
    sleep 1
done
[ -n "$SMALL_TEMPLATE" ] || fail "small-capacity bare image build did not become ready"
case "$SMALL_TEMPLATE" in bare-img-*) : ;; *) fail "small-capacity build produced $SMALL_TEMPLATE (want bare-img-...)" ;; esac

LOW_CREATE_BODY=$(python3 - "$LOW_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.resource": json.dumps({
            "capacity": {"cpu": 2, "memory": "8GiB"},
            "allocatable": {"cpu": 2, "memory": "256MiB"},
        }),
    },
}))
PY
)
case "$LOW_ALLOC_REPEATS" in
    ''|*[!0-9]*) fail "LOW_ALLOC_REPEATS must be a positive integer" ;;
esac
[ "$LOW_ALLOC_REPEATS" -gt 0 ] || fail "LOW_ALLOC_REPEATS must be positive"

run_low_allocatable_case() { # $1=iteration
    local iteration="$1" code LOW_START_MS LOW_SID LOW_TOKEN LOW_ELAPSED_MS
    local LOW_RUN_ID LOW_UNIT LOW_CG LOW_CTL_PID LOW_CTL_CG LOW_VMM_PROCS
    local LOW_JOURNAL VMM_MEMBERS_ERROR

    LOW_START_MS=$(date +%s%3N)
    code=$(req POST /sandboxes "$AK" "$LOW_CREATE_BODY")
    [ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "low-allocatable[$iteration] create=$code"; }
    LOW_SID=$(json_field "$WORK/resp.body" sandboxID)
    LOW_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
    code=$(DP_MAX_TIME=65 dp "49983-$LOW_SID" /health "$LOW_TOKEN" || true)
    { [ "$code" = "204" ] || [ "$code" = "200" ]; } || {
        journalctl KUASAR_SANDBOX_ID="$LOW_SID" --no-pager -n 100 2>/dev/null || true
        fail "low-allocatable[$iteration] health=$code"
    }
    LOW_ELAPSED_MS=$(( $(date +%s%3N) - LOW_START_MS ))
    [ "$LOW_ELAPSED_MS" -le 65000 ] || fail "low-allocatable[$iteration] startup took ${LOW_ELAPSED_MS}ms"
    wait_sandbox_state "$LOW_SID" running 20 || fail "low-allocatable[$iteration] sandbox not running"
    assert_resolved_resource_yaml "$WORK/run/sandboxes/$LOW_SID/$LOW_SID.yaml" 8GiB 8GiB - \
        || fail "low-allocatable[$iteration] static resolved resource YAML"
    LOW_RUN_ID=$(sandbox_run_id "$LOW_SID")
    [ -n "$LOW_RUN_ID" ] || fail "low-allocatable[$iteration] sandbox has no run_id"
    LOW_UNIT="${RUNNER_PREFIX}$LOW_RUN_ID.service"
    LOW_CG=$(systemctl show "$LOW_UNIT" -p ControlGroup --value)
    LOW_CTL_PID=$(systemctl show "$LOW_UNIT" -p MainPID --value)
    [ -n "$LOW_CG" ] && [ "$LOW_CG" != "/" ] || fail "runner ControlGroup is invalid: $LOW_CG"
    [ "$LOW_CTL_PID" -gt 1 ] || fail "runner MainPID is invalid: $LOW_CTL_PID"
    LOW_CTL_CG=$(awk -F: '$1 == "0" {print $3}' "/proc/$LOW_CTL_PID/cgroup")
    [ "$LOW_CTL_CG" = "$LOW_CG/ctl" ] || fail "sandbox-ctl cgroup=$LOW_CTL_CG, want $LOW_CG/ctl"
    LOW_VMM_PROCS="/sys/fs/cgroup$LOW_CG/vmm/cgroup.procs"
    if ! VMM_MEMBERS_ERROR=$(e2e_assert_vmm_cgroup_members /proc "$LOW_VMM_PROCS" 2>&1); then
        fail "low-allocatable[$iteration] $VMM_MEMBERS_ERROR"
    fi
    [ "$(<"/sys/fs/cgroup$LOW_CG/vmm/memory.max")" = "8623489024" ] \
        || fail "vmm memory.max does not reflect 8GiB capacity + 32MiB overhead"
    [ "$(<"/sys/fs/cgroup$LOW_CG/ctl/memory.high")" = "max" ] \
        || fail "ctl memory.high is constrained"

    LOW_JOURNAL="$WORK/low-allocatable-$iteration.journal"
    wait_for_mem_report_progress "$LOW_UNIT" "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] mem_report made no progress after a transient failure"

    local LOW_VM_INFO="$WORK/low-allocatable-$iteration.vm-info.json"
    curl --silent --show-error --fail --max-time 2 \
        --unix-socket "$WORK/run/sandboxes/$LOW_SID/ch.sock" \
        http://localhost/api/v1/vm.info >"$LOW_VM_INFO" \
        || fail "low-allocatable[$iteration] vm.info"
    python3 - "$LOW_VM_INFO" <<'PY' || fail "cold target=0 did not retain a CH balloon device"
import json, sys
info = json.load(open(sys.argv[1]))
balloon = info.get("config", {}).get("balloon")
if not isinstance(balloon, dict) or not isinstance(balloon.get("size"), int):
    raise SystemExit(1)
if not isinstance(info.get("memory_actual_size"), int):
    raise SystemExit(1)
PY

    local LOW_HIGH="max"
    for _ in $(seq 1 80); do
        LOW_HIGH=$(<"/sys/fs/cgroup$LOW_CG/vmm/memory.high")
        [ "$LOW_HIGH" != "max" ] && break
        sleep 0.25
    done
    [[ "$LOW_HIGH" =~ ^[0-9]+$ ]] \
        || fail "low-allocatable[$iteration] memory.high stayed deferred without a trusted report"
    [ "$LOW_HIGH" -gt 0 ] && [ "$LOW_HIGH" -le 8623489024 ] \
        || fail "low-allocatable[$iteration] memory.high=$LOW_HIGH outside (0,memory.max]"

    code=$(req DELETE "/sandboxes/$LOW_SID" "$AK"); [ "$code" = "204" ] || fail "delete low-allocatable[$iteration] sandbox=$code"
    for _ in $(seq 1 50); do
        [ ! -e "/sys/fs/cgroup$LOW_CG/vmm" ] && break
        sleep 0.1
    done
    [ ! -e "/sys/fs/cgroup$LOW_CG/vmm" ] || fail "vmm cgroup remained after StopUnit"
    journalctl -u "$LOW_UNIT" --no-pager >"$LOW_JOURNAL" 2>/dev/null || true
    # KillMode=control-group may terminate CH before sandbox-ctl reaches the API.
    grep -Fq '[sandbox-ctl] received terminated' "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run did not observe StopUnit"
    grep -Fq '[sandbox-ctl] CH exited code=' "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run did not observe CH exit"
    # Under deliberate memory.high pressure, CH's direct systemd signal can race
    # sandbox-ctl's shutdown API and make CH report a non-zero shutdown exit. The
    # lifecycle contract here is bounded exit and cgroup removal without SIGKILL.
    ! grep -Fq "CH didn't exit within" "$LOW_JOURNAL" \
        || fail "low-allocatable[$iteration] run escalated shutdown to SIGKILL"
    echo "==> PASS: 8GiB/256MiB runner[$iteration] ready in ${LOW_ELAPSED_MS}ms; mem_report progressed; ctl/vmm isolated and cleaned"
}

for LOW_ALLOC_ITERATION in $(seq 1 "$LOW_ALLOC_REPEATS"); do
    run_low_allocatable_case "$LOW_ALLOC_ITERATION"
done
echo "==> PASS: repeated 8GiB/256MiB startup $LOW_ALLOC_REPEATS times"

# Keep the low-headroom cgroup check in static mode so its high/balloon state is
# driven only by the sandbox-local loop. With no sandbox left from that check,
# restart against the same store and attach subsequent sandboxes to the
# controller for reservation stats and restore coverage below.
stop_orchestrator
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
echo "==> PASS: conductor restarted with the resource controller after static cgroup validation"

# ---- default managed resource policy: cold boot + exec + pause/resume -------
# LOW_TEMPLATE carries no request/template resource patch. This makes the real
# managed launch exercise conductor defaults rather than merely asserting an
# explicit 256MiB request.
DEFAULT_CREATE_BODY=$(python3 - "$LOW_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({"templateID": sys.argv[1], "timeout": 180}))
PY
)
code=$(req POST /sandboxes "$AK" "$DEFAULT_CREATE_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "default-policy create=$code"; }
DEFAULT_SID=$(json_field "$WORK/resp.body" sandboxID)
DEFAULT_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
code=$(DP_MAX_TIME=65 dp "49983-$DEFAULT_SID" /health "$DEFAULT_TOKEN" || true)
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; fail "default-policy cold health=$code"; }
wait_sandbox_state "$DEFAULT_SID" running 200 || fail "default-policy cold sandbox not running"
assert_native_usage "$DEFAULT_SID" live
assert_resolved_resource_yaml "$WORK/run/sandboxes/$DEFAULT_SID/$DEFAULT_SID.yaml" \
    2GiB 2GiB "$WORK/sandbox-resource.sock" || fail "default-policy dynamic resolved resource YAML"
wait_resource_stats "$DEFAULT_SID" || fail "default-policy resource stats missing"
assert_resource_lease "$DEFAULT_SID" 2147483648 268435456 2147483648 \
    || fail "default-policy lease did not match generated YAML"
DEFAULT_EXEC_TOKEN="$(issue_exec_session "$DEFAULT_SID" "$AK")" || fail "default-policy exec session"
exec_through_connect "$DEFAULT_SID" "$DEFAULT_EXEC_TOKEN" "DEFAULT_COLD_$RANDOM"

code=$(req POST "/sandboxes/$DEFAULT_SID/pause" "$AK")
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "default-policy pause=$code"; }
wait_sandbox_state "$DEFAULT_SID" paused 1200 || fail "default-policy sandbox did not pause"
code=$(req POST "/sandboxes/$DEFAULT_SID/connect" "$AK" '{"timeout":180}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "default-policy resume=$code"; }
default_resumed=""
for _ in $(seq 1 240); do
    code=$(DP_MAX_TIME=5 dp "49983-$DEFAULT_SID" /health "$DEFAULT_TOKEN" || true)
    case "$code" in 200|204) default_resumed=1; break ;; esac
    sleep 0.25
done
[ -n "$default_resumed" ] || fail "default-policy envd did not recover after resume"
wait_sandbox_state "$DEFAULT_SID" running 200 || fail "default-policy resumed sandbox not running"
assert_resolved_resource_yaml "$WORK/run/sandboxes/$DEFAULT_SID/$DEFAULT_SID.yaml" \
    2GiB 2GiB "$WORK/sandbox-resource.sock" || fail "default-policy restore resource YAML"
wait_resource_stats "$DEFAULT_SID" || fail "default-policy resource stats missing after resume"
assert_resource_lease "$DEFAULT_SID" 2147483648 268435456 2147483648 \
    || fail "default-policy restored lease did not match generated YAML"
exec_through_connect "$DEFAULT_SID" "$DEFAULT_EXEC_TOKEN" "DEFAULT_RESUME_$RANDOM"
code=$(req DELETE "/sandboxes/$DEFAULT_SID" "$AK")
[ "$code" = "204" ] || fail "default-policy delete=$code"
echo "==> PASS: default 2GiB capacity / 256MiB headroom cold boot, envd, exec, pause/resume, YAML and lease inventory"

SMALL_CREATE_BODY=$(python3 - "$SMALL_TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.resource": json.dumps({"capacity": {"memory": "192MiB"}}),
    },
}))
PY
)
code=$(req POST /sandboxes "$AK" "$SMALL_CREATE_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "small-capacity create=$code"; }
SMALL_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$SMALL_SID" running 600 || {
    journalctl KUASAR_SANDBOX_ID="$SMALL_SID" --no-pager -n 120 2>/dev/null || true
    fail "small-capacity bare sandbox did not reach running"
}
assert_resolved_resource_yaml "$WORK/run/sandboxes/$SMALL_SID/$SMALL_SID.yaml" \
    192MiB 192MiB "$WORK/sandbox-resource.sock" 192MiB - \
    || fail "capacity<256MiB did not normalize inherited headroom"
assert_resource_lease "$SMALL_SID" 201326592 201326592 201326592 \
    || fail "small-capacity lease did not match normalized YAML"
SMALL_EXEC_TOKEN="$(issue_exec_session "$SMALL_SID" "$AK")" || fail "small-capacity exec session"
exec_through_connect "$SMALL_SID" "$SMALL_EXEC_TOKEN" "SMALL_CAPACITY_$RANDOM"
code=$(req DELETE "/sandboxes/$SMALL_SID" "$AK")
[ "$code" = "204" ] || fail "small-capacity delete=$code"
echo "==> PASS: real bare KVM launch normalized inherited 256MiB headroom to 192MiB capacity"

echo "PASS orchestrator.resource-startup.sh"
