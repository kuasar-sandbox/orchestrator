#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template

# #426 real process-owner acceptance, retained after the unified-case migration.
runner_lifecycle_check() {
    python3 "$E2E_LIB/orchestrator/runner_lifecycle.py" "$1" "$WORK" "$2" "$RUNNER_PREFIX" "$3" "${@:4}"
}
run_runner_exit_case() { # $1=static|controller, $2=ch|runtime|parent, $3=template
    local mode="$1" role="$2" template="$3" body code sid token before started ready_ms killed cleanup_ms runtime_pid
    body=$(python3 - "$template" <<'PY'
import json,sys
print(json.dumps({"templateID":sys.argv[1],"timeout":180}))
PY
    )
    started=$(date +%s%3N)
    code=$(req POST /sandboxes "$AK" "$body")
    [ "$code" = 201 ] || fail "runner-exit $mode/$role Create=$code"
    sid=$(json_field "$WORK/resp.body" sandboxID)
    token=$(json_field "$WORK/resp.body" envdAccessToken)
    code=$(DP_MAX_TIME=120 dp "49983-$sid" /health "$token" || true)
    { [ "$code" = 200 ] || [ "$code" = 204 ]; } || fail "runner-exit $mode/$role guest health=$code"
    wait_sandbox_state "$sid" running 60 || fail "runner-exit did not reach running"
    ready_ms=$(( $(date +%s%3N) - started ))
    before="$WORK/runner-lifecycle-$mode-$role.json"
    runner_lifecycle_check snapshot "$sid" "$before" || fail "runner-exit process identities"
    if [ "$mode" = controller ]; then
        wait_resource_stats "$sid" || fail "runner-exit reservation unavailable"
        runner_lifecycle_check lease "$sid" "$before" || fail "runner-exit runtime-owned lease"
    fi
    if [ "$mode/$role" = controller/parent ]; then
        # Controller and conductor share this process. Restart with a live
        # guest and require StateSync/session recovery without re-execution.
        stop_orchestrator
        start_orchestrator "$WORK/orch-runner-lifecycle-restart.log"
        wait_mmds_listener
        wait_resource_stats "$sid" || fail "live StateSync did not restore reservation"
        runner_lifecycle_check same-run "$sid" "$before" || fail "conductor restart replaced a live run"
        runner_lifecycle_check lease "$sid" "$before" || fail "StateSync did not preserve runtime PID"
        runtime_pid=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["processes"]["runtime"]["pid"])' "$before")
        for _ in $(seq 1 100); do
            grep -Fq "state sync sid=$sid pid=$runtime_pid " "$WORK/orch-runner-lifecycle-restart.log" && break
            sleep 0.1
        done
        grep -Fq "state sync sid=$sid pid=$runtime_pid " "$WORK/orch-runner-lifecycle-restart.log" || fail "no successful runtime StateSync after restart"
        code=$(DP_MAX_TIME=30 dp "49983-$sid" /health "$token" || true)
        { [ "$code" = 200 ] || [ "$code" = 204 ]; } || fail "guest lost connectivity across conductor restart"
        echo "==> PASS: live conductor/controller restart preserved parent/runtime/CH and StateSync identity"
    fi
    killed=$(date +%s%3N)
    runner_lifecycle_check signal "$sid" "$before" --role "$role" || fail "runner-exit guarded signal"
    # No DELETE, explicit StopUnit or restart can help this cleanup path.
    wait_sandbox_state "$sid" dead 600 || fail "runner-exit $mode/$role did not automatically reach dead"
    assert_dead_no_ownership "$sid" || fail "runner-exit retained durable ownership"
    for _ in $(seq 1 100); do
        runner_lifecycle_check dead "$sid" "$before" 2>/dev/null && break
        sleep 0.1
    done
    runner_lifecycle_check dead "$sid" "$before" || fail "runner-exit retained processes/paths or lost result"
    cleanup_ms=$(( $(date +%s%3N) - killed ))
    for _ in $(seq 1 50); do
        code=$(DP_MAX_TIME=2 dp "49983-$sid" /health "$token" || true)
        [ "$code" = 404 ] && break
        sleep 0.1
    done
    [ "$code" = 404 ] || fail "dead runner route still reachable: $code"
    python3 - "$before" "$mode" "$role" "$ready_ms" "$cleanup_ms" <<'PY'
import json,sys
path,mode,role,ready,cleanup=sys.argv[1:]
record=json.load(open(path))
record.update(resource_mode=mode,killed_role=role,ready_ms=int(ready),cleanup_ms=int(cleanup))
with open(path,"w") as stream: json.dump(record,stream,indent=2)
print("runner-lifecycle measurement",json.dumps({k:record[k] for k in ("resource_mode","killed_role","ready_ms","cleanup_ms","parent_measurement")}))
PY
    echo "==> PASS: $mode $role SIGKILL auto-cleaned without DELETE or conductor restart"
}


# Exercise both static and controller-backed ownership using the already-built template.
stop_orchestrator
write_orchestrator_config unset static
start_orchestrator "$WORK/orch-runner-lifecycle-static.log"
wait_mmds_listener
run_runner_exit_case static ch "$TEMPLATE"
run_runner_exit_case static runtime "$TEMPLATE"
run_runner_exit_case static parent "$TEMPLATE"
stop_orchestrator
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch-runner-lifecycle-controller.log"
wait_mmds_listener
run_runner_exit_case controller ch "$TEMPLATE"
run_runner_exit_case controller runtime "$TEMPLATE"
run_runner_exit_case controller parent "$TEMPLATE"

# ---- async launch failure/kill gates --------------------------------------
# These deterministic injections surround the release-candidate binaries; they
# exercise the real conductor, sqlite store, run pool, systemd units, network,
# routes and cleanup without relying on timing races.
RUNNER_UNIT="$UNIT_DIR/${RUNNER_PREFIX}.service"
RUNNER_UNIT_SAVED="$WORK/sandbox-runner@.service.saved"
cp "$RUNNER_UNIT" "$RUNNER_UNIT_SAVED"
python3 - "$RUNNER_UNIT" <<'PY'
import sys
path = sys.argv[1]
lines = open(path, encoding="utf-8").read().splitlines()
lines = ["ExecStart=/bin/sleep 30" if line.startswith("ExecStart=") else line for line in lines]
open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
PY
systemctl daemon-reload
echo "==> inject runner WaitAssignment timeout; Create must still return durable 201"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runner-timeout create=$code (want 201)"; }
RUNNER_TIMEOUT_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNNER_TIMEOUT_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNNER_TIMEOUT_SID" dead 200 || fail "runner-timeout sandbox did not roll back to dead"
assert_dead_no_ownership "$RUNNER_TIMEOUT_SID" || fail "runner-timeout dead ownership"
[ -z "$(sandbox_run_id "$RUNNER_TIMEOUT_SID")" ] || fail "runner-timeout rollback retained run_id"
code=$(DP_MAX_TIME=10 dp "49983-$RUNNER_TIMEOUT_SID" /health "$RUNNER_TIMEOUT_TOKEN" || true)
[ "$code" = "404" ] || fail "runner-timeout route=$code (want prompt 404 after Delete)"
code=$(req DELETE "/sandboxes/$RUNNER_TIMEOUT_SID" "$AK"); [ "$code" = "204" ] || fail "delete runner-timeout sandbox=$code"
cp "$RUNNER_UNIT_SAVED" "$RUNNER_UNIT"
systemctl daemon-reload
stop_owned_units "$RUNNER_PREFIX"
echo "==> PASS: runner wait timeout rolled accepted fresh Create to dead + Delete with empty run_id"

printf '%s\n' runtime-wire-failure >"$WORK/inject-sandbox-run"
echo "==> inject malformed runtime readiness after runner commit"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runtime-failure create=$code (want 201)"; }
RUNTIME_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNTIME_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNTIME_FAILURE_SID" dead 120 || fail "runtime protocol failure did not roll back to dead"
assert_dead_no_ownership "$RUNTIME_FAILURE_SID" || fail "runtime protocol failure dead ownership"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$RUNTIME_FAILURE_SID" /health "$RUNTIME_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "runtime-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$RUNTIME_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete runtime-failure sandbox=$code"
echo "==> PASS: readiness protocol failure fenced the assigned runner and published Delete"

printf '%s\n' envd-init-failure >"$WORK/inject-sandbox-run"
echo "==> inject mandatory envd /init HTTP 500 after valid readiness wire"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "envd-failure create=$code (want 201)"; }
ENVD_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$ENVD_FAILURE_SID" dead 120 || fail "envd init failure did not roll back to dead"
assert_dead_no_ownership "$ENVD_FAILURE_SID" || fail "envd init failure dead ownership"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$ENVD_FAILURE_SID" /health "$ENVD_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "envd-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$ENVD_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete envd-failure sandbox=$code"
echo "==> PASS: mandatory envd /init failure rolled accepted Create to dead + Delete"

printf '%s\n' hold >"$WORK/inject-sandbox-run"
echo "==> hold assigned runtime to exercise starting SetTimeout/Pause/Kill"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "starting-kill create=$code (want 201)"; }
STARTING_KILL_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$STARTING_KILL_SID" starting 50 || fail "held sandbox was not starting"
STARTING_KILL_RUN_ID=""
for _ in $(seq 1 100); do
    STARTING_KILL_RUN_ID="$(sandbox_run_id "$STARTING_KILL_SID")"
    [ -n "$STARTING_KILL_RUN_ID" ] && break
    sleep 0.1
done
[ -n "$STARTING_KILL_RUN_ID" ] || fail "held starting sandbox never bound a runner"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/timeout" "$AK" '{"timeout":77}')
[ "$code" = "204" ] || fail "SetTimeout starting=$code (want 204)"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/pause" "$AK")
[ "$code" = "409" ] || fail "Pause starting=$code (want 409)"
code=$(req DELETE "/sandboxes/$STARTING_KILL_SID" "$AK")
[ "$code" = "204" ] || fail "Kill starting=$code (want 204)"
rm -f "$WORK/inject-sandbox-run"
wait_sandbox_state "$STARTING_KILL_SID" missing 50 || fail "Kill starting left a durable row"
if systemctl is-active --quiet "${RUNNER_PREFIX}$STARTING_KILL_RUN_ID.service"; then
    fail "Kill starting left runner $STARTING_KILL_RUN_ID active"
fi
echo "==> PASS: starting SetTimeout=204, Pause=409, Kill removed row/runner without resurrection"

# ---- create the sandbox (boots the microVM) -------------------------------
# Inject sandbox config via the X-Kuasar-Sandbox-Network header (docs/node.md §4.4): the guest
# hostname should become CFG_HOST, verified by `hostname` in the exec below.
CFG_HOST="e2e-cfg-host"
echo "==> POST /sandboxes (boot microVM from $TEMPLATE; inject hostname=$CFG_HOST via header)"
CREATE_BASE_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.restore": json.dumps({"prefetch": "memory"}),
    },
}))
PY
)
REQ_NET_HEADER="{\"hostname\":\"$CFG_HOST\"}"
REQ_ATTACH_MMDS="$MMDS_ROUTES_E2E"
code=$(req POST /sandboxes "$AK" "$CREATE_BASE_BODY")
unset REQ_NET_HEADER REQ_ATTACH_MMDS
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo
    echo "==> orchestrator log:"; sed 's/^/  orch| /' "$WORK/orch.log"
    SID=$(ls "$WORK/run/sandboxes" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201 durable acceptance)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
CREATE_RETURN_STATE="$(sandbox_state "$SID")"
case "$CREATE_RETURN_STATE" in starting|running) ;; *) fail "state immediately after 201=$CREATE_RETURN_STATE";; esac
echo "==> PASS: Create 201 durably accepted sandbox $SID (observed state=$CREATE_RETURN_STATE)"
python3 - "$WORK/lib/node-ctl.db" "$SID" "$WORK/run/sandboxes/$SID" "$WORK/lib/sandboxes/$SID" <<'PY' \
    || fail "durable Sandbox RunDir/BaseDir layout"
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select run_dir, base_dir from sandboxes where id=?", (sys.argv[2],)).fetchone()
want = (sys.argv[3], sys.argv[4])
if row != want:
    raise SystemExit(f"durable Sandbox directories={row!r}, want={want!r}")
PY
echo "==> issue data request immediately after Create; starting must park until running"
(
    code=$(DP_MAX_TIME=120 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
    printf '%s\n' "$code" >"$WORK/immediate-data.code"
) &
IMMEDIATE_DATA_PID=$!
wait_proxy_traffic_stats "$SID" parking \
    || { sed 's/^/  orch| /' "$WORK/orch.log"; fail "immediate post-Create request was not reported as parking"; }
immediate_data_status=0
wait "$IMMEDIATE_DATA_PID" || immediate_data_status=$?
IMMEDIATE_DATA_PID=""
[ "$immediate_data_status" = 0 ] || fail "immediate post-Create data client exited $immediate_data_status"
code=$(cat "$WORK/immediate-data.code")
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; fail "immediate post-Create data request=$code"; }
wait_sandbox_state "$SID" running 20 || fail "sandbox was not running after parked data request"
wait_proxy_traffic_stats "$SID" idle || fail "Proxy traffic did not converge to idle"
wait_resource_stats "$SID" || fail "controller resource stats were not reported"
assert_resolved_resource_yaml "$WORK/run/sandboxes/$SID/$SID.yaml" 2560MiB 2560MiB "$WORK/sandbox-resource.sock" \
    || fail "snapshot restore resource YAML did not preserve capacity and target-node defaults"
assert_resource_lease "$SID" 2684354560 268435456 2684354560 \
    || fail "snapshot restore lease did not match generated YAML"
[ -f "$WORK/lib/sandboxes/$SID/$SID.overlay.diff" ] \
    || fail "Sandbox writable diff is outside BaseDir or lost logical SandboxID filename"
if find "$WORK/run/sandboxes/$SID" -type f \( -name '*.img' -o -name '*.diff' -o -name '*.sandbox' -o -name '*.snapshot' \) -print -quit | grep -q .; then
    find "$WORK/run/sandboxes/$SID" -type f -print >&2
    fail "Sandbox RunDir contains a large image/diff/checkpoint artifact"
fi
echo "==> PASS: post-Create data request was reported parking through readiness, then idle; resource stats are live (code=$code)"

# ---- list -----------------------------------------------------------------
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list=$code"
grep -q "$SID" "$WORK/resp.body" || fail "sandbox $SID not listed"
echo "==> PASS: sandbox listed"

# ---- detail / info contract -----------------------------------------------
# This is the HTTP endpoint used by the E2B-compatible sandbox info/get-info
# path. Keep this in the real microVM E2E so the detail contract is checked
# against a running sandbox, not only through an in-process handler test.
code=$(req GET "/sandboxes/$SID" "$AK")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "sandbox detail=$code"; }
# These are the node defaults used by this E2E config. The API obtains them
# from node-ctl's a.res, not from the sandbox create request.
assert_sandbox_detail "$WORK/resp.body" "$SID" 2 2048 1024 \
    || { cat "$WORK/resp.body"; fail "sandbox detail contract"; }
echo "==> PASS: sandbox detail/info returned RFC3339 timestamps and resource fields"

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalizer retained row"
[ ! -e "$WORK/run/sandboxes/$SID" ] && [ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "delete retained owned directories"
[ -f "$WORK/lib/node-ctl.db" ] && [ -S "$WORK/node-ctl.socket" ] || fail "finalizer removed node files"
echo "PASS orchestrator.lifecycle.sh"
