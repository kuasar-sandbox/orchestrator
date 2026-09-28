#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"

run_cluster_flow() {
    local code sid envd_token forward_token create_response="$WORK/create.credentials"
    step "creating sandbox explicitly through router"
    code="$(create_sandbox "$create_response")"
    if [ "$code" != "201" ]; then
        [ -s "$create_response" ] && { step "sandbox create response:"; sed 's/^/  create| /' "$create_response" >&2; }
        fail "sandbox create returned $code"
    fi
    assert_no_default_exec_token "$create_response" || {
        rm -f "$create_response"
        fail "create response exposed a default exec token"
    }
    if ! IFS=$'\t' read -r sid envd_token forward_token < <(sandbox_route "$create_response"); then
        rm -f "$create_response"
        fail "sandbox create returned an invalid e2b response"
    fi
    rm -f "$create_response"
    step "created sandbox: $sid"

    step "issuing explicit exec capability through router (stable SID + group + route-key)"
    local exec_token native_mark
    exec_token="$(issue_cluster_exec_session "$sid" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")" \
        || fail "issue conditioned cluster exec capability"
    rm -f "$WORK/exec-session.secret"
    native_mark="CLUSTER_NATIVE_EXEC_$RANDOM"
    exec_through_cluster_connect "$sid" "$exec_token" "$native_mark"
    local node_yaml
    node_yaml="$(find "$WORK/cr/sandboxes" -mindepth 2 -maxdepth 2 -type f -name '*.yaml' | head -1)"
    [ -n "$node_yaml" ] || fail "cluster node generated no sandbox YAML"
    assert_cluster_resource_yaml "$node_yaml" || fail "cluster cold-create resource policy differs from standalone"
    step "capturing Sandbox E through router, then reusing the same KAT for a cold Wake"
    code="$(router_req POST "/sandboxes/$sid/pause" "$CLUSTER_API_KEY" "$ROUTE_KEY" '{"memory":false}')"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "pause returned $code"; }
    exec_argv_denied_through_cluster "$sid" "$exec_token"
    wait_cluster_traffic_stats "$sid" paused \
        || fail "condition-denied cluster exec changed paused state or traffic"
    local resume_mark="CLUSTER_NATIVE_EXEC_RESUME_$RANDOM"
    exec_through_cluster_connect "$sid" "$exec_token" "$resume_mark" 40 &
    CLUSTER_EXEC_PID=$!
    wait_cluster_traffic_stats "$sid" parking \
        || fail "paused cluster exec was not reported as parking by the final node"
    local cluster_exec_status=0
    wait "$CLUSTER_EXEC_PID" || cluster_exec_status=$?
    CLUSTER_EXEC_PID=""
    [ "$cluster_exec_status" = 0 ] || fail "paused cluster exec exited $cluster_exec_status"
    wait_cluster_traffic_stats "$sid" idle \
        || fail "cluster exec traffic did not converge to idle"
    assert_cluster_resource_yaml "$node_yaml" || fail "cluster restore resource policy differs from cold create"

    local websocket_command
    websocket_command=$(python3 "$SCRIPT_DIR/websocket_probe.py" guest-command)
    timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$ROUTER_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header 'E2b-Sandbox-Service: exec' \
        --proxy-header "X-Access-Token: $exec_token" \
        --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
        --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -- /bin/sh -c "$websocket_command" >"$WORK/start-websocket.out" 2>&1 \
        || fail "start cluster guest WebSocket fixture"
    python3 "$SCRIPT_DIR/websocket_probe.py" probe --port "$ROUTER_PORT" \
        --authority "8001-$sid.$DOMAIN" --token "$forward_token" \
        --header "X-Kuasar-Sandbox-Group: $GROUP" \
        --header "X-Kuasar-Route-Key: $ROUTE_KEY" \
        || fail "WebSocket through cluster router and node CONNECT relay"
    wait_cluster_traffic_stats "$sid" idle || fail "cluster WebSocket traffic did not return to idle"
    unset exec_token
    step "PASS: cluster Pause(memory=false) produced wakeable E; stable-SID exec cold-resumed it with traffic parking -> idle"

    step "checking SID-addressed envd data request with X-Access-Token"
    code="$(retry_data_by_sid "$sid" "$envd_token" || true)"
    unset envd_token
    [ "$code" = "204" ] || [ "$code" = "200" ] || fail "data-plane /health returned $code"
    wait_cluster_traffic_stats "$sid" idle || fail "cluster data traffic did not remain single-counted and idle"
    step "PASS: explicit create -> SID route -> real envd /health ($code)"

    code="$(router_req GET "/sandboxes/$sid/stats/resource" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "200" ] || { cat "$WORK/router-resp.body"; fail "static cluster resource stats=$code (want 200)"; }
    python3 - "$WORK/router-resp.body" <<'PY_RESOURCE'
import json, sys
stats = json.load(open(sys.argv[1]))
required = {"cpuCapacity", "cpuAllocatable", "memoryCapacity", "memoryHeadroom", "memoryUsed", "cpuSeconds", "timestampUnix"}
assert set(stats) == required, stats
assert all(stats[key] > 0 for key in required), stats
PY_RESOURCE
    step "PASS: cluster stats control path returned current VMM memory/CPU with the resource controller disabled"

    step "checking group-local sandbox list through router"
    code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
    [ "$code" = "200" ] || { cat "$WORK/router-resp.body"; fail "list returned $code"; }
    python3 - "$WORK/router-resp.body" "$sid" <<'PY' || fail "sandbox list did not contain created sandbox $sid"
import json, sys
rows = json.load(open(sys.argv[1]))
expected = sys.argv[2]
raise SystemExit(0 if any(row.get("sandboxID") == expected for row in rows) else 1)
PY
    step "listed sandbox: $sid"

    step "checking router control DELETE forwards to the real node"
    code="$(router_req DELETE "/sandboxes/$sid" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "delete returned $code"; }
    for _ in $(seq 1 80); do
        code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
        [ "$code" = "200" ] || { sleep 0.25; continue; }
        if python3 - "$WORK/router-resp.body" "$sid" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
sid = sys.argv[2]
raise SystemExit(0 if all(r.get("sandboxID") != sid for r in rows) else 1)
PY
        then
            wait_node_sandbox_finalized "$sid" \
                || fail "sandbox delete route converged before node-local row/RunDir/BaseDir finalization"
            [ -f "$WORK/cl/node-ctl.db" ] || fail "sandbox finalizer removed node-level database"
            step "PASS: sandbox delete converged through route_link and node-local finalizer"
            return 0
        fi
        sleep 0.25
    done
    fail "deleted sandbox still appears in list"
}

GROUP=/e2e/cluster/lifecycle
ROUTE_KEY=user1/lifecycle
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record
write_registry_configs 1
write_placer_router_configs
start_cluster_control_plane
NODE_ID=real-node-1
start_cluster_node "$NODE_ID"
wait_cluster_node_key_pair
PLACER_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/placer_readiness.py" \
    --url "http://127.0.0.1:$PLACER_PORT" --group "$GROUP" --expected-node "$NODE_ID" \
    --route-key "$ROUTE_KEY" --registry-url "http://127.0.0.1:$CONTROL_PORT"
run_cluster_flow
# Prove Build ownership routing through the group, then recover retained status
# through an empty Router cache. New writes wait for the current placer view.
PYTHONPATH="$SCRIPT_DIR" BUILD_ACTION_API_KEY="$CLUSTER_API_KEY" python3 - \
    --url "http://127.0.0.1:$ROUTER_PORT" --host "api.$DOMAIN" --group "$GROUP" \
    --db "$WORK/cl/node-ctl.db" --run-root "$WORK/cr" --base-root "$WORK/cl" \
    --socket "$WORK/cn.sock" --bin "$BIN" --switch "$SWITCH" --conductor-pid "$CLUSTER_CONDUCTOR_PID" \
    --source "$TEMPLATE_REF" --evidence "$WORK/build-actions.json" \
    --restart-request "$WORK/restart-request" --restart-ready "$WORK/restart-ready" \
    --placer-url "http://127.0.0.1:$PLACER_PORT" --expected-node "$NODE_ID" \
    --registry-url "http://127.0.0.1:$CONTROL_PORT" <<'PY_CLUSTER_BUILD' &
from build_client import *
args = configure()
fds_before = conductor_fds()
tid, bid = register("cluster-hang")
marker = "CLUSTER_BUILD_HANG_" + bid
trigger(tid, bid, "echo " + marker + "; while :; do sleep 1; done")

def live_hang():
    current = row(bid)
    assert current and current["status"] != "error", current
    log = command("journalctl", "KUASAR_BUILD_ID=" + bid, "--no-pager", "--output=cat")
    return current if current["run_id"] and current["phase_sandbox_id"] and marker in log.splitlines() else None

owned = wait_for("routed Build guest RUN", live_hang)
unit = "sandbox-builder@" + owned["run_id"] + ".service"
cg, processes = snapshot_processes(unit)
assert owned["execution_claimed"] == 1 and owned["runtime_vswitch_port"], owned
admin = json.loads(command(str(Path(args.bin)/"node-ctl"), "builder", "status", "--socket", args.socket))
matching = [b for b in admin["builds"] if b["buildID"] == bid]
assert len(matching) == 1 and matching[0]["templateID"] == tid and matching[0]["executionClaimed"], admin
wt, wb = register("cluster-waiter")
trigger(wt, wb, "echo CLUSTER_BUILD_NEXT_" + wb)
waiting = row(wb)
assert waiting["status"] == "waiting" and waiting["execution_claimed"] == 0, waiting
require("DELETE", "/templates/" + tid, 409)
require("DELETE", "/templates/" + tid + "?cancel=true", 409, header='{"cancel":false}')
assert row(bid)["cancel_requested_unix"] == 0 and not unit_empty(unit)
started = time.monotonic()
require("POST", f"/templates/{tid}/builds/{bid}/cancel", 202)
cancelled = wait_for("routed cancellation finishes", lambda: check_terminal(tid, bid, "error"), 60)
assert cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
require("POST", f"/templates/{tid}/builds/{bid}/cancel", 204)
assert row(bid)["execution_claimed"] == 0
assert_reclaimed(bid, unit, cg, processes)
elapsed = time.monotonic() - started
assert elapsed < 60, "cancellation waited for the normal deadline"
ready = wait_for("queued routed Build obtains capacity", lambda: check_terminal(wt, wb, "ready"))
assert not ready["cancelRequested"] and not ready["deleteRequested"], ready
assert row(wb)["execution_claimed"] == 0
log = command("journalctl", "KUASAR_BUILD_ID=" + wb, "--no-pager", "--output=cat")
assert "CLUSTER_BUILD_NEXT_" + wb in log.splitlines(), log
slots = json.loads(command(str(Path(args.bin)/"connector-ctl"), "vswitch", "show", "slots", args.switch))
released = [s for s in slots if str(s["port"]) == owned["runtime_vswitch_port"]]
assert len(released) == 1 and not released[0]["allocated"], released
canonical = ready["templateID"]
assert canonical.startswith("e2b-img-"), canonical
Path(args.restart_request).touch()
wait_for("fixture Router/Registry restart", lambda: Path(args.restart_ready).exists(), 60)
value, _ = require("GET", status_path(wt, wb), 200)
assert value["status"] == "ready" and value["templateID"] == canonical, value
args.post_restart = True

pt, pb = register("cluster-same-artifact-peer")
require("POST", f"/v2/templates/{pt}/builds/{pb}", 202, {"fromTemplate": canonical})
peer = wait_for("same-artifact peer ready", lambda: check_terminal(pt, pb, "ready"))
assert peer["templateID"] == canonical
peer_before = row(pb)
assert pt != wt and pb != wb and peer_before["persist_id"] == row(wb)["persist_id"] == canonical
assert peer_before["execution_claimed"] == 0
require("DELETE", "/templates/" + canonical, 400)
require("DELETE", "/templates/" + wt, 204)
assert row(wb) is None
peer, _ = require("GET", status_path(pt, pb), 200)
assert peer["status"] == "ready" and peer["templateID"] == canonical
assert row(pb) == peer_before
rt, rb = register("cluster-canonical-reuse")
require("POST", f"/v2/templates/{rt}/builds/{rb}", 202, {"fromTemplate": canonical})
reused = wait_for("canonical survives transient deletion", lambda: check_terminal(rt, rb, "ready"))
assert reused["templateID"] == canonical
for transient, build_id in ((rt, rb), (pt, pb), (tid, bid)):
    assert row(build_id) is not None
    require("DELETE", "/templates/" + transient, 204)
    assert row(build_id) is None
fds_after = conductor_fds()
for build_id in (bid, wb, pb, rb):
    for root in (args.run_root, args.base_root):
        directory = str(Path(root)/"builds"/build_id)
        assert not any(fd == directory or fd.startswith(directory + "/") for fd in fds_after)
Path(args.evidence).write_text(json.dumps({"cancel_seconds": elapsed, "canonical": canonical,
    "retained_status_after_restart": True, "peer_preserved": True,
    "conductor_fds_before": len(fds_before), "conductor_fds_after": len(fds_after),
    "build_directory_fds_retained": 0}, indent=2) + "\n")
PY_CLUSTER_BUILD
    ACTION_TEST_PID=$!
    PIDS+=("$ACTION_TEST_PID")
    while kill -0 "$ACTION_TEST_PID" 2>/dev/null; do
        if [ -e "$WORK/restart-request" ] && [ ! -e "$WORK/restart-ready" ]; then
            kill "$ROUTER_PID"
            wait "$ROUTER_PID" || true
            for i in "${!PIDS[@]}"; do
                [ "${PIDS[$i]}" != "$ROUTER_PID" ] || PIDS[$i]=""
            done
            "$BIN/cluster-ctl" router --config "$WORK/router.yaml" >>"$WORK/router.log" 2>&1 &
            ROUTER_PID=$!
            PIDS+=("$ROUTER_PID")
            wait_port "$ROUTER_PORT" router-restarted
            touch "$WORK/restart-ready"
            step "restarted router; testing retained transient routing from empty caches"
        fi
        sleep 0.1
    done
    action_status=0
    wait "$ACTION_TEST_PID" || action_status=$?
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" != "$ACTION_TEST_PID" ] || PIDS[$i]=""
    done
    [ "$action_status" = 0 ] || fail "Build action capacity/restart acceptance"
echo "PASS orchestrator.cluster-lifecycle.sh"
