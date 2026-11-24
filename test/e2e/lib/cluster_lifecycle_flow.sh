#!/usr/bin/env bash
# Shared real-VM lifecycle across all three metadata backends.
run_cluster_flow() {
    local code sid envd_token forward_token create_response="$WORK/create.credentials"
    step "creating sandbox explicitly through router"
    code="$(create_sandbox "$create_response")"
    if [ "$code" != "201" ]; then
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
    [ "$code" = "204" ] || { fail "pause returned $code"; }
    exec_argv_denied_through_cluster "$sid" "$exec_token"
    wait_cluster_traffic_stats "$sid" paused \
        || fail "condition-denied cluster exec changed paused state or traffic"
    local resume_mark="CLUSTER_NATIVE_EXEC_RESUME_$RANDOM"
    exec_through_cluster_connect "$sid" "$exec_token" "$resume_mark" &
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
    [ "$code" = "200" ] || { fail "static cluster resource stats=$code (want 200)"; }
    python3 - "$WORK/router-resp.body" <<'PY_RESOURCE'
import json, sys
stats = json.load(open(sys.argv[1]))
required = {"cpuCapacity", "cpuAllocatable", "memoryCapacity", "memoryHeadroom", "memoryUsed", "cpuSeconds", "timestampUnix"}
assert set(stats) == required, "cluster resource stats properties mismatch"
assert all(stats[key] > 0 for key in required), "cluster resource stats must be positive"
PY_RESOURCE
    step "PASS: cluster stats control path returned current VMM memory/CPU with the resource controller disabled"

    step "checking group-local sandbox list through router"
    code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
    [ "$code" = "200" ] || { fail "list returned $code"; }
    python3 - "$WORK/router-resp.body" "$sid" <<'PY' || fail "sandbox list did not contain created sandbox $sid"
import json, sys
rows = json.load(open(sys.argv[1]))
expected = sys.argv[2]
raise SystemExit(0 if any(row.get("sandboxID") == expected for row in rows) else 1)
PY
    step "listed sandbox: $sid"

    step "checking router control DELETE forwards to the real node"
    local node_sid
    node_sid="$(node_sandbox_id "$sid")" || fail "missing exact Node identity before Delete"
    code="$(router_req DELETE "/sandboxes/$sid" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { fail "delete returned $code"; }
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
            wait_node_sandbox_finalized "$node_sid" \
                || fail "sandbox delete route converged before node-local row/RunDir/BaseDir finalization"
            [ -f "$WORK/cl/node-ctl.db" ] || fail "sandbox finalizer removed node-level database"
            step "PASS: sandbox delete converged through route_link and node-local finalizer"
            return 0
        fi
        sleep 0.25
    done
    fail "deleted sandbox still appears in list"
}

