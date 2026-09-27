#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"

choose_redirect_node_id() {
    python3 - "http://127.0.0.1:$CONTROL_PORT/node-link/session" "${CONTROL_PORTS[@]}" <<'PY'
import json, struct, sys, urllib.request
url = sys.argv[1]
members = {"registry-%d" % idx: "127.0.0.1:" + port
           for idx, port in enumerate(sys.argv[2:], 1)}
for idx in range(1, 80):
    node_id = "real-node-redirect-%02d" % idx
    msg = {
        "type": "node_register",
        "node_register": {
            "node_id": node_id,
            "labels": {"pool": "probe"},
            "api_endpoint": "127.0.0.1:2",
            "data_endpoint": "127.0.0.1:1",
            "capacity": 1,
            "accept_redirect": True,
        },
    }
    payload = json.dumps(msg, separators=(",", ":")).encode()
    body = struct.pack("<I", len(payload)) + payload
    req = urllib.request.Request(url, data=body, method="PUT", headers={"Content-Type": "application/octet-stream"})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            hdr = resp.read(4)
            if len(hdr) != 4:
                continue
            n = struct.unpack("<I", hdr)[0]
            raw = resp.read(n)
            hello = json.loads(raw.decode())
    except Exception:
        continue
    redir = (((hello.get("hello") or {}).get("redirect") or {}).get("targets") or [])
    if redir:
        if (len(redir) != 1 or redir[0].get("member_id") not in members
                or members[redir[0]["member_id"]] != redir[0].get("endpoint")):
            raise SystemExit("redirect did not identify one fixture Registry owner")
        print(node_id, redir[0]["member_id"], redir[0]["endpoint"], sep="\t")
        raise SystemExit(0)
raise SystemExit(1)
PY
}

wait_redirect_placer() {
    python3 "$SCRIPT_DIR/placer_readiness.py" \
        --url "http://127.0.0.1:$PLACER_PORT" --group "$GROUP" --expected-node "$NODE_ID"
}

stop_redirect_process() {
    local pid="$1" name="$2" deadline=$((SECONDS + 20))
    kill "$pid" || fail "$name exited before the restart"
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            fail "$name did not stop for restart"
        fi
        sleep 0.05
    done
    wait "$pid" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" != "$pid" ] || PIDS[$i]=""
    done
}

restart_redirect_owner() {
    local index=$(( ${REDIRECT_OWNER#registry-} - 1 ))
    REDIRECT_CONNECTIONS="$(grep -c "node-link: node connected.*node=$NODE_ID " "$WORK/$REDIRECT_OWNER.log" || true)"
    [ "$REDIRECT_CONNECTIONS" -gt 0 ] || fail "redirected node never connected to $REDIRECT_OWNER"
    step "restarting actual redirect owner=$REDIRECT_OWNER pid=${REGISTRY_PIDS[$index]} endpoint=$REDIRECT_ENDPOINT"
    stop_redirect_process "${REGISTRY_PIDS[$index]}" "$REDIRECT_OWNER"
    "$BIN/cluster-ctl" registry --config "$WORK/$REDIRECT_OWNER.yaml" > >(tee -a "$WORK/$REDIRECT_OWNER.log" >&2) 2>&1 &
    REGISTRY_PIDS[$index]=$!
    PIDS+=("$!")
    wait_port "${CONTROL_PORTS[$index]}" "$REDIRECT_OWNER-restarted"

    stop_redirect_process "$ROUTER_PID" router
    "$BIN/cluster-ctl" router --config "$WORK/router.yaml" > >(tee -a "$WORK/router.log" >&2) 2>&1 &
    ROUTER_PID=$!
    PIDS+=("$!")
    wait_port "$ROUTER_PORT" router-restarted
    step "restarted redirect Registry owner and Router with empty Router cache"
}

wait_redirect_node_link() {
    local connections index=$(( ${REDIRECT_OWNER#registry-} - 1 )) deadline=$((SECONDS + 20))
    while [ "$SECONDS" -lt "$deadline" ]; do
        kill -0 "${REGISTRY_PIDS[$index]}" 2>/dev/null || fail "restarted Registry owner exited"
        kill -0 "$ROUTER_PID" 2>/dev/null || fail "restarted Router exited"
        connections="$(grep -c "node-link: node connected.*node=$NODE_ID " "$WORK/$REDIRECT_OWNER.log" || true)"
        if [ "$connections" -gt "$REDIRECT_CONNECTIONS" ]; then
            step "PASS: $REDIRECT_OWNER accepted a new node-link after owner restart"
            return 0
        fi
        sleep 0.25
    done
    fail "redirected node-link did not reconnect"
}

run_redirect_recovery() {
    restart_redirect_owner || fail "redirect owner restart failed"
    wait_redirect_node_link || fail "redirected node-link recovery failed"
    wait_redirect_placer || fail "placer did not converge to the redirected node"
}

run_redirect_flow() {
    local code sid envd_token forward_token create_response="$WORK/create.credentials"
    run_redirect_recovery
    step "creating one sandbox through the recovered redirected registry topology"
    code="$(create_sandbox "$create_response")"
    [ "$code" = "201" ] || { [ -s "$create_response" ] && cat "$create_response" >&2; fail "redirect create returned $code"; }
    assert_no_default_exec_token "$create_response" || fail "redirect create exposed a default exec token"
    IFS=$'\t' read -r sid envd_token forward_token < <(sandbox_route "$create_response") \
        || fail "redirect create returned an invalid e2b response"
    rm -f "$create_response"

    code="$(retry_data_by_sid "$sid" "$envd_token" || true)"
    [ "$code" = "204" ] || [ "$code" = "200" ] || fail "post-restart redirect data-plane /health returned $code"
    wait_cluster_traffic_stats "$sid" idle || fail "redirect traffic did not publish/converge to idle"
    step "PASS: post-restart redirected topology placed a real sandbox and routed envd health ($code)"
    unset envd_token

    code="$(router_req DELETE "/sandboxes/$sid" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "redirect delete returned $code"; }
    for _ in $(seq 1 80); do
        code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
        if [ "$code" = "200" ] && python3 - "$WORK/router-resp.body" "$sid" <<'PY_DELETE'
import json, sys
rows = json.load(open(sys.argv[1]))
sid = sys.argv[2]
raise SystemExit(0 if all(row.get("sandboxID") != sid for row in rows) else 1)
PY_DELETE
        then
            wait_node_sandbox_finalized "$sid" || fail "redirect delete returned before node-local finalization"
            step "PASS: redirected topology delete reached node-local finalization"
            return 0
        fi
        sleep 0.25
    done
    fail "redirected sandbox still appears after delete"
}

GROUP=/e2e/cluster/recovery
ROUTE_KEY=user1/recovery
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
write_registry_configs 3 1
write_placer_router_configs
start_cluster_control_plane
REDIRECT_SELECTION="$(choose_redirect_node_id)" || fail "could not find a node_id redirected away from bootstrap registry"
IFS=$'\t' read -r NODE_ID REDIRECT_OWNER REDIRECT_ENDPOINT <<< "$REDIRECT_SELECTION"
start_cluster_node "$NODE_ID"
redirected=0
        for _ in $(seq 1 80); do
            if grep -F "redirecting to node owner" "$WORK/cluster-node.log" | grep -Fq "node=$NODE_ID member=$REDIRECT_OWNER endpoint=$REDIRECT_ENDPOINT"; then
                step "observed node-link redirect node=$NODE_ID owner=$REDIRECT_OWNER endpoint=$REDIRECT_ENDPOINT"
                redirected=1
                break
            fi
            sleep 0.25
        done
        [ "$redirected" = 1 ] || fail "node-link redirect was not observed in cluster-node.log"

wait_cluster_node_key_pair
run_redirect_flow
echo "PASS orchestrator.cluster-recovery.sh"
