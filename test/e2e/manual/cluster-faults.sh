#!/usr/bin/env bash
# Manual real-product acceptance; uses the normal prepared cluster fixture.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"
GROUP=/e2e/cluster/faults
ROUTE_KEY=fault/bootstrap
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
RELAY_PORT="$(free_port)"
printf 'normal\n' > "$WORK/fault-mode"
"${FAULT_RELAY:?}" --listen "127.0.0.1:$RELAY_PORT" --target "http://127.0.0.1:$CONTROL_PORT/node-link/session" --mode-file "$WORK/fault-mode" --events "$CASE_OUT/relay-events.jsonl" >"$WORK/relay.log" 2>&1 &
PIDS+=("$!")
wait_port "$RELAY_PORT" fault-relay
NODE_ID=real-fault-node
ORIGINAL_CONTROL_PORT=$CONTROL_PORT
CONTROL_PORT=$RELAY_PORT
start_cluster_node "$NODE_ID"
CONTROL_PORT=$ORIGINAL_CONTROL_PORT
wait_cluster_node_key_pair
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --group "$GROUP" --registry-url "http://127.0.0.1:$CONTROL_PORT"
export FAULT_WORK="$WORK" FAULT_OUT="$CASE_OUT" FAULT_GROUP="$GROUP" FAULT_DOMAIN="$DOMAIN" FAULT_API_KEY="$CLUSTER_API_KEY"
export FAULT_ROUTER="http://127.0.0.1:$ROUTER_PORT" FAULT_NODE="http://127.0.0.1:$NODE_PORT" FAULT_SANDBOX_CTL="$BIN/sandbox-ctl"
export FAULT_REGISTRY_PID="${REGISTRY_PIDS[0]}" FAULT_CLUSTER_CTL="$BIN/run-cluster-ctl"
python3 "$SCRIPT_DIR/cluster_faults.py"
