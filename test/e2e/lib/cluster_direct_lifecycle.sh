#!/usr/bin/env bash
# The native no-Placer onboarding path: one Registry (SQLite) or three (PG),
# two real Nodes, explicit root installation and versioned Group CRUD.
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"
. "$SCRIPT_DIR/cluster_lifecycle_flow.sh"
GROUP=/e2e/cluster/direct
ROUTE_KEY=direct/node-1
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
start_store_zot_vswitch
make_ext4_templates
TEMPLATE_REF=""
registries=1
[ "$CLUSTER_STORAGE_BACKEND" != postgres ] || registries=3
write_registry_configs "$registries"
write_placer_router_configs
start_cluster_control_plane
NODE_ID=direct-node-1
start_cluster_node "$NODE_ID"
install_cluster_group
registry_args=()
for port in "${CONTROL_PORTS[@]}"; do registry_args+=(--registry-url "http://127.0.0.1:$port"); done
# Follow the documented minimum path using the only registered Node: a Group
# may own Builds before it has a default Sandbox template. No Placer or
# standalone prebuild supplies either the context or the resulting artifact.
BUILD_ACTION_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/cluster_direct_build.py" \
    --url "http://127.0.0.1:$ROUTER_PORT" --host "api.$DOMAIN" --group "$GROUP" \
    --db "$WORK/cl/node-ctl.db" --run-root "$WORK/cr" --base-root "$WORK/cl" \
    --socket "$WORK/cn.sock" --bin "$BIN" --switch "$SWITCH" --conductor-pid "$CLUSTER_CONDUCTOR_PID" \
    --source "$GUEST_REF" --evidence "$WORK/build-actions.json" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-id "$NODE_ID" "${registry_args[@]}" \
    >"$WORK/cluster-build.log" 2>&1 || fail "direct empty Group Build"
TEMPLATE_REF=$(json_field "$WORK/build-actions.json" canonical)
PRIMARY_WORK=$WORK
PRIMARY_PORT=$NODE_PORT
PRIMARY_ID=$NODE_ID
PRIMARY_PID=$CLUSTER_CONDUCTOR_PID
SECOND_RUNNER="sandbox-runner-direct-${BASHPID}@.service"
SECOND_BUILDER="sandbox-builder-direct-${BASHPID}@.service"
for unit in "$SECOND_RUNNER" "$SECOND_BUILDER"; do
    [ ! -e "$UNIT_DIR/$unit" ] || fail "second node fixture unit already exists"
    OURS+=("$UNIT_DIR/$unit")
done
cluster_extra_cleanup() {
    WORK=$PRIMARY_WORK
    stop_cluster_extra_node_units "$PRIMARY_WORK/second" "$SECOND_RUNNER" "$SECOND_BUILDER"
}
WORK=$PRIMARY_WORK/second
mkdir -p "$WORK/cr" "$WORK/cl"
cp "$PRIMARY_WORK/manifest.yaml" "$WORK/manifest.yaml"
NODE_ID=direct-node-2
start_cluster_node "$NODE_ID" "$SECOND_RUNNER" "$SECOND_BUILDER"
SECOND_PORT=$NODE_PORT
SECOND_ID=$NODE_ID
SECOND_PID=$CLUSTER_CONDUCTOR_PID
install_cluster_group
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" \
    --group "$GROUP" "${registry_args[@]}" --fence-file "$PRIMARY_WORK/second.fence"
WORK=$PRIMARY_WORK
NODE_PORT=$PRIMARY_PORT
NODE_ID=$PRIMARY_ID
CLUSTER_CONDUCTOR_PID=$PRIMARY_PID
# Exercise optimistic update, stale revision rejection, explicit delete and
# recreation through the real Node API without an importer supplying the Group.
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/cluster_group_crud.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --host "api.$DOMAIN" --group "$GROUP"
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" \
    --group "$GROUP" "${registry_args[@]}" --fence-file "$PRIMARY_WORK/primary.fence"
# Registry restart must reopen the same metadata DB and accept a new complete
# Node publication before either real lifecycle write.
for i in "${!REGISTRY_PIDS[@]}"; do
    previous=${REGISTRY_PIDS[$i]}
    kill "$previous"
    wait "$previous" || true
    for j in "${!PIDS[@]}"; do [ "${PIDS[$j]}" != "$previous" ] || PIDS[$j]=""; done
    "$BIN/run-cluster-ctl" registry --config "$WORK/registry-$((i+1)).yaml" >>"$WORK/registry-$((i+1)).log" 2>&1 &
    REGISTRY_PIDS[$i]=$!
    PIDS+=("$!")
    wait_port "${CONTROL_PORTS[$i]}" restarted-registry
done
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$SECOND_PORT" --node-host "api.$DOMAIN" --node-id "$SECOND_ID" \
    --group "$GROUP" "${registry_args[@]}" --after-fence "$(cat "$PRIMARY_WORK/second.fence")"
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" \
    --group "$GROUP" "${registry_args[@]}" --after-fence "$(cat "$PRIMARY_WORK/primary.fence")"
CLUSTER_SELECTED_NODE=$NODE_ID
run_cluster_flow
WORK=$PRIMARY_WORK/second
NODE_PORT=$SECOND_PORT
NODE_ID=$SECOND_ID
CLUSTER_CONDUCTOR_PID=$SECOND_PID
CLUSTER_SELECTED_NODE=$NODE_ID
ROUTE_KEY=direct/node-2
run_cluster_flow
WORK=$PRIMARY_WORK
NODE_PORT=$PRIMARY_PORT
NODE_ID=$PRIMARY_ID
CLUSTER_CONDUCTOR_PID=$PRIMARY_PID
printf 'PASS: %s no-Placer empty Group Build, two real Node lifecycles, Group CRUD and Registry restart\n' "$CLUSTER_STORAGE_BACKEND"
