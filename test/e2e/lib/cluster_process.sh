#!/usr/bin/env bash
# Process, configuration and request primitives for real cluster cases.
start_store_zot_vswitch() {
    STORE_PORT="$(free_port)"
    cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/s, verify_content_key: true }
EOF
    "$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { fail "store init"; }
    "$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
    PIDS+=("$!")
    wait_port "$STORE_PORT" store-ctl

    ZOT_PORT="$(free_port)"
    cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/z/d", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
    "$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
    PIDS+=("$!")
    wait_port "$ZOT_PORT" zot

    REF="127.0.0.1:$ZOT_PORT/e2e/cluster-real:v1"
    docker image inspect "$ORCHESTRATOR_BASE_IMAGE" >/dev/null || fail "prepared base image is missing"
    docker tag "$ORCHESTRATOR_BASE_IMAGE" "$REF"
    TAGS+=("$REF")
    docker push "$REF" >"$WORK/push.log" 2>&1 || fail "docker push e2e image"
    step "store-ctl + zot up; seeded $REF"

    MGMT_VIP="169.254.169.254"
    if "$BIN/connector-ctl" vswitch status "$SWITCH" >/dev/null 2>&1; then fail "fixture switch already exists: $SWITCH"; fi
    ip netns add "$SW_NETNS"
    NETNS_CREATED=1
    SW_STARTED=1
    step "starting vswitch $SWITCH (netns=$SW_NETNS)"
    "$BIN/connector-ctl" vswitch start "$SWITCH" \
        --netns="$SW_NETNS" \
        --ports=64 \
        --mac-addr=02:00:00:00:00:01 \
        --floating-ip-base=100.100.96.0 \
        --mode=tap \
        --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 >"$WORK/vswitch-start.log" 2>&1 || fail "vswitch start"
    SW_STARTED=1
    ip addr replace "$MGMT_VIP/32" dev "${SWITCH}m0" \
        || fail "configure management VIP on ${SWITCH}m0"
    GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/cluster-real:v1"
    step "vswitch up; build sandboxes pull $GUEST_REF"

    cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF
}

make_ext4_templates() {
    MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
    [ -x "$MKFS_EXT4" ] || fail "mkfs.ext4 not found"
    OVL="$WORK/overlay-1G.ext4"
    truncate -s 1G "$OVL"
    "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL" >"$WORK/mkfs-overlay.log" 2>&1 || { fail "mkfs overlay"; }
    BLD="$WORK/builder-2G.ext4"
    truncate -s 2G "$BLD"
    "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$BLD" >"$WORK/mkfs-builder.log" 2>&1 || { fail "mkfs builder"; }
}

build_template_with_standalone_node() {
    BUILD_PORT="$(free_port)"
    cat > "$WORK/build-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$BUILD_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/br, base_root: $WORK/bl, config_socket: $WORK/bn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
checkpoint: { mode: local }
EOF
    step "starting temporary standalone node-ctl for template build (:${BUILD_PORT})"
    "$BIN/node-ctl" conductor serve --config "$WORK/build-node.yaml" >"$WORK/build-node.log" 2>&1 &
    local build_pid="$!"
    PIDS+=("$build_pid")
    wait_api_health "$BUILD_PORT" "temporary node-ctl"
    "$BIN/node-ctl" manifest-key add --socket "$WORK/bn.sock" "$MANIFEST_KEY" >/dev/null || fail "temporary manifest-key add"

    local code tid bid status run_id
    # Build memory supplies admission and the A/B sandbox specification.
    # Target Sandbox resources resolve independently from node policy;
    # parent Builder services/slices add no CPU or memory policy.
    code="$(node_req "$BUILD_PORT" POST /v3/templates "$BUILD_API_KEY" '{"name":"cluster-real-tmpl","cpuCount":2,"memoryMB":6144}')"
    [ "$code" = "202" ] || { fail "template register returned $code"; }
    tid="$(json_field "$WORK/node-resp.body" templateID)"
    bid="$(json_field "$WORK/node-resp.body" buildID)"
    step "building template through temporary node: templateID=$tid buildID=$bid"
    code="$(node_req "$BUILD_PORT" POST "/v2/templates/$tid/builds/$bid" "$BUILD_API_KEY" "{\"fromImage\":\"$GUEST_REF\"}")"
    [ "$code" = "202" ] || { fail "template build trigger returned $code"; }
    TEMPLATE_REF=""
    for _ in $(seq 1 180); do
        node_req "$BUILD_PORT" GET "/templates/$tid/builds/$bid/status" "$BUILD_API_KEY" >/dev/null
        status="$(json_field "$WORK/node-resp.body" status)"
        case "$status" in
            ready)
                TEMPLATE_REF="$(json_field "$WORK/node-resp.body" templateID)"
                break
                ;;
            error)
                fail "template build error"
                ;;
        esac
        sleep 1
    done
    [ -n "$TEMPLATE_REF" ] || fail "template build did not become ready"
    step "built reusable template"

    kill "$build_pid" 2>/dev/null || true
    wait "$build_pid" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" != "$build_pid" ] || PIDS[$i]=""
    done
    stop_build_fixture_units "$WORK" 2>/dev/null || true
}

write_group_record() {
    # Per-request ports cover both envd (49983) and the WebSocket user port (8001).
    cat > "$WORK/g/group.json" <<EOF
{
  "group": "$GROUP",
  "manifest_key": { "type": "inline", "value": "$MANIFEST_KEY" },
  "api_secret": { "type": "inline", "value": "$API_SECRET" },
  "template_ref": "$TEMPLATE_REF",
  "target_port": 0,
  "node_selectors": [{ "pool": "real" }]
}
EOF
    step "wrote placer file group source for $GROUP"
}

cluster_fdb_cli() {
    env -u LD_PRELOAD -u LD_LIBRARY_PATH -u LD_AUDIT \
        "$CLUSTER_FDB_RUNTIME/ld.so" --inhibit-cache --library-path "$CLUSTER_FDB_RUNTIME" \
        "$FDB_CLI_TEST_BIN" -C "$CLUSTER_FDB_FILE" "$@"
}

start_cluster_fdb() {
    if [ -n "${CLUSTER_FDB_PID:-}" ]; then
        kill -0 "$CLUSTER_FDB_PID" 2>/dev/null || fail "case FoundationDB server exited"
        return
    fi
    [ -n "${FDB_SERVER_TEST_BIN:-}" ] && [ -x "$FDB_SERVER_TEST_BIN" ] \
        || fail "prepared FoundationDB server helper is missing"
    [ -n "${FDB_CLI_TEST_BIN:-}" ] && [ -x "$FDB_CLI_TEST_BIN" ] \
        || fail "prepared FoundationDB CLI helper is missing"
    CLUSTER_FDB_RUNTIME="$(dirname "$BIN")/lib/cluster-ctl"
    [ -x "$CLUSTER_FDB_RUNTIME/ld.so" ] && [ -r "$CLUSTER_FDB_RUNTIME/libfdb_c.so" ] \
        || fail "prepared cluster native runtime is missing"
    local port identity address
    port="$(free_port)"
    identity="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
    address="127.0.0.1:$port"
    CLUSTER_FDB_FILE="$WORK/fdb/fdb.cluster"
    CLUSTER_FDB_PREFIX="/orchestrator-e2e/$identity/"
    mkdir -p "$WORK/fdb/data" "$WORK/fdb/logs"
    printf 'test:%s@%s\n' "$identity" "$address" > "$CLUSTER_FDB_FILE"
    env -u LD_PRELOAD -u LD_LIBRARY_PATH -u LD_AUDIT \
        "$CLUSTER_FDB_RUNTIME/ld.so" --inhibit-cache --library-path "$CLUSTER_FDB_RUNTIME" \
        "$FDB_SERVER_TEST_BIN" -C "$CLUSTER_FDB_FILE" -p "$address" \
        -d "$WORK/fdb/data" -L "$WORK/fdb/logs" \
        --locality-machineid test-machine --locality-zoneid test-zone >"$WORK/fdb-server.log" 2>&1 &
    CLUSTER_FDB_PID=$!
    PIDS+=("$CLUSTER_FDB_PID")

    # A new SSD database needs time to recruit its roles. These are bounded
    # infrastructure waits, not retries of a Registry business operation.
    local configured="" attempt
    for attempt in 1 2 3; do
        kill -0 "$CLUSTER_FDB_PID" 2>/dev/null || fail "case FoundationDB exited before configuration"
        if cluster_fdb_cli --exec 'configure new single ssd' --timeout 20 >>"$WORK/fdb-configure.log" 2>&1; then
            configured=1
            break
        fi
        # A timed-out configure may have committed. Read its status before
        # issuing another configure instead of mistaking the timeout for loss.
        if cluster_fdb_cli --exec 'status minimal' --timeout 2 >"$WORK/fdb-status.log" 2>&1 \
            && grep -Fq 'The database is available' "$WORK/fdb-status.log"; then
            configured=1
            break
        fi
        sleep 0.1
    done
    [ -n "$configured" ] || fail "case FoundationDB configuration failed"
    for attempt in $(seq 1 40); do
        kill -0 "$CLUSTER_FDB_PID" 2>/dev/null || fail "case FoundationDB exited during readiness"
        if cluster_fdb_cli --exec 'status minimal' --timeout 2 >"$WORK/fdb-status.log" 2>&1 \
            && grep -Fq 'The database is available' "$WORK/fdb-status.log"; then
            step "FoundationDB 7.3.77 ready (isolated single SSD)"
            return
        fi
        sleep 0.1
    done
    fail "case FoundationDB did not become available"
}

write_registry_configs() {
    local registries="$1"
    case "${CLUSTER_STORAGE_BACKEND:-foundationdb}" in
        foundationdb)
            start_cluster_fdb
            CLUSTER_STORAGE_YAML="storage: { backend: fdb }
foundationdb:
  cluster_file: \"$CLUSTER_FDB_FILE\"
  prefix: \"$CLUSTER_FDB_PREFIX\""
            ;;
        sqlite)
            [ "$registries" = 1 ] || fail "SQLite requires a single Registry"
            CLUSTER_STORAGE_YAML="storage:
  backend: sqlite
  sqlite_path: \"$WORK/registry.db\""
            ;;
        postgres)
            start_test_postgres || fail "PostgreSQL readiness"
            CLUSTER_STORAGE_YAML="storage:
  backend: postgres
  postgres_dsn_env: STORAGE_TEST_POSTGRES_DSN
  namespace: \"/cluster-e2e/$(python3 -c 'import uuid; print(uuid.uuid4().hex)')/\""
            ;;
        *) fail "unsupported cluster test backend" ;;
    esac
    CONTROL_PORTS=()
    for _ in $(seq 1 "$registries"); do
        CONTROL_PORTS+=("$(free_port)")
    done
    CONTROL_PORT="${CONTROL_PORTS[0]}"
    ACTIVE_MEMBERS_YAML=""
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        ACTIVE_MEMBERS_YAML="$ACTIVE_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\", node_advertise: \"127.0.0.1:$port\" }
"
    done
    ROUTE_OWNER_COUNT="$registries"
    NODE_OWNER_COUNT="${2:-$registries}"
    PLACER_OWNER_COUNT="$registries"
    NODE_LIST_OWNER_COUNT="$registries"
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        cat >"$WORK/registry-$i.yaml" <<EOF
member:
  id: registry-$i
  listen: "127.0.0.1:$port"
$CLUSTER_STORAGE_YAML
membership:
  active: 1
  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML
  owners:
    route_link: $ROUTE_OWNER_COUNT
    node_link: $NODE_OWNER_COUNT
    placer_link: $PLACER_OWNER_COUNT
    node_list: $NODE_LIST_OWNER_COUNT
node_link:
  heartbeat_interval: "500ms"
  node_dead_after: "5s"
route_link:
  park_timeout: "180s"
placement:
  candidates: 2
  zone_admit_max: "yellow"
EOF
    done
}

write_placer_router_configs() {
    PLACER_PORT="$(free_port)"
    ROUTER_PORT="$(free_port)"
    cat > "$WORK/placer.yaml" <<EOF
placer:
  id: placer-1
  listen: "127.0.0.1:$PLACER_PORT"
  advertise: "http://127.0.0.1:$PLACER_PORT"
  memberlist_label: "placer.default"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
import_groups:
  - source_id: cluster-real-file-source
    source_type: file
    path: "$WORK/g"
placement:
  import_source_lease_ttl: "10s"
  import_refresh_interval: "1s"
EOF
    cat > "$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  data_plane: "enforce"
  cache_ttl: "500ms"
cache:
  route_ttl: "5m"
  idle_timeout: "2m"
EOF
}

start_cluster_control_plane() {
    local registries="${#CONTROL_PORTS[@]}"
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        step "starting registry-$i (:${port})"
        "$BIN/run-cluster-ctl" registry --config "$WORK/registry-$i.yaml" >"$WORK/registry-$i.log" 2>&1 &
        PIDS+=("$!")
        REGISTRY_PIDS+=("$!")
        wait_port "$port" "registry-$i"
    done
    step "checking registry membership endpoint"
    python3 - "${CONTROL_PORTS[0]}" "$registries" <<'PY' || fail "membership endpoint failed"
import json, sys, urllib.request
port, want = sys.argv[1], int(sys.argv[2])
m = json.load(urllib.request.urlopen("http://127.0.0.1:%s/cluster/membership" % port, timeout=2))
active = m.get("active", m.get("Active"))
versions = m.get("versions", m.get("Versions", []))
assert active, "Registry membership has no active version"
active_versions = [v for v in versions if v.get("version", v.get("Version")) == active]
assert len(active_versions) == 1, "Registry membership active version is ambiguous"
members = active_versions[0].get("members", active_versions[0].get("Members", []))
assert len(members) == want, "Registry membership count differs from fixture"
assert active_versions[0].get("label", active_versions[0].get("Label")), "Registry membership label is missing"
PY

    if [ "$CLUSTER_USE_PLACER" = 1 ]; then
        step "starting placer (:${PLACER_PORT})"
        "$BIN/run-cluster-ctl" placer --config "$WORK/placer.yaml" >"$WORK/placer.log" 2>&1 &
        PIDS+=("$!")
        wait_port "$PLACER_PORT" placer
    elif [ "$CLUSTER_USE_PLACER" != 0 ]; then
        fail "invalid Placer fixture mode"
    fi

    step "starting router (:${ROUTER_PORT})"
    "$BIN/run-cluster-ctl" router --config "$WORK/router.yaml" >"$WORK/router.log" 2>&1 &
    ROUTER_PID=$!
    PIDS+=("$ROUTER_PID")
    wait_port "$ROUTER_PORT" router
}

start_cluster_node() {
    NODE_PORT="$(free_port)"
    NODE_DATA_PORT="$(free_port)"
    local node_id="$1"
    local runner="${2:-sandbox-runner@.service}" builder="${3:-sandbox-builder@.service}"
    cat > "$WORK/cluster-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$NODE_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/cr, base_root: $WORK/cl, config_socket: $WORK/cn.sock }
units: { dir: $UNIT_DIR, runner: "$runner", builder: "$builder" }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: ${CLUSTER_NODE_RUNTIME:-$BIN/sandbox-runtime.bundle}, overlay_diff_template: $OVL }
builder:
  admission:
    registration: { max_builds: 16 }
    execution: { max_builds: 1 }
  terminal_ttl: 1h
  total_timeout_sec: 1200
  step_timeout_sec: 180
  insecure_registry: true
  diff_template: $BLD
checkpoint: { mode: local }
cluster:
  node_link: { endpoint: "127.0.0.1:$CONTROL_PORT" }
  node_id: "$node_id"
  api_endpoint: "127.0.0.1:$NODE_PORT"
  data_endpoint: "127.0.0.1:$NODE_DATA_PORT"
  heartbeat_interval: "500ms"
  labels: { pool: "real", fixture_node: "$node_id" }
EOF
    step "starting cluster conductor node_id=$node_id (API :${NODE_PORT})"
    "$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" >"$WORK/cluster-node.log" 2>&1 &
    CLUSTER_CONDUCTOR_PID=$!
    PIDS+=("$CLUSTER_CONDUCTOR_PID")
    wait_api_health "$NODE_PORT" "cluster node-ctl"
    write_proxy_config "$WORK/cluster-proxy.yaml" \
        "$WORK/cn.sock" "$WORK/cr" "127.0.0.1:$NODE_DATA_PORT" - \
        "$WORK/cluster-proxy-stats.sock" "$WORK/cluster-proxy-routes.shm" \
        1024 2 enforce 180s -
    start_proxy "$BIN/node-ctl" "$WORK/cluster-proxy.yaml" "$WORK/cluster-proxy.log"
    PIDS+=("$PROXY_HELPER_PID")
    wait_proxy_ready "$PROXY_HELPER_PID" 127.0.0.1 "$NODE_DATA_PORT" \
        "$WORK/cluster-proxy-stats.sock" "$WORK/cluster-proxy.log" \
        || fail "cluster Proxy did not become ready"
    step "cluster Proxy data endpoint ready (:${NODE_DATA_PORT})"

}

wait_cluster_node_key_pair() {
    step "waiting for node_link credential-pair cache on $NODE_ID"
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" manifest-key list --socket "$WORK/cn.sock" >"$WORK/cluster-node-keys.out" 2>&1; then
            if grep -q "^api=${API_SECRET_FP}[[:space:]]" "$WORK/cluster-node-keys.out"; then
                step "node credential-pair cache ready: $API_SECRET_FP"
                return 0
            fi
        fi
        sleep 0.5
    done
    fail "node credential-pair cache did not receive $API_SECRET_FP"
}

install_cluster_group() {
    # The no-Placer path deliberately follows the documented local admin flow.
    # Installing root credentials is separate from versioned business context.
    "$BIN/node-ctl" manifest-key add --socket "$WORK/cn.sock" "$MANIFEST_KEY" \
        >"$WORK/group-root.private" 2>&1 || fail "Node root installation"
    python3 - "$WORK/direct-group.json" "$GROUP" "${TEMPLATE_REF:-}" <<'PY_GROUP'
import json, sys
with open(sys.argv[1], "w") as output:
    context = {"templateID": sys.argv[3]} if sys.argv[3] else {}
    json.dump({"group": sys.argv[2], "context": context}, output)
PY_GROUP
    E2B_API_KEY="$CLUSTER_API_KEY" "$BIN/node-ctl" group apply --socket "$WORK/cn.sock" \
        -f "$WORK/direct-group.json" >"$WORK/group-apply.private" 2>&1 || fail "direct Group apply"
}

stop_cluster_extra_node_units() {
    local root="$1" runner="$2" builder="$3" unit spec
    while IFS= read -r unit; do
        [ -n "$unit" ] || continue
        spec=$(systemctl show --property=ExecStart --value "$unit") || continue
        case "$spec" in *"$root/"*) systemctl stop "$unit" ;; esac
    done < <(systemctl list-units "${runner/@.service/@*.service}" "${builder/@.service/@*.service}" \
        --all --output=json --no-pager | python3 -c 'import json,sys; [print(u["unit"]) for u in json.load(sys.stdin)]')
}
