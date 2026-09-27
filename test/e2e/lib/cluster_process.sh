#!/usr/bin/env bash
# Process, configuration and request primitives for real cluster cases.
start_store_zot_vswitch() {
    STORE_PORT="$(free_port)"
    cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/s, verify_content_key: true }
EOF
    "$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
    "$BIN/store-ctl" serve --config "$WORK/store.yaml" > >(tee "$WORK/store-serve.log" >&2) 2>&1 &
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
    docker push "$REF" > >(tee "$WORK/push.log" >&2) 2>&1 || fail "docker push e2e image"
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
        --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 > >(tee "$WORK/vswitch-start.log" >&2) 2>&1 || fail "vswitch start"
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
    "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL" >"$WORK/mkfs-overlay.log" 2>&1 || { cat "$WORK/mkfs-overlay.log"; fail "mkfs overlay"; }
    BLD="$WORK/builder-2G.ext4"
    truncate -s 2G "$BLD"
    "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$BLD" >"$WORK/mkfs-builder.log" 2>&1 || { cat "$WORK/mkfs-builder.log"; fail "mkfs builder"; }
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
    "$BIN/node-ctl" conductor serve --config "$WORK/build-node.yaml" > >(tee "$WORK/build-node.log" >&2) 2>&1 &
    local build_pid="$!"
    PIDS+=("$build_pid")
    wait_api_health "$BUILD_PORT" "temporary node-ctl"
    "$BIN/node-ctl" manifest-key add --socket "$WORK/bn.sock" "$MANIFEST_KEY" >/dev/null || fail "temporary manifest-key add"

    local code tid bid status run_id
    # Build memory supplies admission and the A/B sandbox specification.
    # Target Sandbox resources resolve independently from node policy;
    # parent Builder services/slices add no CPU or memory policy.
    code="$(node_req "$BUILD_PORT" POST /v3/templates "$BUILD_API_KEY" '{"name":"cluster-real-tmpl","cpuCount":2,"memoryMB":6144}')"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template register returned $code"; }
    tid="$(json_field "$WORK/node-resp.body" templateID)"
    bid="$(json_field "$WORK/node-resp.body" buildID)"
    step "building template through temporary node: templateID=$tid buildID=$bid"
    code="$(node_req "$BUILD_PORT" POST "/v2/templates/$tid/builds/$bid" "$BUILD_API_KEY" "{\"fromImage\":\"$GUEST_REF\"}")"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template build trigger returned $code"; }
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
                run_id="$(json_field "$WORK/node-resp.body" runID)"
                if [ -n "$run_id" ]; then
                    echo "---- journal sandbox-builder@${run_id}.service ----" >&2
                    journalctl -u "sandbox-builder@${run_id}.service" --no-pager -n 100 2>/dev/null \
                        | sed 's/^/  builder| /' >&2 || true
                fi
                cat "$WORK/node-resp.body"
                fail "template build error"
                ;;
        esac
        sleep 1
    done
    [ -n "$TEMPLATE_REF" ] || fail "template build did not become ready"
    step "built reusable template: $TEMPLATE_REF"

    kill "$build_pid" 2>/dev/null || true
    wait "$build_pid" 2>/dev/null || true
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

write_registry_configs() {
    CONTROL_PORTS=()
    local registries="$1"
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
placer_link:
  min_ready_placers: 1
  place_timeout: "5s"
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
  candidates: 2
  zone_admit_max: "yellow"
  node_dead_after: "5s"
  import_source_lease_ttl: "10s"
  selector_patch_refresh_interval: "1s"
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
        "$BIN/cluster-ctl" registry --config "$WORK/registry-$i.yaml" > >(tee "$WORK/registry-$i.log" >&2) 2>&1 &
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
assert active, m
active_versions = [v for v in versions if v.get("version", v.get("Version")) == active]
assert len(active_versions) == 1, m
members = active_versions[0].get("members", active_versions[0].get("Members", []))
assert len(members) == want, m
assert active_versions[0].get("label", active_versions[0].get("Label")), m
PY

    step "starting placer (:${PLACER_PORT})"
    "$BIN/cluster-ctl" placer --config "$WORK/placer.yaml" > >(tee "$WORK/placer.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$PLACER_PORT" placer

    step "starting router (:${ROUTER_PORT})"
    "$BIN/cluster-ctl" router --config "$WORK/router.yaml" > >(tee "$WORK/router.log" >&2) 2>&1 &
    ROUTER_PID=$!
    PIDS+=("$ROUTER_PID")
    wait_port "$ROUTER_PORT" router
}

start_cluster_node() {
    NODE_PORT="$(free_port)"
    NODE_DATA_PORT="$(free_port)"
    local node_id="$1"
    cat > "$WORK/cluster-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$NODE_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/cr, base_root: $WORK/cl, config_socket: $WORK/cn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
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
  labels: { pool: "real" }
EOF
    step "starting cluster conductor node_id=$node_id (API :${NODE_PORT}, no manual manifest-key add)"
    "$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" > >(tee "$WORK/cluster-node.log" >&2) 2>&1 &
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
    cat "$WORK/cluster-node-keys.out" >&2 || true
    fail "node credential-pair cache did not receive $API_SECRET_FP"
}
