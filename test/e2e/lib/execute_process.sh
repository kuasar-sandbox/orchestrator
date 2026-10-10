#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
write_orchestrator_config() { # $1=unset|node-policy, $2=static|controller, $3=local|bundle
    local policy_mode="$1"
    local resource_mode="${2:-controller}"
    local checkpoint_mode="${3:-local}"
    local resource_controller_config=""
    case "$resource_mode" in
        static) ;;
        controller)
            resource_controller_config="resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/resource-state.json
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 1GiB, cpu: 0.5 }
  admission: { rate: 50, burst: 50, startup_ttl: 180s, queue_ttl: 30s, queue_max_depth: 256 }"
            ;;
        *) fail "unknown resource mode: $resource_mode" ;;
    esac
    cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { auth: enforce, park_timeout: ${PROXY_PARK_TIMEOUT:-120s} }
mmds:
  enabled: true
  listen: "$PROXY_NS_IP:$MMDS_PORT"
$MMDS_ROUTES_CONFIG
$MMDS_SERVICES_CONFIG
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $EXECUTE_RUN_ROOT, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units:
  dir: $UNIT_DIR
  # Identical entries remain independent round-robin positions across cold,
  # artifact, pause/resume, deletion and conductor-restart cases below.
  runner_pools:
    - {unit: '${RUNNER_PREFIX}.service', size: 0}
    - {unit: '${RUNNER_PREFIX}.service', size: 0}
  builder: '${BUILDER_PREFIX}.service'
sandbox:
  timeout_sec: 120
  usage:
    enabled: ${NATIVE_USAGE_ENABLED:-true}
    sample_interval: $NATIVE_USAGE_SAMPLE
    flush_interval: $NATIVE_USAGE_FLUSH
  resources:
    capacity: { cpu: 2, memory: 2GiB }
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
$resource_controller_config
checkpoint:
  mode: $checkpoint_mode
EOF
    if [ "$policy_mode" = "node-policy" ]; then
        cat >> "$WORK/config.yaml" <<'EOF'
  merge_ref: true
  drop_caches: false
EOF
    fi
}

ORCH_PID=""
ORCH_LOG=""
start_orchestrator() { # $1=log path
    local log_path="$1" ready="" proxy_ns_before proxy_addresses_before
    ORCH_LOG="$log_path"
    "$ORCH_BIN_DIR/node-ctl" conductor serve --config "$WORK/config.yaml" >"$log_path" 2>&1 &
    ORCH_PID=$!
    PIDS+=("$ORCH_PID")
    for _ in $(seq 1 30); do
        if curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null; then
            ready=1
            break
        fi
        kill -0 "$ORCH_PID" 2>/dev/null || { sed 's/^/  /' "$log_path"; fail "orchestrator exited"; }
        sleep 0.5
    done
    [ -n "$ready" ] || { sed 's/^/  /' "$log_path"; fail "orchestrator health did not become ready"; }

    write_proxy_config "$WORK/proxy.yaml" \
        "$WORK/node-ctl.socket" "$EXECUTE_RUN_ROOT" "127.0.0.1:$PROXY_PORT" \
        "$PROXY_NETNS" "$WORK/proxy-stats.sock" "$WORK/proxy-routes.shm" \
        1024 2 enforce "${PROXY_PARK_TIMEOUT:-120s}" "${PROXY_METRICS_LISTEN:--}" - - "${PROXY_EXECUTABLE:--}"
    # Preserve the pre-start namespace identity and addresses so a failed bind
    # can be distinguished from teardown/recreation during Proxy startup.
    proxy_ns_before=$(stat -Lc '%d:%i' "/var/run/netns/$PROXY_NETNS" 2>&1 || true)
    proxy_addresses_before=$(ip -n "$PROXY_NETNS" -brief address show 2>&1 || true)
    check_proxy_netns_address before-proxy
    start_proxy "$ORCH_BIN_DIR/node-ctl" "$WORK/proxy.yaml" "$WORK/proxy.log"
    PROXY_PID="$PROXY_HELPER_PID"
    PIDS+=("$PROXY_PID")
    if ! wait_proxy_ready "$PROXY_PID" 127.0.0.1 "$PROXY_PORT" "$WORK/proxy-stats.sock" "$WORK/proxy.log"; then
        printf '==> Proxy startup namespace diagnostics: namespace=%s expected_mmds=%s:%s pid=%s\n' \
            "$PROXY_NETNS" "$PROXY_NS_IP" "$MMDS_PORT" "$PROXY_PID" >&2
        printf 'before: namespace=%s\n%s\n' "$proxy_ns_before" "$proxy_addresses_before" >&2
        stat -Lc 'after: namespace=%d:%i' "/var/run/netns/$PROXY_NETNS" >&2 || true
        ip -n "$PROXY_NETNS" -brief address show >&2 || true
        ip -n "$PROXY_NETNS" route show table all >&2 || true
        ip -brief address show dev "$PROXY_VETH_HOST" >&2 || true
        sed -n '/^proxy_netns:/p' "$WORK/proxy.yaml" >&2
        if [ -d "/proc/$PROXY_PID" ]; then
            stat -Lc 'proxy process namespace=%d:%i' "/proc/$PROXY_PID/ns/net" >&2 || true
        fi
        fail "Proxy did not become ready"
    fi
    return 0
}

stop_orchestrator() {
    [ -n "$ORCH_PID" ] || return 0
    if [ -n "$PROXY_PID" ]; then
        local stopped_proxy_pid="$PROXY_PID"
        stop_proxy "$stopped_proxy_pid"
        for i in "${!PIDS[@]}"; do
            [ "${PIDS[$i]}" = "$stopped_proxy_pid" ] && unset 'PIDS[i]'
        done
        PROXY_PID=""
    fi
    local stopped_pid="$ORCH_PID"
    kill -TERM "$stopped_pid" 2>/dev/null || true
    wait "$stopped_pid" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" = "$stopped_pid" ] && unset 'PIDS[i]'
    done
    ORCH_PID=""
}
