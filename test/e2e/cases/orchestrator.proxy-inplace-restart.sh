#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
. "$SCRIPT_DIR/proxy_lifecycle.sh"
PROXY_WORKERS=2
for helper in CUSTOM_PROXY_BIN CUSTOM_PROXY_INPLACE_TARGET_BIN NODE_CTL_INPLACE_TARGET_BIN PROXY_LIFECYCLE_BIN PROXY_LIFECYCLE_TARGET_BIN; do
    [ -x "${!helper:-}" ] || fail "missing prepared $helper"
done
lifecycle_cleanup() {
    local status=$?
    trap - EXIT
    if ! proxy_lifecycle_cleanup_units "$WORK" "$RUN_KEY"; then
        echo "owned lifecycle units could not be cleaned; preserving the recovery record and topology: $WORK" >&2
        exit 1
    fi
    cleanup || status=1
    exit "$status"
}
trap lifecycle_cleanup EXIT
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":1800}"
wait_sandbox_state "$SID" running 1200 || fail "lifecycle Guest did not reach running"
# These fixture programs execute in the real Guest. Finite data responses drain
# completely; separate Guest requests verify MMDS on each ready replacement.
for fixture in proxy_lifecycle_guest websocket_probe; do
    options=(--sid "$SID")
    [ "$fixture" != websocket_probe ] || options=(--idle-timeout 45)
    command=$(python3 "$SCRIPT_DIR/$fixture.py" guest-command "${options[@]}")
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$command" >"$WORK/$fixture.log" 2>&1 || true
    grep -q 'EXIT_CODE 0' "$WORK/$fixture.log" || { cat "$WORK/$fixture.log"; fail "start $fixture in Guest"; }
done
stop_orchestrator

for mode in builtin:address builtin:datafd builtin:mmdsfd builtin:bothfd custom:bothfd app:bothfd; do
    entry=${mode%:*}; source=${mode#*:}; label=${mode/:/-}
    echo "==> real lifecycle: $label"
    write_orchestrator_config unset static
    # Exercise the existing PID admission rule with only the fixed main PID.
    # The launcher updates it on ordinary service restart, then exec preserves it.
    plugin_pidfile="$WORK/proxy-main.pid"
    sed -i "/^paths:$/a\\  plugin_pidfile: $plugin_pidfile" "$WORK/config.yaml"
    grep -q "plugin_pidfile: $plugin_pidfile" "$WORK/config.yaml" || fail "missing paths for PID admission"
    case "$source" in
        mmdsfd|bothfd) sed -i "s/listen: \"$PROXY_NS_IP:$MMDS_PORT\"/listen: fd=proxy-mmds/" "$WORK/config.yaml";;
    esac
    start_conductor "$WORK/orch-$label.log"
    deployment="$WORK/entry-$label"
    proxy_executable=-
    case "$entry" in
        builtin) initial="$BIN/node-ctl"; target="$NODE_CTL_INPLACE_TARGET_BIN"; launcher="$deployment";;
        custom) initial="$CUSTOM_PROXY_BIN"; target="$CUSTOM_PROXY_INPLACE_TARGET_BIN"; launcher="$BIN/node-ctl"; proxy_executable="$deployment";;
        app) initial="$PROXY_LIFECYCLE_BIN"; target="$PROXY_LIFECYCLE_TARGET_BIN"; launcher="$BIN/node-ctl"; proxy_executable="$deployment";;
    esac
    install -m 0700 "$initial" "$deployment"
    cat >"$WORK/start-proxy" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
pidfile=$1
shift
umask 077
printf '%s\n' "$$" >"$pidfile"
exec "$@"
EOF
    chmod 0700 "$WORK/start-proxy"
    data_listen="127.0.0.1:$PROXY_PORT"
    case "$source" in datafd|bothfd) data_listen=fd=proxy-data;; esac
    write_proxy_config "$WORK/proxy.yaml" "$WORK/node-ctl.socket" "$EXECUTE_RUN_ROOT" "$data_listen" \
        "$PROXY_NETNS" "$WORK/proxy-stats.sock" "$WORK/proxy-routes.shm" 1024 2 enforce 120s - - - "$proxy_executable"
    printf 'shutdown_timeout: 15s\ninplace_restart: %s\n' "$([ "$entry" = app ] && echo false || echo true)" >>"$WORK/proxy.yaml"
    proxy_lifecycle_reserve_units "$WORK" "$RUN_KEY" || fail "reserve test units"
    service="kuasar-proxy-$RUN_KEY.service"
    sockets=""
    case "$source" in datafd|bothfd)
        socket="kuasar-proxy-$RUN_KEY-data.socket"
        sockets="$socket"
        cat >"$UNIT_DIR/$socket" <<EOF
[Socket]
ListenStream=127.0.0.1:$PROXY_PORT
FileDescriptorName=proxy-data
Accept=no
Service=$service
EOF
        ;;
    esac
    case "$source" in mmdsfd|bothfd)
        socket="kuasar-proxy-$RUN_KEY-mmds.socket"
        sockets="$sockets $socket"
        cat >"$UNIT_DIR/$socket" <<EOF
[Socket]
ListenStream=$PROXY_NS_IP:$MMDS_PORT
NetworkNamespacePath=/run/netns/$PROXY_NETNS
FileDescriptorName=proxy-mmds
Accept=no
Service=$service
EOF
        ;;
    esac
    cat >"$UNIT_DIR/$service" <<EOF
[Unit]
Requires=$sockets
After=$sockets
[Service]
Type=simple
ExecStart=$WORK/start-proxy $plugin_pidfile $launcher proxy serve --config $WORK/proxy.yaml
ExecStopPost=/usr/bin/python3 -B $SCRIPT_DIR/proxy_lifecycle_client.py record-stop $WORK/service-exit-$label.log
Sockets=$sockets
KillMode=mixed
SendSIGKILL=no
TimeoutStopSec=20s
Restart=no
EOF
    systemctl daemon-reload
    systemctl start "$service"
    (umask 077; python3 - "$WORK/client-$label.json" "$WORK/proxy-stats.sock" "$service" "$WORK/client-$label.log" \
        "$PROXY_PORT" "$SID" "$DOMAIN" "$FORWARD_TOKEN" "$entry" "$source" "$deployment" "$initial" "$target" "$WORK/proxy.yaml" "$PORT" "$AK" "$PROXY_NETNS" "$MMDS_PORT" "$sockets" "$WORK/service-exit-$label.log" "$plugin_pidfile" <<'PY'
import json, sys
names = ('stats', 'unit', 'output', 'port', 'sid', 'domain', 'token', 'entry', 'source', 'deployment', 'initial', 'target', 'proxy_config', 'api_port', 'api_key', 'proxy_netns', 'mmds_port', 'socket_units', 'exit_record', 'plugin_pidfile')
config = dict(zip(names, sys.argv[2:], strict=True))
config['port'] = int(config['port'])
with open(sys.argv[1], 'x') as stream:
    json.dump(config, stream)
PY
    )
    python3 "$SCRIPT_DIR/proxy_lifecycle_client.py" "$WORK/client-$label.json"
    # Stop socket units as well so full shutdown cannot activate the service.
    mapfile -t units < <(proxy_lifecycle_units "$RUN_KEY")
    # Missing socket variants are not submitted to systemctl stop.
    selected=("$service")
    for unit in "${units[@]}"; do
        [ "$unit" = "$service" ] || [ ! -f "$UNIT_DIR/$unit" ] || selected+=("$unit")
    done
    systemctl stop "${selected[@]}"
    [ "$(systemctl show --value -p MainPID "$service")" = 0 ] || fail "main process survived complete shutdown"
    [ ! -e "$WORK/proxy-stats.sock" ] && [ ! -e "$WORK/proxy-routes.shm" ] || fail "service resources survived shutdown"
    proxy_lifecycle_cleanup_units "$WORK" "$RUN_KEY" || fail "remove owned test units"
    # Preserve each mode's journals before the next reservation reuses names.
    for journal in "$WORK"/kuasar-proxy-*.journal; do
        [ ! -f "$journal" ] || mv "$journal" "$journal.$label.log"
    done
    if [ "$entry" = app ]; then
        # Release the real Guest's slot while its owning conductor is alive.
        # Stopping the runner alone does not finalize the vSwitch allocation.
        code=$(req DELETE "/sandboxes/$SID" "$AK")
        [ "$code" = 204 ] || fail "delete lifecycle Guest=$code"
        wait_sandbox_state "$SID" missing 120 || fail "lifecycle Guest deletion did not finalize"
    fi
    stop_orchestrator
done
echo "PASS orchestrator.proxy-inplace-restart.sh"
