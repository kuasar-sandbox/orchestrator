#!/usr/bin/env bash
# Uses the existing real Proxy/envd fixture; no second lifecycle or network setup.

start_telemetry() {
    "$BIN/node-ctl" telemetry serve --config "$WORK/telemetry.yaml" >>"$WORK/telemetry.log" 2>&1 &
    TELEMETRY_PID=$!
    PIDS+=("$TELEMETRY_PID")
    TELEMETRY_PID_SLOT=$((${#PIDS[@]} - 1))
}

stop_telemetry() {
    kill -TERM "$TELEMETRY_PID"
    local ended=""
    for _ in $(seq 1 100); do
        process_live "$TELEMETRY_PID" || { ended=1; break; }
        sleep 0.1
    done
    [ -n "$ended" ] || { dump_logs; fail "telemetry shutdown timeout"; }
    wait "$TELEMETRY_PID" || { dump_logs; fail "telemetry shutdown failed"; }
    PIDS[$TELEMETRY_PID_SLOT]=""
}

wait_telemetry_history() {
    local code=""
    for _ in $(seq 1 100); do
        code=$(req GET "/sandboxes/$SID/metrics" "$AK")
        if [ "$code" = 200 ] && python3 "$SCRIPT_DIR/lib/telemetry_probe.py" check-history "$WORK/resp.body" 2>/dev/null; then
            return 0
        fi
        process_live "$TELEMETRY_PID" || { dump_logs; fail "telemetry exited before readable history"; }
        sleep 0.1
    done
    dump_logs
    fail "telemetry envd history did not become readable (status=$code)"
}

run_telemetry_guest_probe() {
    # Sandbox creation already succeeded before telemetry was started: telemetry
    # registration must not be part of create/resume admission.
    cat >"$WORK/telemetry.yaml" <<EOF
config_socket: $WORK/node-ctl.socket
api_socket: $WORK/telemetry.sock
sandbox_netns: $PROXY_NETNS
route_capacity: 1024
telemetry:
  otlp:
    grpc_listen: $PROXY_NS_IP:4317
    http_listen: $PROXY_NS_IP:4318
  storage:
    type: local
    path: $WORK/telemetry-db
    retention: 1h
    max_size: 64MiB
EOF
    local code command
    code=$(req GET "/sandboxes/$SID/metrics" "$AK")
    [ "$code" = 503 ] || fail "metrics without telemetry=$code (want 503)"
    start_telemetry
    wait_telemetry_history
    code=$(req GET "/sandboxes/$SID/metrics?start=bad" "$AK")
    [ "$code" = 400 ] || fail "telemetry query validation=$code"
    code=$(req GET "/sandboxes/$IDENTITY_STABLE_ID/metrics" "$AK")
    [ "$code" = 404 ] || fail "metrics used StableID fallback ($code)"
    code=$(req GET "/sandboxes/$SID/metrics" "invalid-key")
    [ "$code" = 401 ] || fail "metrics auth=$code"
    local rejected_status=0
    code=$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/otlp-unknown.body" -w '%{http_code}' \
        -H 'Content-Type: application/json' -H 'X-Forwarded-For: 100.100.96.1' -d '{}' \
        "http://$PROXY_NS_IP:4318/v1/metrics" 2>"$WORK/otlp-unknown.err") || rejected_status=$?
    case "$rejected_status" in
        52|56) [ "$code" = 000 ] || fail "unexpected unknown-peer HTTP response ($code)" ;;
        *) fail "unknown OTLP peer was not rejected at accept (curl=$rejected_status HTTP=$code)" ;;
    esac
    command=$(python3 "$SCRIPT_DIR/lib/telemetry_probe.py" guest-command "http://$MGMT_VIP:4318/v1/metrics")
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$command" >"$WORK/telemetry-guest.out" 2>&1
    grep -q 'OTLP_PEER_IDENTITY_OK' "$WORK/telemetry-guest.out" \
        || { cat "$WORK/telemetry-guest.out"; dump_logs; fail "guest OTLP through mgmt-extract FloatingIP path"; }
    echo "==> PASS: real envd -> Collector -> local TSDB -> E2B; direct guest FloatingIP OTLP, unknown peer rejected"
}

telemetry_paused_restart_probe() {
    wait_telemetry_history
    wait_traffic_stats "$SID" paused || fail "history query woke paused sandbox"
    stop_telemetry
    local code
    code=$(req GET "/sandboxes/$SID/metrics" "$AK")
    [ "$code" = 503 ] || fail "disconnected telemetry query lease=$code"
    wait_traffic_stats "$SID" paused || fail "telemetry failure changed lifecycle"
    start_telemetry
    wait_telemetry_history
    wait_traffic_stats "$SID" paused || fail "TSDB restart/history query woke sandbox"
    echo "==> PASS: paused history never woke sandbox; disconnect returned 503; TSDB restart retained history"
}
