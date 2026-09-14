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
    # The source integration environment has the exact component sources. Run
    # the real namespace/error/cleanup Collector checks with capabilities, so
    # their ordinary unprivileged unit-test skips cannot count as CI evidence.
    local telemetry_source="${CUSTOM_PROXY_SOURCE_ROOT:-$REPO_ROOT}"
    if [ -f "$telemetry_source/internal/telemetry/otlp_netns_linux_test.go" ]; then
        (cd "$telemetry_source" && GOWORK=off CGO_ENABLED=0 go test -c -o "$WORK/telemetry.test" ./internal/telemetry)
        REQUIRE_TELEMETRY_NETNS=1 "$WORK/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS' \
            >"$WORK/telemetry-netns.out" 2>&1 || { cat "$WORK/telemetry-netns.out"; fail "real Collector netns regression"; }
        cat "$WORK/telemetry-netns.out"
    fi
    # Sandbox creation already succeeded before telemetry was started: telemetry
    # registration must not be part of create/resume admission.
    cat >"$WORK/telemetry.yaml" <<EOF
config_socket: $WORK/node-ctl.socket
api_socket: $WORK/telemetry.sock
proxy_netns: $PROXY_NETNS
route_capacity: 1024
telemetry:
  storage:
    type: local
    path: $WORK/telemetry-db
    retention: 1h
    max_size: 64MiB
collector:
  receivers:
    envd: {collection_interval: 1s}
    sandboxstats: {resource_interval: 5s, traffic_interval: 10s, usage_interval: 1m}
    sandboxotlp:
      grpc_listen: $PROXY_NS_IP:4317
      http_listen: $PROXY_NS_IP:4318
  processors:
    batch: {timeout: 200ms}
  exporters:
    sandboxstorage: {}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics:
        receivers: [envd, sandboxstats, sandboxotlp]
        processors: [batch]
        exporters: [sandboxstorage]
EOF
    local code command
    code=$(req GET "/sandboxes/$SID/metrics" "$AK")
    [ "$code" = 503 ] || fail "metrics without telemetry=$code (want 503)"
    start_telemetry
    wait_telemetry_history
    # Listener ownership is established from socket inodes, not successful
    # nonlocal address binding. The telemetry process itself stays on the host.
    python3 "$SCRIPT_DIR/lib/telemetry_probe.py" check-netns "$TELEMETRY_PID" "$PROXY_NETNS" 4317 4318
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
    # A dependency-free static HTTP/2 client sends a real unary MetricsService
    # request and checks gRPC trailers. The helper is built only for this case.
    GOWORK=off CGO_ENABLED=0 go build -trimpath -o "$WORK/telemetry-grpc-probe" "$SCRIPT_DIR/telemetryprobe/main.go"
    code=$(curl --noproxy '*' --unix-socket "$ENVD_SOCK" -sS --max-time 20 \
        -o "$WORK/telemetry-upload.json" -w '%{http_code}' -H "X-Access-Token: $ENVD_TOKEN" \
        -F "file=@$WORK/telemetry-grpc-probe;filename=telemetry-grpc-probe" \
        'http://envd/files?path=/tmp/telemetry-grpc-probe')
    [ "$code" = 200 ] || { cat "$WORK/telemetry-upload.json"; fail "upload gRPC probe=$code"; }
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
        "chmod 700 /tmp/telemetry-grpc-probe && /tmp/telemetry-grpc-probe http://$MGMT_VIP:4317" \
        >"$WORK/telemetry-guest-grpc.out" 2>&1
    grep -q 'OTLP_GRPC_PEER_IDENTITY_OK' "$WORK/telemetry-guest-grpc.out" \
        || { cat "$WORK/telemetry-guest-grpc.out"; dump_logs; fail "guest OTLP/gRPC through management service"; }
    echo "==> PASS: envd -> Collector -> local TSDB -> E2B; real guest FloatingIP OTLP HTTP/gRPC; listener namespace ownership"
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
