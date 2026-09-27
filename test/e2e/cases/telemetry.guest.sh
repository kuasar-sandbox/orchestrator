#!/usr/bin/env bash
set -euo pipefail
fail() { echo "$*" >&2; exit 1; }
[ -x "${TELEMETRY_GRPC_PROBE_BIN:-}" ] || fail "prepared TELEMETRY_GRPC_PROBE_BIN is required"
NATIVE_USAGE_ENABLED=true
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
IDENTITY_ID="identity-$(cat /proc/sys/kernel/random/uuid)"
IDENTITY_STABLE_ID="logical-$IDENTITY_ID"
IDENTITY_CREATE_BODY="$(python3 - "$TEMPLATE" "$IDENTITY_ID" "$IDENTITY_STABLE_ID" <<'PY_IDENTITY'
import json, sys
print(json.dumps({"templateID": sys.argv[1], "timeout": 120, "metadata": {
    "kuasar-sandbox.identity": json.dumps({"id": sys.argv[2], "stable_id": sys.argv[3]})
}}))
PY_IDENTITY
)"
create_guest "$IDENTITY_CREATE_BODY"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_traffic_stats "$SID" idle || fail "guest did not become idle"
. "$SCRIPT_DIR/telemetry.sh"
native_traffic_probe 0
IDLE_BEFORE_MANAGEMENT="$(traffic_idle_since "$SID")"
    # Privileged Collector unit regressions run in the source-check stage.
    # Sandbox creation already succeeded before telemetry was started: telemetry
    # registration must not be part of create/resume admission.
    start_stats_sink
    cat >"$WORK/telemetry.yaml" <<EOF
config_socket: $WORK/node-ctl.socket
api_socket: $WORK/telemetry.sock
proxy_netns: $PROXY_NETNS
route_capacity: 1024
query: {backend: local, handler: e2b}
local:
  enabled: true
  path: $WORK/telemetry-db
  retention: 1h
  max_size: 64MiB
collector:
  receivers:
    envd: {collection_interval: 1s}
    sandboxstats: {resource_interval: 1s, traffic_interval: 1s, usage_interval: 1s}
    sandboxotlp:
      grpc_listen: $PROXY_NS_IP:4317
      http_listen: $PROXY_NS_IP:4318
  processors:
    batch: {timeout: 200ms}
  exporters:
    sandboxlocal: {}
    otlp_http/probe: {endpoint: 'http://127.0.0.1:$STATS_SINK_PORT', encoding: json, compression: none}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics:
        receivers: [envd, sandboxstats, sandboxotlp]
        processors: [batch]
        exporters: [sandboxlocal, otlp_http/probe]
EOF
    code=""; command=""; section=""
    code=$(req GET "/sandboxes/$SID/metrics" "$AK")
    [ "$code" = 503 ] || fail "metrics without telemetry=$code (want 503)"
    start_telemetry
    wait_telemetry_history
    # Listener ownership is established from socket inodes, not successful
    # nonlocal address binding. The telemetry process itself stays on the host.
    python3 "$SCRIPT_DIR/telemetry_probe.py" check-netns "$TELEMETRY_PID" "$PROXY_NETNS" 4317 4318
    code=$(req GET "/sandboxes/$SID/metrics?start=bad" "$AK")
    [ "$code" = 400 ] || fail "telemetry query validation=$code"
    code=$(req GET "/sandboxes/$IDENTITY_STABLE_ID/metrics" "$AK")
    [ "$code" = 404 ] || fail "metrics used StableID fallback ($code)"
    code=$(req GET "/sandboxes/$SID/metrics" "invalid-key")
    [ "$code" = 401 ] || fail "metrics auth=$code"
    rejected_status=0
    code=$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/otlp-unknown.body" -w '%{http_code}' \
        -H 'Content-Type: application/json' -H 'X-Forwarded-For: 100.100.96.1' -d '{}' \
        "http://$PROXY_NS_IP:4318/v1/metrics" 2>"$WORK/otlp-unknown.err") || rejected_status=$?
    case "$rejected_status" in
        52|56) [ "$code" = 000 ] || fail "unexpected unknown-peer HTTP response ($code)" ;;
        *) fail "unknown OTLP peer was not rejected at accept (curl=$rejected_status HTTP=$code)" ;;
    esac
    command=$(python3 "$SCRIPT_DIR/telemetry_probe.py" guest-command "http://$MGMT_VIP:4318/v1/metrics")
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$command" >"$WORK/telemetry-guest.out" 2>&1
    grep -q 'OTLP_PEER_IDENTITY_OK' "$WORK/telemetry-guest.out" \
        || { cat "$WORK/telemetry-guest.out"; dump_logs; fail "guest OTLP through mgmt-extract FloatingIP path"; }
    # Upload the prepared static client; the real guest request remains here.
    : "${TELEMETRY_GRPC_PROBE_BIN:?prepared telemetry gRPC probe is required}"
    [ -x "$TELEMETRY_GRPC_PROBE_BIN" ] || fail "prepared telemetry gRPC probe is not executable"
    cp "$TELEMETRY_GRPC_PROBE_BIN" "$WORK/telemetry-grpc-probe"
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
    for section in resource traffic; do
        code=$(req GET "/sandboxes/$SID/stats/$section" "$AK")
        [ "$code" = 200 ] || fail "native $section reference=$code"
        cp "$WORK/resp.body" "$WORK/telemetry-native-$section.json"
        wait_native_export "$SID" "$section" "$WORK/telemetry-native-$section.json"
    done
    echo "==> PASS: real native resource/traffic -> conductor lease -> sandboxstats -> batch -> HTTP exporter; exact SID and host timestamps"
    echo "==> PASS: envd -> Collector -> local TSDB -> E2B; real guest FloatingIP OTLP HTTP/gRPC; listener namespace ownership"
native_traffic_probe 1
[ "$(traffic_idle_since "$SID")" = "$IDLE_BEFORE_MANAGEMENT" ] || fail "OTLP management refreshed idleSince"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
[ "$code" = 204 ] || fail "telemetry guest pause=$code"
    wait_telemetry_history
    wait_traffic_stats "$SID" paused || fail "history query woke paused sandbox"
    stop_telemetry
    code=""
    code=$(req GET "/sandboxes/$SID/metrics" "$AK")
    [ "$code" = 503 ] || fail "disconnected telemetry query lease=$code"
    wait_traffic_stats "$SID" paused || fail "telemetry failure changed lifecycle"
    start_telemetry
    wait_telemetry_history
    wait_traffic_stats "$SID" paused || fail "TSDB restart/history query woke sandbox"
    echo "==> PASS: paused history never woke sandbox; disconnect returned 503; TSDB restart retained history"
stop_telemetry
assert_native_usage "$SID" paused
# The previous Collector has stopped; only the new write-only export may satisfy this check.
: > "$WORK/telemetry-native.jsonl"
ended=""
        cat >"$WORK/telemetry-usage.yaml" <<EOF
config_socket: $WORK/node-ctl.socket
collector:
  receivers:
    sandboxstats: {resource_interval: 0s, traffic_interval: 0s, usage_interval: 1s}
  processors:
    batch: {timeout: 100ms}
  exporters:
    otlp_http/probe: {endpoint: 'http://127.0.0.1:$STATS_SINK_PORT', encoding: json, compression: none}
  service:
    telemetry: {metrics: {level: none}}
    pipelines:
      metrics: {receivers: [sandboxstats], processors: [batch], exporters: [otlp_http/probe]}
EOF
    "$BIN/node-ctl" telemetry serve --config "$WORK/telemetry-usage.yaml" \
        >"$WORK/telemetry-usage.log" 2>&1 &
    observer_pid=$!
    PIDS+=("$observer_pid")
    pid_slots=("${!PIDS[@]}")
    observer_slot=${pid_slots[-1]}
    wait_native_export "$SID" usage "$WORK/usage-$SID-paused.json"
    [ ! -e "$WORK/telemetry.sock" ] || fail "write-only Collector published a query socket"
    [ "$(req GET "/sandboxes/$SID/metrics" "$AK")" = 503 ] || fail "write-only metrics API did not return 503"
    [ "$(sandbox_state "$SID")" = paused ] || fail "native usage Collector woke sandbox"
    kill -TERM "$observer_pid"
    for _ in $(seq 1 100); do
        kill -0 "$observer_pid" 2>/dev/null || { ended=1; break; }
        sleep 0.1
    done
    [ -n "$ended" ] || { kill -KILL "$observer_pid"; fail "native usage Collector shutdown timeout"; }
    wait "$observer_pid" || { cat "$WORK/telemetry-usage.log"; fail "native usage Collector shutdown failed"; }
    PIDS[$observer_slot]=""
    kill -TERM "$STATS_SINK_PID"
    wait "$STATS_SINK_PID" || [ "$?" = 143 ]
    PIDS[$STATS_SINK_SLOT]=""
    [ "$(sandbox_state "$SID")" = paused ] || fail "Collector shutdown changed sandbox state"
    echo "==> PASS: real paused native usage -> conductor lease -> sandboxstats -> batch -> HTTP exporter; saved time and cumulative CPU/memory preserved"
code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
stop_proxy_master
echo "PASS telemetry.guest.sh"
