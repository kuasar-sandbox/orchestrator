#!/usr/bin/env bash
# Actual conductor -> sandboxstats -> native Collector -> host HTTP exporter.
# The owning real guest suites supply WORK, PIDS, BIN, req and fail.

start_stats_sink() {
    local -a pid_slots
    : >"$WORK/telemetry-native.jsonl"
    rm -f "$WORK/telemetry-native.port"
    python3 "$SCRIPT_DIR/lib/telemetry_probe.py" serve-metrics \
        "$WORK/telemetry-native.port" "$WORK/telemetry-native.jsonl" \
        >"$WORK/telemetry-native-sink.log" 2>&1 &
    STATS_SINK_PID=$!
    PIDS+=("$STATS_SINK_PID")
    pid_slots=("${!PIDS[@]}")
    STATS_SINK_SLOT=${pid_slots[-1]}
    for _ in $(seq 1 100); do
        if [ -s "$WORK/telemetry-native.port" ]; then
            STATS_SINK_PORT=$(cat "$WORK/telemetry-native.port")
            return
        fi
        sleep 0.05
    done
    cat "$WORK/telemetry-native-sink.log"
    fail "native stats export fixture did not bind"
}

wait_native_export() { # exact SID, section, native public API reference
    local sid="$1" section="$2" reference="$3"
    for _ in $(seq 1 100); do
        if python3 "$SCRIPT_DIR/lib/telemetry_probe.py" check-native \
            "$WORK/telemetry-native.jsonl" "$sid" "$section" "$reference" \
            >"$WORK/telemetry-native-check.log" 2>&1; then
            cat "$WORK/telemetry-native-check.log"
            return
        fi
        sleep 0.1
    done
    cat "$WORK/telemetry-native-check.log" "$WORK/telemetry-native-sink.log"
    for log in "$WORK/telemetry.log" "$WORK/telemetry-usage.log"; do
        [ ! -f "$log" ] || cat "$log"
    done
    fail "actual conductor $section did not pass through Collector/exporter for $sid"
}

assert_paused_usage_export() { # The suite already read this stable saved endpoint.
    local sid="$1" reference="$2" observer_pid observer_slot ended=""
    local -a pid_slots
    start_stats_sink
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
    wait_native_export "$sid" usage "$reference"
    [ ! -e "$WORK/telemetry.sock" ] || fail "write-only Collector published a query socket"
    [ "$(req GET "/sandboxes/$sid/metrics" "$AK")" = 503 ] || fail "write-only metrics API did not return 503"
    [ "$(sandbox_state "$sid")" = paused ] || fail "native usage Collector woke sandbox"
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
    [ "$(sandbox_state "$sid")" = paused ] || fail "Collector shutdown changed sandbox state"
    echo "==> PASS: real paused native usage -> conductor lease -> sandboxstats -> batch -> HTTP exporter; saved time and cumulative CPU/memory preserved"
}
