#!/usr/bin/env bash
# Actual conductor -> sandboxstats -> native Collector -> host HTTP exporter.
# The owning real guest suites supply WORK, PIDS, BIN, req and fail.

start_stats_sink() {
    local -a pid_slots
    : >"$WORK/telemetry-native.jsonl"
    rm -f "$WORK/telemetry-native.port"
    python3 "$SCRIPT_DIR/telemetry_probe.py" serve-metrics \
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
        if python3 "$SCRIPT_DIR/telemetry_probe.py" check-native \
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
