#!/usr/bin/env bash
# Uses the existing real Proxy/envd fixture; no second lifecycle or network setup.
. "$SCRIPT_DIR/telemetry_stats.sh"

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
        if [ "$code" = 200 ] && python3 "$SCRIPT_DIR/telemetry_probe.py" check-history "$WORK/resp.body" 2>/dev/null; then
            return 0
        fi
        process_live "$TELEMETRY_PID" || { dump_logs; fail "telemetry exited before readable history"; }
        sleep 0.1
    done
    dump_logs
    fail "telemetry envd history did not become readable (status=$code)"
}
