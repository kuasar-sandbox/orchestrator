#!/usr/bin/env bash
start_conductor() {
    "$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >>"$WORK/orch.log" 2>&1 &
    CONDUCTOR_PID=$!
    PIDS+=("$CONDUCTOR_PID")
    for _ in $(seq 1 60); do
        if [ -S "$WORK/node-ctl.socket" ] && \
            curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" \
            -H "Host: api.$DOMAIN" 2>/dev/null; then
            return 0
        fi
        kill -0 "$CONDUCTOR_PID" 2>/dev/null \
            || { sed 's/^/    /' "$WORK/orch.log"; fail "orchestrator serve exited"; }
        sleep 0.5
    done
    fail "orchestrator health endpoint did not become ready"
}
stop_conductor() {
    [ -n "$CONDUCTOR_PID" ] || return 0
    local stopped_pid="$CONDUCTOR_PID" index
    kill "$stopped_pid" 2>/dev/null || true
    for _ in $(seq 1 100); do
        kill -0 "$stopped_pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$stopped_pid" 2>/dev/null; then
        kill -KILL "$stopped_pid" 2>/dev/null || true
    fi
    wait "$stopped_pid" 2>/dev/null || true
    for index in "${!PIDS[@]}"; do
        [ "${PIDS[$index]}" != "$stopped_pid" ] || PIDS[index]=""
    done
    CONDUCTOR_PID=""
}
crash_conductor() {
    # Kill only this test's conductor. The live Build unit and its guest must
    # survive, retaining their existing durable ownership for reconciliation.
    local stopped_pid="$CONDUCTOR_PID" index
    [ -n "$stopped_pid" ] || fail "no test conductor to crash"
    kill -KILL "$stopped_pid" || fail "crash test conductor $stopped_pid"
    wait "$stopped_pid" 2>/dev/null || true
    for index in "${!PIDS[@]}"; do
        [ "${PIDS[$index]}" != "$stopped_pid" ] || PIDS[index]=""
    done
    CONDUCTOR_PID=""
}
configure_checkpoint_policy() { # mode remote-manifest
    python3 - "$WORK/config.yaml" "$1" "$2" "$REF_LOCATION_PARENT" <<'PY'
import pathlib, sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
head, marker, _ = text.partition("\ncheckpoint:")
assert marker, "checkpoint config marker is missing"
path.write_text(
    head
    + "\ncheckpoint:\n"
    + f"  mode: {sys.argv[2]}\n"
    + "  remote:\n"
    + f"    ref_location_parent: {sys.argv[4]}\n"
    + f"    manifest: {sys.argv[3]}\n"
)
PY
}
restart_conductor() { # mode remote-manifest
    stop_conductor
    configure_checkpoint_policy "$1" "$2"
    start_conductor
    echo "==> conductor restarted (checkpoint.mode=$1, parent configured, remote.manifest=$2)"
}
