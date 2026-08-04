#!/usr/bin/env bash

# Test-only helpers for sandbox-ctl's one-shot readiness descriptor. Application
# stdout/stderr remain on their normal log sink; only the inherited pipe reaches
# READY_CAPTURE_FILE.

readiness_begin_capture() {
    READY_CAPTURE_FILE="$1"
    : > "$READY_CAPTURE_FILE"
    exec {READY_WRITE_FD}> >(cat > "$READY_CAPTURE_FILE")
    READY_READER_PID=$!
}

readiness_close_parent_writer() {
    exec {READY_WRITE_FD}>&-
}

# Background this function to make the target a session leader without leaving
# a wrapper process that owns inherited descriptors. Cloud Hypervisor moves to
# its own process group, but remains in this session, so a hard timeout can
# still clean up the complete sandbox process tree.
readiness_exec_in_new_session() {
    exec python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "$@"
}

readiness_kill_session() {
    local signal="$1" session_id="$2"
    pkill "-$signal" -s "$session_id" 2>/dev/null || true
}

# Start this only after readiness_close_parent_writer. Unlike wrapping the run
# in GNU timeout, this watchdog never inherits the readiness writer, so
# sandbox-ctl closing its owned descriptor produces EOF immediately at ready.
readiness_start_watchdog() {
    local target_pid="$1" timeout_s="$2" grace_s="$3"
    if [ -n "${READINESS_WATCHDOG_PID:-}" ]; then
        echo "readiness watchdog is already running" >&2
        return 1
    fi
    readiness_watchdog_worker "$target_pid" "$timeout_s" "$grace_s" &
    READINESS_WATCHDOG_PID=$!
}

readiness_watchdog_worker() {
    local target_pid="$1" timeout_s="$2" grace_s="$3" sleep_pid=""
    trap 'if [ -n "$sleep_pid" ]; then kill "$sleep_pid" 2>/dev/null || true; wait "$sleep_pid" 2>/dev/null || true; fi; exit 0' TERM INT

    sleep "$timeout_s" &
    sleep_pid=$!
    if ! wait "$sleep_pid"; then
        exit 0
    fi
    sleep_pid=""
    kill -TERM "$target_pid" 2>/dev/null || exit 0

    sleep "$grace_s" &
    sleep_pid=$!
    if ! wait "$sleep_pid"; then
        exit 0
    fi
    sleep_pid=""
    readiness_kill_session KILL "$target_pid"
}

readiness_stop_watchdog() {
    local watchdog_pid="${READINESS_WATCHDOG_PID:-}"
    [ -n "$watchdog_pid" ] || return 0
    kill -TERM "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    READINESS_WATCHDOG_PID=""
}

readiness_wait_event() {
    local file="$1" line_no="$2" want="$3" run_pid="$4" attempts="${5:-1200}"
    local got=""
    for _ in $(seq 1 "$attempts"); do
        got="$(sed -n "${line_no}p" "$file" 2>/dev/null || true)"
        if [ -n "$got" ]; then
            [ "$got" = "$want" ] || {
                echo "readiness event $line_no = [$got], want [$want]" >&2
                return 1
            }
            return 0
        fi
        if ! kill -0 "$run_pid" 2>/dev/null; then
            echo "sandbox run exited before readiness event $line_no ($want)" >&2
            return 1
        fi
        sleep 0.05
    done
    echo "timed out waiting for readiness event $line_no ($want)" >&2
    return 1
}

readiness_assert_wire() {
    local file="$1" reader_pid="$2" expected="$3" attempts="${4:-200}"
    for _ in $(seq 1 "$attempts"); do
        kill -0 "$reader_pid" 2>/dev/null || break
        sleep 0.01
    done
    if kill -0 "$reader_pid" 2>/dev/null; then
        echo "readiness descriptor did not reach EOF" >&2
        return 1
    fi
    wait "$reader_pid" 2>/dev/null || true
    if ! cmp -s <(printf '%s' "$expected") "$file"; then
        echo "unexpected readiness wire:" >&2
        od -An -tx1 "$file" >&2
        return 1
    fi
}

readiness_connect_ctl() {
    local path="$1"
    python3 - "$path" <<'PY'
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(2)
s.connect(sys.argv[1])
s.close()
PY
}
