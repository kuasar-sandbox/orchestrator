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
