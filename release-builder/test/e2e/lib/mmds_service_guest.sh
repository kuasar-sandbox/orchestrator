#!/usr/bin/env bash
# Shared real-guest assertions for conductor-owned HTTP-over-UDS services.

MMDS_SERVICE_PATH="${MMDS_SERVICE_PATH:-/e2e/service}"
MMDS_SERVICE_NAME="${MMDS_SERVICE_NAME:-e2e_service}"

start_mmds_service_backend() {
    local work="$1"
    MMDS_SERVICE_SOCKET="$work/mmds-service.sock"
    MMDS_SERVICE_STATE="$work/mmds-service.state"
    MMDS_SERVICE_REQUESTS="$work/mmds-service.requests"
    export MMDS_SERVICE_SOCKET MMDS_SERVICE_STATE MMDS_SERVICE_REQUESTS
    [ -f "$MMDS_SERVICE_STATE" ] || printf '%s\n' ok >"$MMDS_SERVICE_STATE"
    [ -f "$MMDS_SERVICE_REQUESTS" ] || : >"$MMDS_SERVICE_REQUESTS"

    python3 - "$MMDS_SERVICE_SOCKET" "$MMDS_SERVICE_STATE" "$MMDS_SERVICE_REQUESTS" <<'PY' &
import http.server, json, os, socketserver, sys, time
socket_path, state_path, requests_path = sys.argv[1:]
try:
    os.unlink(socket_path)
except FileNotFoundError:
    pass

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *_):
        pass
    def do_GET(self):
        headers = {key.lower(): value for key, value in self.headers.items()}
        with open(requests_path, "a", encoding="utf-8") as log:
            log.write(json.dumps({"method": self.command, "path": self.path,
                                  "headers": headers}, sort_keys=True) + "\n")
        state = open(state_path, encoding="utf-8").read().strip()
        if state == "slow":
            time.sleep(3)
            status, content_type, body, location = 200, "text/plain", b"late", None
        elif state == "oversized":
            status, content_type, body, location = 200, "text/plain", b"x" * 65537, None
        elif state == "missing_content_type":
            status, content_type, body, location = 200, None, b"MMDS_SERVICE_DEFAULT_TYPE", None
        elif state == "redirect":
            status, content_type, body, location = 302, "text/plain", b"MMDS_SERVICE_REDIRECT", "/must-not-follow"
        else:
            status, content_type, body, location = 200, "application/json", json.dumps({
                "marker": "MMDS_SERVICE_GUEST_E2E",
                "sandbox_id": headers.get("e2b-sandbox-id"),
                "service": headers.get("e2b-sandbox-service"),
            }, sort_keys=True).encode(), None
        try:
            self.send_response(status)
            if content_type is not None:
                self.send_header("Content-Type", content_type)
            if location is not None:
                self.send_header("Location", location)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
with Server(socket_path, Handler) as server:
    server.serve_forever()
PY
    MMDS_SERVICE_BACKEND_PID=$!
    export MMDS_SERVICE_BACKEND_PID
    for _ in $(seq 1 40); do
        [ -S "$MMDS_SERVICE_SOCKET" ] && return 0
        kill -0 "$MMDS_SERVICE_BACKEND_PID" 2>/dev/null || return 1
        sleep 0.05
    done
    return 1
}

stop_mmds_service_backend() {
    if [ -n "${MMDS_SERVICE_BACKEND_PID:-}" ]; then
        kill -TERM "$MMDS_SERVICE_BACKEND_PID" 2>/dev/null || true
        wait "$MMDS_SERVICE_BACKEND_PID" 2>/dev/null || true
        MMDS_SERVICE_BACKEND_PID=""
    fi
}

restart_mmds_service_backend() {
    local work="$1"
    stop_mmds_service_backend
    start_mmds_service_backend "$work"
}

mmds_service_guest_command() {
    local expected_status="$1" state="$2" marker="$3"
    python3 - "$MMDS_SERVICE_PATH" "$MMDS_SERVICE_NAME" "$expected_status" "$state" "$marker" <<'PY'
import base64, json, sys
path, service, expected_status, state, marker = sys.argv[1:]
program = f'''
import http.client, json

def request(method, target, token=None, body=None):
    conn = http.client.HTTPConnection("169.254.169.254", 80, timeout=10)
    headers = {{
        "Host": "guest-controlled.invalid",
        "Authorization": "must-not-forward",
        "Cookie": "must-not-forward",
        "E2b-Sandbox-Id": "must-not-forward",
        "E2b-Sandbox-Service": "must-not-forward",
        "X-Kuasar-Guest": "must-not-forward",
    }}
    if method == "PUT":
        headers["X-metadata-token-ttl-seconds"] = "60"
    if token is not None:
        headers["X-metadata-token"] = token
    conn.request(method, target, body=body, headers=headers)
    response = conn.getresponse()
    result = response.status, {{k.lower(): v for k, v in response.getheaders()}}, response.read()
    conn.close()
    return result

status, headers, token_body = request("PUT", "/latest/api/token")
assert status == 200, ("token", status, token_body)
token = token_body.decode()
status, headers, body = request("GET", {json.dumps(path)} + "?guest=1", token)
assert status == 400, ("query", status, headers, body)
status, headers, body = request("GET", {json.dumps(path)}, token, b"guest-body")
assert status == 400, ("body", status, headers, body)
status, headers, body = request("GET", {json.dumps(path)}, token)
assert status == {int(expected_status)}, ("service", status, headers, body)
assert headers.get("cache-control") == "no-store", headers
assert headers.get("x-content-type-options") == "nosniff", headers
if {json.dumps(state)} == "ok":
    assert headers.get("content-type") == "application/json", headers
    payload = json.loads(body)
    assert payload["marker"] == "MMDS_SERVICE_GUEST_E2E", payload
    assert payload["sandbox_id"] and payload["service"] == {json.dumps(service)}, payload
elif {json.dumps(state)} == "missing_content_type":
    assert headers.get("content-type") == "text/plain", headers
    assert body == b"MMDS_SERVICE_DEFAULT_TYPE", body
elif {json.dumps(state)} == "redirect":
    assert body == b"MMDS_SERVICE_REDIRECT", body
    assert "location" not in headers, headers
print({json.dumps(marker)})
'''
print("python3 -c 'import base64; exec(base64.b64decode(\"%s\"))'" %
      base64.b64encode(program.encode()).decode())
PY
}

run_mmds_service_guest_get() {
    local envd_exec="$1" envd_sock="$2" envd_token="$3" work="$4" mode="$5"
    local expected_status="$6" state="$7" marker="$8"
    local command output="$work/mmds-service-guest-$mode-$state.out"
    command="$(mmds_service_guest_command "$expected_status" "$state" "$marker")"
    for _ in $(seq 1 20); do
        python3 "$envd_exec" "$envd_sock" "$envd_token" "$command" >"$output" 2>&1 || true
        if grep -q "$marker" "$output" && grep -q 'EXIT_CODE 0' "$output"; then
            sed 's/^/  mmds-service-guest| /' "$output"
            return 0
        fi
        sleep 0.25
    done
    sed 's/^/  mmds-service-guest| /' "$output" >&2
    return 1
}

mmds_service_assert_requests() {
    local requests="$1" sid="$2"
    python3 - "$requests" "$sid" "$MMDS_SERVICE_PATH" "$MMDS_SERVICE_NAME" <<'PY'
import json, sys
requests, sid, path, service = sys.argv[1:]
rows = [json.loads(line) for line in open(requests, encoding="utf-8") if line.strip()]
assert rows, "service backend received no request"
for row in rows:
    assert row["method"] == "GET" and row["path"] == path, row
    headers = row["headers"]
    assert set(headers) == {"host", "e2b-sandbox-id", "e2b-sandbox-service"}, headers
    assert headers["host"] == "mmds-service", headers
    assert headers["e2b-sandbox-id"] == sid, headers
    assert headers["e2b-sandbox-service"] == service, headers
PY
}

run_mmds_service_standalone_e2e() {
    local sid="$1" envd_exec="$2" envd_sock="$3" envd_token="$4" work="$5" mode="$6"
    printf '%s\n' ok >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode" \
        200 ok MMDS_SERVICE_OK
    printf '%s\n' missing_content_type >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-default" \
        200 missing_content_type MMDS_SERVICE_DEFAULT_TYPE_OK
    printf '%s\n' redirect >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-redirect" \
        302 redirect MMDS_SERVICE_REDIRECT_OK
    printf '%s\n' slow >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-timeout" \
        504 slow MMDS_SERVICE_TIMEOUT_OK
    printf '%s\n' oversized >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-oversized" \
        502 oversized MMDS_SERVICE_OVERSIZED_OK

    stop_mmds_service_backend
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-unavailable" \
        503 unavailable MMDS_SERVICE_UNAVAILABLE_OK
    restart_mmds_service_backend "$work"
    printf '%s\n' ok >"$MMDS_SERVICE_STATE"
    run_mmds_service_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-recovered" \
        200 ok MMDS_SERVICE_RECOVERED_OK
    mmds_service_assert_requests "$MMDS_SERVICE_REQUESTS" "$sid"
}
