#!/usr/bin/env bash
#
# Shared real-VM MMDS static-route guest assertion library.
#
# Scenario implemented today: declaration + static route.
#   - mint an MMDSv2 token from the guest;
#   - GET /e2e/static and verify status, body, content type, and security headers;
#   - verify an undeclared path is 404;
#   - verify trailing slash, empty segment, percent escape, and query variants
#     are rejected with 400 and are never redirected.
#
# Add reusable static-route assertions here as that backend gains lifecycle
# scenarios. Other backends (for example secret or service) keep their own
# helpers; topology-specific setup remains in the internal, external, or
# cluster suite entry.
#
# Source this file after envd_exec.py has been created, then call:
#   run_mmds_static_guest_get <envd-exec.py> <envd.sock> <envd-token> <work-dir> <mode>
#
# The command itself runs inside the guest. It therefore exercises the real
# 169.254.169.254 -> vswitch mgmt-service -> MMDS server path rather than a
# host-side HTTP shortcut.

# Scenario: static route -- build the guest-side HTTP assertion command.
mmds_static_guest_command() {
    local guest_program
    guest_program="$(python3 - <<'PY'
import base64

program = r'''
import http.client

HOST = "169.254.169.254"
MARKER = "MMDS_STATIC_GUEST_E2E"

def request(method, target, token=None):
    conn = http.client.HTTPConnection(HOST, 80, timeout=10)
    headers = {}
    if method == "PUT":
        headers["X-metadata-token-ttl-seconds"] = "60"
    if token is not None:
        headers["X-metadata-token"] = token
    conn.request(method, target, headers=headers)
    response = conn.getresponse()
    body = response.read()
    result = response.status, {k.lower(): v for k, v in response.getheaders()}, body
    conn.close()
    return result

status, headers, body = request("PUT", "/latest/api/token")
assert status == 200, ("token status", status, body)
token = body.decode()
assert token, "empty MMDS token"

status, headers, body = request("GET", "/e2e/static", token)
assert status == 200, ("static status", status, body)
assert body == MARKER.encode(), ("static body", body)
assert headers.get("content-type") == "text/plain", headers
assert headers.get("cache-control") == "no-store", headers
assert headers.get("x-content-type-options") == "nosniff", headers

for target, expected_status in (
    ("/e2e/undeclared", 404),
    ("/e2e/static/", 400),
    ("/e2e//static", 400),
    ("/e2e%2Fstatic", 400),
    ("/e2e/static?query=1", 400),
):
    status, headers, body = request("GET", target, token)
    assert status == expected_status, (target, status, headers, body)
    assert "location" not in headers, (target, "redirected", headers)
    assert headers.get("cache-control") == "no-store", (target, headers)
    assert headers.get("x-content-type-options") == "nosniff", (target, headers)

print("MMDS_STATIC_GUEST_GET_OK")
'''
print(base64.b64encode(program.encode()).decode())
PY
)"
    printf "python3 -c 'import base64; exec(base64.b64decode(\"%s\"))'\n" "$guest_program"
}

# Scenario: static route -- execute the assertion through envd.
run_mmds_static_guest_get() {
    local envd_exec="$1" envd_sock="$2" envd_token="$3" work="$4" mode="$5"
    local guest_command output

    guest_command="$(mmds_static_guest_command)"
    output="$work/mmds-static-guest-$mode.out"
    python3 "$envd_exec" "$envd_sock" "$envd_token" "$guest_command" >"$output" 2>&1 || true
    sed 's/^/  mmds-static-guest| /' "$output"
    grep -q 'MMDS_STATIC_GUEST_GET_OK' "$output" || return 1
    grep -q 'EXIT_CODE 0' "$output" || return 1
}
