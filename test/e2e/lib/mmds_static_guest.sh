#!/usr/bin/env bash
# Shared real-guest assertions for a declared static MMDS exact route.

mmds_static_guest_command() {
    python3 - <<'PY'
import base64

program = r'''
import http.client

def request(method, target, token=None, body=None, headers=None):
    conn = http.client.HTTPConnection("169.254.169.254", 80, timeout=10)
    request_headers = dict(headers or {})
    if method == "PUT":
        request_headers["X-metadata-token-ttl-seconds"] = "60"
    if token is not None:
        request_headers["X-metadata-token"] = token
    conn.request(method, target, body=body, headers=request_headers)
    response = conn.getresponse()
    result = response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    conn.close()
    return result

status, headers, body = request("PUT", "/latest/api/token")
assert status == 200, ("token", status, body)
token = body.decode()
assert token, "empty MMDS token"

status, headers, body = request("GET", "/e2e/static", token)
assert status == 200, ("static", status, body)
assert body == b"MMDS_STATIC_GUEST_E2E", body
# content_type is omitted in the declaration: text/plain is a runtime default.
assert headers.get("content-type") == "text/plain", headers
assert headers.get("cache-control") == "no-store", headers
assert headers.get("x-content-type-options") == "nosniff", headers

for target, expected in (
    ("/e2e/undeclared", 404),
    ("/e2e/static/", 400),
    ("/e2e//static", 400),
    ("/e2e%2Fstatic", 400),
    ("/e2e/static?query=1", 400),
):
    status, headers, body = request("GET", target, token)
    assert status == expected, (target, status, headers, body)
    assert "location" not in headers, (target, headers)
    assert headers.get("cache-control") == "no-store", (target, headers)
    assert headers.get("x-content-type-options") == "nosniff", (target, headers)

print("MMDS_STATIC_GUEST_GET_OK")
'''
print(base64.b64encode(program.encode()).decode())
PY
}

run_mmds_static_guest_get() {
    local envd_exec="$1" envd_sock="$2" envd_token="$3" work="$4" mode="$5"
    local command output="$work/mmds-static-guest-$mode.out"
    command="$(mmds_static_guest_command)"
    python3 "$envd_exec" "$envd_sock" "$envd_token" \
        "python3 -c 'import base64; exec(base64.b64decode(\"$command\"))'" \
        >"$output" 2>&1 || true
    sed 's/^/  mmds-static-guest| /' "$output"
    grep -q 'MMDS_STATIC_GUEST_GET_OK' "$output" && grep -q 'EXIT_CODE 0' "$output"
}
