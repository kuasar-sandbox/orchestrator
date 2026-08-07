#!/usr/bin/env bash
# Shared real-guest/admin assertions for initial, rotated, and deleted values.

MMDS_SECRET_NAME="${MMDS_SECRET_NAME:-e2e_secret}"
MMDS_SECRET_PATH="${MMDS_SECRET_PATH:-/e2e/secret}"
MMDS_UNRESOLVED_PATH="${MMDS_UNRESOLVED_PATH:-/e2e/unresolved}"
MMDS_SECRET_CONTENT_TYPE="${MMDS_SECRET_CONTENT_TYPE:-application/x-kuasar-e2e-secret}"
MMDS_SECRET_INITIAL_VALUE="${MMDS_SECRET_INITIAL_VALUE:-MMDS_SECRET_INITIAL_GUEST_E2E}"

mmds_secret_admin_request() {
    local socket="$1" method="$2" sid="$3" name="$4" body="${5:-}" output="$6"
    local args=(--silent --show-error --noproxy '*' --max-time 10
        --unix-socket "$socket" -o "$output" -w '%{http_code}' -X "$method")
    if [ "$method" = PUT ]; then
        # Content-Type is deliberately arbitrary: value-level content type does
        # not exist and the immutable route declaration remains authoritative.
        printf '%s' "$body" | curl "${args[@]}" -H 'Content-Type: application/octet-stream' \
            --data-binary @- "http://localhost/internal/admin/sandboxes/$sid/mmds/secrets/$name"
    else
        curl "${args[@]}" "http://localhost/internal/admin/sandboxes/$sid/mmds/secrets/$name"
    fi
}

mmds_secret_assert_not_plaintext_at_rest() {
    local root="$1" value="$2"
    ! grep -R -a -F -q -- "$value" "$root"
}

mmds_secret_guest_command() {
    local path="$1" expected_status="$2" expected_body="$3" expected_content_type="$4" marker="$5"
    python3 - "$path" "$expected_status" "$expected_body" "$expected_content_type" "$marker" <<'PY'
import base64, hashlib, json, sys

path, expected_status, expected_body, content_type, marker = sys.argv[1:]
expected_hash = hashlib.sha256(expected_body.encode()).hexdigest()
program = f'''
import hashlib, http.client

def request(method, target, token=None):
    conn = http.client.HTTPConnection("169.254.169.254", 80, timeout=10)
    headers = {{}}
    if method == "PUT":
        headers["X-metadata-token-ttl-seconds"] = "60"
    if token is not None:
        headers["X-metadata-token"] = token
    conn.request(method, target, headers=headers)
    response = conn.getresponse()
    result = response.status, {{k.lower(): v for k, v in response.getheaders()}}, response.read()
    conn.close()
    return result

status, headers, token_body = request("PUT", "/latest/api/token")
assert status == 200, ("token", status)
status, headers, body = request("GET", {json.dumps(path)}, token_body.decode())
assert status == {int(expected_status)}, ("secret", status, headers)
assert headers.get("cache-control") == "no-store", headers
assert headers.get("x-content-type-options") == "nosniff", headers
if status == 200:
    assert hashlib.sha256(body).hexdigest() == {json.dumps(expected_hash)}, "secret value digest mismatch"
    assert headers.get("content-type") == {json.dumps(content_type)}, headers
print({json.dumps(marker)})
'''
print("python3 -c 'import base64; exec(base64.b64decode(\"%s\"))'" %
      base64.b64encode(program.encode()).decode())
PY
}

run_mmds_secret_guest_get() {
    local envd_exec="$1" envd_sock="$2" envd_token="$3" work="$4" mode="$5"
    local path="$6" expected_status="$7" expected_body="$8" content_type="$9" marker="${10}"
    local command output="$work/mmds-secret-guest-$mode.out"
    command="$(mmds_secret_guest_command "$path" "$expected_status" "$expected_body" "$content_type" "$marker")"
    for _ in $(seq 1 30); do
        python3 "$envd_exec" "$envd_sock" "$envd_token" "$command" >"$output" 2>&1 || true
        if grep -q "$marker" "$output" && grep -q 'EXIT_CODE 0' "$output"; then
            sed 's/^/  mmds-secret-guest| /' "$output"
            return 0
        fi
        sleep 0.25
    done
    sed 's/^/  mmds-secret-guest| /' "$output" >&2
    return 1
}

run_mmds_secret_standalone_e2e() {
    local socket="$1" sid="$2" envd_exec="$3" envd_sock="$4" envd_token="$5" work="$6" mode="$7"
    local response="$work/mmds-secret-admin-$mode.out" code
    local updated="MMDS_SECRET_UPDATED_GUEST_E2E" rotated="MMDS_SECRET_ROTATED_GUEST_E2E"

    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-initial" \
        "$MMDS_SECRET_PATH" 200 "$MMDS_SECRET_INITIAL_VALUE" "$MMDS_SECRET_CONTENT_TYPE" MMDS_SECRET_INITIAL_OK
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-unresolved" \
        "$MMDS_UNRESOLVED_PATH" 404 "" "" MMDS_SECRET_UNRESOLVED_OK
    mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$MMDS_SECRET_INITIAL_VALUE"

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" "$MMDS_SECRET_NAME" "$updated" "$response")"
    [ "$code" = 204 ]
    mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$updated"
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-updated" \
        "$MMDS_SECRET_PATH" 200 "$updated" "$MMDS_SECRET_CONTENT_TYPE" MMDS_SECRET_UPDATED_OK

    if [ -n "${MMDS_SECRET_AFTER_UPDATE_HOOK:-}" ]; then
        "$MMDS_SECRET_AFTER_UPDATE_HOOK"
        run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-resynced" \
            "$MMDS_SECRET_PATH" 200 "$updated" "$MMDS_SECRET_CONTENT_TYPE" MMDS_SECRET_RESYNCED_OK
    fi

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" "$MMDS_SECRET_NAME" "$rotated" "$response")"
    [ "$code" = 204 ]
    mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$rotated"
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-rotated" \
        "$MMDS_SECRET_PATH" 200 "$rotated" "$MMDS_SECRET_CONTENT_TYPE" MMDS_SECRET_ROTATED_OK

    code="$(mmds_secret_admin_request "$socket" DELETE "$sid" "$MMDS_SECRET_NAME" "" "$response")"
    [ "$code" = 204 ]
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-deleted" \
        "$MMDS_SECRET_PATH" 404 "" "" MMDS_SECRET_DELETED_OK
    code="$(mmds_secret_admin_request "$socket" DELETE "$sid" "$MMDS_SECRET_NAME" "" "$response")"
    [ "$code" = 204 ]

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" undeclared "$updated" "$response")"
    [ "$code" = 400 ]
}
