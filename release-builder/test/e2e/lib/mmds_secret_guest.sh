#!/usr/bin/env bash
#
# Shared MMDS secret-backend E2E helpers.
#
# The topology-specific suites create a sandbox whose kuasar-sandbox.mmds
# declaration contains secret name "e2e_secret" and route /e2e/secret. These
# helpers drive the operator-only config-socket API and assert the result from
# inside the real guest:
#
#   admin UDS PUT -> encrypted node store -> route publication/sync
#     -> 169.254.169.254 -> MMDS secret backend.
#
# The lifecycle covers initial install, overwrite, revoke, and rejection of an
# undeclared name. Secret bytes are never placed in the sandbox declaration.

MMDS_SECRET_NAME="${MMDS_SECRET_NAME:-e2e_secret}"
MMDS_SECRET_PATH="${MMDS_SECRET_PATH:-/e2e/secret}"

mmds_secret_admin_request() {
    local socket="$1" method="$2" sid="$3" name="$4" body="${5:-}" content_type="${6:-}"
    local output="$7"
    # Success is 204 No Content (per the confirmed #42 design): the new
    # revision travels on X-Kuasar-MMDS-Revision, not a JSON body, so the
    # response headers are captured alongside whatever body an error path
    # (which still carries a JSON {"error":...}) might return.
    local args=(--silent --show-error --noproxy '*' --max-time 10
        --unix-socket "$socket" -o "$output" -D "$output.headers" -w '%{http_code}' -X "$method")
    [ -n "$content_type" ] && args+=(-H "Content-Type: $content_type")
    if [ "$method" = "PUT" ]; then
        # Feed the value over stdin so it is not exposed in curl's argv.
        printf '%s' "$body" | curl "${args[@]}" --data-binary @- \
            "http://localhost/internal/admin/sandboxes/$sid/mmds/secrets/$name"
    else
        curl "${args[@]}" "http://localhost/internal/admin/sandboxes/$sid/mmds/secrets/$name"
    fi
}

mmds_secret_assert_revision() {
    local response="$1" expected="$2"
    local actual
    actual="$(tr -d '\r' <"$response.headers" | sed -n 's/^[Xx]-[Kk]uasar-[Mm][Mm][Dd][Ss]-[Rr]evision: //Ip')"
    if [ "$actual" != "$expected" ]; then
        echo "revision=${actual:-<absent>}, want $expected" >&2
        return 1
    fi
}

# The node base root contains the local metadata database. Secret values must
# be encrypted before reaching it; a raw scan is a useful end-to-end backstop
# against accidentally persisting the admin PUT body in plaintext.
mmds_secret_assert_not_plaintext_at_rest() {
    local base_root="$1" value="$2"
    ! grep -R -a -F -q -- "$value" "$base_root"
}

# Build a guest command that obtains a fresh MMDSv2 token and checks the exact
# secret route response plus the mandatory response-hardening headers.
mmds_secret_guest_command() {
    local expected_status="$1" expected_body="$2" expected_content_type="$3" marker="$4"
    python3 - "$MMDS_SECRET_PATH" "$expected_status" "$expected_body" "$expected_content_type" "$marker" <<'PY'
import base64
import json
import sys

path, expected_status, body, content_type, marker = sys.argv[1:]
program = f'''
import http.client

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
assert status == 200, ("token", status, token_body)
token = token_body.decode()
status, headers, response_body = request("GET", {json.dumps(path)}, token)
assert status == {int(expected_status)}, ("secret status", status, headers, response_body)
assert headers.get("cache-control") == "no-store", headers
assert headers.get("x-content-type-options") == "nosniff", headers
if status == 200:
    assert response_body == {body.encode()!r}, ("secret body", response_body)
    assert headers.get("content-type") == {json.dumps(content_type)}, headers
print({json.dumps(marker)})
'''
encoded = base64.b64encode(program.encode()).decode()
print(f'''python3 -c 'import base64; exec(base64.b64decode("{encoded}"))' ''')
PY
}

# Route sync is asynchronous in external mode, so retry briefly rather than
# accepting a stale value.
run_mmds_secret_guest_get() {
    local envd_exec="$1" envd_sock="$2" envd_token="$3" work="$4" mode="$5"
    local expected_status="$6" expected_body="$7" expected_content_type="$8" marker="$9"
    local command output="$work/mmds-secret-guest-$mode.out"

    command="$(mmds_secret_guest_command "$expected_status" "$expected_body" "$expected_content_type" "$marker")"
    for _ in $(seq 1 20); do
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

# Full standalone lifecycle shared by internal and external proxy modes. A
# topology may set MMDS_SECRET_BEFORE_REVOKE_HOOK to exercise restart/resume
# while the updated secret is still installed; the helper then revalidates the
# encrypted-at-rest and guest-visible value before revocation.
run_mmds_secret_standalone_e2e() {
    local socket="$1" sid="$2" envd_exec="$3" envd_sock="$4" envd_token="$5" work="$6" mode="$7"
    local response="$work/mmds-secret-admin-$mode.json" code
    local initial="MMDS_SECRET_INITIAL_$RANDOM" updated="MMDS_SECRET_UPDATED_$RANDOM"
    local restored="MMDS_SECRET_RESTORED_$RANDOM" revoke_revision=3

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" "$MMDS_SECRET_NAME" "$initial" \
        "text/plain" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 1 || return 1
    mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$initial" || return 1
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-initial" \
        200 "$initial" "text/plain" "MMDS_SECRET_INITIAL_OK" || return 1

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" "$MMDS_SECRET_NAME" "$updated" \
        "application/x-kuasar-e2e-secret" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 2 || return 1
    mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$updated" || return 1
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-updated" \
        200 "$updated" "application/x-kuasar-e2e-secret" "MMDS_SECRET_UPDATED_OK" || return 1

    if [ -n "${MMDS_SECRET_BEFORE_REVOKE_HOOK:-}" ]; then
        "$MMDS_SECRET_BEFORE_REVOKE_HOOK" || return 1
        mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$updated" || return 1
        run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-preserved" \
            200 "$updated" "application/x-kuasar-e2e-secret" "MMDS_SECRET_PRESERVED_OK" || return 1

        # Write a distinct value through the restarted conductor. Revision
        # continuity proves it recovered the encrypted row (rather than creating
        # a fresh secret), and the guest read also covers a post-snapshot delta.
        code="$(mmds_secret_admin_request "$socket" PUT "$sid" "$MMDS_SECRET_NAME" "$restored" \
            "application/x-kuasar-e2e-restored" "$response")"
        [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 3 || return 1
        mmds_secret_assert_not_plaintext_at_rest "$work/lib" "$restored" || return 1
        run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-restored" \
            200 "$restored" "application/x-kuasar-e2e-restored" "MMDS_SECRET_RESTORED_OK" || return 1
        revoke_revision=4
    fi

    code="$(mmds_secret_admin_request "$socket" DELETE "$sid" "$MMDS_SECRET_NAME" "" "" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" "$revoke_revision" || return 1
    run_mmds_secret_guest_get "$envd_exec" "$envd_sock" "$envd_token" "$work" "$mode-revoked" \
        404 "" "" "MMDS_SECRET_REVOKED_OK" || return 1

    code="$(mmds_secret_admin_request "$socket" PUT "$sid" undeclared "$initial" \
        "text/plain" "$response")"
    [ "$code" = "400" ] || return 1
}
