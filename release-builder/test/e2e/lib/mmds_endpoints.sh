#!/usr/bin/env bash

# Complete MMDS store-endpoint assertions shared by the internal and external
# real-VM e2e entry points. The caller provides WORK, SID, ENVD_SOCK,
# ENVD_TOKEN, CONFIG_SOCKET, and fail(), and has already declared user-data at
# /latest/user-data on sandbox Create.
mmds_relay_setup() {
    command -v openssl >/dev/null 2>&1 || skip "openssl not found (MMDS relay HTTPS upstream)"
    RELAY_IP="${RELAY_IP:-11.0.0.1}"
    RELAY_HOST="${RELAY_HOST:-relay.mmds-e2e.invalid}"
    RELAY_PORT="$(free_port)"
    RELAY_URL="https://$RELAY_HOST:$RELAY_PORT/credentials"
    RELAY_CA="$WORK/mmds-relay-cert.pem"
    RELAY_AUDIT="$WORK/mmds-relay-auth.audit"
    RELAY_HOSTS_MARKER="kuasar-mmds-e2e-$RANDOM"

    ip addr add "$RELAY_IP/32" dev lo
    printf '%s %s # %s\n' "$RELAY_IP" "$RELAY_HOST" "$RELAY_HOSTS_MARKER" >> /etc/hosts
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "$WORK/mmds-relay-key.pem" -out "$RELAY_CA" \
        -subj "/CN=$RELAY_HOST" \
        -addext "subjectAltName=DNS:$RELAY_HOST" \
        -addext "basicConstraints=critical,CA:TRUE" \
        >"$WORK/mmds-relay-cert.log" 2>&1 \
        || { sed 's/^/  relay-cert| /' "$WORK/mmds-relay-cert.log"; fail "MMDS relay certificate generation"; }

    cat > "$WORK/mmds_relay_upstream.py" <<'PY'
import http.server
import ssl
import sys

listen, port, cert, key, audit = sys.argv[1:]
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        auth = self.headers.get("X-MMDS-E2E-Auth", "")
        with open(audit, "w") as output:
            output.write(auth)
        if self.path != "/credentials" or not auth:
            self.send_response(401)
            self.end_headers()
            return
        body = b'{"result":"RELAY_UPSTREAM_OK"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Upstream-Must-Be-Dropped", "not-for-the-guest")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *_):
        pass

server = http.server.ThreadingHTTPServer((listen, int(port)), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
PY
    python3 "$WORK/mmds_relay_upstream.py" "$RELAY_IP" "$RELAY_PORT" \
        "$RELAY_CA" "$WORK/mmds-relay-key.pem" "$RELAY_AUDIT" \
        >"$WORK/mmds-relay-upstream.log" 2>&1 &
    PIDS+=($!)
    for _ in $(seq 1 40); do
        curl -sSk --noproxy '*' --max-time 1 -o /dev/null "$RELAY_URL" 2>/dev/null && break
        kill -0 "${PIDS[-1]}" 2>/dev/null || fail "MMDS relay HTTPS upstream exited during startup"
        sleep 0.25
    done
    kill -0 "${PIDS[-1]}" 2>/dev/null || fail "MMDS relay HTTPS upstream failed to start"
    export SSL_CERT_FILE="$RELAY_CA"
}

mmds_relay_cleanup() {
    if [ -n "${RELAY_HOSTS_MARKER:-}" ]; then
        sed -i "/# $RELAY_HOSTS_MARKER$/d" /etc/hosts 2>/dev/null || true
    fi
    if [ -n "${RELAY_IP:-}" ]; then
        ip addr del "$RELAY_IP/32" dev lo 2>/dev/null || true
    fi
}

mmds_endpoints_e2e() {
    local admin_path="/internal/admin/sandboxes/$SID/mmds/user-data"
    local value1="MMDS_STORE_VALUE_1_$RANDOM"
    local value2="MMDS_STORE_VALUE_2_$RANDOM"
    local code

    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-admin.body" \
        -w '%{http_code}' -X PUT -H 'Content-Type: text/plain; charset=utf-8' \
        --data-binary "$value1" "http://localhost$admin_path")
    [ "$code" = "204" ] || { cat "$WORK/mmds-admin.body"; fail "MMDS admin initial PUT=$code (want 204)"; }

    cat > "$WORK/mmds_guest.py" <<'PY'
import sys
import urllib.error
import urllib.request

path = sys.argv[1]
expected = sys.argv[2] if len(sys.argv) > 2 else ""
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
token_req = urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT")
token_req.add_header("X-metadata-token-ttl-seconds", "21600")
with opener.open(token_req, timeout=10) as response:
    token = response.read().decode()
    assert response.status == 200, response.status
    assert response.headers["X-metadata-token-ttl-seconds"] == "21600"

get_req = urllib.request.Request("http://169.254.169.254" + path)
get_req.add_header("X-metadata-token", token)
try:
    with opener.open(get_req, timeout=10) as response:
        body = response.read().decode()
        print("MMDS_STATUS", response.status)
        print("MMDS_BODY", body)
        print("MMDS_CONTENT_TYPE", response.headers.get("Content-Type"))
        print("MMDS_REVISION", response.headers.get("X-Kuasar-MMDS-Revision"))
        print("MMDS_CACHE_CONTROL", response.headers.get("Cache-Control"))
        print("MMDS_NOSNIFF", response.headers.get("X-Content-Type-Options"))
        print("MMDS_UPSTREAM_DROPPED", response.headers.get("X-Upstream-Must-Be-Dropped"))
        if expected:
            assert body == expected, (body, expected)
except urllib.error.HTTPError as error:
    print("MMDS_STATUS", error.code)
    print("MMDS_BODY", error.read().decode())
PY
    local guest_script
    guest_script="$(base64 -w0 "$WORK/mmds_guest.py")"
    run_mmds_guest() {
        local path="$1" expected="$2" output="$3"
        python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
            "python3 -c \"import base64;exec(base64.b64decode('$guest_script'))\" '$path' '$expected'" \
            >"$output" 2>&1 || true
    }

    run_mmds_guest "/latest/user-data" "$value1" "$WORK/mmds-get-1.out"
    sed 's/^/  mmds| /' "$WORK/mmds-get-1.out"
    grep -q 'MMDS_STATUS 200' "$WORK/mmds-get-1.out" || fail "guest MMDS initial GET did not return 200"
    grep -q "MMDS_BODY $value1" "$WORK/mmds-get-1.out" || fail "guest MMDS initial GET returned the wrong value"
    grep -q 'MMDS_CONTENT_TYPE text/plain; charset=utf-8' "$WORK/mmds-get-1.out" || fail "guest MMDS Content-Type was not preserved"
    grep -q 'MMDS_REVISION 1' "$WORK/mmds-get-1.out" || fail "guest MMDS initial revision != 1"
    grep -q 'MMDS_CACHE_CONTROL no-store' "$WORK/mmds-get-1.out" || fail "guest MMDS response missing no-store"
    grep -q 'MMDS_NOSNIFF nosniff' "$WORK/mmds-get-1.out" || fail "guest MMDS response missing nosniff"
    echo "==> PASS: MMDS Create -> admin PUT -> guest token/GET"

    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-admin.body" \
        -w '%{http_code}' -X PUT -H 'Content-Type: text/plain; charset=utf-8' \
        --data-binary "$value2" "http://localhost$admin_path")
    [ "$code" = "204" ] || fail "MMDS admin replacement PUT=$code (want 204)"
    run_mmds_guest "/latest/user-data" "$value2" "$WORK/mmds-get-2.out"
    grep -q 'MMDS_STATUS 200' "$WORK/mmds-get-2.out" || fail "guest MMDS replacement GET did not return 200"
    grep -q "MMDS_BODY $value2" "$WORK/mmds-get-2.out" || fail "guest MMDS replacement GET returned the wrong value"
    grep -q 'MMDS_REVISION 2' "$WORK/mmds-get-2.out" || fail "guest MMDS replacement revision != 2"
    echo "==> PASS: MMDS store replacement became visible without sandbox restart"

    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-admin.body" \
        -w '%{http_code}' -X DELETE "http://localhost$admin_path")
    [ "$code" = "204" ] || fail "MMDS admin DELETE=$code (want 204)"
    run_mmds_guest "/latest/user-data" "" "$WORK/mmds-get-deleted.out"
    grep -q 'MMDS_STATUS 404' "$WORK/mmds-get-deleted.out" || {
        sed 's/^/  mmds| /' "$WORK/mmds-get-deleted.out"
        fail "guest MMDS GET after DELETE did not return 404"
    }
    echo "==> PASS: MMDS admin DELETE revoked the guest-visible store value"

    local relay_admin="/internal/admin/sandboxes/$SID/mmds/credentials/auth"
    local relay_auth1="RELAY_AUTH_1_$RANDOM"
    local relay_auth2="RELAY_AUTH_2_$RANDOM"
    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-relay-admin.body" \
        -w '%{http_code}' -X PUT --data-binary "$relay_auth1" \
        "http://localhost$relay_admin")
    [ "$code" = "204" ] || fail "MMDS relay auth PUT=$code (want 204)"
    sleep 1
    run_mmds_guest "/latest/credentials" '{"result":"RELAY_UPSTREAM_OK"}' "$WORK/mmds-relay-get-1.out"
    sed 's/^/  relay| /' "$WORK/mmds-relay-get-1.out"
    grep -q 'MMDS_STATUS 200' "$WORK/mmds-relay-get-1.out" || fail "guest MMDS relay GET did not return 200"
    grep -q 'MMDS_CONTENT_TYPE application/json' "$WORK/mmds-relay-get-1.out" || fail "relay Content-Type was not preserved"
    grep -q 'MMDS_UPSTREAM_DROPPED None' "$WORK/mmds-relay-get-1.out" || fail "relay leaked an upstream response header"
    [ "$(cat "$RELAY_AUDIT")" = "$relay_auth1" ] || fail "relay upstream did not receive the configured auth"
    ! grep -q "$relay_auth1" "$WORK/mmds-relay-get-1.out" || fail "relay auth leaked into the guest response"
    echo "==> PASS: MMDS relay auth PUT -> HTTPS upstream -> filtered guest response"

    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-relay-admin.body" \
        -w '%{http_code}' -X PUT --data-binary "$relay_auth2" \
        "http://localhost$relay_admin")
    [ "$code" = "204" ] || fail "MMDS relay auth rotation=$code (want 204)"
    sleep 1
    run_mmds_guest "/latest/credentials" '{"result":"RELAY_UPSTREAM_OK"}' "$WORK/mmds-relay-get-2.out"
    grep -q 'MMDS_STATUS 200' "$WORK/mmds-relay-get-2.out" || fail "guest MMDS relay GET after rotation did not return 200"
    [ "$(cat "$RELAY_AUDIT")" = "$relay_auth2" ] || fail "relay auth rotation did not reach upstream"
    echo "==> PASS: MMDS relay auth rotation became visible without sandbox restart"

    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-relay-admin.body" \
        -w '%{http_code}' -X DELETE "http://localhost$relay_admin")
    [ "$code" = "204" ] || fail "MMDS relay auth DELETE=$code (want 204)"
    run_mmds_guest "/latest/credentials" "" "$WORK/mmds-relay-revoked.out"
    grep -q 'MMDS_STATUS 404' "$WORK/mmds-relay-revoked.out" || fail "guest MMDS relay GET after auth DELETE did not return 404"
    echo "==> PASS: MMDS relay auth revocation returned 404"
}

mmds_assert_sandbox_cleanup() {
    local code
    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-admin-after-kill.body" \
        -w '%{http_code}' -X DELETE \
        "http://localhost/internal/admin/sandboxes/$SID/mmds/user-data")
    [ "$code" = "404" ] || fail "MMDS endpoint after sandbox DELETE=$code (want 404 cascade cleanup)"
    code=$(curl -sS --unix-socket "$CONFIG_SOCKET" -o "$WORK/mmds-relay-after-kill.body" \
        -w '%{http_code}' -X DELETE \
        "http://localhost/internal/admin/sandboxes/$SID/mmds/credentials/auth")
    [ "$code" = "404" ] || fail "MMDS relay endpoint after sandbox DELETE=$code (want 404 cascade cleanup)"
    echo "==> PASS: sandbox DELETE cascaded the MMDS endpoint"
}
