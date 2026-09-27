#!/usr/bin/env bash
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
. "$SCRIPT_DIR/native_traffic.sh"
PROXY_WORKERS=2
METRICS_PORT="$(free_port)"
PROXY_METRICS_LISTEN="127.0.0.1:$METRICS_PORT"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
CONDUCTOR_PID=$ORCH_PID
PROXY_MASTER_PID=$PROXY_PID
wait_proxy_topology_ready "$PROXY_MASTER_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
# ---- create the sandbox (boots the microVM; serve pushes the route) -------
echo "==> POST /sandboxes (boot microVM from $TEMPLATE)"
IDENTITY_ID="identity-$(cat /proc/sys/kernel/random/uuid)"
IDENTITY_STABLE_ID="logical-$IDENTITY_ID"
IDENTITY_CREATE_BODY="$(python3 - "$TEMPLATE" "$IDENTITY_ID" "$IDENTITY_STABLE_ID" <<'PY_IDENTITY'
import json, sys
print(json.dumps({"templateID": sys.argv[1], "timeout": 120, "metadata": {
    "kuasar-sandbox.identity": json.dumps({"id": sys.argv[2], "stable_id": sys.argv[3]})
}}))
PY_IDENTITY
)"
REQ_ATTACH_MMDS=0
code=$(req POST /sandboxes "$AK" "$IDENTITY_CREATE_BODY")
unset REQ_ATTACH_MMDS
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo; dump_logs
    SID=$(ls "$WORK/run/sandboxes" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
[ "$SID" = "$IDENTITY_ID" ] || fail "Create did not use the requested local ID"
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
[ -n "$SID" ] && [ -n "$ENVD_TOKEN" ] && [ -n "$FORWARD_TOKEN" ] \
    || fail "missing sandboxID/envdAccessToken/forwardAccessToken in create response"
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
echo "==> PASS: sandbox $SID durably accepted after Proxy route ACK (tokens captured; no default exec token)"
echo "==> issue native exec capability immediately and park its Proxy CONNECT from starting"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv[0] == '/bin/sh'\"}]}")" \
    || fail "issue conditioned immediate native exec capability"
rm -f "$WORK/exec-session.secret"
IMMEDIATE_NATIVE_MARK="PROXY_IMMEDIATE_NATIVE_EXEC_$RANDOM"
(
    exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$IMMEDIATE_NATIVE_MARK" 1 130
) &
IMMEDIATE_EXEC_PID=$!
echo "==> request envd immediately through Proxy; ACKed starting route must park to running"
(
    code=$(DP_MAX_TIME=120 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
    printf '%s\n' "$code" >"$WORK/immediate-data.code"
    [ "$code" = "204" ] || [ "$code" = "200" ]
) &
IMMEDIATE_DATA_PID=$!
wait_traffic_stats "$SID" parking \
    || { dump_logs; fail "Proxy starting request was not visible as parking"; }
echo "==> PASS: Proxy master cache observed authorized starting ingress as parking"
immediate_data_status=0
wait "$IMMEDIATE_DATA_PID" || immediate_data_status=$?
IMMEDIATE_DATA_PID=""
code="$(<"$WORK/immediate-data.code")"
[ "$immediate_data_status" = "0" ] \
    || { cat "$WORK/dp.body"; dump_logs; fail "immediate Proxy request=$code"; }
echo "==> PASS: Proxy parked post-Create request through starting to running (code=$code)"
immediate_exec_status=0
wait "$IMMEDIATE_EXEC_PID" || immediate_exec_status=$?
IMMEDIATE_EXEC_PID=""
[ "$immediate_exec_status" = "0" ] || { dump_logs; fail "immediate Proxy native exec did not park to running"; }
echo "==> PASS: Proxy native exec parked post-Create CONNECT from ACKed starting to running"

# Conflict must not replace the running object or disclose its credentials.
code=$(req POST /sandboxes "$AK" "$IDENTITY_CREATE_BODY")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "duplicate identity Create=$code (want 409)"; }
code=$(req GET "/sandboxes/$SID" "$AK")
[ "$code" = "200" ] || fail "identity readback=$code"
python3 - "$WORK/resp.body" "$IDENTITY_ID" "$FORWARD_TOKEN" "$IDENTITY_STABLE_ID" <<'PY_IDENTITY'
import base64, json, sys
sandbox = json.load(open(sys.argv[1]))
assert sandbox["sandboxID"] == sys.argv[2]
assert "kuasar-sandbox.identity" not in sandbox.get("metadata", {})
encoded = sys.argv[3].split(".")[1]
claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
assert claims["sid"] == sys.argv[4]
PY_IDENTITY
echo "==> PASS: explicit local/stable identity survived real Proxy exec/envd; duplicate Create is 409"
wait_traffic_stats "$SID" idle || { dump_logs; fail "Proxy traffic did not converge to idle"; }
echo "==> PASS: Proxy master cache converged parking/connected to idle"

code=$(req GET "/sandboxes/$SID/stats/resource" "$AK")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "static resource stats=$code (want 200)"; }
python3 - "$WORK/resp.body" <<'PY_RESOURCE'
import json, sys
stats = json.load(open(sys.argv[1]))
required = {"cpuCapacity", "cpuAllocatable", "memoryCapacity", "memoryHeadroom", "memoryUsed", "cpuSeconds", "timestampUnix"}
assert set(stats) == required, stats
assert all(stats[key] > 0 for key in required), stats
PY_RESOURCE
echo "==> PASS: static resource stats observes VMM memory/CPU with controller, usage and telemetry disabled"
native_traffic_probe 0
echo "==> PASS: conductor native traffic reads work while telemetry is stopped"

ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
for _ in $(seq 1 40); do [ -S "$ENVD_SOCK" ] && break; sleep 0.25; done
[ -S "$ENVD_SOCK" ] || fail "envd.sock not found at $ENVD_SOCK"
ok=""
for _ in $(seq 1 20); do
    code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
    { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "envd /health via proxy with token = $code (want 200/204)"; }
echo "==> PASS: route-synced + data plane forwarded through the proxy to real envd (X-Access-Token accepted)"

wait_traffic_stats "$SID" idle || { dump_logs; fail "traffic did not return idle before auth rejection checks"; }
IDLE_BEFORE_INVALID="$(traffic_idle_since "$SID")"
[ -n "$IDLE_BEFORE_INVALID" ] || fail "running idle traffic stats omitted idleSince"
code=$(dp "49983-$SID" /health "")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy WITHOUT token = $code (want 401 enforce)"; }
echo "==> PASS: proxy enforces X-Access-Token (missing -> 401)"

code=$(dp "49983-$SID" /health "wrong-token")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy with WRONG token = $code (want 401)"; }
code=$(curl -sS --noproxy '*' \
    -D "$WORK/typed-error.headers" \
    -o "$WORK/typed-error.body" \
    -w '%{http_code}' \
    -H "Host: 49983-$SID.$DOMAIN" \
    -H 'X-Access-Token: wrong-token' \
    "http://127.0.0.1:$PROXY_PORT/health")
[ "$code" = "401" ] || { cat "$WORK/typed-error.body"; fail "typed unauthorized response=$code"; }
grep -Eiq '^X-Kuasar-Proxy-Error:[[:space:]]*unauthorized' "$WORK/typed-error.headers" \
    || { cat "$WORK/typed-error.headers"; fail "DataEndpoint omitted the typed unauthorized proxy error"; }
echo "==> PASS: Proxy rejects a wrong token with the typed DataEndpoint error"
wait_traffic_stats "$SID" idle || { dump_logs; fail "invalid credentials changed traffic inflight"; }
IDLE_AFTER_INVALID="$(traffic_idle_since "$SID")"
[ "$IDLE_AFTER_INVALID" = "$IDLE_BEFORE_INVALID" ] \
    || fail "invalid credentials changed idleSince ($IDLE_BEFORE_INVALID -> $IDLE_AFTER_INVALID)"
echo "==> PASS: invalid credentials caused no parking/connected and did not refresh idleSince"

USER_MARK="proxy-netns-user-port-$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "mkdir -p /home/user/e2e-site; echo '$USER_MARK' > /home/user/e2e-site/index.html; cd /home/user/e2e-site; python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/e2e-http-8000.log 2>&1 &" \
    >"$WORK/start-user-port.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-user-port.out" || { sed 's/^/  envd| /' "$WORK/start-user-port.out"; fail "start guest user-port server"; }
ok=""
for _ in $(seq 1 30); do
    code=$(DP_MAX_TIME=8 dp "8000-$SID" / "$FORWARD_TOKEN" || true)
    grep -q "$USER_MARK" "$WORK/dp.body" 2>/dev/null && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "user port via proxy_netns -> floatingip did not return marker"; }
echo "==> PASS: proxy_netns worker reached sandbox floatingip:8000 (real user port, marker=$USER_MARK)"

# WebSocket uses the same user-port route and traffic lifecycle as HTTP/CONNECT.
WS_GUEST_COMMAND=$(python3 "$SCRIPT_DIR/websocket_probe.py" guest-command)
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WS_GUEST_COMMAND" \
    >"$WORK/start-websocket.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-websocket.out" \
    || { cat "$WORK/start-websocket.out"; fail "start guest WebSocket fixture"; }
python3 "$SCRIPT_DIR/websocket_probe.py" probe --port "$PROXY_PORT" \
    --authority "8001-$SID.$DOMAIN" --token "$FORWARD_TOKEN" \
    || { dump_logs; fail "canonical WebSocket through Proxy"; }
wait_traffic_stats "$SID" idle || { dump_logs; fail "WebSocket traffic did not return to idle"; }

# A signed envd /files request carries no X-Access-Token. Both the Proxy and
# envd verify the same signature; the file bytes still travel only through the
# independent DataEndpoint.
SIGNED_FILE="/tmp/kuasar-proxy-signed-$RANDOM.txt"
SIGNED_MARK="PROXY_SIGNED_FILE_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "printf '%s' '$SIGNED_MARK' > '$SIGNED_FILE'" >"$WORK/write-signed-file.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/write-signed-file.out" \
    || { sed 's/^/  envd| /' "$WORK/write-signed-file.out"; fail "create signed-file fixture"; }
SIGNED_QUERY=$(python3 - "$SIGNED_FILE" "$ENVD_TOKEN" <<'PY'
import base64, hashlib, sys, urllib.parse
path, token = sys.argv[1:]
digest = hashlib.sha256(f"{path}:read::{token}".encode()).digest()
signature = "v1_" + base64.b64encode(digest).decode().rstrip("=")
print(urllib.parse.urlencode({"path": path, "signature": signature}))
PY
)
code=$(curl -sS --noproxy '*' \
    -o "$WORK/signed-file.body" \
    -w '%{http_code}' \
    -H "Host: 49983-$SID.$DOMAIN" \
    "http://127.0.0.1:$PROXY_PORT/files?$SIGNED_QUERY")
[ "$code" = "200" ] && grep -Fq "$SIGNED_MARK" "$WORK/signed-file.body" \
    || { cat "$WORK/signed-file.body"; dump_logs; fail "signed /files through DataEndpoint=$code"; }
echo "==> PASS: signed /files crossed only the independent Proxy DataEndpoint"

# ---- (2) initially missing route waits passively without unauthorized Wake -
# The full passive propagation window is 120s. Cancel this probe after two
# seconds: the retired pre-#70 flow emitted Wake immediately, and OnWake's Delete
# made the request return 404 well before this client deadline.
UNKNOWN_SID="deadbeefdeadbeef"
if code=$(DP_MAX_TIME=2 dp "49983-$UNKNOWN_SID" /health "$ENVD_TOKEN" 2>"$WORK/unknown-route.err"); then
    dump_logs
    fail "initially missing route returned $code before its passive propagation window"
else
    curl_status=$?
fi
[ "$curl_status" = "28" ] && [ "$code" = "000" ] \
    || { cat "$WORK/unknown-route.err"; dump_logs; fail "initially missing route probe status=$curl_status code=$code (want client timeout without Wake)"; }
echo "==> PASS: initially missing route waited passively without unauthorized Wake"

# ---- (3b) CONNECT tunnel THROUGH the proxy to envd control -----------------
# Drive CONNECT with raw TCP so the test does not depend on curl proxy-header
# feature variations. The proxy auths the CONNECT and then tunnels a GET /health
# request to the envd control socket.
cc=$(python3 - "$PROXY_PORT" "49983-$SID.$DOMAIN:49983" "$ENVD_TOKEN" <<'PY'
import socket, sys

proxy_port, target, token = int(sys.argv[1]), sys.argv[2], sys.argv[3]
with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as s:
    s.settimeout(10)
    req = (
        f"CONNECT {target} HTTP/1.1\r\n"
        f"Host: {target}\r\n"
        f"X-Access-Token: {token}\r\n"
        "\r\n"
    )
    s.sendall(req.encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    status = data.split(b"\r\n", 1)[0].decode("latin1", "replace")
    if " 200 " not in status and not status.endswith(" 200"):
        print(status or "no-connect-response")
        sys.exit(0)
    s.sendall(f"GET /health HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n".encode())
    data = b""
    while b"\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    print((data.split(b"\r\n", 1)[0] or b"no-health-response").decode("latin1", "replace"))
PY
)
if echo "$cc" | grep -Eq 'HTTP/[0-9.]+ (200|204)'; then
    echo "==> PASS: CONNECT tunnel through the proxy reached envd /health ($cc)"
else
    dump_logs
    fail "CONNECT tunnel through proxy failed: $cc"
fi

# ---- (4) native exec capability THROUGH the Proxy -------------------------
echo "==> reuse the explicit native exec capability after the sandbox is running"
NATIVE_MARK="PROXY_NATIVE_EXEC_$RANDOM"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
echo "==> PASS: real sandbox-ctl CONNECT through the Proxy verified stdin/stdout/stderr and exit status"

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalization"
stop_proxy_master
echo "PASS orchestrator.proxy-auth.sh"
