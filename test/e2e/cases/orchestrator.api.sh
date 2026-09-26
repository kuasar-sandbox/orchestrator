#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"
NODE="$BIN/node-ctl"
KEY="$BIN/e2b-key-ctl"
for tool in "$NODE" "$KEY"; do [ -x "$tool" ] || { echo "missing prepared product: $tool" >&2; exit 1; }; done
command -v curl >/dev/null
command -v python3 >/dev/null
[ -d /run/systemd/system ] || { echo "systemd manager is required" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "root is required" >&2; exit 1; }

PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
DOMAIN="sandboxes.e2e.local"
UNIT_DIR="$WORK/units"
mkdir -p "$UNIT_DIR" "$WORK/run" "$WORK/lib"
PID=""
cleanup() { set +e; [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null || true; }
trap cleanup EXIT
fail() { echo "FAIL orchestrator.api.sh: $*" >&2; exit 1; }

MK="$("$KEY" gen-key)"
API_SECRET="$("$KEY" derive-api-secret "$MK")"
AK="$("$KEY" gen-apikey "$API_SECRET")"
MK_OTHER="$("$KEY" gen-key)"
API_SECRET_OTHER="$("$KEY" derive-api-secret "$MK_OTHER")"
AK_OTHER="$("$KEY" gen-apikey "$API_SECRET_OTHER")"
ENC="$("$KEY" gen-key)"
cat >"$WORK/manifest.yaml" <<EOF
manifest:
  key: ""
EOF
cat >"$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  network: { switch: sw0 }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle }
checkpoint: { mode: local }
EOF
"$NODE" conductor serve --config "$WORK/config.yaml" >"$WORK/node.log" 2>&1 &
PID=$!
for _ in $(seq 1 60); do
  curl -fsS --noproxy '*' -o /dev/null -H "Host: api.$DOMAIN" "http://127.0.0.1:$PORT/health" 2>/dev/null && break
  kill -0 "$PID" 2>/dev/null || { cat "$WORK/node.log"; fail "conductor exited"; }
  sleep 0.25
done
curl -fsS --noproxy '*' -o /dev/null -H "Host: api.$DOMAIN" "http://127.0.0.1:$PORT/health" || fail "health not ready"

for unit in sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice; do
  [ -f "$UNIT_DIR/$unit" ] || fail "unit not generated: $unit"
done
grep -q "run-sandbox .*--run-id=%i" "$UNIT_DIR/sandbox-runner@.service" || fail "runner unit run-id contract"
grep -q '^Delegate=yes$' "$UNIT_DIR/sandbox-runner@.service" || fail "runner delegation"
! grep -q '^DelegateSubgroup=' "$UNIT_DIR/sandbox-runner@.service" || fail "non-portable DelegateSubgroup"
grep -q "run-builder .*--run-id=%i" "$UNIT_DIR/sandbox-builder@.service" || fail "builder unit run-id contract"
grep -q "ExecStopPost=/bin/rm -f .*runners/%i.pid" "$UNIT_DIR/sandbox-runner@.service" || fail "runner pidfile cleanup"
grep -q "ExecStopPost=/bin/rm -f .*runners/%i.pid" "$UNIT_DIR/sandbox-builder@.service" || fail "builder pidfile cleanup"
! grep -q "StandardOutput=file:" "$UNIT_DIR/sandbox-builder@.service" || fail "obsolete builder result capture"

req() {
  local method="$1" path="$2" key="${3:-}" body="${4:-}"
  local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN")
  [ -n "$key" ] && args+=(-H "X-API-KEY: $key")
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
[ "$(req GET /health)" = 204 ] || fail "health status"
[ "$(req POST /sandboxes "" '{"templateID":"x"}')" = 401 ] || fail "missing API key not rejected"
[ "$(req POST /sandboxes e2b_deadbeef_not_a_real_token '{"templateID":"x"}')" = 401 ] || fail "malformed API key not rejected"
[ "$(req POST /v3/templates "$AK_OTHER" '{"name":"denied","cpuCount":1,"memoryMB":1024}')" = 403 ] || fail "non-allowlisted credential pair not rejected"

"$NODE" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
[ "$(req POST /v3/templates "$AK" '{"name":"allowed","cpuCount":1,"memoryMB":1024}')" = 202 ] || { cat "$WORK/resp.body"; fail "allowlisted register"; }

echo "PASS orchestrator.api.sh"
