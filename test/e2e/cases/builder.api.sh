#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"
: "${E2E_LIB:?E2E_LIB must point to prepared E2E helpers}"
. "$E2E_LIB/orchestrator/build_fixture_units.sh"
NODE="$BIN/node-ctl"
KEY="$BIN/e2b-key-ctl"
for tool in "$NODE" "$KEY"; do [ -x "$tool" ] || { echo "missing prepared product: $tool" >&2; exit 1; }; done
command -v curl >/dev/null
command -v python3 >/dev/null
[ -d /run/systemd/system ] || { echo "systemd manager is required" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "root is required" >&2; exit 1; }

PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
DOMAIN="sandboxes.e2e.local"
mkdir -p "$WORK/units" "$WORK/run" "$WORK/lib"
PID=""
cleanup() { set +e; stop_build_fixture_units "$WORK" 2>/dev/null || true; [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null || true; }
trap cleanup EXIT
fail() { echo "FAIL builder.api.sh: $*" >&2; exit 1; }
field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }
req() {
  local method="$1" path="$2" key="$3" body="${4:-}"
  local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

MK="$("$KEY" gen-key)"; SECRET="$("$KEY" derive-api-secret "$MK")"; AK="$("$KEY" gen-apikey "$SECRET")"
MK2="$("$KEY" gen-key)"; SECRET2="$("$KEY" derive-api-secret "$MK2")"; AK2="$("$KEY" gen-apikey "$SECRET2")"
ENC="$("$KEY" gen-key)"
printf 'manifest:\n  key: ""\n' >"$WORK/manifest.yaml"
cat >"$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $WORK/units }
sandbox:
  network: { switch: sw0 }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle }
checkpoint: { mode: local }
EOF
"$NODE" conductor serve --config "$WORK/config.yaml" >"$WORK/node.log" 2>&1 &
PID=$!
for _ in $(seq 1 60); do curl -fsS --noproxy '*' -o /dev/null -H "Host: api.$DOMAIN" "http://127.0.0.1:$PORT/health" 2>/dev/null && break; kill -0 "$PID" 2>/dev/null || { cat "$WORK/node.log"; fail "conductor exited"; }; sleep 0.25; done
curl -fsS --noproxy '*' -o /dev/null -H "Host: api.$DOMAIN" "http://127.0.0.1:$PORT/health" || fail "health not ready"
"$NODE" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null

code="$(req POST /v3/templates "$AK" '{"name":"e2e-tmpl","tags":["e2e"],"cpuCount":1,"memoryMB":1024}')"
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID="$(field "$WORK/resp.body" templateID)"; BID="$(field "$WORK/resp.body" buildID)"
case "$TID" in transient-*) ;; *) fail "non-transient template id: $TID";; esac
[ "$(req GET "/templates/$TID/builds/$BID/status" "$AK2")" = 404 ] || fail "cross-tenant build ownership leak"
code="$(req POST "/v2/templates/$TID/builds/$BID" "$AK" '{"fromImage":"docker.io/library/alpine:3.19"}')"
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
status=""
for _ in $(seq 1 30); do
  [ "$(req GET "/templates/$TID/builds/$BID/status" "$AK")" = 200 ] || fail "status request"
  status="$(field "$WORK/resp.body" status)"
  case "$status" in ready|error) break;; waiting|building) ;; *) fail "unexpected status: $status";; esac
  sleep 1
done
case "$status" in ready|error) ;; *) fail "build did not terminate";; esac
echo "PASS builder.api.sh ($status)"
