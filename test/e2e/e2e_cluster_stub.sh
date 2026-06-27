#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${BIN:-$ROOT/bin/$(uname -m)}"
CLUSTER_CTL="${CLUSTER_CTL:-$BIN/cluster-ctl}"
NODE_STUB_CTL="${NODE_STUB_CTL:-$BIN/node-stub-ctl}"
E2B_KEY_CTL="${E2B_KEY_CTL:-$BIN/e2b-key-ctl}"
DOMAIN="${DOMAIN:-cluster.stub.local}"
GROUP="${GROUP:-/e2e/stub/group}"
NODES="${NODES:-4}"

skip() {
    echo "==> SKIP: $*" >&2
    if [ "${REQUIRE_CLUSTER_STUB:-0}" = "1" ]; then
        exit 1
    fi
    exit 0
}

fail() {
    echo "==> FAIL: $*" >&2
    if [ -n "${WORK:-}" ] && [ -d "$WORK" ]; then
        for f in "$WORK"/*.log; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,220p' "$f" >&2 || true
        done
    fi
    exit 1
}

command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
[ -x "$CLUSTER_CTL" ] || skip "missing cluster-ctl at $CLUSTER_CTL (run make build)"
[ -x "$NODE_STUB_CTL" ] || skip "missing node-stub-ctl at $NODE_STUB_CTL (run make build)"
[ -x "$E2B_KEY_CTL" ] || skip "missing e2b-key-ctl at $E2B_KEY_CTL (run make build)"

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

wait_tcp() {
    local port="$1" name="$2"
    for _ in $(seq 1 100); do
        if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket,sys
s=socket.socket()
s.settimeout(0.2)
s.connect(("127.0.0.1", int(sys.argv[1])))
s.close()
PY
        then
            return 0
        fi
        sleep 0.05
    done
    fail "$name did not open port $port"
}

wait_http() {
    local url="$1" name="$2"
    for _ in $(seq 1 100); do
        if curl -fsS --noproxy '*' --max-time 1 -o /dev/null "$url" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    fail "$name did not become healthy at $url"
}

http_code() {
    local out="$1"; shift
    curl -sS --noproxy '*' --max-time 20 -o "$out" -w '%{http_code}' "$@"
}

retry_code() {
    local want="$1" out="$2"; shift 2
    local code="000"
    for _ in $(seq 1 120); do
        code="$(http_code "$out" "$@" 2>/dev/null || echo 000)"
        if [ "$code" = "$want" ]; then
            echo "$code"
            return 0
        fi
        sleep 0.1
    done
    echo "$code"
    return 1
}

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]:-}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

NODE_PORT="$(free_port)"
ROUTE_PORT="$(free_port)"
SCALE_PORT="$(free_port)"
ROUTER_PORT="$(free_port)"
ADMIN_PORT="$(free_port)"
DATA_PORT="$(free_port)"

AUTH_KEY="$("$E2B_KEY_CTL" gen-key)"
MANIFEST_KEY="$("$E2B_KEY_CTL" gen-key)"
API_KEY="$("$E2B_KEY_CTL" gen-apikey "$AUTH_KEY")"
ENC_KEY="0000000000000000000000000000000000000000000000000000000000000001"

cat >"$WORK/registry.yaml" <<EOF
sandbox_group:
  encryption_key: "$ENC_KEY"
node_link:
  listen: "127.0.0.1:$NODE_PORT"
  heartbeat_interval: "500ms"
  node_dead_after: "3s"
route_link:
  listen: "127.0.0.1:$ROUTE_PORT"
scale_link:
  listen: "127.0.0.1:$SCALE_PORT"
reserve:
  park_timeout: "5s"
EOF

cat >"$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
route_link:
  endpoint: "127.0.0.1:$ROUTE_PORT"
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: "enforce"
  data_plane: "off"
  cache_ttl: "500ms"
EOF

cat >"$WORK/scaler.yaml" <<EOF
scale_link:
  endpoint: "127.0.0.1:$SCALE_PORT"
placement:
  candidates: 2
  zone_admit_max: "yellow"
  node_dead_after: "3s"
EOF

cat >"$WORK/groups.jsonl" <<EOF
{"type":"group","group":{"group":"$GROUP","manifest_key":"$MANIFEST_KEY","auth_key":"$AUTH_KEY","template_ref":"tmpl-stub","node_selectors":[{"pool":"stub"}],"sandbox_config":{"stub.create_delay_ms":"15","stub.http_status":"204"}}}
EOF

"$CLUSTER_CTL" registry --config "$WORK/registry.yaml" >"$WORK/registry.log" 2>&1 &
PIDS+=("$!")
wait_tcp "$NODE_PORT" "registry node_link"
wait_tcp "$ROUTE_PORT" "registry route_link"
wait_tcp "$SCALE_PORT" "registry scale_link"

"$CLUSTER_CTL" registry import --config "$WORK/registry.yaml" --endpoint "127.0.0.1:$ROUTE_PORT" -i "$WORK/groups.jsonl" >"$WORK/import.log" 2>&1 ||
    fail "registry import failed"

"$CLUSTER_CTL" scaler --config "$WORK/scaler.yaml" >"$WORK/scaler.log" 2>&1 &
PIDS+=("$!")

"$NODE_STUB_CTL" serve \
    --node-link "127.0.0.1:$NODE_PORT" \
    --nodes "$NODES" \
    --node-prefix stub \
    --admin-listen "127.0.0.1:$ADMIN_PORT" \
    --data-listen "127.0.0.1:$DATA_PORT" \
    --label pool=stub \
    --heartbeat 500ms >"$WORK/node-stub.log" 2>&1 &
PIDS+=("$!")
ADMIN="http://127.0.0.1:$ADMIN_PORT"
wait_http "$ADMIN/healthz" "node-stub admin"

"$CLUSTER_CTL" router --config "$WORK/router.yaml" >"$WORK/router.log" 2>&1 &
PIDS+=("$!")
wait_tcp "$ROUTER_PORT" "router"

python3 - "$ADMIN" "$NODES" <<'PY' || fail "manifest keys were not distributed to all stub nodes"
import json, sys, time, urllib.request
admin, want = sys.argv[1], int(sys.argv[2])
for _ in range(120):
    try:
        nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=1))
        if len(nodes) == want and all(len(n.get("keys", [])) >= 1 for n in nodes):
            sys.exit(0)
    except Exception:
        pass
    time.sleep(0.1)
sys.exit(1)
PY

code="$(retry_code 204 "$WORK/data1.body" \
    -H "Host: data.$DOMAIN" \
    -H "X-Kuasar-Sandbox-Group: $GROUP" \
    -H "X-Kuasar-Route-Key: user1/session1" \
    -H "X-API-KEY: $API_KEY" \
    -H "E2b-Sandbox-Port: 49983" \
    "http://127.0.0.1:$ROUTER_PORT/health" || true)"
[ "$code" = "204" ] || fail "data by group/route_key returned $code"

python3 - "$ADMIN" "$GROUP" <<'PY' || fail "first data hit was not recorded with the expected group"
import json, sys, urllib.request
hits = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/data-hits", timeout=2))
assert hits, "no data hits"
last = hits[-1]
assert last["group"] == sys.argv[2], last
assert last["route_key"] == "user1/session1", last
assert last.get("access_token", "").startswith("sat_"), last
PY

create_count_before="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
print(sum(1 for c in cmds if c.get("kind") == "create"))
PY
)"
[ "$create_count_before" = "1" ] || fail "expected one create after first reserve, got $create_count_before"

code="$(retry_code 204 "$WORK/data2.body" \
    -H "Host: data.$DOMAIN" \
    -H "X-Kuasar-Sandbox-Group: $GROUP" \
    -H "X-Kuasar-Route-Key: user1/session1" \
    -H "X-API-KEY: $API_KEY" \
    -H "E2b-Sandbox-Port: 49983" \
    "http://127.0.0.1:$ROUTER_PORT/health" || true)"
[ "$code" = "204" ] || fail "second data by group/route_key returned $code"

create_count_after="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
print(sum(1 for c in cmds if c.get("kind") == "create"))
PY
)"
[ "$create_count_after" = "$create_count_before" ] || fail "active route cache caused another create ($create_count_before -> $create_count_after)"

code="$(http_code "$WORK/build.body" -X POST \
    -H "Host: api.$DOMAIN" \
    -H "X-Kuasar-Sandbox-Group: $GROUP" \
    -H "X-API-KEY: $API_KEY" \
    -H "Content-Type: application/json" \
    --data '{"name":"stub-template","cpuCount":1,"memoryMB":128}' \
    "http://127.0.0.1:$ROUTER_PORT/v3/templates")"
[ "$code" = "202" ] || fail "build register returned $code: $(cat "$WORK/build.body")"

python3 - "$ADMIN" <<'PY' || fail "build_register command was not observed"
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
assert any(c.get("kind") == "build_register" for c in cmds), cmds
PY

"$NODE_STUB_CTL" sandbox orphan --admin "$ADMIN" --node stub-1 --group "$GROUP" --route-key orphan --sid sb-orphan >"$WORK/orphan.out"
python3 - "$ADMIN" <<'PY' || fail "orphan route did not trigger delete command"
import json, sys, time, urllib.request
admin = sys.argv[1]
for _ in range(100):
    cmds = json.load(urllib.request.urlopen(admin + "/v1/nodes/stub-1/commands", timeout=2))
    if any(c.get("kind") == "delete" and c.get("sid") == "sb-orphan" for c in cmds):
        sys.exit(0)
    time.sleep(0.1)
sys.exit(1)
PY

python3 - "$ADMIN" "$WORK/first_sandbox.env" <<'PY'
import json, shlex, sys, urllib.request
nodes = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/nodes", timeout=2))
for n in nodes:
    for s in n.get("sandboxes", []):
        if s.get("route_key") == "user1/session1":
            open(sys.argv[2], "w").write("NODE=%s\nSID=%s\n" % (shlex.quote(n["node_id"]), shlex.quote(s["sid"])))
            sys.exit(0)
raise SystemExit("sandbox not found")
PY
# shellcheck disable=SC1090
source "$WORK/first_sandbox.env"
"$NODE_STUB_CTL" node reboot-empty "$NODE" --admin "$ADMIN" >"$WORK/reboot.out"

python3 - "$ADMIN" "$SID" <<'PY' || fail "reboot-empty did not clear the node-local sandbox"
import json, sys, time, urllib.request
admin, sid = sys.argv[1], sys.argv[2]
for _ in range(100):
    nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=2))
    if all(s.get("sid") != sid for n in nodes for s in n.get("sandboxes", [])):
        sys.exit(0)
    time.sleep(0.1)
sys.exit(1)
PY

curl -sS --noproxy '*' --get --data-urlencode "group=$GROUP" \
    "http://127.0.0.1:$ROUTE_PORT/route-link/list" >"$WORK/routes.json"
python3 - "$WORK/routes.json" "$SID" <<'PY' || fail "route_link still contains rebooted sandbox"
import json, sys
routes = json.load(open(sys.argv[1]))
assert all(r.get("sandboxID") != sys.argv[2] for r in routes), routes
PY

echo "==> PASS: sandbox-orchestrator cluster stub e2e"
