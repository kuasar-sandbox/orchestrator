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

step() {
    echo "==> $*" >&2
}

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

if [ -z "${CLUSTER_STUB_CASE:-}" ]; then
    for spec in registry-n1:1 registry-n3:3 registry-joint:4; do
        CLUSTER_STUB_CASE="${spec%%:*}"
        REGISTRIES="${spec##*:}"
        step "running case $CLUSTER_STUB_CASE (registries=$REGISTRIES)"
        CLUSTER_STUB_CASE="$CLUSTER_STUB_CASE" REGISTRIES="$REGISTRIES" "$0"
    done
    exit 0
fi

command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
[ -x "$CLUSTER_CTL" ] || skip "missing cluster-ctl at $CLUSTER_CTL (run make build)"
[ -x "$NODE_STUB_CTL" ] || skip "missing node-stub-ctl at $NODE_STUB_CTL (run make build)"
[ -x "$E2B_KEY_CTL" ] || skip "missing e2b-key-ctl at $E2B_KEY_CTL (run make build)"
REGISTRIES="${REGISTRIES:-1}"
step "cluster stub e2e: case=$CLUSTER_STUB_CASE registries=$REGISTRIES using BIN=$BIN"

free_port() {
    python3 <<'PY'
import socket, sys
try:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
    s.close()
except PermissionError:
    sys.exit(2)
PY
}

alloc_port() {
    local var="$1"
    local name="$2"
    local port
    if ! port="$(free_port)"; then
        skip "cannot allocate local TCP port for $name (socket permission denied in this environment)"
    fi
    printf -v "$var" '%s' "$port"
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

CONTROL_PORTS=()
for i in $(seq 1 "$REGISTRIES"); do
    alloc_port REGISTRY_PORT "registry-$i"
    CONTROL_PORTS+=("$REGISTRY_PORT")
done
CONTROL_PORT="${CONTROL_PORTS[0]}"
alloc_port SCALER_PORT scaler
alloc_port ROUTER_PORT router
alloc_port ADMIN_PORT node-stub-admin
alloc_port DATA_PORT node-stub-data

AUTH_KEY="$("$E2B_KEY_CTL" gen-key)"
MANIFEST_KEY="$("$E2B_KEY_CTL" gen-key)"
API_KEY="$("$E2B_KEY_CTL" gen-apikey "$AUTH_KEY")"

OWNER_COUNT="$REGISTRIES"
ACTIVE_IDS=()
NEXT_IDS=()
if [ "$CLUSTER_STUB_CASE" = "registry-joint" ]; then
    OWNER_COUNT=3
    ACTIVE_IDS=(1 2 3)
    NEXT_IDS=(2 3 4)
else
    for i in $(seq 1 "$REGISTRIES"); do
        ACTIVE_IDS+=("$i")
    done
fi

ACTIVE_MEMBERS_YAML=""
for i in "${ACTIVE_IDS[@]}"; do
    port="${CONTROL_PORTS[$((i-1))]}"
    ACTIVE_MEMBERS_YAML="$ACTIVE_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\" }
"
done
NEXT_LINE=""
NEXT_MEMBERS_BLOCK=""
if [ "${#NEXT_IDS[@]}" -gt 0 ]; then
    NEXT_LINE="  next: 2
"
    NEXT_MEMBERS_YAML=""
    for i in "${NEXT_IDS[@]}"; do
        port="${CONTROL_PORTS[$((i-1))]}"
        NEXT_MEMBERS_YAML="$NEXT_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\" }
"
    done
    NEXT_MEMBERS_BLOCK="    - version: 2
      members:
$NEXT_MEMBERS_YAML"
fi

for i in $(seq 1 "$REGISTRIES"); do
    port="${CONTROL_PORTS[$((i-1))]}"
    cat >"$WORK/registry-$i.yaml" <<EOF
member:
  id: registry-$i
  listen: "127.0.0.1:$port"
  advertise: "http://127.0.0.1:$port"
membership:
  active: 1
${NEXT_LINE}  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML$NEXT_MEMBERS_BLOCK
  owners:
    route_link: $OWNER_COUNT
    node_link: $OWNER_COUNT
    node_list: $OWNER_COUNT
node_link:
  heartbeat_interval: "500ms"
  node_dead_after: "3s"
route_link:
  park_timeout: "5s"
EOF
done

cat >"$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: "enforce"
  data_plane: "off"
  cache_ttl: "500ms"
EOF

cat >"$WORK/scaler.yaml" <<EOF
member:
  id: scaler-1
  listen: "127.0.0.1:$SCALER_PORT"
  advertise: "http://127.0.0.1:$SCALER_PORT"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
placement:
  candidates: 2
  zone_admit_max: "yellow"
  node_dead_after: "3s"
EOF

cat >"$WORK/groups.jsonl" <<EOF
{"type":"group","group":{"group":"$GROUP","manifest_key":"$MANIFEST_KEY","auth_key":"$AUTH_KEY","template_ref":"tmpl-stub","node_selectors":[{"pool":"stub"}],"sandbox_config":{"stub.create_delay_ms":"15","stub.http_status":"204"}}}
EOF

for i in $(seq 1 "$REGISTRIES"); do
    port="${CONTROL_PORTS[$((i-1))]}"
    "$CLUSTER_CTL" registry --config "$WORK/registry-$i.yaml" >"$WORK/registry-$i.log" 2>&1 &
    PIDS+=("$!")
    step "starting registry-$i"
    wait_tcp "$port" "registry-$i control"
done

step "checking registry membership endpoints"
for port in "${CONTROL_PORTS[@]}"; do
    python3 - "http://127.0.0.1:$port" "$REGISTRIES" <<'PY' || fail "membership endpoint failed on $port"
import json, sys, urllib.request
base = sys.argv[1]
want = int(sys.argv[2])
m = json.load(urllib.request.urlopen(base + "/cluster/membership", timeout=2))
active = m.get("active", m.get("Active"))
versions = m.get("versions", m.get("Versions", []))
assert active, m
active_versions = [v for v in versions if v.get("version", v.get("Version")) == active]
assert len(active_versions) == 1, m
assert active_versions[0].get("label", active_versions[0].get("Label")), m
next_version = m.get("next", m.get("Next"))
ids = set()
for v in versions:
    version = v.get("version", v.get("Version"))
    if version not in (active, next_version):
        continue
    assert v.get("label", v.get("Label")), m
    for member in v.get("members", v.get("Members", [])):
        ids.add(member.get("id", member.get("ID")))
assert len(ids) == want, m
PY
done

step "starting scaler"
"$CLUSTER_CTL" scaler --config "$WORK/scaler.yaml" >"$WORK/scaler.log" 2>&1 &
PIDS+=("$!")
wait_tcp "$SCALER_PORT" "scaler"

step "importing sandbox group into scaler"
"$CLUSTER_CTL" scaler import --config "$WORK/scaler.yaml" --endpoint "127.0.0.1:$SCALER_PORT" -i "$WORK/groups.jsonl" >"$WORK/import.log" 2>&1 ||
    fail "scaler import failed"

step "starting node-stub-ctl with $NODES nodes"
"$NODE_STUB_CTL" serve \
    --node-link "127.0.0.1:$CONTROL_PORT" \
    --nodes "$NODES" \
    --node-prefix stub \
    --admin-listen "127.0.0.1:$ADMIN_PORT" \
    --data-listen "127.0.0.1:$DATA_PORT" \
    --label pool=stub \
    --heartbeat 500ms >"$WORK/node-stub.log" 2>&1 &
PIDS+=("$!")
ADMIN="http://127.0.0.1:$ADMIN_PORT"
wait_http "$ADMIN/healthz" "node-stub admin"

step "starting router"
"$CLUSTER_CTL" router --config "$WORK/router.yaml" >"$WORK/router.log" 2>&1 &
PIDS+=("$!")
wait_tcp "$ROUTER_PORT" "router"

step "waiting for key distribution"
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

step "checking Reserve -> READY -> data forward"
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

if [ "$CLUSTER_STUB_CASE" = "registry-joint" ]; then
    step "checking joint route visibility from next-only registry"
    curl -sS --noproxy '*' --get --data-urlencode "group=$GROUP" \
        "http://127.0.0.1:${CONTROL_PORTS[3]}/route-link/list" >"$WORK/joint-routes.json"
    python3 - "$WORK/joint-routes.json" <<'PY' || fail "next-only registry did not expose the reserved route"
import json, sys
routes = json.load(open(sys.argv[1]))
assert any(r.get("sandboxID") and r.get("state") == "ready" for r in routes), routes
PY
fi

create_count_before="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
print(sum(1 for c in cmds if c.get("kind") == "create"))
PY
)"
[ "$create_count_before" = "1" ] || fail "expected one create after first reserve, got $create_count_before"

step "checking active route cache"
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

step "checking build_register"
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

step "checking orphan route cleanup"
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

step "checking reboot-empty cleanup"
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
    "http://127.0.0.1:$CONTROL_PORT/route-link/list" >"$WORK/routes.json"
python3 - "$WORK/routes.json" "$SID" <<'PY' || fail "route_link still contains rebooted sandbox"
import json, sys
routes = json.load(open(sys.argv[1]))
assert all(r.get("sandboxID") != sys.argv[2] for r in routes), routes
PY

echo "==> PASS: sandbox-orchestrator cluster stub e2e"
