#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_CTL_EXPLICIT="${CLUSTER_CTL+x}"
NODE_STUB_CTL_EXPLICIT="${NODE_STUB_CTL+x}"
E2B_KEY_CTL_EXPLICIT="${E2B_KEY_CTL+x}"
BIN_EXPLICIT="${BIN+x}"
NODES_EXPLICIT="${NODES+x}"
SCALERS_EXPLICIT="${SCALERS+x}"
BIN="${BIN:-$ROOT/bin/$(uname -m)}"
CLUSTER_CTL="${CLUSTER_CTL:-$BIN/cluster-ctl}"
NODE_STUB_CTL="${NODE_STUB_CTL:-$BIN/node-stub-ctl}"
E2B_KEY_CTL="${E2B_KEY_CTL:-$BIN/e2b-key-ctl}"
DOMAIN="${DOMAIN:-cluster.stub.local}"
GROUP="${GROUP:-/e2e/stub/group}"
NODES="${NODES:-4}"
SCALERS="${SCALERS:-1}"

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
        if [ -n "${ADMIN:-}" ]; then
            curl -sS --noproxy '*' --max-time 2 "$ADMIN/v1/commands" >"$WORK/admin-commands.json" 2>/dev/null || true
            curl -sS --noproxy '*' --max-time 2 "$ADMIN/v1/events" >"$WORK/admin-events.json" 2>/dev/null || true
            curl -sS --noproxy '*' --max-time 2 "$ADMIN/v1/nodes" >"$WORK/admin-nodes.json" 2>/dev/null || true
            curl -sS --noproxy '*' --max-time 2 "$ADMIN/v1/data-hits" >"$WORK/admin-data-hits.json" 2>/dev/null || true
        fi
        for f in "$WORK"/*.body "$WORK"/*.json; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,220p' "$f" >&2 || true
        done
        for f in "$WORK"/*.log; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,220p' "$f" >&2 || true
        done
    fi
    exit 1
}

build_cluster_stub_binaries() {
    if [ "${CLUSTER_STUB_BUILD:-1}" = "0" ] || [ -n "${CLUSTER_STUB_BUILT:-}" ] ||
        [ -n "$BIN_EXPLICIT" ] || [ -n "$CLUSTER_CTL_EXPLICIT" ] || [ -n "$NODE_STUB_CTL_EXPLICIT" ] || [ -n "$E2B_KEY_CTL_EXPLICIT" ]; then
        return
    fi
    command -v make >/dev/null 2>&1 || skip "make not on PATH"
    step "building cluster e2e binaries with make build"
    make -C "$ROOT" build
    export CLUSTER_STUB_BUILT=1
}

if [ -z "${CLUSTER_STUB_CASE:-}" ]; then
    build_cluster_stub_binaries
    for spec in registry-n1:1:1 registry-n3:3:1 registry-redirect:3:1 registry-placer-ha:3:2 registry-joint:4:1; do
        CLUSTER_STUB_CASE="${spec%%:*}"
        rest="${spec#*:}"
        REGISTRIES="${rest%%:*}"
        SCALERS="${rest##*:}"
        step "running case $CLUSTER_STUB_CASE (registries=$REGISTRIES placers=$SCALERS)"
        CLUSTER_STUB_BUILT="${CLUSTER_STUB_BUILT:-1}" CLUSTER_STUB_CASE="$CLUSTER_STUB_CASE" REGISTRIES="$REGISTRIES" SCALERS="$SCALERS" "$0"
    done
    exit 0
fi

command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
build_cluster_stub_binaries
[ -x "$CLUSTER_CTL" ] || skip "missing cluster-ctl at $CLUSTER_CTL (run make build)"
[ -x "$NODE_STUB_CTL" ] || skip "missing node-stub-ctl at $NODE_STUB_CTL (run make build)"
[ -x "$E2B_KEY_CTL" ] || skip "missing e2b-key-ctl at $E2B_KEY_CTL (run make build)"
REGISTRIES="${REGISTRIES:-1}"
SCALERS="${SCALERS:-1}"
if [ "$CLUSTER_STUB_CASE" = "registry-redirect" ] && [ -z "$NODES_EXPLICIT" ]; then
    NODES=10
fi
if [ "$CLUSTER_STUB_CASE" = "registry-placer-ha" ] && [ -z "$SCALERS_EXPLICIT" ]; then
    SCALERS=2
fi
step "cluster stub e2e: case=$CLUSTER_STUB_CASE registries=$REGISTRIES placers=$SCALERS using BIN=$BIN"

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

ALLOCATED_PORTS=()

alloc_port() {
    local var="$1"
    local name="$2"
    local port
    for _ in $(seq 1 100); do
        if ! port="$(free_port)"; then
            skip "cannot allocate local TCP port for $name (socket permission denied in this environment)"
        fi
        local used=0
        for existing in "${ALLOCATED_PORTS[@]:-}"; do
            if [ "$existing" = "$port" ]; then
                used=1
                break
            fi
        done
        if [ "$used" = "0" ]; then
            ALLOCATED_PORTS+=("$port")
            printf -v "$var" '%s' "$port"
            return
        fi
    done
    fail "could not allocate a unique local TCP port for $name"
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
    for _ in $(seq 1 "${CLUSTER_STUB_RETRY_LIMIT:-120}"); do
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
step "work dir: $WORK"
PIDS=()
cleanup() {
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        pid="${PIDS[$i]}"
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [ "${CLUSTER_STUB_KEEP_WORK:-0}" = "1" ]; then
        step "keeping work dir: $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

CONTROL_PORTS=()
for i in $(seq 1 "$REGISTRIES"); do
    alloc_port REGISTRY_PORT "registry-$i"
    CONTROL_PORTS+=("$REGISTRY_PORT")
done
CONTROL_PORT="${CONTROL_PORTS[0]}"
SCALER_PORTS=()
for i in $(seq 1 "$SCALERS"); do
    alloc_port SCALER_PORT "placer-$i"
    SCALER_PORTS+=("$SCALER_PORT")
done
alloc_port ROUTER_PORT router
alloc_port ADMIN_PORT node-stub-admin
alloc_port DATA_PORT node-stub-data

AUTH_KEY="$("$E2B_KEY_CTL" gen-key)"
MANIFEST_KEY="$("$E2B_KEY_CTL" gen-key)"
API_KEY="$("$E2B_KEY_CTL" gen-apikey "$AUTH_KEY")"

OWNER_COUNT="$REGISTRIES"
ROUTE_OWNER_COUNT="$OWNER_COUNT"
NODE_OWNER_COUNT="$OWNER_COUNT"
SCALE_OWNER_COUNT="$OWNER_COUNT"
NODE_LIST_OWNER_COUNT="$OWNER_COUNT"
ACTIVE_IDS=()
NEXT_IDS=()
if [ "$CLUSTER_STUB_CASE" = "registry-joint" ]; then
    OWNER_COUNT=3
    ROUTE_OWNER_COUNT=3
    NODE_OWNER_COUNT=3
    SCALE_OWNER_COUNT=3
    NODE_LIST_OWNER_COUNT=3
    ACTIVE_IDS=(1 2 3)
    NEXT_IDS=(2 3 4)
else
    for i in $(seq 1 "$REGISTRIES"); do
        ACTIVE_IDS+=("$i")
    done
fi
if [ "$CLUSTER_STUB_CASE" = "registry-redirect" ]; then
    NODE_OWNER_COUNT=1
fi

ACTIVE_MEMBERS_YAML=""
for i in "${ACTIVE_IDS[@]}"; do
    port="${CONTROL_PORTS[$((i-1))]}"
    ACTIVE_MEMBERS_YAML="$ACTIVE_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\", node_advertise: \"127.0.0.1:$port\" }
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
        NEXT_MEMBERS_YAML="$NEXT_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\", node_advertise: \"127.0.0.1:$port\" }
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
membership:
  active: 1
${NEXT_LINE}  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML$NEXT_MEMBERS_BLOCK
  owners:
    route_link: $ROUTE_OWNER_COUNT
    node_link: $NODE_OWNER_COUNT
    placer_link: $SCALE_OWNER_COUNT
    node_list: $NODE_LIST_OWNER_COUNT
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

for i in $(seq 1 "$SCALERS"); do
    port="${SCALER_PORTS[$((i-1))]}"
    cat >"$WORK/placer-$i.yaml" <<EOF
placer:
  id: placer-$i
  listen: "127.0.0.1:$port"
  advertise: "http://127.0.0.1:$port"
  memberlist_label: "placer.default"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
import_groups:
  - source_id: stub-file-source
    source_type: file
    path: "$WORK/groups"
placement:
  candidates: 2
  zone_admit_max: "yellow"
  selector_patch_refresh_interval: "2s"
EOF
done

mkdir -p "$WORK/groups"
cat >"$WORK/groups/group.json" <<EOF
{"group":"$GROUP","manifest_key":"$MANIFEST_KEY","auth_key":"$AUTH_KEY","template_ref":"tmpl-stub","node_selectors":[{"pool":"stub"}],"sandbox_config":{"stub.create_delay_ms":"15","stub.http_status":"204"}}
EOF

for i in $(seq 1 "$REGISTRIES"); do
    port="${CONTROL_PORTS[$((i-1))]}"
    "$CLUSTER_CTL" registry --config "$WORK/registry-$i.yaml" > >(tee "$WORK/registry-$i.log" >&2) 2>&1 &
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

SCALER_PIDS=()
for i in $(seq 1 "$SCALERS"); do
    port="${SCALER_PORTS[$((i-1))]}"
    step "starting placer-$i"
    "$CLUSTER_CTL" placer --config "$WORK/placer-$i.yaml" > >(tee "$WORK/placer-$i.log" >&2) 2>&1 &
    pid="$!"
    PIDS+=("$pid")
    SCALER_PIDS+=("$pid")
    wait_tcp "$port" "placer-$i"
done

step "starting node-stub-ctl with $NODES nodes"
"$NODE_STUB_CTL" serve \
    --node-link "127.0.0.1:$CONTROL_PORT" \
    --nodes "$NODES" \
    --node-prefix stub \
    --admin-listen "127.0.0.1:$ADMIN_PORT" \
    --data-listen "127.0.0.1:$DATA_PORT" \
    --label pool=stub \
    --heartbeat 500ms > >(tee "$WORK/node-stub.log" >&2) 2>&1 &
PIDS+=("$!")
ADMIN="http://127.0.0.1:$ADMIN_PORT"
wait_http "$ADMIN/healthz" "node-stub admin"

step "starting router"
"$CLUSTER_CTL" router --config "$WORK/router.yaml" > >(tee "$WORK/router.log" >&2) 2>&1 &
PIDS+=("$!")
wait_tcp "$ROUTER_PORT" "router"

step "waiting for node_link key-lease cache"
python3 - "$ADMIN" "$NODES" <<'PY' || fail "key leases were not distributed to all stub nodes"
import json, sys, time, urllib.request
admin, want = sys.argv[1], int(sys.argv[2])
for _ in range(300):
    try:
        nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=1))
        if len(nodes) == want and all(len(n.get("keys", [])) >= 1 for n in nodes):
            sys.exit(0)
    except Exception:
        pass
    time.sleep(0.1)
sys.exit(1)
PY

if [ "$CLUSTER_STUB_CASE" = "registry-redirect" ]; then
    step "checking node_link redirect to node owners"
    python3 - "$ADMIN" <<'PY' || fail "node_link redirect was not observed"
import json, sys, time, urllib.request
admin = sys.argv[1]
last = []
for _ in range(200):
    nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=2))
    redirected = [n for n in nodes if n.get("redirect_endpoint")]
    if redirected and all(n.get("link_endpoint") == n.get("redirect_endpoint") for n in redirected):
        sys.exit(0)
    last = nodes
    time.sleep(0.1)
raise SystemExit("nodes=%r" % last)
PY
fi

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
location = json.loads(last["metadata"]["kuasar-sandbox.cluster"])
assert location["group"] == sys.argv[2], last
assert location["route_key"] == "user1/session1", last
assert last.get("access_token", "").startswith("sat_"), last
PY

if [ "$CLUSTER_STUB_CASE" = "registry-placer-ha" ]; then
    step "checking placer failover after one placer exits"
    kill "${SCALER_PIDS[0]}" 2>/dev/null || true
    wait "${SCALER_PIDS[0]}" 2>/dev/null || true
    code="$(retry_code 204 "$WORK/data-placer-ha.body" \
        -H "Host: data.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: user1/session-placer-failover" \
        -H "X-API-KEY: $API_KEY" \
        -H "E2b-Sandbox-Port: 49983" \
        "http://127.0.0.1:$ROUTER_PORT/health" || true)"
    [ "$code" = "204" ] || fail "data after placer failure returned $code"
fi

if [ "$CLUSTER_STUB_CASE" = "registry-joint" ]; then
    step "checking joint route visibility from next-only registry"
    curl -sS --noproxy '*' --get --data-urlencode "group=$GROUP" \
        "http://127.0.0.1:${CONTROL_PORTS[3]}/route-link/list" >"$WORK/joint-routes.json"
    python3 - "$WORK/joint-routes.json" <<'PY' || fail "next-only registry did not expose the ready route"
import json, sys
routes = json.load(open(sys.argv[1]))
assert any(r.get("sandboxID") and r.get("state") == "ready" for r in routes), routes
PY

    step "cutting registry membership over to version 2 with old_grace version 1"
    for i in 1 2 3 4; do
        port="${CONTROL_PORTS[$((i-1))]}"
        cat >"$WORK/registry-$i.yaml" <<EOF
member:
  id: registry-$i
  listen: "127.0.0.1:$port"
membership:
  active: 2
  old_grace: 1
  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML
    - version: 2
      members:
$NEXT_MEMBERS_YAML  owners:
    route_link: $ROUTE_OWNER_COUNT
    node_link: $NODE_OWNER_COUNT
    placer_link: $SCALE_OWNER_COUNT
    node_list: $NODE_LIST_OWNER_COUNT
node_link:
  heartbeat_interval: "500ms"
  node_dead_after: "3s"
route_link:
  park_timeout: "5s"
EOF
        code="$(http_code "$WORK/reload-$i.body" -X POST "http://127.0.0.1:$port/cluster/reload" || true)"
        [ "$code" = "204" ] || fail "registry-$i cutover reload returned $code: $(cat "$WORK/reload-$i.body")"
    done

    step "checking registries report active membership version 2 with old_grace version 1"
    for i in 1 2 3 4; do
        port="${CONTROL_PORTS[$((i-1))]}"
        python3 - "http://127.0.0.1:$port" <<'PY' || fail "registry cutover membership check failed on $port"
import json, sys, urllib.request
m = json.load(urllib.request.urlopen(sys.argv[1] + "/cluster/membership", timeout=2))
assert m.get("active", m.get("Active")) == 2, m
assert not m.get("next", m.get("Next", 0)), m
assert m.get("old_grace", m.get("OldGrace")) == 1, m
PY
    done

    step "checking router/placer refresh through known members after cutover"
    code="$(retry_code 204 "$WORK/data-cutover.body" \
        -H "Host: data.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: user1/session-cutover" \
        -H "X-API-KEY: $API_KEY" \
        -H "E2b-Sandbox-Port: 49983" \
        "http://127.0.0.1:$ROUTER_PORT/health" || true)"
    [ "$code" = "204" ] || fail "data after registry cutover returned $code"

    python3 - "$ADMIN" "$GROUP" <<'PY' || fail "cutover data hit was not recorded with the expected route key"
import json, sys, urllib.request
hits = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/data-hits", timeout=2))
assert hits, "no data hits"
last = hits[-1]
location = json.loads(last["metadata"]["kuasar-sandbox.cluster"])
assert location["group"] == sys.argv[2], last
assert location["route_key"] == "user1/session-cutover", last
PY
fi

create_count_before="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
def route_key(c):
    raw = c.get("metadata", {}).get("kuasar-sandbox.cluster")
    return json.loads(raw).get("route_key") if raw else None
print(sum(1 for c in cmds if c.get("kind") == "create" and route_key(c) == "user1/session1"))
PY
)"
[ "$create_count_before" = "1" ] || fail "expected one create for user1/session1 before active-cache check, got $create_count_before"

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
def route_key(c):
    raw = c.get("metadata", {}).get("kuasar-sandbox.cluster")
    return json.loads(raw).get("route_key") if raw else None
print(sum(1 for c in cmds if c.get("kind") == "create" and route_key(c) == "user1/session1"))
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
    "http://127.0.0.1:$ROUTER_PORT/v3/templates" || true)"
[ "$code" = "202" ] || fail "build register returned $code: $(cat "$WORK/build.body")"

python3 - "$ADMIN" <<'PY' || fail "build_register command with default e2b profile was not observed"
import json, sys, urllib.request
cmds = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
assert any(c.get("kind") == "build_register" and c.get("profile") == "e2b" for c in cmds), cmds
PY

step "checking unowned node-local route isolation"
"$NODE_STUB_CTL" sandbox orphan --admin "$ADMIN" --node stub-1 --sid sb-orphan >"$WORK/orphan.out"
python3 - "$ADMIN" <<'PY' || fail "unowned node-local route triggered a delete command"
import json, sys, time, urllib.request
admin = sys.argv[1]
time.sleep(1)
cmds = json.load(urllib.request.urlopen(admin + "/v1/nodes/stub-1/commands", timeout=2))
assert not any(c.get("kind") == "delete" and c.get("sid") == "sb-orphan" for c in cmds), cmds
PY

step "checking reboot-empty cleanup"
python3 - "$ADMIN" "$WORK/first_sandbox.env" <<'PY'
import json, shlex, sys, urllib.request
nodes = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/nodes", timeout=2))
for n in nodes:
    for s in n.get("sandboxes", []):
        raw = s.get("metadata", {}).get("kuasar-sandbox.cluster")
        location = json.loads(raw) if raw else {}
        if location.get("route_key") == "user1/session1":
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

ROUTE_LIST_PORT="$CONTROL_PORT"
if [ "$CLUSTER_STUB_CASE" = "registry-joint" ]; then
    ROUTE_LIST_PORT="${CONTROL_PORTS[3]}"
fi
python3 - "$ROUTE_LIST_PORT" "$GROUP" "$SID" "$WORK/routes.json" <<'PY' || fail "route_link still contains rebooted sandbox"
import json, sys, time, urllib.parse, urllib.request
port, group, sid, out = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
url = "http://127.0.0.1:%s/route-link/list?%s" % (port, urllib.parse.urlencode({"group": group}))
last = None
for _ in range(100):
    with urllib.request.urlopen(url, timeout=2) as resp:
        last = json.load(resp)
    with open(out, "w") as f:
        json.dump(last, f)
    if all(r.get("sandboxID") != sid for r in last):
        sys.exit(0)
    time.sleep(0.1)
raise SystemExit("routes=%r" % (last,))
PY

echo "==> PASS: orchestrator cluster stub e2e"
