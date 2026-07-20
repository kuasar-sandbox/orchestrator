#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${BIN:-$ROOT/bin/$(uname -m)}"
CLUSTER_CTL="${CLUSTER_CTL:-$BIN/cluster-ctl}"
NODE_STUB_CTL="${NODE_STUB_CTL:-$BIN/node-stub-ctl}"
E2B_KEY_CTL="${E2B_KEY_CTL:-$BIN/e2b-key-ctl}"
DOMAIN="${DOMAIN:-cluster.stub.local}"
GROUP="${GROUP:-/e2e/stub/group}"
NODES="${NODES:-6}"
VIRTUAL_SHARDS="${VIRTUAL_SHARDS:-8}"
STARTED_AT="$(date +%s)"

step() { echo "==> $*" >&2; }

for command in curl openssl python3 stat; do
    command -v "$command" >/dev/null 2>&1 || {
        echo "missing required command: $command" >&2
        exit 1
    }
done
for binary in "$CLUSTER_CTL" "$NODE_STUB_CTL" "$E2B_KEY_CTL"; do
    [ -x "$binary" ] || {
        echo "missing e2e binary: $binary (run make build)" >&2
        exit 1
    }
done
if [ "$(stat -f -c %T /dev/shm)" != "tmpfs" ]; then
    echo "/dev/shm must be tmpfs for the explicit ephemeral Registry storage mode" >&2
    exit 1
fi

WORK="$(mktemp -d /dev/shm/kuasar-e2e.XXXXXX)"
ADMIN=""
PIDS=()
REGISTRY_PIDS=()

cleanup() {
    for ((index=${#PIDS[@]}-1; index>=0; index--)); do
        kill "${PIDS[$index]}" 2>/dev/null || true
    done
    for ((index=${#PIDS[@]}-1; index>=0; index--)); do
        wait "${PIDS[$index]}" 2>/dev/null || true
    done
    if [ "${E2E_KEEP_WORK:-0}" = "1" ]; then
        step "keeping work directory $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

fail() {
    echo "==> FAIL: $*" >&2
    if [ -n "$ADMIN" ]; then
        curl -fsS --noproxy '*' --max-time 2 "$ADMIN/v1/nodes" >"$WORK/failure-nodes.json" 2>/dev/null || true
        curl -fsS --noproxy '*' --max-time 2 "$ADMIN/v1/commands" >"$WORK/failure-commands.json" 2>/dev/null || true
        curl -fsS --noproxy '*' --max-time 2 "$ADMIN/v1/events" >"$WORK/failure-events.json" 2>/dev/null || true
    fi
    for file in "$WORK"/*.json "$WORK"/*.body "$WORK"/*.log; do
        [ -f "$file" ] || continue
        echo "---- $file ----" >&2
        tail -n 240 "$file" >&2 || true
    done
    exit 1
}

USED_PORTS=()
alloc_port() {
    local target="$1" candidate used
    for _ in $(seq 1 100); do
        candidate="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
        used=0
        for existing in "${USED_PORTS[@]:-}"; do
            [ "$existing" = "$candidate" ] && used=1
        done
        if [ "$used" = 0 ]; then
            USED_PORTS+=("$candidate")
            printf -v "$target" '%s' "$candidate"
            return
        fi
    done
    fail "could not allocate a unique local port"
}

TLS_DIR="$WORK/tls"
mkdir -p "$TLS_DIR"
openssl ecparam -name prime256v1 -genkey -noout -out "$TLS_DIR/ca.key" 2>/dev/null
openssl req -x509 -new -key "$TLS_DIR/ca.key" -sha256 -days 2 \
    -subj '/CN=kuasar-e2e-ca' -out "$TLS_DIR/ca.crt" 2>/dev/null

make_role_certificate() {
    local role="$1"
    openssl ecparam -name prime256v1 -genkey -noout -out "$TLS_DIR/$role.key" 2>/dev/null
    openssl req -new -key "$TLS_DIR/$role.key" -subj "/CN=kuasar-e2e-$role" \
        -out "$TLS_DIR/$role.csr" 2>/dev/null
    cat >"$TLS_DIR/$role.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=IP:127.0.0.1,URI:spiffe://kuasar.internal/$role/e2e-$role
EOF
    openssl x509 -req -in "$TLS_DIR/$role.csr" -CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" \
        -CAcreateserial -days 2 -sha256 -extfile "$TLS_DIR/$role.ext" -out "$TLS_DIR/$role.crt" 2>/dev/null
}

for role in registry placer router node operator; do
    make_role_certificate "$role"
done

REGISTRY_PORTS=()
RAFT_PORTS=()
for _ in 1 2 3; do
    alloc_port port
    REGISTRY_PORTS+=("$port")
    alloc_port port
    RAFT_PORTS+=("$port")
done
alloc_port PLACER_PORT
alloc_port ROUTER_PORT
alloc_port ADMIN_PORT
alloc_port DATA_PORT

BOOTSTRAP_SECRET="$WORK/bootstrap.secret"
printf '%s\n' 'kuasar-e2e-generation-1' >"$BOOTSTRAP_SECRET"
openssl genpkey -algorithm ED25519 -out "$WORK/registry-layout-signing.pem" 2>/dev/null
chmod 0600 "$WORK/registry-layout-signing.pem"

python3 - "$WORK/members.json" \
    "${REGISTRY_PORTS[0]}" "${RAFT_PORTS[0]}" \
    "${REGISTRY_PORTS[1]}" "${RAFT_PORTS[1]}" \
    "${REGISTRY_PORTS[2]}" "${RAFT_PORTS[2]}" <<'PY'
import json, sys
out = []
for index in range(3):
    control, raft = sys.argv[2 + index * 2:4 + index * 2]
    out.append({
        "member_id": f"registry-{index + 1}",
        "internal_endpoint": f"https://127.0.0.1:{control}",
        "raft_endpoint": f"127.0.0.1:{raft}",
    })
with open(sys.argv[1], "w") as f:
    json.dump(out, f)
PY

"$CLUSTER_CTL" registry-layout bootstrap \
    --members "$WORK/members.json" \
    --cluster-id kuasar-e2e \
    --registry-generation generation-e2e-1 \
    --bootstrap-secret-file "$BOOTSTRAP_SECRET" \
    --signing-key "$WORK/registry-layout-signing.pem" \
    --key-id e2e-root \
    --chain-out "$WORK/registry-layout-chain.json" \
    --keyring-out "$WORK/registry-layout-keyring.json" \
    --virtual-shards "$VIRTUAL_SHARDS" \
    --serve-permit-max 3s

AUTH_KEY="$("$E2B_KEY_CTL" gen-key)"
MANIFEST_KEY="$("$E2B_KEY_CTL" gen-key)"
API_KEY="$("$E2B_KEY_CTL" gen-apikey "$AUTH_KEY")"
mkdir -p "$WORK/groups"
cat >"$WORK/groups/group.json" <<EOF
{
  "group": "$GROUP",
  "manifest_key": "$MANIFEST_KEY",
  "auth_key": "$AUTH_KEY",
  "template_ref": "e2b-img-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "target_port": 49983,
  "node_selectors": [{"pool": "stub"}],
  "sandbox_config": {"stub.create_delay_ms": "15", "stub.http_status": "204"}
}
EOF

cat >"$WORK/placer.yaml" <<EOF
placer:
  id: placer-1
  listen: "127.0.0.1:$PLACER_PORT"
  tls: { cert: "$TLS_DIR/placer.crt", key: "$TLS_DIR/placer.key", ca: "$TLS_DIR/ca.crt" }
group_sources:
  - { source_id: e2e-file, source_type: file, path: "$WORK/groups" }
placement:
  candidates: 4
EOF

cat >"$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
registry_layout:
  chain: "$WORK/registry-layout-chain.json"
  keys: "$WORK/registry-layout-keyring.json"
  guard: "$WORK/router-registry-layout.guard"
registry_tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
node_tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
providers:
  endpoints:
    - { name: placer-1, endpoint: "https://127.0.0.1:$PLACER_PORT" }
  tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: enforce
  data_plane: off
  cache_ttl: 1s
cache:
  route_ttl: 30s
  idle_timeout: 30s
EOF

for index in 0 1 2; do
    member=$((index + 1))
    root="$WORK/registry-$member"
    mkdir -p "$root"
    cat >"$WORK/registry-$member.yaml" <<EOF
member:
  id: registry-$member
  listen: "127.0.0.1:${REGISTRY_PORTS[$index]}"
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
registry_layout:
  chain: "$WORK/registry-layout-chain.json"
  keys: "$WORK/registry-layout-keyring.json"
  guard: "$root/registry-layout.guard"
storage:
  nodehost_dir: "$root/nodehost"
  wal_dir: "$root/wal"
  state_engine_dir: "$root/state"
  enrollment_path: "$root/enrollment.json"
  raft_listen: "127.0.0.1:${RAFT_PORTS[$index]}"
  open_mode: bootstrap
  bootstrap_secret_file: "$BOOTSTRAP_SECRET"
  storage_protection: ephemeral-tmpfs
  initialize_workers: 4
  transition_workers: 4
  snapshot_workers: 2
  operation_timeout: 5s
  fence_retention: 100ms
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
placers:
  endpoints:
    - { name: placer-1, endpoint: "https://127.0.0.1:$PLACER_PORT" }
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
session:
  max_nodes: 100
  anti_entropy: 200ms
  event_workers: 16
  reconnect_per_second: 100
workflow:
  park_timeout: 10s
  poll_interval: 10ms
  permit_refresh: 200ms
  recovery_scan_interval: 100ms
  recovery_shards_per_scan: 8
  recovery_workers: 2
  recovery_per_node_workers: 1
  compaction_workers: 2
  pending_workflows_per_page: 64
  recovery_page_objects: 32
  recovery_page_bytes: 262144
  recovery_max_report_bytes: 8388608
  recovery_lookup_page: 64
EOF
done

operator_curl() {
    curl -sS --noproxy '*' --cacert "$TLS_DIR/ca.crt" \
        --cert "$TLS_DIR/operator.crt" --key "$TLS_DIR/operator.key" "$@"
}

wait_https() {
    local url="$1" name="$2"
    for _ in $(seq 1 900); do
        if operator_curl --max-time 1 -o /dev/null -f "$url" 2>/dev/null; then
            return
        fi
        sleep 0.1
    done
    fail "$name did not become healthy at $url"
}

wait_http() {
    local url="$1" name="$2"
    for _ in $(seq 1 300); do
        if curl -fsS --noproxy '*' --max-time 1 -o /dev/null "$url" 2>/dev/null; then
            return
        fi
        sleep 0.1
    done
    fail "$name did not become healthy at $url"
}

step "starting final Placer"
"$CLUSTER_CTL" placer --config "$WORK/placer.yaml" >"$WORK/placer.log" 2>&1 &
PIDS+=("$!")
wait_https "https://127.0.0.1:$PLACER_PORT/health" placer

step "starting three-replica System Group and $VIRTUAL_SHARDS data shards"
for index in 0 1 2; do
    member=$((index + 1))
    "$CLUSTER_CTL" registry --config "$WORK/registry-$member.yaml" >"$WORK/registry-$member.log" 2>&1 &
    pid="$!"
    PIDS+=("$pid")
    REGISTRY_PIDS[$index]="$pid"
done
for index in 0 1 2; do
    wait_https "https://127.0.0.1:${REGISTRY_PORTS[$index]}/health" "registry-$((index + 1))"
done

REGISTRY_BASE="https://127.0.0.1:${REGISTRY_PORTS[0]}"
operator_curl -f "$REGISTRY_BASE/internal/operator/system/state" >"$WORK/system-before.json"
python3 - "$WORK/system-before.json" "$WORK/generation.json" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
identity = {
    "cluster_id": state["cluster_id"],
    "registry_generation": state["registry_generation"],
    "system_epoch": state["system_epoch"],
    "registry_layout_digest": state["active_registry_layout_digest"],
}
json.dump(identity, open(sys.argv[2], "w"))
PY

post_operator() {
    local path="$1" body="$2" output="$3" code
    for _ in $(seq 1 100); do
        code="$(operator_curl --max-time 3 -o "$output" -w '%{http_code}' \
            -H 'Content-Type: application/json' --data-binary "@$body" "$REGISTRY_BASE$path" 2>/dev/null || true)"
        if [ "$code" = 204 ]; then
            return
        fi
        sleep 0.1
    done
    fail "operator POST $path returned ${code:-000}"
}

step "activating the empty Registry History Generation"
post_operator /internal/operator/generation/activate "$WORK/generation.json" "$WORK/activate.body"

for index in $(seq 1 "$NODES"); do
    python3 - "$WORK/generation.json" "$WORK/enroll-$index.json" "$index" "$DATA_PORT" <<'PY'
import json, sys
request = json.load(open(sys.argv[1]))
index = int(sys.argv[3])
request.update({
    "node_id": f"stub-{index}",
    "enrollment_id": f"stub-enrollment-stub-{index}",
    "node_epoch": 1,
    "data_endpoint": f"127.0.0.1:{sys.argv[4]}",
})
json.dump(request, open(sys.argv[2], "w"))
PY
    post_operator /internal/operator/node/enroll "$WORK/enroll-$index.json" "$WORK/enroll-$index.body"
done

step "starting durable node-link stubs"
"$NODE_STUB_CTL" serve \
    --node-link "$REGISTRY_BASE" \
    --nodes "$NODES" \
    --node-prefix stub \
    --admin-listen "127.0.0.1:$ADMIN_PORT" \
    --data-listen "127.0.0.1:$DATA_PORT" \
    --state-dir "$WORK/node-state" \
    --tls-cert "$TLS_DIR/node.crt" \
    --tls-key "$TLS_DIR/node.key" \
    --tls-ca "$TLS_DIR/ca.crt" \
    --label pool=stub \
    --heartbeat 100ms >"$WORK/node-stub.log" 2>&1 &
PIDS+=("$!")
ADMIN="http://127.0.0.1:$ADMIN_PORT"
wait_http "$ADMIN/healthz" node-stub

python3 - "$ADMIN" "$NODES" <<'PY' || fail "node-link sessions did not converge"
import json, sys, time, urllib.request
admin, expected = sys.argv[1], int(sys.argv[2])
for _ in range(400):
    try:
        nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=1))
        if len(nodes) == expected and all(node.get("link_endpoint") for node in nodes):
            raise SystemExit(0)
    except Exception:
        pass
    time.sleep(0.1)
raise SystemExit(1)
PY

step "starting final Router"
"$CLUSTER_CTL" router --config "$WORK/router.yaml" >"$WORK/router.log" 2>&1 &
ROUTER_PID="$!"
PIDS+=("$ROUTER_PID")
for _ in $(seq 1 300); do
    if curl -fsS --noproxy '*' --max-time 1 -o /dev/null -H "Host: api.$DOMAIN" \
        "http://127.0.0.1:$ROUTER_PORT/health" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

request_data() {
    local route_key="$1" output="$2" code
    for _ in $(seq 1 300); do
        code="$(curl -sS --noproxy '*' --max-time 5 -o "$output" -w '%{http_code}' \
            -H "Host: data.$DOMAIN" \
            -H "X-Kuasar-Sandbox-Group: $GROUP" \
            -H "X-Kuasar-Route-Key: $route_key" \
            -H "X-API-KEY: $API_KEY" \
            -H 'E2b-Sandbox-Port: 49983' \
            "http://127.0.0.1:$ROUTER_PORT/health" 2>/dev/null || true)"
        if [ "$code" = 204 ]; then
            return
        fi
        sleep 0.1
    done
    fail "data request for $route_key returned ${code:-000}"
}

command_count() {
    python3 - "$ADMIN" "$1" <<'PY'
import json, sys, urllib.request
commands = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
print(sum(command.get("kind") == sys.argv[2] for command in commands))
PY
}

step "checking Reserve -> node Admission -> READY -> fenced data forwarding"
request_data user1/session1 "$WORK/data-first.body"
dispatch_before="$(command_count sandbox_admit_dispatch)"
[ "$dispatch_before" -ge 1 ] || fail "Sandbox Admission command was not observed"
request_data user1/session1 "$WORK/data-cached.body"
dispatch_after="$(command_count sandbox_admit_dispatch)"
[ "$dispatch_before" = "$dispatch_after" ] || fail "READY cache caused duplicate Sandbox Admission"

read -r ROUTE_NODE ROUTE_SID < <(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
hits = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/data-hits", timeout=2))
assert hits
print(hits[-1]["node_id"], hits[-1]["sid"])
PY
)

step "checking Holder failure reconnect and two-replica quorum service"
python3 - "$ADMIN" "${REGISTRY_PORTS[1]}" "${REGISTRY_PORTS[2]}" "$WORK/kill-holder.json" <<'PY'
import json, sys, urllib.request
nodes = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/nodes", timeout=2))
endpoints = [f"https://127.0.0.1:{sys.argv[2]}", f"https://127.0.0.1:{sys.argv[3]}"]
counts = [sum(node.get("link_endpoint") == endpoint for node in nodes) for endpoint in endpoints]
index = 1 if counts[0] >= counts[1] else 2
json.dump({"index": index, "endpoint": endpoints[index - 1], "affected": counts[index - 1]}, open(sys.argv[4], "w"))
PY
read -r KILL_INDEX KILLED_ENDPOINT AFFECTED < <(python3 - "$WORK/kill-holder.json" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
print(value["index"], value["endpoint"], value["affected"])
PY
)
kill "${REGISTRY_PIDS[$KILL_INDEX]}"
wait "${REGISTRY_PIDS[$KILL_INDEX]}" 2>/dev/null || true

if [ "$AFFECTED" -gt 0 ]; then
    python3 - "$ADMIN" "$KILLED_ENDPOINT" <<'PY' || fail "sessions assigned to the failed Holder did not reconnect"
import json, sys, time, urllib.request
for _ in range(400):
    nodes = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/nodes", timeout=2))
    if all(node.get("link_endpoint") and node.get("link_endpoint") != sys.argv[2] for node in nodes):
        raise SystemExit(0)
    time.sleep(0.1)
raise SystemExit(1)
PY
fi
request_data user1/session1 "$WORK/data-after-registry-failure.body"
request_data user1/session-after-registry-failure "$WORK/data-new-after-registry-failure.body"

step "checking newer NodeEpoch proof replaces, rather than revives, the old execution"
"$NODE_STUB_CTL" node reboot-empty "$ROUTE_NODE" --admin "$ADMIN" >"$WORK/reboot.json"
request_data user1/session1 "$WORK/data-after-node-epoch.body"
NEW_SID="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
hits = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/data-hits", timeout=2))
print(hits[-1]["sid"])
PY
)"
[ "$NEW_SID" != "$ROUTE_SID" ] || fail "new NodeEpoch reused the fenced Sandbox execution"

step "checking bound-node Build trigger, idempotency, and local lifecycle"
for _ in $(seq 1 200); do
    build_code="$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/build.body" -w '%{http_code}' \
        -X POST -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-API-KEY: $API_KEY" -H 'Content-Type: application/json' \
        --data '{"name":"e2e-template","cpuCount":1,"memoryMB":128}' \
        "http://127.0.0.1:$ROUTER_PORT/v3/templates" 2>/dev/null || true)"
    [ "$build_code" = 202 ] && break
    sleep 0.1
done
[ "${build_code:-000}" = 202 ] || fail "Build registration returned ${build_code:-000}"
read -r BUILD_ID BUILD_TEMPLATE < <(python3 - "$WORK/build.body" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
print(value["buildID"], value["templateID"])
PY
)
BUILD_DISPATCHES="$(command_count build_admit_dispatch)"
[ "$BUILD_DISPATCHES" -ge 1 ] || fail "Build Admission command was not observed"
BUILD_TRIGGER='{"fromImage":"registry.stub/base:latest"}'
for attempt in first retry; do
    trigger_code="$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/build-trigger-$attempt.body" -w '%{http_code}' \
        -X POST -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-API-KEY: $API_KEY" -H 'Content-Type: application/json' --data "$BUILD_TRIGGER" \
        "http://127.0.0.1:$ROUTER_PORT/v2/templates/$BUILD_TEMPLATE/builds/$BUILD_ID" 2>/dev/null || true)"
    [ "$trigger_code" = 202 ] || fail "Build trigger $attempt returned ${trigger_code:-000}"
done
conflict_code="$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/build-trigger-conflict.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" \
    -H "X-API-KEY: $API_KEY" -H 'Content-Type: application/json' \
    --data '{"fromImage":"registry.stub/other:latest"}' \
    "http://127.0.0.1:$ROUTER_PORT/v2/templates/$BUILD_TEMPLATE/builds/$BUILD_ID" 2>/dev/null || true)"
[ "$conflict_code" = 409 ] || fail "conflicting Build trigger returned ${conflict_code:-000}"
[ "$(command_count build_admit_dispatch)" = "$BUILD_DISPATCHES" ] || fail "Build trigger caused another placement"

build_ready=0
for _ in $(seq 1 300); do
    status_code="$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/build-status.body" -w '%{http_code}' \
        -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-API-KEY: $API_KEY" \
        "http://127.0.0.1:$ROUTER_PORT/templates/$BUILD_TEMPLATE/builds/$BUILD_ID/status" 2>/dev/null || true)"
    if [ "$status_code" = 200 ] && python3 - "$WORK/build-status.body" <<'PY'
import json, sys
raise SystemExit(0 if json.load(open(sys.argv[1])).get("status") == "ready" else 1)
PY
    then
        build_ready=1
        break
    fi
    sleep 0.1
done
[ "$build_ready" = 1 ] || fail "node-local Build did not become ready"

python3 - "$ADMIN" <<'PY' || fail "removed node-link command kinds were emitted"
import json, sys, urllib.request
forbidden = {"create", "connect", "delete", "key_put", "key_drop", "build_register"}
commands = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/commands", timeout=2))
bad = [command for command in commands if command.get("kind") in forbidden]
assert not bad, bad
PY

step "capturing the node-authoritative execution before Registry History Generation rollover"
RECOVERY_SID="$NEW_SID"
RECOVERY_DISPATCHES="$(command_count sandbox_admit_dispatch)"
python3 - "$ADMIN" "$RECOVERY_SID" "$WORK/source-execution.json" "$WORK/source-node-sessions.json" <<'PY' \
    || fail "current execution Binding was not observable before rollover"
import json, sys, urllib.request
nodes = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/nodes", timeout=2))
sid = sys.argv[2]
matches = [
    (node, sandbox)
    for node in nodes
    for sandbox in node.get("sandboxes", [])
    if sandbox.get("sid") == sid
]
assert len(matches) == 1, matches
node, sandbox = matches[0]
assert sandbox.get("registry_generation") == "generation-e2e-1", sandbox
assert len(sandbox.get("binding_digest", "")) == 64, sandbox
assert sandbox.get("access_token"), sandbox
json.dump({
    "node_id": node["node_id"],
    "node_epoch": node["node_epoch"],
    "data_endpoint": node["data_endpoint"],
    "sid": sid,
    "registry_generation": sandbox["registry_generation"],
    "binding_digest": sandbox["binding_digest"],
    "access_token": sandbox["access_token"],
}, open(sys.argv[3], "w"))
json.dump({node["node_id"]: node["session_seq"] for node in nodes}, open(sys.argv[4], "w"))
PY

TARGET_BOOTSTRAP_SECRET="$WORK/bootstrap-v2.secret"
printf '%s\n' 'kuasar-e2e-generation-2' >"$TARGET_BOOTSTRAP_SECRET"
python3 - "$WORK/registry-layout-chain.json" "$WORK/generation.json" "$TARGET_BOOTSTRAP_SECRET" \
    "$WORK/successor-intent.json" "$WORK/close-generation.json" <<'PY'
import hashlib, json, sys
chain = json.load(open(sys.argv[1]))
source = json.load(open(sys.argv[2]))
successor = dict(chain[-1]["registry_layout"])
successor.update({
    "registry_generation": "generation-e2e-2",
    "registry_layout_version": 1,
    "bootstrap_token_digest": hashlib.sha256(open(sys.argv[3], "rb").read().strip()).hexdigest(),
    "predecessor": {
        "registry_generation": source["registry_generation"],
        "registry_layout_digest": source["registry_layout_digest"],
        "serve_permit_max_millis": successor["serve_permit_max_millis"],
        "kind": "CONSENSUS_CLOSURE",
    },
})
successor.pop("previous_registry_layout_version", None)
successor.pop("previous_registry_layout_digest", None)
json.dump(successor, open(sys.argv[4], "w"))
request = dict(source)
request["successor"] = successor
json.dump(request, open(sys.argv[5], "w"))
PY

step "permanently closing generation-e2e-1 for the exact successor intent"
close_code=""
for _ in $(seq 1 100); do
    close_code="$(operator_curl --max-time 5 -o "$WORK/close-generation.body" -w '%{http_code}' \
        -H 'Content-Type: application/json' --data-binary @"$WORK/close-generation.json" \
        "$REGISTRY_BASE/internal/operator/generation/close" 2>/dev/null || true)"
    [ "$close_code" = 200 ] && break
    sleep 0.1
done
[ "$close_code" = 200 ] || fail "generation closure returned ${close_code:-000}"

python3 - "$WORK/successor-intent.json" "$WORK/close-generation.body" "$WORK/successor.json" <<'PY' \
    || fail "generation closure returned an invalid predecessor proof"
import json, sys
successor = json.load(open(sys.argv[1]))
response = json.load(open(sys.argv[2]))
proof = response["predecessor_proof"]
assert proof["registry_generation"] == "generation-e2e-1", proof
assert proof["kind"] == "CONSENSUS_CLOSURE", proof
assert proof["commit_index"] > 0, proof
assert len(proof["proof_digest"]) == 64, proof
successor["predecessor"] = proof
json.dump(successor, open(sys.argv[3], "w"))
PY

step "signing the generation-e2e-2 immutable Registry Layout artifact"
"$CLUSTER_CTL" registry-layout append \
    --chain-in "$WORK/registry-layout-chain.json" \
    --keys "$WORK/registry-layout-keyring.json" \
    --registry-layout "$WORK/successor.json" \
    --signing-key "$WORK/registry-layout-signing.pem" \
    --key-id e2e-root \
    --chain-out "$WORK/registry-layout-chain-v2.json"

step "stopping every generation-e2e-1 Router and Registry endpoint"
kill "$ROUTER_PID" 2>/dev/null || true
wait "$ROUTER_PID" 2>/dev/null || true
for pid in "${REGISTRY_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
done
for pid in "${REGISTRY_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
for member in 1 2 3; do
    rm -rf "$WORK/registry-$member"
done

cat >"$WORK/router-v2.yaml" <<EOF
domain: "$DOMAIN"
registry_layout:
  chain: "$WORK/registry-layout-chain-v2.json"
  keys: "$WORK/registry-layout-keyring.json"
  guard: "$WORK/router-v2-registry-layout.guard"
registry_tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
node_tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
providers:
  endpoints:
    - { name: placer-1, endpoint: "https://127.0.0.1:$PLACER_PORT" }
  tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: enforce
  data_plane: off
  cache_ttl: 1s
cache:
  route_ttl: 30s
  idle_timeout: 30s
EOF

write_target_registry_config() {
    local index="$1"
    local mode="$2"
    local recovery_scan="$3"
    local config_path="$4"
    local member=$((index + 1))
    local root="$WORK/registry-v2-$member"
    local bootstrap_secret=""
    if [ "$mode" = bootstrap ]; then
        bootstrap_secret="  bootstrap_secret_file: \"$TARGET_BOOTSTRAP_SECRET\""
    fi
    cat >"$config_path" <<EOF
member:
  id: registry-$member
  listen: "127.0.0.1:${REGISTRY_PORTS[$index]}"
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
registry_layout:
  chain: "$WORK/registry-layout-chain-v2.json"
  keys: "$WORK/registry-layout-keyring.json"
  guard: "$root/registry-layout.guard"
storage:
  nodehost_dir: "$root/nodehost"
  wal_dir: "$root/wal"
  state_engine_dir: "$root/state"
  enrollment_path: "$root/enrollment.json"
  raft_listen: "127.0.0.1:${RAFT_PORTS[$index]}"
  open_mode: $mode
$bootstrap_secret
  storage_protection: ephemeral-tmpfs
  initialize_workers: 4
  transition_workers: 4
  snapshot_workers: 2
  operation_timeout: 5s
  fence_retention: 100ms
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
placers:
  endpoints:
    - { name: placer-1, endpoint: "https://127.0.0.1:$PLACER_PORT" }
  tls: { cert: "$TLS_DIR/registry.crt", key: "$TLS_DIR/registry.key", ca: "$TLS_DIR/ca.crt" }
session:
  max_nodes: 100
  anti_entropy: 200ms
  event_workers: 16
  reconnect_per_second: 100
workflow:
  park_timeout: 10s
  poll_interval: 10ms
  permit_refresh: 200ms
  recovery_scan_interval: $recovery_scan
  recovery_shards_per_scan: 8
  recovery_workers: 2
  recovery_per_node_workers: 1
  compaction_workers: 2
  pending_workflows_per_page: 64
  recovery_page_objects: 32
  recovery_page_bytes: 262144
  recovery_max_report_bytes: 8388608
  recovery_lookup_page: 64
EOF
}

for index in 0 1 2; do
    member=$((index + 1))
    mkdir -p "$WORK/registry-v2-$member"
    write_target_registry_config "$index" bootstrap 30s "$WORK/registry-v2-$member.yaml"
    write_target_registry_config "$index" restart 100ms "$WORK/registry-v2-restart-$member.yaml"
done

step "bootstrapping the empty three-replica generation-e2e-2"
REGISTRY_PIDS=()
for index in 0 1 2; do
    member=$((index + 1))
    "$CLUSTER_CTL" registry --config "$WORK/registry-v2-$member.yaml" >"$WORK/registry-v2-$member.log" 2>&1 &
    pid="$!"
    PIDS+=("$pid")
    REGISTRY_PIDS[$index]="$pid"
done
for index in 0 1 2; do
    wait_https "https://127.0.0.1:${REGISTRY_PORTS[$index]}/health" "registry-v2-$((index + 1))"
done

operator_curl -f "$REGISTRY_BASE/internal/operator/system/state" >"$WORK/system-v2-before.json"
python3 - "$WORK/system-v2-before.json" "$WORK/generation-v2.json" <<'PY' \
    || fail "successor System Group did not start with closed cutover gates"
import json, sys
state = json.load(open(sys.argv[1]))
assert state["registry_generation"] == "generation-e2e-2", state
assert state["has_predecessor"] and not state["predecessor_drain_complete"], state
assert not state["serve_gate"] and not state["write_gate"] and not state["cutover_gate"], state
json.dump({
    "cluster_id": state["cluster_id"],
    "registry_generation": state["registry_generation"],
    "system_epoch": state["system_epoch"],
    "registry_layout_digest": state["active_registry_layout_digest"],
}, open(sys.argv[2], "w"))
PY

step "enrolling the exact durable NodeEpoch set in generation-e2e-2"
for index in $(seq 1 "$NODES"); do
    python3 - "$WORK/generation-v2.json" "$ADMIN" "$index" "$WORK/enroll-v2-$index.json" <<'PY'
import json, sys, urllib.request
identity = json.load(open(sys.argv[1]))
node_id = f"stub-{sys.argv[3]}"
nodes = json.load(urllib.request.urlopen(sys.argv[2] + "/v1/nodes", timeout=2))
node = next(value for value in nodes if value["node_id"] == node_id)
identity.update({
    "node_id": node_id,
    "enrollment_id": "stub-enrollment-" + node_id,
    "node_epoch": node["node_epoch"],
    "data_endpoint": node["data_endpoint"],
})
json.dump(identity, open(sys.argv[4], "w"))
PY
    post_operator /internal/operator/node/enroll "$WORK/enroll-v2-$index.json" "$WORK/enroll-v2-$index.body"
done

python3 - "$ADMIN" "$REGISTRY_BASE" "$WORK/source-node-sessions.json" "$NODES" <<'PY' \
    || fail "nodes did not establish new-generation sessions"
import json, ssl, sys, time, urllib.request
admin, expected_endpoint = sys.argv[1], sys.argv[2]
before = json.load(open(sys.argv[3]))
expected = int(sys.argv[4])
for _ in range(600):
    try:
        nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=2))
        if len(nodes) == expected and all(
            node.get("link_endpoint") and
            node.get("session_seq", 0) > before[node["node_id"]]
            for node in nodes
        ):
            raise SystemExit(0)
    except Exception:
        pass
    time.sleep(0.1)
raise SystemExit(1)
PY

step "waiting the full predecessor Serve Permit lifetime"
python3 - "$WORK/generation-v2.json" "$WORK/confirm-drain.json" <<'PY'
import hashlib, json, sys
request = json.load(open(sys.argv[1]))
request["evidence_digest"] = hashlib.sha256(b"e2e-all-old-endpoints-and-processes-fenced").hexdigest()
json.dump(request, open(sys.argv[2], "w"))
PY
drain_code="$(operator_curl --max-time 15 -o "$WORK/confirm-drain.body" -w '%{http_code}' \
    -H 'Content-Type: application/json' --data-binary @"$WORK/confirm-drain.json" \
    "$REGISTRY_BASE/internal/operator/generation/confirm-predecessor-drain" 2>/dev/null || true)"
[ "$drain_code" = 204 ] || fail "predecessor Permit drain returned ${drain_code:-000}"

step "running #34 node-authoritative recovery and digest-CAS rebind"
post_operator /internal/operator/recovery/begin "$WORK/generation-v2.json" "$WORK/recovery-begin.body"
operator_curl -f "$REGISTRY_BASE/internal/operator/system/state" >"$WORK/system-v2-open-recovery.json"
python3 - "$WORK/system-v2-open-recovery.json" "$ADMIN" "$WORK/recovery-restart-sessions.json" <<'PY' \
    || fail "target recovery epoch was not durably opened before Registry restart"
import json, sys, urllib.request
state = json.load(open(sys.argv[1]))
recovery = state.get("recovery")
assert recovery and recovery["source_registry_generation"] == "generation-e2e-1", state
assert recovery["target_registry_generation"] == "generation-e2e-2", state
nodes = json.load(urllib.request.urlopen(sys.argv[2] + "/v1/nodes", timeout=2))
json.dump({node["node_id"]: node["session_seq"] for node in nodes}, open(sys.argv[3], "w"))
PY

step "restarting every target Registry during the open recovery epoch"
for pid in "${REGISTRY_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
done
for pid in "${REGISTRY_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
REGISTRY_PIDS=()
for index in 0 1 2; do
    member=$((index + 1))
    "$CLUSTER_CTL" registry --config "$WORK/registry-v2-restart-$member.yaml" \
        >"$WORK/registry-v2-restart-$member.log" 2>&1 &
    pid="$!"
    PIDS+=("$pid")
    REGISTRY_PIDS[$index]="$pid"
done
for index in 0 1 2; do
    wait_https "https://127.0.0.1:${REGISTRY_PORTS[$index]}/health" "registry-v2-restart-$((index + 1))"
done
python3 - "$ADMIN" "$WORK/recovery-restart-sessions.json" "$NODES" <<'PY' \
    || fail "nodes did not rebuild current-generation sessions after Registry restart"
import json, sys, time, urllib.request
admin = sys.argv[1]
before = json.load(open(sys.argv[2]))
expected = int(sys.argv[3])
for _ in range(600):
    try:
        nodes = json.load(urllib.request.urlopen(admin + "/v1/nodes", timeout=2))
        if len(nodes) == expected and all(
            node.get("link_endpoint") and node.get("session_seq", 0) > before[node["node_id"]]
            for node in nodes
        ):
            raise SystemExit(0)
    except Exception:
        pass
    time.sleep(0.1)
raise SystemExit(1)
PY

recovery_complete=0
for _ in $(seq 1 900); do
    if operator_curl --max-time 2 -f "$REGISTRY_BASE/internal/operator/system/state" \
        >"$WORK/system-v2-recovery.json" 2>/dev/null && \
        python3 - "$WORK/system-v2-recovery.json" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
raise SystemExit(0 if state.get("recovery") is None and state.get("recovery_completion") else 1)
PY
    then
        recovery_complete=1
        break
    fi
    sleep 0.1
done
[ "$recovery_complete" = 1 ] || fail "#34 recovery did not reach durable completion"
[ "$(command_count rebind_execution)" -ge 1 ] || fail "recovery did not issue a Binding digest-CAS rebind"
[ "$(command_count ack_recovery_event)" -ge 1 ] || fail "recovery exposed state without node event ACK"

python3 - "$WORK/system-v2-recovery.json" "$WORK/activate-v2.json" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
json.dump({
    "cluster_id": state["cluster_id"],
    "registry_generation": state["registry_generation"],
    "system_epoch": state["system_epoch"],
    "registry_layout_digest": state["active_registry_layout_digest"],
}, open(sys.argv[2], "w"))
PY
step "atomically activating generation-e2e-2 after recovery completion"
post_operator /internal/operator/generation/activate "$WORK/activate-v2.json" "$WORK/activate-v2.body"

"$CLUSTER_CTL" router --config "$WORK/router-v2.yaml" >"$WORK/router-v2.log" 2>&1 &
ROUTER_V2_PID="$!"
PIDS+=("$ROUTER_V2_PID")
for _ in $(seq 1 300); do
    if curl -fsS --noproxy '*' --max-time 1 -o /dev/null -H "Host: api.$DOMAIN" \
        "http://127.0.0.1:$ROUTER_PORT/health" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

step "verifying recovered Route continuity without a second execution"
request_data user1/session1 "$WORK/data-after-rollover.body"
RECOVERED_SID="$(python3 - "$ADMIN" <<'PY'
import json, sys, urllib.request
hits = json.load(urllib.request.urlopen(sys.argv[1] + "/v1/data-hits", timeout=2))
print(hits[-1]["sid"])
PY
)"
[ "$RECOVERED_SID" = "$RECOVERY_SID" ] || fail "recovery replaced the node-authoritative Sandbox execution"
[ "$(command_count sandbox_admit_dispatch)" = "$RECOVERY_DISPATCHES" ] || \
    fail "recovered Route caused a second Sandbox Admission"

step "verifying recovered Build registration still reaches node-local terminal state"
recovered_build_code="$(curl -sS --noproxy '*' --max-time 5 -o "$WORK/build-status-after-rollover.body" -w '%{http_code}' \
    -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-API-KEY: $API_KEY" \
    "http://127.0.0.1:$ROUTER_PORT/templates/$BUILD_TEMPLATE/builds/$BUILD_ID/status" 2>/dev/null || true)"
[ "$recovered_build_code" = 200 ] || fail "recovered Build status returned ${recovered_build_code:-000}"
python3 - "$WORK/build-status-after-rollover.body" <<'PY' || fail "recovery changed node-local Build terminal state"
import json, sys
value = json.load(open(sys.argv[1]))
assert value.get("status") == "ready", value
assert value.get("templateID", "").startswith("e2b-img-"), value
PY
[ "$(command_count build_admit_dispatch)" = "$BUILD_DISPATCHES" ] || \
    fail "Build recovery caused another Admission or placement"

read -r SOURCE_NODE SOURCE_EPOCH SOURCE_DATA SOURCE_GENERATION SOURCE_DIGEST SOURCE_TOKEN < <(
    python3 - "$WORK/source-execution.json" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
print(value["node_id"], value["node_epoch"], value["data_endpoint"], value["registry_generation"], value["binding_digest"], value["access_token"])
PY
)
old_binding_code="$(curl -sS --noproxy '*' --max-time 3 -o "$WORK/old-binding.body" \
    -D "$WORK/old-binding.headers" -w '%{http_code}' -X CONNECT \
    --cacert "$TLS_DIR/ca.crt" --cert "$TLS_DIR/router.crt" --key "$TLS_DIR/router.key" \
    -H "E2b-Sandbox-Id: $RECOVERY_SID" \
    -H 'E2b-Sandbox-Port: 49983' \
    -H "X-Kuasar-Node-Id: $SOURCE_NODE" \
    -H "X-Kuasar-Node-Epoch: $SOURCE_EPOCH" \
    -H "X-Kuasar-Storage-Generation: $SOURCE_GENERATION" \
    -H "X-Kuasar-Binding-Digest: $SOURCE_DIGEST" \
    -H "X-Access-Token: $SOURCE_TOKEN" \
    "https://$SOURCE_DATA/" 2>/dev/null || true)"
[ "$old_binding_code" = 409 ] || fail "old-generation Binding returned ${old_binding_code:-000}, expected 409"
grep -qi '^X-Kuasar-Proxy-Error: wrong_binding' "$WORK/old-binding.headers" || \
    fail "node proxy did not return the typed WRONG_BINDING fence"

elapsed=$(($(date +%s) - STARTED_AT))
step "PASS: final three-replica cluster rollover and recovery e2e (${elapsed}s)"
