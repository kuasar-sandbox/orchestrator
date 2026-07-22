#!/usr/bin/env bash
#
# e2e_cluster_real.sh -- final three-replica cluster with one real node.
# An e2b create request addressed by (group, route_key) drives Route Reserve ->
# node-local Admission -> real microVM boot. The returned execution capability
# then fences envd /health forwarding before the Route is deleted.
#
# This is intentionally a release-builder test: it needs artifacts from all
# repos. Missing heavy prerequisites exits 0 ("skipped") unless
# REQUIRE_CLUSTER_REAL=1.

set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-cluster.real.local}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_cluster_sw}"

step() { echo "==> $*" >&2; }

skip() {
    echo >&2
    echo "==> e2e_cluster_real: skipping ($*)" >&2
    if [ "${REQUIRE_CLUSTER_REAL:-0}" = "1" ]; then
        echo "REQUIRE_CLUSTER_REAL=1; failing" >&2
        exit 1
    fi
    exit 0
}

fail() {
    echo "==> FAIL: $*" >&2
    if [ -n "${WORK:-}" ] && [ -d "$WORK" ]; then
        for f in "$WORK"/*.body "$WORK"/*.json "$WORK"/*.out; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,220p' "$f" >&2 || true
        done
        for f in "$WORK"/*.log; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,260p' "$f" >&2 || true
        done
        echo "---- sandbox systemd units ----" >&2
        systemctl list-units 'sandbox-builder@*.service' 'sandbox-runner@*.service' --all --no-pager >&2 2>/dev/null || true
        while IFS= read -r unit; do
            [ -n "$unit" ] || continue
            echo "---- journal $unit ----" >&2
            journalctl -u "$unit" --no-pager -n 100 2>/dev/null | sed 's/^/  unit| /' >&2 || true
        done < <(systemctl list-units 'sandbox-builder@*.service' 'sandbox-runner@*.service' --all --no-legend --no-pager 2>/dev/null | awk '{print $1}')
        if [ -d "$WORK/cr" ]; then
            while IFS= read -r sid; do
                [ -n "$sid" ] || continue
                echo "---- journal sandbox $sid ----" >&2
                journalctl KUASAR_SANDBOX_ID="$sid" --no-pager -n 100 2>/dev/null | sed 's/^/  sandbox| /' >&2 || true
            done < <(find "$WORK/cr" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null)
        fi
    fi
    exit 1
}

CLUSTER_REAL_CASE="${CLUSTER_REAL_CASE:-final}"
[ "$CLUSTER_REAL_CASE" = final ] || fail "unknown CLUSTER_REAL_CASE=$CLUSTER_REAL_CASE"

for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cluster-ctl cloud-hypervisor; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b"
done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.erofs" ] || skip "missing $BIN/sandbox-runtime.erofs"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v openssl >/dev/null 2>&1 || skip "openssl not on PATH"
command -v stat >/dev/null 2>&1 || skip "stat not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "base image $E2E_IMAGE unavailable (set E2E_IMAGE to a local or pullable image)"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then
    export PATH="$BIN:$PATH"
fi
[ "$(stat -f -c %T /dev/shm)" = tmpfs ] || skip "/dev/shm is not tmpfs"

WORK="$(mktemp -d /tmp/e2e-cr-XXXXXX)"
RAFT_ROOT="$(mktemp -d /dev/shm/kuasar-real-raft-XXXXXX)"
DRAGONBOAT_PROFILE="$REPO_ROOT/deploy/dragonboat-soft-settings.json"
if [ ! -f "$DRAGONBOAT_PROFILE" ]; then
    DRAGONBOAT_PROFILE="$REPO_ROOT/../deploy/dragonboat-soft-settings.json"
fi
[ -f "$DRAGONBOAT_PROFILE" ] || fail "missing deploy/dragonboat-soft-settings.json"
cp "$DRAGONBOAT_PROFILE" "$WORK/dragonboat-soft-settings.json"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
mkdir -p "$WORK/r" "$WORK/l" "$WORK/s" "$WORK/z/d" "$WORK/g" "$WORK/br" "$WORK/bl" "$WORK/cr" "$WORK/cl"
declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""

cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        p="${PIDS[$i]}"
        [ -n "$p" ] && kill "$p" 2>/dev/null
        [ -n "$p" ] && wait "$p" 2>/dev/null
    done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    ip netns del "$SWITCH" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    if [ -n "${E2E_KEEP:-}" ]; then
        echo "kept work dirs: $WORK $RAFT_ROOT" >&2
    else
        rm -rf "$WORK" "$RAFT_ROOT"
    fi
}
trap cleanup EXIT

free_port() {
    python3 <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_port() {
    local port="$1" name="$2"
    for _ in $(seq 1 120); do
        if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.socket()
s.settimeout(0.2)
s.connect(("127.0.0.1", int(sys.argv[1])))
s.close()
PY
        then
            return 0
        fi
        sleep 0.25
    done
    fail "$name did not open port $port"
}

wait_api_health() {
    local port="$1" name="$2"
    for _ in $(seq 1 120); do
        if curl -fsS --noproxy '*' --max-time 1 -H "Host: api.$DOMAIN" "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    fail "$name did not become healthy"
}

operator_curl() {
    curl -sS --noproxy '*' --cacert "$TLS_DIR/ca.crt" \
        --cert "$TLS_DIR/operator.crt" --key "$TLS_DIR/operator.key" "$@"
}

wait_https() {
    local url="$1" name="$2"
    for _ in $(seq 1 900); do
        if operator_curl --max-time 1 -o /dev/null -f "$url" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    fail "$name did not become healthy at $url"
}

post_operator() {
    local path="$1" body="$2" output="$3" code=""
    for _ in $(seq 1 100); do
        code="$(operator_curl --max-time 3 -o "$output" -w '%{http_code}' \
            -H 'Content-Type: application/json' --data-binary "@$body" "$REGISTRY_BASE$path" 2>/dev/null || true)"
        [ "$code" = 204 ] && return 0
        sleep 0.1
    done
    fail "operator POST $path returned ${code:-000}"
}

wait_cluster_node_health() {
    for _ in $(seq 1 240); do
        if "$BIN/node-ctl" key-lease list --socket "$WORK/cn.sock" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    fail "cluster node-ctl did not become healthy"
}

http_code() {
    local out="$1"; shift
    curl -sS --noproxy '*' --max-time 260 -o "$out" -w '%{http_code}' "$@"
}

node_req() {
    local port="$1" method="$2" path="$3" key="$4" body="${5:-}"
    local args=(-sS --noproxy '*' --max-time 260 -o "$WORK/node-resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$port$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

router_req() {
    local method="$1" path="$2" key="$3" route_key="${4:-}" body="${5:-}"
    local args=(-sS --noproxy '*' --max-time 260 -o "$WORK/router-resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-API-KEY: $key")
    [ -n "$route_key" ] && args+=(-H "X-Kuasar-Route-Key: $route_key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$ROUTER_PORT$path"
}

data_by_key_code() {
    local out="$1"
    http_code "$out" \
        -H "Host: data.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -H "X-API-KEY: $CLUSTER_API_KEY" \
        -H "X-Access-Token: $ROUTE_ACCESS_TOKEN" \
        "http://127.0.0.1:$ROUTER_PORT/health"
}

retry_data_by_key() {
    local code="000"
    for i in $(seq 1 12); do
        step "data-plane attempt $i: router -> registry Reserve -> node -> real envd"
        code="$(data_by_key_code "$WORK/data-health.body" 2>/dev/null || echo 000)"
        if [ "$code" = "204" ] || [ "$code" = "200" ]; then
            echo "$code"
            return 0
        fi
        if grep -qiE 'AuthKey is not allowed|key lease|key not distributed' "$WORK/data-health.body" 2>/dev/null; then
            cat "$WORK/data-health.body" >&2
            fail "data-plane hit node before the exact key lease was ready"
        fi
        step "data-plane attempt $i returned $code; retrying while sandbox boot converges"
        sleep 2
    done
    echo "$code"
    return 1
}

start_store_zot_vswitch() {
    STORE_PORT="$(free_port)"
    cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/s, verify_content_key: true }
EOF
    "$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
    "$BIN/store-ctl" serve --config "$WORK/store.yaml" > >(tee "$WORK/store-serve.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$STORE_PORT" store-ctl

    ZOT_PORT="$(free_port)"
    cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/z/d", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
    "$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
    PIDS+=("$!")
    wait_port "$ZOT_PORT" zot

    REF="127.0.0.1:$ZOT_PORT/e2e/cluster-real:v1"
    cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -c|-n) shift 2 ;;
    -c*|-n*) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
exec "$@"
SH
    cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice \
 && if ! id -u user >/dev/null 2>&1; then \
      if command -v useradd >/dev/null 2>&1; then useradd -m -d /home/user -s /bin/sh user; \
      elif command -v adduser >/dev/null 2>&1; then adduser -D -h /home/user -s /bin/sh user; \
      else echo "missing useradd/adduser" >&2; exit 1; fi; \
    fi \
 && mkdir -p /home/user \
 && chown user:user /home/user \
 && id user >/dev/null
EOF
    docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" > >(tee "$WORK/imgbuild.log" >&2) 2>&1 || fail "docker build e2e image"
    TAGS+=("$REF")
    docker push "$REF" > >(tee "$WORK/push.log" >&2) 2>&1 || fail "docker push e2e image"
    step "store-ctl + zot up; seeded $REF"

    MGMT_VIP="169.254.169.254"
    "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
    ip netns del "$SW_NETNS" 2>/dev/null || true
    ip netns del "$SWITCH" 2>/dev/null || true
    ip netns add "$SW_NETNS" 2>/dev/null || true
    step "starting vswitch $SWITCH (netns=$SW_NETNS)"
    "$BIN/connector-ctl" vswitch start "$SWITCH" \
        --netns="$SW_NETNS" \
        --ports=64 \
        --mac-addr=02:00:00:00:00:01 \
        --floating-ip-base=100.100.96.0 \
        --mode=tap \
        --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 > >(tee "$WORK/vswitch-start.log" >&2) 2>&1 || fail "vswitch start"
    SW_STARTED=1
    GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/cluster-real:v1"
    step "vswitch up; build sandboxes pull $GUEST_REF"

    cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF
}

make_ext4_templates() {
    MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
    [ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found"
    OVL="$WORK/overlay-1G.ext4"
    truncate -s 1G "$OVL"
    "$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs-overlay.log" 2>&1 || { cat "$WORK/mkfs-overlay.log"; fail "mkfs overlay"; }
    BLD="$WORK/builder-2G.ext4"
    truncate -s 2G "$BLD"
    "$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-builder.log" 2>&1 || { cat "$WORK/mkfs-builder.log"; fail "mkfs builder"; }
}

build_template_with_standalone_node() {
    BUILD_PORT="$(free_port)"
    cat > "$WORK/build-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$BUILD_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/br, base_root: $WORK/bl, config_socket: $WORK/bn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.erofs, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint: { mode: remote }
EOF
    step "starting temporary standalone node-ctl for template build (:${BUILD_PORT})"
    "$BIN/node-ctl" conductor serve --config "$WORK/build-node.yaml" > >(tee "$WORK/build-node.log" >&2) 2>&1 &
    local build_pid="$!"
    PIDS+=("$build_pid")
    wait_api_health "$BUILD_PORT" "temporary node-ctl"
    "$BIN/node-ctl" key-lease put --socket "$WORK/bn.sock" --group "$GROUP" \
        --auth-key "$AUTH_KEY" --manifest-key "$MANIFEST_KEY" >/dev/null || fail "temporary key-lease put"

    local code tid bid status
    code="$(node_req "$BUILD_PORT" POST /v3/templates "$BUILD_API_KEY" \
        '{"name":"cluster-real-tmpl","cpuCount":2,"memoryMB":4096}')"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template register returned $code"; }
    tid="$(json_field "$WORK/node-resp.body" templateID)"
    bid="$(json_field "$WORK/node-resp.body" buildID)"
    step "building template through temporary node: templateID=$tid buildID=$bid"
    code="$(node_req "$BUILD_PORT" POST "/v2/templates/$tid/builds/$bid" "$BUILD_API_KEY" "{\"fromImage\":\"$GUEST_REF\"}")"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template build trigger returned $code"; }
    TEMPLATE_REF=""
    for _ in $(seq 1 180); do
        node_req "$BUILD_PORT" GET "/templates/$tid/builds/$bid/status" "$BUILD_API_KEY" >/dev/null
        status="$(json_field "$WORK/node-resp.body" status)"
        case "$status" in
            ready)
                TEMPLATE_REF="$(json_field "$WORK/node-resp.body" templateID)"
                break
                ;;
            error)
                cat "$WORK/node-resp.body"
                fail "template build error"
                ;;
        esac
        sleep 1
    done
    [ -n "$TEMPLATE_REF" ] || fail "template build did not become ready"
    step "built reusable template: $TEMPLATE_REF"

    kill "$build_pid" 2>/dev/null || true
    wait "$build_pid" 2>/dev/null || true
    systemctl stop 'sandbox-builder@*.service' 2>/dev/null || true
}

write_group_record() {
    cat > "$WORK/g/group.json" <<EOF
{
  "group": "$GROUP",
  "key_revision": 1,
  "manifest_key": { "type": "inline", "value": "$MANIFEST_KEY" },
  "auth_key": { "type": "inline", "value": "$AUTH_KEY" },
  "template_ref": "$TEMPLATE_REF",
  "target_port": 49983,
  "node_selectors": [{ "pool": "real" }]
}
EOF
    step "wrote placer file group source for $GROUP"
}

make_role_certificate() {
    local role="$1"
    openssl ecparam -name prime256v1 -genkey -noout -out "$TLS_DIR/$role.key" 2>/dev/null
    openssl req -new -key "$TLS_DIR/$role.key" -subj "/CN=kuasar-real-e2e-$role" \
        -out "$TLS_DIR/$role.csr" 2>/dev/null
    cat >"$TLS_DIR/$role.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=IP:127.0.0.1,URI:spiffe://kuasar.internal/$role/real-e2e-$role
EOF
    openssl x509 -req -in "$TLS_DIR/$role.csr" -CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" \
        -CAcreateserial -days 2 -sha256 -extfile "$TLS_DIR/$role.ext" -out "$TLS_DIR/$role.crt" 2>/dev/null
}

write_final_cluster_configs() {
    local -a ports
    mapfile -t ports < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(8):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
for sock in sockets:
    print(sock.getsockname()[1])
PY
)
    [ "${#ports[@]}" = 8 ] || fail "could not allocate final cluster ports"
    CONTROL_PORTS=("${ports[@]:0:3}")
    RAFT_PORTS=("${ports[@]:3:3}")
    CONTROL_PORT="${CONTROL_PORTS[0]}"
    PLACER_PORT="${ports[6]}"
    ROUTER_PORT="${ports[7]}"

    TLS_DIR="$WORK/tls"
    mkdir -p "$TLS_DIR"
    openssl ecparam -name prime256v1 -genkey -noout -out "$TLS_DIR/ca.key" 2>/dev/null
    openssl req -x509 -new -key "$TLS_DIR/ca.key" -sha256 -days 2 \
        -subj '/CN=kuasar-real-e2e-ca' -out "$TLS_DIR/ca.crt" 2>/dev/null
    for role in registry placer router node operator; do
        make_role_certificate "$role"
    done

    BOOTSTRAP_SECRET="$WORK/bootstrap.secret"
    printf '%s\n' 'kuasar-real-e2e-generation-1' >"$BOOTSTRAP_SECRET"
    openssl genpkey -algorithm ED25519 -out "$WORK/registry-layout-signing.pem" 2>/dev/null
    chmod 0600 "$WORK/registry-layout-signing.pem"
    python3 - "$WORK/members.json" \
        "${CONTROL_PORTS[0]}" "${RAFT_PORTS[0]}" \
        "${CONTROL_PORTS[1]}" "${RAFT_PORTS[1]}" \
        "${CONTROL_PORTS[2]}" "${RAFT_PORTS[2]}" <<'PY'
import json, sys
members = []
for index in range(3):
    control, raft = sys.argv[2 + index * 2:4 + index * 2]
    members.append({
        "member_id": f"registry-{index + 1}",
        "internal_endpoint": f"https://127.0.0.1:{control}",
        "raft_endpoint": f"127.0.0.1:{raft}",
    })
json.dump(members, open(sys.argv[1], "w"))
PY
    "$BIN/cluster-ctl" registry-layout bootstrap \
        --members "$WORK/members.json" \
        --cluster-id kuasar-real-e2e \
        --registry-generation generation-real-e2e-1 \
        --bootstrap-secret-file "$BOOTSTRAP_SECRET" \
        --signing-key "$WORK/registry-layout-signing.pem" \
        --key-id real-e2e-root \
        --chain-out "$WORK/registry-layout-chain.json" \
        --keyring-out "$WORK/registry-layout-keyring.json" \
        --virtual-shards 8 \
        --serve-permit-max 3s

    cat >"$WORK/placer.yaml" <<EOF
placer:
  id: placer-1
  listen: "127.0.0.1:$PLACER_PORT"
  tls: { cert: "$TLS_DIR/placer.crt", key: "$TLS_DIR/placer.key", ca: "$TLS_DIR/ca.crt" }
group_sources:
  - { source_id: cluster-real-file-source, source_type: file, path: "$WORK/g" }
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
registry_response_timeout: 185s
node_tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
providers:
  endpoints:
    - { name: placer-1, endpoint: "https://127.0.0.1:$PLACER_PORT" }
  tls: { cert: "$TLS_DIR/router.crt", key: "$TLS_DIR/router.key", ca: "$TLS_DIR/ca.crt" }
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: enforce
  data_plane: enforce
  cache_ttl: 500ms
cache:
  route_ttl: 5m
  idle_timeout: 2m
EOF

    for index in 0 1 2; do
        local member=$((index + 1))
        local root="$RAFT_ROOT/registry-$member"
        mkdir -p "$root"
        cat >"$WORK/registry-$member.yaml" <<EOF
member:
  id: registry-$member
  listen: "127.0.0.1:${CONTROL_PORTS[$index]}"
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
  park_timeout: 180s
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
        "$BIN/cluster-ctl" config registry --config "$WORK/registry-$member.yaml" --resolve >/dev/null
    done
    "$BIN/cluster-ctl" config placer --config "$WORK/placer.yaml" --resolve >/dev/null
    "$BIN/cluster-ctl" config router --config "$WORK/router.yaml" --resolve >/dev/null
}

start_cluster_control_plane() {
    step "starting final placer (:${PLACER_PORT})"
    "$BIN/cluster-ctl" placer --config "$WORK/placer.yaml" > >(tee "$WORK/placer.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_https "https://127.0.0.1:$PLACER_PORT/health" placer

    step "starting final three-replica Registry History Generation"
    for index in 0 1 2; do
        local member=$((index + 1))
        (
            cd "$WORK"
            exec "$BIN/cluster-ctl" registry --config "$WORK/registry-$member.yaml"
        ) > >(tee "$WORK/registry-$member.log" >&2) 2>&1 &
        PIDS+=("$!")
    done
    for index in 0 1 2; do
        wait_https "https://127.0.0.1:${CONTROL_PORTS[$index]}/health" "registry-$((index + 1))"
    done

    REGISTRY_BASE="https://127.0.0.1:$CONTROL_PORT"
    operator_curl -f "$REGISTRY_BASE/internal/operator/system/state" >"$WORK/system-before.json"
    python3 - "$WORK/system-before.json" "$WORK/generation.json" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
json.dump({
    "cluster_id": state["cluster_id"],
    "registry_generation": state["registry_generation"],
    "system_epoch": state["system_epoch"],
    "registry_layout_digest": state["active_registry_layout_digest"],
}, open(sys.argv[2], "w"))
PY
    step "activating the empty Registry History Generation"
    post_operator /internal/operator/registry-generation/activate "$WORK/generation.json" "$WORK/activate.body"

    step "starting final router (:${ROUTER_PORT})"
    "$BIN/cluster-ctl" router --config "$WORK/router.yaml" > >(tee "$WORK/router.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$ROUTER_PORT" router
}

start_cluster_node() {
    NODE_PORT="$(free_port)"
    local node_id="$1"
    cat > "$WORK/cluster-node.yaml" <<EOF
api:
  domain: $DOMAIN
  listen: "127.0.0.1:$NODE_PORT"
  tls: { cert: "$TLS_DIR/node.crt", key: "$TLS_DIR/node.key", client_ca: "$TLS_DIR/ca.crt" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/cr, base_root: $WORK/cl, config_socket: $WORK/cn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  capacity: 8
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.erofs, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint: { mode: remote }
cluster:
  node_link:
    endpoint: "$REGISTRY_BASE"
    tls: { cert: "$TLS_DIR/node.crt", key: "$TLS_DIR/node.key", ca: "$TLS_DIR/ca.crt" }
  node_id: "$node_id"
  data_endpoint: "127.0.0.1:$NODE_PORT"
  heartbeat_interval: "500ms"
  labels: { pool: "real" }
EOF
    "$BIN/node-ctl" cluster-identity init --config "$WORK/cluster-node.yaml" \
        --node-id "$node_id" --data-endpoint "127.0.0.1:$NODE_PORT" >"$WORK/node-identity.json"
    python3 - "$WORK/generation.json" "$WORK/node-identity.json" "$WORK/enroll-node.json" <<'PY'
import json, sys
request = json.load(open(sys.argv[1]))
identity = json.load(open(sys.argv[2]))
request.update({
    "node_id": identity["node_id"],
    "enrollment_id": identity["enrollment_id"],
    "node_epoch": identity["node_epoch"],
    "data_endpoint": identity["data_endpoint"],
})
json.dump(request, open(sys.argv[3], "w"))
PY
    step "enrolling durable node identity $node_id"
    post_operator /internal/operator/node/enroll "$WORK/enroll-node.json" "$WORK/enroll-node.body"

    step "starting final cluster node-ctl node_id=$node_id (:${NODE_PORT})"
    "$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" > >(tee "$WORK/cluster-node.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_cluster_node_health
}

assert_cluster_node_key_lease() {
    step "checking dispatch installed the exact node key lease"
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" key-lease list --socket "$WORK/cn.sock" >"$WORK/cluster-node-keys.out" 2>&1; then
            if grep -Fq "group=$GROUP auth=$AUTH_FP manifest=$MANIFEST_FP " "$WORK/cluster-node-keys.out"; then
                step "node key lease ready: auth=$AUTH_FP manifest=$MANIFEST_FP"
                return 0
            fi
        fi
        sleep 0.5
    done
    cat "$WORK/cluster-node-keys.out" >&2 || true
    fail "node key lease did not receive $MANIFEST_FP"
}

run_cluster_flow() {
    local code sid
    step "creating the logical Route through the extended e2b API"
    code="$(router_req POST /v2/sandboxes "$CLUSTER_API_KEY" "$ROUTE_KEY" '{}')"
    [ "$code" = "201" ] || { cat "$WORK/router-resp.body"; fail "sandbox create returned $code"; }
    ROUTE_ACCESS_TOKEN="$(json_field "$WORK/router-resp.body" accessToken)"
    [ -n "$ROUTE_ACCESS_TOKEN" ] || fail "sandbox create returned an empty execution AccessToken"
    assert_cluster_node_key_lease

    step "checking by-key data request uses the execution capability and group target_port"
    code="$(retry_data_by_key || true)"
    [ "$code" = "204" ] || [ "$code" = "200" ] || fail "data-plane /health returned $code"
    step "PASS: Router -> Registry Reserve -> node Admission -> real envd /health ($code)"

    step "checking group-local sandbox list through router"
    code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
    [ "$code" = "200" ] || { cat "$WORK/router-resp.body"; fail "list returned $code"; }
    sid="$(python3 - "$WORK/router-resp.body" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
for row in rows:
    sid = row.get("sandboxID")
    if sid:
        print(sid)
        raise SystemExit(0)
raise SystemExit(1)
PY
)" || fail "sandbox list did not contain a sandboxID"
    step "listed sandbox: $sid"

    step "checking router control DELETE forwards to the real node"
    code="$(router_req DELETE "/sandboxes/$sid" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "delete returned $code"; }
    for _ in $(seq 1 80); do
        code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
        [ "$code" = "200" ] || { sleep 0.25; continue; }
        if python3 - "$WORK/router-resp.body" "$sid" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
sid = sys.argv[2]
raise SystemExit(0 if all(r.get("sandboxID") != sid for r in rows) else 1)
PY
        then
            step "PASS: sandbox delete converged through route_link"
            return 0
        fi
        sleep 0.25
    done
    fail "deleted sandbox still appears in list"
}

step "cluster real e2e case=$CLUSTER_REAL_CASE work=$WORK using BIN=$BIN"
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
MANIFEST_FP="$("$BIN/e2b-key-ctl" fingerprint "$MANIFEST_KEY")"
AUTH_KEY="$("$BIN/e2b-key-ctl" gen-key)"
AUTH_FP="$("$BIN/e2b-key-ctl" fingerprint "$AUTH_KEY")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$AUTH_KEY")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$AUTH_KEY")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
GROUP="/e2e/cluster/real/$CLUSTER_REAL_CASE"
ROUTE_KEY="user1-$CLUSTER_REAL_CASE"

start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record
write_final_cluster_configs
start_cluster_control_plane

NODE_ID="real-node-1"
start_cluster_node "$NODE_ID"
run_cluster_flow

echo "==> PASS: e2e_cluster_real $CLUSTER_REAL_CASE"
