#!/usr/bin/env bash
#
# e2e_cluster_real.sh -- real cluster e2e:
#
#   Phase 1 / registry-n1:
#     registry + placer + router + one real node-ctl conductor serve.
#     A data-plane request by (group, route_key) drives Reserve -> node-link
#     create -> real microVM boot -> envd /health through router -> delete.
#
#   Phase 2 / registry-redirect:
#     three registries, node_link owner_count=1. The node first connects to the
#     bootstrap registry, gets a node-link redirect to its owner, reconnects, and
#     then runs the same real sandbox flow.
#
# This is intentionally a release-builder test: it needs artifacts from all
# repos. Missing heavy prerequisites exits 0 ("skipped") unless
# REQUIRE_CLUSTER_REAL=1.

set -euo pipefail

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

if [ -z "${CLUSTER_REAL_CASE:-}" ]; then
    for case_name in registry-n1 registry-redirect; do
        step "running $case_name"
        CLUSTER_REAL_CASE="$case_name" "$0"
    done
    exit 0
fi

case "$CLUSTER_REAL_CASE" in
    registry-n1|registry-redirect) ;;
    *) fail "unknown CLUSTER_REAL_CASE=$CLUSTER_REAL_CASE" ;;
esac

for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cluster-ctl cloud-hypervisor; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b"
done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.erofs" ] || skip "missing $BIN/sandbox-runtime.erofs"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
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

WORK="$(mktemp -d /tmp/e2e-cr-XXXXXX)"
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
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" >&2 || rm -rf "$WORK"
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

write_registry_configs() {
    CONTROL_PORTS=()
    local registries="$1"
    for _ in $(seq 1 "$registries"); do
        CONTROL_PORTS+=("$(free_port)")
    done
    CONTROL_PORT="${CONTROL_PORTS[0]}"
    ACTIVE_MEMBERS_YAML=""
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        ACTIVE_MEMBERS_YAML="$ACTIVE_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\", node_advertise: \"127.0.0.1:$port\" }
"
    done
    ROUTE_OWNER_COUNT="$registries"
    NODE_OWNER_COUNT="$registries"
    PLACER_OWNER_COUNT="$registries"
    NODE_LIST_OWNER_COUNT="$registries"
    if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
        NODE_OWNER_COUNT=1
    fi
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        cat >"$WORK/registry-$i.yaml" <<EOF
member:
  id: registry-$i
  listen: "127.0.0.1:$port"
membership:
  active: 1
  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML
  owners:
    route_link: $ROUTE_OWNER_COUNT
    node_link: $NODE_OWNER_COUNT
    placer_link: $PLACER_OWNER_COUNT
    node_list: $NODE_LIST_OWNER_COUNT
node_link:
  heartbeat_interval: "500ms"
  node_dead_after: "5s"
route_link:
  park_timeout: "180s"
placer_link:
  min_ready_placers: 1
  place_timeout: "5s"
EOF
    done
}

write_placer_router_configs() {
    PLACER_PORT="$(free_port)"
    ROUTER_PORT="$(free_port)"
    cat > "$WORK/placer.yaml" <<EOF
placer:
  id: placer-1
  listen: "127.0.0.1:$PLACER_PORT"
  advertise: "http://127.0.0.1:$PLACER_PORT"
  memberlist_label: "placer.default"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
import_groups:
  - source_id: cluster-real-file-source
    source_type: file
    path: "$WORK/g"
placement:
  candidates: 2
  zone_admit_max: "yellow"
  node_dead_after: "5s"
  import_source_lease_ttl: "10s"
  selector_patch_refresh_interval: "1s"
EOF
    cat > "$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  api_key: "enforce"
  data_plane: "enforce"
  cache_ttl: "500ms"
cache:
  route_ttl: "5m"
  idle_timeout: "2m"
EOF
}

start_cluster_control_plane() {
    local registries="${#CONTROL_PORTS[@]}"
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        step "starting registry-$i (:${port})"
        "$BIN/cluster-ctl" registry --config "$WORK/registry-$i.yaml" > >(tee "$WORK/registry-$i.log" >&2) 2>&1 &
        PIDS+=("$!")
        wait_port "$port" "registry-$i"
    done
    step "checking registry membership endpoint"
    python3 - "${CONTROL_PORTS[0]}" "$registries" <<'PY' || fail "membership endpoint failed"
import json, sys, urllib.request
port, want = sys.argv[1], int(sys.argv[2])
m = json.load(urllib.request.urlopen("http://127.0.0.1:%s/cluster/membership" % port, timeout=2))
active = m.get("active", m.get("Active"))
versions = m.get("versions", m.get("Versions", []))
assert active, m
active_versions = [v for v in versions if v.get("version", v.get("Version")) == active]
assert len(active_versions) == 1, m
members = active_versions[0].get("members", active_versions[0].get("Members", []))
assert len(members) == want, m
assert active_versions[0].get("label", active_versions[0].get("Label")), m
PY

    step "starting placer (:${PLACER_PORT})"
    "$BIN/cluster-ctl" placer --config "$WORK/placer.yaml" > >(tee "$WORK/placer.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$PLACER_PORT" placer

    step "starting router (:${ROUTER_PORT})"
    "$BIN/cluster-ctl" router --config "$WORK/router.yaml" > >(tee "$WORK/router.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$ROUTER_PORT" router
}

choose_redirect_node_id() {
    local protocol_source="$REPO_ROOT/../internal/routesync/proto.go"
    local protocol_version
    protocol_version="$(sed -nE 's/^const Version = ([0-9]+)$/\1/p' "$protocol_source")"
    [[ "$protocol_version" =~ ^[1-9][0-9]*$ ]] || fail "could not resolve node-link protocol version from $protocol_source"
    python3 - "http://127.0.0.1:$CONTROL_PORT/node-link/session" "$protocol_version" <<'PY'
import json, struct, sys, urllib.request
url, protocol_version = sys.argv[1], int(sys.argv[2])
for idx in range(1, 80):
    node_id = "real-node-redirect-%02d" % idx
    msg = {
        "type": "node_register",
        "node_register": {
            "version": protocol_version,
            "node_id": node_id,
            "labels": {"pool": "probe"},
            "data_endpoint": "127.0.0.1:1",
            "capacity": 1,
            "accept_redirect": True,
        },
    }
    payload = json.dumps(msg, separators=(",", ":")).encode()
    body = struct.pack("<I", len(payload)) + payload
    req = urllib.request.Request(url, data=body, method="PUT", headers={"Content-Type": "application/octet-stream"})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            hdr = resp.read(4)
            if len(hdr) != 4:
                continue
            n = struct.unpack("<I", hdr)[0]
            raw = resp.read(n)
            hello = json.loads(raw.decode())
    except Exception:
        continue
    redir = (((hello.get("hello") or {}).get("redirect") or {}).get("targets") or [])
    if redir:
        print(node_id)
        raise SystemExit(0)
raise SystemExit(1)
PY
}

start_cluster_node() {
    NODE_PORT="$(free_port)"
    local node_id="$1"
    cat > "$WORK/cluster-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$NODE_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/cr, base_root: $WORK/cl, config_socket: $WORK/cn.sock }
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
cluster:
  node_link: { endpoint: "127.0.0.1:$CONTROL_PORT" }
  node_id: "$node_id"
  data_endpoint: "127.0.0.1:$NODE_PORT"
  heartbeat_interval: "500ms"
  labels: { pool: "real" }
EOF
    step "starting cluster node-ctl node_id=$node_id (:${NODE_PORT}, no manual key-lease put)"
    "$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" > >(tee "$WORK/cluster-node.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_api_health "$NODE_PORT" "cluster node-ctl"
    if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
        for _ in $(seq 1 80); do
            if grep -q "redirecting to node owner" "$WORK/cluster-node.log"; then
                step "observed node-link redirect to node owner"
                return 0
            fi
            sleep 0.25
        done
        fail "node-link redirect was not observed in cluster-node.log"
    fi
}

wait_cluster_node_manifest_key() {
    step "waiting for node_link key lease on $NODE_ID"
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" key-lease list --socket "$WORK/cn.sock" >"$WORK/cluster-node-keys.out" 2>&1; then
            if grep -q "manifest=$MANIFEST_FP" "$WORK/cluster-node-keys.out"; then
                step "node key lease ready: $MANIFEST_FP"
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
    step "checking by-key data request uses group target_port (no E2b-Sandbox-Port header)"
    code="$(retry_data_by_key || true)"
    [ "$code" = "204" ] || [ "$code" = "200" ] || fail "data-plane /health returned $code"
    step "PASS: router -> registry Reserve -> node-link create -> real envd /health ($code)"

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
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$AUTH_KEY")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$AUTH_KEY")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
GROUP="/e2e/cluster/real/$CLUSTER_REAL_CASE"
ROUTE_KEY="user1/$CLUSTER_REAL_CASE"

start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record

if [ "$CLUSTER_REAL_CASE" = "registry-n1" ]; then
    write_registry_configs 1
else
    write_registry_configs 3
fi
write_placer_router_configs
start_cluster_control_plane

NODE_ID="real-node-1"
if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
    step "probing node ids until bootstrap registry returns a node-link redirect"
    NODE_ID="$(choose_redirect_node_id)" || fail "could not find a node_id redirected away from bootstrap registry"
    step "selected redirected node_id=$NODE_ID"
fi
start_cluster_node "$NODE_ID"
wait_cluster_node_manifest_key
run_cluster_flow

echo "==> PASS: e2e_cluster_real $CLUSTER_REAL_CASE"
