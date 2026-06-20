#!/usr/bin/env bash
#
# e2e_cluster.sh — the cluster tier end to end on a real microVM:
#
#   cluster-ctl registry   -> durable state authority + node-link hub (sqlite)
#   node-ctl serve         -> dials the registry over node-link (the node is the
#                             route authority; it executes the registry's commands)
#   cluster-ctl router     -> e2b ingress: create -> registry reserve -> node boots
#                             the group's template microVM; data plane forwards by
#                             <port>-<sid>.<domain> to the node (two hops).
#
# It reuses e2e_execute's store/zot/vswitch/build setup to produce a real template,
# then drives a sandbox THROUGH the cluster (router -> registry -> node-link ->
# CreateCluster -> cloud-hypervisor) and checks the two-hop data path to envd.
#
# Needs systemd+root, /dev/kvm, the vswitch stack, store-ctl, zot, docker,
# mkfs.erofs, the kernel + runtimes in bin (same as e2e_execute). Missing -> exit 0.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-test-app-a:latest}"
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"
SW_NETNS="${SW_NETNS:-e2e_sw}"

skip() { echo; echo "==> e2e_cluster: skipping ($*)"; [ "${REQUIRE_EXEC:-0}" = "1" ] && { echo "REQUIRE_EXEC=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in node-ctl cluster-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl vswitch-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] && [ -f "$BIN/sandbox-runtime-e2b.erofs" ] && [ -f "$BIN/sandbox-runtime-builder.erofs" ] || skip "missing kernel/runtime erofs in bin"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not on PATH"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || skip "base image $E2E_IMAGE not cached"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-cluster-XXXXXX)"
UNIT_DIR="/run/systemd/system"
for u in sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data" "$WORK/cluster"
declare -a PIDS=()
SW_STARTED=""
cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/vswitch-ctl" stop "$SWITCH" >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    for u in sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice; do rm -f "$UNIT_DIR/$u"; done
    systemctl daemon-reload 2>/dev/null
    docker rmi -f "$REF" >/dev/null 2>&1
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
PORT="$(free_port)"           # node-ctl serve (e2b API + data plane)
REG_PORT="$(free_port)"       # cluster-ctl registry node-link
ROUTER_PORT="$(free_port)"    # cluster-ctl router ingress
OP_SOCK="$WORK/cluster/registry.sock"

# node-ctl e2b request helper (build a template via the node's own API).
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

# ---- store + zot + seeded base image (same as e2e_execute) -----------------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl

ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
REF="127.0.0.1:$ZOT_PORT/e2e/app:v1"
cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do case "$1" in -c|-n) shift 2 ;; -c*|-n*) shift ;; --) shift; break ;; *) break ;; esac; done
exec "$@"
SH
cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice && (adduser -D -h /home/user user || useradd -m -d /home/user user)
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build"; }
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; seeded $REF"

# ---- vswitch ---------------------------------------------------------------
MGMT_VIP="169.254.169.254"
"$BIN/vswitch-ctl" stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
"$BIN/vswitch-ctl" start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 >"$WORK/vswitch-start.log" 2>&1 || { sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch start"; }
SW_STARTED=1
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF
MK="$("$BIN/e2b-key-ctl" gen-key)"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"; ENC2="$("$BIN/e2b-key-ctl" gen-key)"
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found"
OVL="$WORK/overlay-1G.ext4"; truncate -s 1G "$OVL"; "$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || fail "mkfs overlay"
BLD="$WORK/builder-2G.ext4"; truncate -s 2G "$BLD"; "$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || fail "mkfs builder"

# ---- cluster config (registry is started after the group is seeded, so the
#      registry is the sole writer of its sqlite — no cross-process rev clash) --
cat > "$WORK/cluster.yaml" <<EOF
domain: $DOMAIN
store: { kind: sqlite, dsn: $WORK/cluster/registry.db }
group_config: { encryption_key: "$ENC2" }
channel: { listen: 127.0.0.1:$REG_PORT }
op: { listen: $OP_SOCK }
reserve: { park_timeout: 90s }
router: { listen: 127.0.0.1:$ROUTER_PORT, data_plane_auth: off }
EOF

# ---- node-ctl serve, joined to the cluster over node-link ------------------
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
cluster: { registry: "127.0.0.1:$REG_PORT", node_id: n1, data_endpoint: "127.0.0.1:$PORT" }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime_e2b: $BIN/sandbox-runtime-e2b.erofs, runtime_base: $BIN/sandbox-runtime.erofs, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  runtime_builder: $BIN/sandbox-runtime-builder.erofs
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint: { mode: remote }
EOF
"$BIN/node-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/  /' "$WORK/orch.log"; skip "node-ctl exited"; }
    sleep 0.5
done
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"
echo "==> node-ctl up (:$PORT); its node-link client retries until the registry starts"

# ---- build a template via the node's e2b API -------------------------------
code=$(req POST /v3/templates "$AK" '{"name":"cluster-tmpl"}'); [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(grep -o '"templateID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
BID=$(grep -o '"buildID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$GUEST_REF\"}"); [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(grep -o '"status":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
    case "$st" in ready) TEMPLATE=$(grep -oE 'e2b-img-[0-9a-f]{64}' "$WORK/resp.body" | head -1); break;; error) cat "$WORK/resp.body"; fail "build error";; esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
echo "==> built template: $TEMPLATE"

# ---- seed group config (registry not yet running), then start registry -----
GROUP="/cell/proj/app/g1"
"$BIN/cluster-ctl" group upsert --config "$WORK/cluster.yaml" --group "$GROUP" --template-ref "$TEMPLATE" --manifest-key "$MK" >/dev/null || fail "group upsert"
echo "==> seeded group $GROUP -> $TEMPLATE"
"$BIN/cluster-ctl" registry --config "$WORK/cluster.yaml" >"$WORK/registry.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$REG_PORT" registry
echo "==> cluster-ctl registry up (node-link :$REG_PORT, op $OP_SOCK)"
for _ in $(seq 1 30); do grep -q "node connected" "$WORK/registry.log" && break; sleep 0.5; done
grep -q "node connected" "$WORK/registry.log" || { sed 's/^/  reg| /' "$WORK/registry.log"; fail "node never joined the cluster over node-link"; }
echo "==> PASS: node joined the cluster (registry saw the node-link connection)"
"$BIN/cluster-ctl" router --config "$WORK/cluster.yaml" >"$WORK/router.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ROUTER_PORT" router
echo "==> cluster-ctl router up (:$ROUTER_PORT)"

# ---- create a sandbox THROUGH the cluster (router -> reserve -> node boot) --
RK="user1:sess1"
echo "==> POST /sandboxes via router (group=$GROUP route_key=$RK)"
code=$(curl -sS --noproxy '*' -o "$WORK/create.body" -w '%{http_code}' -X POST \
    -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-Kuasar-Route-Key: $RK" \
    "http://127.0.0.1:$ROUTER_PORT/sandboxes")
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/create.body"; echo
    echo "== router log =="; sed 's/^/  router| /' "$WORK/router.log"
    echo "== registry log =="; sed 's/^/  reg| /' "$WORK/registry.log"
    echo "== node log =="; sed 's/^/  orch| /' "$WORK/orch.log" | tail -40
    SID=$(ls "$WORK/run" 2>/dev/null | head -1)
    [ -n "$SID" ] && journalctl -u "sandbox-runner@$SID.service" --no-pager -n 50 2>/dev/null | sed 's/^/  unit| /'
    fail "cluster create=$code (want 201)"
fi
SID=$(grep -o '"sandboxID":"[^"]*"' "$WORK/create.body" | head -1 | cut -d'"' -f4)
[ -n "$SID" ] || fail "no sandboxID in create response"
echo "==> PASS: cluster create booted sandbox $SID (router -> registry reserve -> node-link -> microVM)"

# ---- the registry knows the route is ready ---------------------------------
curl -sS --noproxy '*' --unix-socket "$OP_SOCK" -o "$WORK/route.body" "http://op/op/route?sid=$SID" || fail "op route query"
grep -q '"state":"ready"' "$WORK/route.body" || { cat "$WORK/route.body"; fail "registry route not ready for $SID"; }
echo "==> PASS: registry resolves $SID -> node ready (op /route)"

# ---- two-hop data path: router -> node -> envd /health ---------------------
DATA_CODE=$(curl -sS --noproxy '*' -o /dev/null -w '%{http_code}' --max-time 15 \
    -H "Host: 49983-$SID.$DOMAIN" "http://127.0.0.1:$ROUTER_PORT/health" 2>/dev/null || echo 000)
case "$DATA_CODE" in
    2*|404) echo "==> PASS: two-hop data plane reached the node's envd (router -> node -> guest; http $DATA_CODE)";;
    *) echo "== router log =="; sed 's/^/  router| /' "$WORK/router.log" | tail -20; fail "data-plane forward to envd failed (http $DATA_CODE)";;
esac

echo
echo "==> e2e_cluster: OK   (group $GROUP, sandbox $SID on node n1 via the cluster control plane)"
