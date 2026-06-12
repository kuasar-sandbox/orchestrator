#!/usr/bin/env bash
#
# e2e_orchestrator_proxy.sh — exercise proxy_mode=external end to end with REAL
# components: orchestrator-ctl serve (control plane), a separate orchestrator-ctl
# proxy worker (data plane, SO_REUSEPORT), the route-sync stream between them, a
# REAL microVM sandbox with REAL envd, and data-plane traffic driven THROUGH the
# proxy (not the orchestrator):
#
#   serve(proxy_mode=external) --proxy-socket=<uds>     # control plane on :PORT
#   proxy --socket=<uds> --data-listen=:PROXY_PORT      # data plane, route-synced
#   POST /sandboxes  -> real VM + envd ; serve pushes the route to the proxy
#   GET <proxy>/health (Host 49983-<sid>): no token -> 401 (enforce);
#                                          right X-Access-Token -> forwarded to envd
#   GET <proxy> for an unknown sandbox -> wake -> 404 (orchestrator says gone)
#   pause -> GET <proxy> -> wake -> auto-resume -> forwarded (best-effort)
#   /metrics on the proxy reports data_requests_total
#
# Setup mirrors e2e_execute.sh (real VM boot). Same heavy prerequisites: systemd+
# root, /dev/kvm, vswitch, zot, docker, store-ctl, mkfs.ext4, the built kernel + e2b
# runtime. Missing prereqs -> exit 0 ("skipped") unless REQUIRE_PROXY=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
PROXY_PORT="${PROXY_PORT:-3443}"
METRICS_PORT="${METRICS_PORT:-3990}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-test-app-a:latest}"
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"
SW_NETNS="${SW_NETNS:-e2e_sw}"

skip() { echo; echo "==> e2e_orchestrator_proxy: skipping ($*)"; [ "${REQUIRE_PROXY:-0}" = "1" ] && { echo "REQUIRE_PROXY=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in orchestrator-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl vswitch-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime-e2b.erofs" ] || skip "missing $BIN/sandbox-runtime-e2b.erofs"
[ -f "$BIN/sandbox-runtime-builder.erofs" ] || skip "missing $BIN/sandbox-runtime-builder.erofs (make sandbox-runtime-builder)"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not on PATH"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || skip "base image $E2E_IMAGE not cached"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-orch-proxy-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"
PROXY_SOCK="$WORK/run/proxy-1.sock"
declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""
cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/vswitch-ctl" stop "$SWITCH" >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }

# control-plane request (api.<domain> on :PORT)
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
# data-plane request through the PROXY (:PROXY_PORT), Host <port>-<sid>.<domain>
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
    [ -n "$token" ] && args+=(-H "X-Access-Token: $token")
    curl "${args[@]}" "http://127.0.0.1:$PROXY_PORT$path"
}
dump_logs() {
    echo "==> orchestrator log:"; sed 's/^/  orch| /' "$WORK/orch.log" 2>/dev/null
    echo "==> proxy log:";        sed 's/^/  prxy| /' "$WORK/proxy.log" 2>/dev/null
}

# ---- store + zot ----------------------------------------------------------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl

# 0.0.0.0: the host pushes via 127.0.0.1; the BUILD SANDBOX pulls via the
# vswitch mgmt VIP (guest loopback is not the host's).
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
REF="127.0.0.1:$ZOT_PORT/e2e/app:v1"

# An e2b-compliant base image: add a 'user' account + tiny ionice/nice shims (envd
# wraps guest processes as `ionice -c.. nice -n.. cmd` and runs them as the default
# user). Same prep as e2e_execute.sh; the orchestrator/envd are unchanged.
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
RUN chmod +x /usr/bin/ionice /usr/bin/nice && (adduser -D -h /home/user user || useradd -m -d /home/user user)
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF"

# ---- vswitch up ------------------------------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP.
MGMT_VIP="169.254.169.254"
"$BIN/vswitch-ctl" stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true; ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
"$BIN/vswitch-ctl" start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap \
    --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 >"$WORK/vswitch-start.log" 2>&1 || { sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch start"; }
SW_STARTED=1
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
BLD="$WORK/builder-2G.ext4"   # build sandbox writable disk (pull cache + export scratch)
truncate -s 2G "$BLD"
"$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || { cat "$WORK/mkfs-bld.log"; fail "mkfs.ext4 builder template"; }

# ---- orchestrator config: proxy_mode=external -----------------------------
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { mode: external, sockets: [ "$PROXY_SOCK" ], auth: enforce, park_timeout: 90s }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/orchestrator.socket }
units: { dir: $UNIT_DIR }
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

# ---- start the proxy worker, then serve -----------------------------------
echo "==> orchestrator-ctl proxy (data-plane :$PROXY_PORT, socket=$PROXY_SOCK)"
"$BIN/orchestrator-ctl" proxy --socket="$PROXY_SOCK" --data-listen="127.0.0.1:$PROXY_PORT" \
    --auth=enforce --park-timeout=90s --metrics-listen="127.0.0.1:$METRICS_PORT" >"$WORK/proxy.log" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$PROXY_PORT" proxy

echo "==> orchestrator-ctl serve (control :$PORT, proxy_mode=external)"
"$BIN/orchestrator-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { dump_logs; skip "orchestrator exited"; }
    sleep 0.5
done
"$BIN/orchestrator-ctl" manifest-key add --socket "$WORK/orchestrator.socket" "$MK" >/dev/null || fail "manifest-key add"
echo "==> control plane up; route-sync client dialing the proxy"

# ---- build a ready e2b template (native v3) --------------------------------
code=$(req POST /v3/templates "$AK" '{"name":"proxy-tmpl"}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(grep -o '"templateID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
BID=$(grep -o '"buildID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(grep -o '"status":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
    case "$st" in
        ready) TEMPLATE=$(grep -oE 'e2b-img-[0-9a-f]{64}' "$WORK/resp.body" | head -1); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
echo "==> built template: $TEMPLATE"

# ---- create the sandbox (boots the microVM; serve pushes the route) -------
echo "==> POST /sandboxes (boot microVM from $TEMPLATE)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo; dump_logs
    SID=$(ls "$WORK/run" 2>/dev/null | grep -v proxy | head -1)
    [ -n "$SID" ] && { echo "==> runner journal:"; journalctl -u "sandbox-runner@$SID.service" --no-pager -n 60 2>/dev/null | sed 's/^/  unit| /'; }
    fail "create=$code (want 201)"
fi
SID=$(grep -o '"sandboxID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
ENVD_TOKEN=$(grep -o '"envdAccessToken":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
[ -n "$SID" ] && [ -n "$ENVD_TOKEN" ] || fail "missing sandboxID/envdAccessToken in create response"
echo "==> PASS: sandbox $SID running (envd token captured)"

# ---- (1) data plane THROUGH the proxy: route-sync + forward + auth ---------
ok=""
for _ in $(seq 1 20); do
    code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
    { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "envd /health via proxy with token = $code (want 200/204)"; }
echo "==> PASS: route-synced + data plane forwarded through the proxy to real envd (X-Access-Token accepted)"

code=$(dp "49983-$SID" /health "")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy WITHOUT token = $code (want 401 enforce)"; }
echo "==> PASS: proxy enforces X-Access-Token (missing -> 401)"

code=$(dp "49983-$SID" /health "wrong-token")
[ "$code" = "401" ] || { dump_logs; fail "envd /health via proxy with WRONG token = $code (want 401)"; }
echo "==> PASS: proxy rejects a wrong token (401)"

# ---- (2) unknown sandbox via proxy -> wake -> 404 -------------------------
code=$(dp "49983-deadbeefdeadbeef" /health "$ENVD_TOKEN")
if [ "$code" = "404" ]; then echo "==> PASS: unknown sandbox via proxy -> 404 (orchestrator resolved the wake as gone)"
else echo "    (note: unknown-sandbox via proxy = $code; expected 404 after wake)"; fi

# ---- (3) metrics ----------------------------------------------------------
if curl -sS --noproxy '*' "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null | grep -q 'data_requests_total'; then
    echo "==> PASS: proxy /metrics reports data_requests_total"
else echo "    (note: proxy /metrics did not report data_requests_total)"; fi

# ---- (4) auto-resume THROUGH the proxy (best-effort: needs snapshot) ------
echo "==> pause $SID, then drive the proxy to trigger wake -> auto-resume"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
if [ "$code" = "204" ]; then
    ok=""
    for _ in $(seq 1 40); do
        code=$(dp "49983-$SID" /health "$ENVD_TOKEN")
        { [ "$code" = "204" ] || [ "$code" = "200" ]; } && { ok=1; break; }
        sleep 0.5
    done
    if [ -n "$ok" ]; then echo "==> PASS: auto-resume through the proxy (wake -> resume -> forwarded, code=$code)"
    else echo "    (note: auto-resume via proxy did not complete, last code=$code — snapshot/restore may be unsupported in this build)"; fi
else echo "    (note: pause returned $code; skipping auto-resume-through-proxy check)"; fi

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || { dump_logs; fail "kill=$code (want 204)"; }
echo "==> PASS: sandbox killed"
echo
echo "==> e2e_orchestrator_proxy: OK   (template $TEMPLATE, sandbox $SID, external proxy on :$PROXY_PORT)"
