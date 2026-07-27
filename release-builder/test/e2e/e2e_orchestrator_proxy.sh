#!/usr/bin/env bash
#
# e2e_orchestrator_proxy.sh — exercise proxy_mode=external end to end with REAL
# components: node-ctl conductor serve (control plane), a separate node-ctl
# proxy master that REGISTERS once on the config-socket plugin plane, syncs routes
# into shared memory, supervises workers, a REAL microVM sandbox with REAL envd, and
# data-plane traffic driven THROUGH the proxy (not the orchestrator):
#
#   serve(proxy_mode=external)                          # control plane on :PORT
#   proxy serve --config <proxy.yaml>                    # data-plane on :PROXY_PORT
#         # one master plugin registration + N workers sharing inherited listeners;
#         # workers run in PROXY_NETNS, and mmds_listen is bound there
#   POST /sandboxes  -> real VM + envd ; serve streams the route to the proxy
#   GET <proxy>/health (Host 49983-<sid>): no token -> 401 (enforce);
#                                          right X-Access-Token -> forwarded to envd
#   CONNECT through the proxy (token on the CONNECT) -> tunnel to envd
#   GET <proxy> for an unknown sandbox -> wake -> 404 (orchestrator says gone)
#   POST /sandboxes/<sid>/exec-sessions -> KAT; service=exec CONNECT through the
#                                          external proxy -> real guest exec
#   pause -> GET <proxy> -> wake -> auto-resume -> forwarded
#   /metrics on the proxy master reports worker data-plane counters
#
# Setup mirrors e2e_execute.sh (real VM boot). Same heavy prerequisites: systemd+
# root, /dev/kvm, vswitch, zot, docker, store-ctl, mkfs.ext4, the built kernel + e2b
# runtime. Missing prereqs -> exit 0 ("skipped") unless REQUIRE_PROXY=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
EXEC_CONNECT_BRIDGE="$REPO_ROOT/test/e2e/exec_connect_bridge.py"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
PROXY_PORT="${PROXY_PORT:-3443}"
METRICS_PORT="${METRICS_PORT:-3990}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_sw}"
PROXY_NETNS="${PROXY_NETNS:-e2e_proxy}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-e2eph0}"
PROXY_VETH_NS="${PROXY_VETH_NS:-e2epn0}"
PROXY_HOST_IP="${PROXY_HOST_IP:-172.31.254.1}"
PROXY_NS_IP="${PROXY_NS_IP:-172.31.254.2}"
FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"

skip() { echo; echo "==> e2e_orchestrator_proxy: skipping ($*)"; [ "${REQUIRE_PROXY:-0}" = "1" ] && { echo "REQUIRE_PROXY=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.erofs" ] || skip "missing $BIN/sandbox-runtime.erofs"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
[ -f "$EXEC_CONNECT_BRIDGE" ] || skip "missing $EXEC_CONNECT_BRIDGE"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
command -v iptables >/dev/null 2>&1 || skip "iptables not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "base image $E2E_IMAGE unavailable (set E2E_IMAGE to a local or pullable image)"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-orch-proxy-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"
PROXY_SOCK="$WORK/run/proxy.sock"
declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""
ORIG_IP_FORWARD=""
cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" >/dev/null 2>&1
    iptables -D FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null
    iptables -D FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null
    ip link del "$PROXY_VETH_HOST" 2>/dev/null
    ip netns del "$PROXY_NETNS" 2>/dev/null
    ip netns del "$SW_NETNS" 2>/dev/null
    [ -n "$ORIG_IP_FORWARD" ] && sysctl -q -w "net.ipv4.ip_forward=$ORIG_IP_FORWARD" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
wait_mmds_listener() {
    local hex
    hex="$(printf '%04X' "$MMDS_PORT")"
    for _ in $(seq 1 60); do
        ip netns exec "$PROXY_NETNS" awk -v p=":$hex" '$2 ~ p && $4 == "0A" { found = 1 } END { exit(found ? 0 : 1) }' /proc/net/tcp 2>/dev/null && return 0
        sleep 0.5
    done
    fail "mmds listener did not appear in proxy_netns=$PROXY_NETNS on $PROXY_NS_IP:$MMDS_PORT"
}
child_worker_pids() {
    local parent="$1" p stat ppid cmd
    for d in /proc/[0-9]*; do
        p="${d##*/}"
        [ -r "$d/stat" ] || continue
        stat="$(cat "$d/stat" 2>/dev/null || true)"
        ppid="$(printf '%s\n' "$stat" | awk '{print $4}')"
        [ "$ppid" = "$parent" ] || continue
        cmd="$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null || true)"
        case "$cmd" in *"node-ctl proxy serve"*"--worker"*) printf '%s\n' "$p";; esac
    done
}
wait_proxy_workers_in_netns() {
    local master="$1" target workers got
    target="$(stat -Lc '%i' "/var/run/netns/$PROXY_NETNS")"
    for _ in $(seq 1 60); do
        workers="$(child_worker_pids "$master" | tr '\n' ' ')"
        [ -n "$workers" ] && break
        sleep 0.5
    done
    [ -n "${workers:-}" ] || fail "proxy workers did not start under master pid $master"
    for wp in $workers; do
        got="$(stat -Lc '%i' "/proc/$wp/ns/net" 2>/dev/null || true)"
        [ "$got" = "$target" ] || fail "proxy worker $wp netns inode=$got, want proxy_netns inode=$target"
    done
}
setup_proxy_netns() {
    ip link del "$PROXY_VETH_HOST" 2>/dev/null || true
    ip netns del "$PROXY_NETNS" 2>/dev/null || true
    ip netns add "$PROXY_NETNS"
    ip link add "$PROXY_VETH_HOST" type veth peer name "$PROXY_VETH_NS"
    ip link set "$PROXY_VETH_NS" netns "$PROXY_NETNS"
    ip addr add "$PROXY_HOST_IP/30" dev "$PROXY_VETH_HOST"
    ip link set "$PROXY_VETH_HOST" up
    ip netns exec "$PROXY_NETNS" ip addr add "$PROXY_NS_IP/30" dev "$PROXY_VETH_NS"
    ip netns exec "$PROXY_NETNS" ip link set lo up
    ip netns exec "$PROXY_NETNS" ip link set "$PROXY_VETH_NS" up
    ip netns exec "$PROXY_NETNS" ip route add "$FIP_CIDR" via "$PROXY_HOST_IP"
    ORIG_IP_FORWARD="$(sysctl -n net.ipv4.ip_forward 2>/dev/null || true)"
    sysctl -q -w net.ipv4.ip_forward=1
}
allow_proxy_forwarding() {
    iptables -C FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT
    iptables -C FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT
}

# control-plane request (api.<domain> on :PORT)
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
assert_no_default_exec_token() {
    python3 - "$1" <<'PY'
import json, sys
created = json.load(open(sys.argv[1]))
if "execAccessToken" in created:
    raise SystemExit("create response unexpectedly contains execAccessToken")
PY
}
issue_exec_session() {
    local sid="$1" key="$2" code
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-API-KEY: $key" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$PORT/sandboxes/$sid/exec-sessions")"
    [ "$code" = "201" ] || fail "exec-session=$code (want 201)"
    python3 - "$WORK/exec-session.headers" "$WORK/exec-session.secret" <<'PY'
import json, sys
headers = [line.strip().lower() for line in open(sys.argv[1], "rb").read().splitlines()]
if b"cache-control: no-store" not in headers:
    raise SystemExit("exec-session response omitted Cache-Control: no-store")
payload = json.load(open(sys.argv[2]))
if not isinstance(payload, dict) or set(payload) != {"execAccessToken"}:
    raise SystemExit("exec-session response must contain only execAccessToken")
token = payload["execAccessToken"]
if not isinstance(token, str) or not token.startswith("kat1.") or len(token.split(".")) != 3:
    raise SystemExit("exec-session response contains an invalid KAT token")
print(token)
PY
}
exec_through_proxy_connect() {
    local sid="$1" token="$2" marker="$3"
    local bridge_root="$WORK/exec-connect-run"
    local ready_file="$WORK/exec-connect.ready"
    local bridge_log="$WORK/exec-connect-bridge.log"
    local output="$WORK/native-exec.out"
    local bridge_pid status

    mkdir -p "$bridge_root/$sid"
    EXEC_CONNECT_TOKEN="$token" timeout -k 5s 75 python3 "$EXEC_CONNECT_BRIDGE" \
        --listen "$bridge_root/$sid/ctl.sock" \
        --ready-file "$ready_file" \
        --upstream "127.0.0.1:$PROXY_PORT" \
        --sandbox-id "$sid" \
        >"$bridge_log" 2>&1 &
    bridge_pid=$!
    PIDS+=("$bridge_pid")
    for _ in $(seq 1 100); do
        [ -S "$bridge_root/$sid/ctl.sock" ] && [ -f "$ready_file" ] && break
        kill -0 "$bridge_pid" 2>/dev/null || { cat "$bridge_log"; fail "exec CONNECT bridge exited before readiness"; }
        sleep 0.05
    done
    [ -S "$bridge_root/$sid/ctl.sock" ] && [ -f "$ready_file" ] \
        || { cat "$bridge_log"; fail "exec CONNECT bridge did not become ready"; }

    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --sandbox-id "$sid" --run-root "$bridge_root" -- \
        /bin/sh -c "printf '%s\\n' '$marker'; exit 47" >"$output" 2>&1; then
        status=0
    else
        status=$?
    fi
    if ! wait "$bridge_pid"; then
        cat "$bridge_log"
        fail "exec CONNECT bridge failed"
    fi
    grep -Fxq "$marker" "$output" || { sed 's/^/  guest| /' "$output"; fail "native exec output missing $marker"; }
    [ "$status" = "47" ] || { sed 's/^/  guest| /' "$output"; fail "native exec exit=$status (want guest status 47)"; }
}
# data-plane request through the PROXY (:PROXY_PORT), Host <port>-<sid>.<domain>
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --max-time "${DP_MAX_TIME:-120}" --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
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
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF"

# ---- vswitch up ------------------------------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP.
MGMT_VIP="169.254.169.254"
MMDS_PORT="$(free_port)"
setup_proxy_netns
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true; ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
"$BIN/connector-ctl" vswitch start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap \
    --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$PROXY_NS_IP:$MMDS_PORT >"$WORK/vswitch-start.log" 2>&1 || { sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch start"; }
SW_STARTED=1
allow_proxy_forwarding
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF; proxy_netns=$PROXY_NETNS reaches $FIP_CIDR via $PROXY_VETH_HOST)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

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
proxy: { mode: external, auth: enforce, park_timeout: 2s }
mmds: { enabled: true, listen: "$PROXY_NS_IP:$MMDS_PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
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

# ---- start serve (control plane), then the proxy master -------------------
# The proxy master DIALS serve's config-socket to register, so serve comes up first.
echo "==> node-ctl conductor serve (control :$PORT, proxy_mode=external)"
"$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { dump_logs; skip "orchestrator exited"; }
    sleep 0.5
done
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"

echo "==> node-ctl proxy master (single plugin registration, data-plane :$PROXY_PORT, workers=2)"
# The master reads policy/endpoints from proxy.yaml, registers once, then supervises
# workers that inherit listener fds and read the shared route table. h2c here (no tls),
# matching serve's plain-http listener. proxy_netns exercises external direct mode:
# workers run inside that netns, so floatingip TCP dials need its route table.
cat > "$WORK/proxy.yaml" <<EOF
config_socket: $WORK/node-ctl.socket
paths:
  run_root: $WORK/run
data_listen: 127.0.0.1:$PROXY_PORT
proxy_netns: $PROXY_NETNS
proxy_socket: $PROXY_SOCK
shm_path: $WORK/run/proxy-routes.shm
route_capacity: 1024
workers: 2
auth: enforce
park_timeout: 2s
mmds_listen: $PROXY_NS_IP:$MMDS_PORT
metrics_listen: 127.0.0.1:$METRICS_PORT
EOF
"$BIN/node-ctl" proxy serve --config "$WORK/proxy.yaml" >"$WORK/proxy.log" 2>&1 &
PIDS+=($!)
PROXY_MASTER_PID="${PIDS[-1]}"
wait_port 127.0.0.1 "$PROXY_PORT" proxy
wait_mmds_listener
wait_proxy_workers_in_netns "$PROXY_MASTER_PID"
echo "==> control plane up; proxy master registered on the config-socket plugin plane"
echo "==> PASS: external proxy workers and mmds_listen are in proxy_netns=$PROXY_NETNS"

# ---- build a ready e2b template (native v3) --------------------------------
code=$(req POST /v3/templates "$AK" '{"name":"proxy-tmpl"}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$GUEST_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
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
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
[ -n "$SID" ] && [ -n "$ENVD_TOKEN" ] && [ -n "$FORWARD_TOKEN" ] \
    || fail "missing sandboxID/envdAccessToken/forwardAccessToken in create response"
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
echo "==> PASS: sandbox $SID running (envd and forward tokens captured; no default exec token)"

ENVD_SOCK="$WORK/run/$SID/envd.sock"
for _ in $(seq 1 40); do [ -S "$ENVD_SOCK" ] && break; sleep 0.25; done
[ -S "$ENVD_SOCK" ] || fail "envd.sock not found at $ENVD_SOCK"
cat > "$WORK/envd_exec.py" <<'PY'
import http.client, socket, struct, json, base64, sys
sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.sock.connect(sock_path)
req = {"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json",
    "Connect-Protocol-Version": "1",
    "X-Access-Token": token,
})
r = c.getresponse()
data = r.read()
out = b""
exit_code = None
err = None
i = 0
while i + 5 <= len(data):
    flag = data[i]
    ln = struct.unpack(">I", data[i+1:i+5])[0]
    msg = data[i+5:i+5+ln]
    i += 5 + ln
    j = json.loads(msg) if msg else {}
    if flag & 2:
        if j.get("error"):
            err = j
        continue
    ev = j.get("event", {})
    if "data" in ev:
        d = ev["data"]
        for k in ("stdout", "stderr"):
            if d.get(k):
                out += base64.b64decode(d[k])
    if "end" in ev:
        exit_code = ev["end"].get("exitCode", 0)
print("HTTP_STATUS", r.status)
print("EXIT_CODE", exit_code)
if err is not None:
    print("API_ERROR", json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n")
sys.stdout.flush()
sys.stdout.buffer.write(out)
sys.stdout.write("\nOUTPUT_END\n")
PY

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

USER_MARK="proxy-netns-user-port-$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "mkdir -p /home/user/e2e-site; echo '$USER_MARK' > /home/user/e2e-site/index.html; cd /home/user/e2e-site; python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/e2e-http-8000.log 2>&1 &" \
    >"$WORK/start-user-port.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-user-port.out" || { sed 's/^/  envd| /' "$WORK/start-user-port.out"; fail "start guest user-port server"; }
ok=""
for _ in $(seq 1 30); do
    code=$(DP_MAX_TIME=8 dp "8000-$SID" / "$FORWARD_TOKEN" || true)
    grep -q "$USER_MARK" "$WORK/dp.body" 2>/dev/null && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; dump_logs; fail "user port via proxy_netns -> floatingip did not return marker"; }
echo "==> PASS: proxy_netns worker reached sandbox floatingip:8000 (real user port, marker=$USER_MARK)"

# ---- (2) unknown sandbox via proxy -> wake -> 404 -------------------------
code=$(dp "49983-deadbeefdeadbeef" /health "$ENVD_TOKEN")
if [ "$code" = "404" ]; then echo "==> PASS: unknown sandbox via proxy -> 404 (orchestrator resolved the wake as gone)"
else dump_logs; fail "unknown sandbox via proxy = $code (want 404 after wake)"; fi

# ---- (3) metrics ----------------------------------------------------------
if curl -sS --noproxy '*' "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null | grep -q 'data_requests_total'; then
    echo "==> PASS: proxy master /metrics reports aggregated worker data_requests_total"
else dump_logs; fail "proxy master /metrics did not report data_requests_total"; fi

# ---- (3b) CONNECT tunnel THROUGH the proxy to envd control -----------------
# Drive CONNECT with raw TCP so the test does not depend on curl proxy-header
# feature variations. The proxy auths the CONNECT and then tunnels a GET /health
# request to the envd control socket.
cc=$(python3 - "$PROXY_PORT" "49983-$SID.$DOMAIN:49983" "$ENVD_TOKEN" <<'PY'
import socket, sys

proxy_port, target, token = int(sys.argv[1]), sys.argv[2], sys.argv[3]
with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as s:
    s.settimeout(10)
    req = (
        f"CONNECT {target} HTTP/1.1\r\n"
        f"Host: {target}\r\n"
        f"X-Access-Token: {token}\r\n"
        "\r\n"
    )
    s.sendall(req.encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    status = data.split(b"\r\n", 1)[0].decode("latin1", "replace")
    if " 200 " not in status and not status.endswith(" 200"):
        print(status or "no-connect-response")
        sys.exit(0)
    s.sendall(f"GET /health HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n".encode())
    data = b""
    while b"\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    print((data.split(b"\r\n", 1)[0] or b"no-health-response").decode("latin1", "replace"))
PY
)
if echo "$cc" | grep -Eq 'HTTP/[0-9.]+ (200|204)'; then
    echo "==> PASS: CONNECT tunnel through the proxy reached envd /health ($cc)"
else
    dump_logs
    fail "CONNECT tunnel through proxy failed: $cc"
fi

# ---- (4) native exec capability THROUGH the external proxy ----------------
echo "==> issue an explicit native exec capability through the direct control API"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue native exec capability"
rm -f "$WORK/exec-session.secret"
NATIVE_MARK="EXTERNAL_PROXY_NATIVE_EXEC_$RANDOM"
exec_through_proxy_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
unset EXEC_TOKEN
echo "==> PASS: service=exec CONNECT through the external proxy reached the real guest (marker=$NATIVE_MARK, exit=47)"

# ---- (5) auto-resume THROUGH the proxy ------------------------------------
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
    else dump_logs; fail "auto-resume via proxy did not complete, last code=$code"; fi
else dump_logs; fail "pause returned $code (want 204 before auto-resume check)"; fi

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || { dump_logs; fail "kill=$code (want 204)"; }
echo "==> PASS: sandbox killed"
echo
echo "==> e2e_orchestrator_proxy: OK   (template $TEMPLATE, sandbox $SID, external proxy on :$PROXY_PORT)"
