#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
SCRIPT_DIR="${E2E_LIB:?E2E_LIB must point to prepared helpers}/orchestrator"
. "$SCRIPT_DIR/vmm_cgroup.sh"
. "$SCRIPT_DIR/proxy.sh"
. "$SCRIPT_DIR/execute_state.sh"
. "$SCRIPT_DIR/native_usage.sh"
. "$SCRIPT_DIR/telemetry_stats.sh"
NATIVE_USAGE_SAMPLE=1s
NATIVE_USAGE_FLUSH=2s
: "${BIN:?BIN must point to prepared products}"
MMDS_ROUTES_E2E="${MMDS_ROUTES_E2E:-0}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-}"
PROXY_PORT="${PROXY_PORT:-}"
SWITCH="${SWITCH:-}"
: "${ORCHESTRATOR_EXECUTE_IMAGE:?ORCHESTRATOR_EXECUTE_IMAGE must name the prepared execute image}"
E2E_IMAGE="$ORCHESTRATOR_EXECUTE_IMAGE"
: "${ZOT_BIN:?ZOT_BIN must point to the prepared registry binary}"
SW_NETNS="${SW_NETNS:-}"
PROXY_NETNS="${PROXY_NETNS:-}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-}"
PROXY_VETH_NS="${PROXY_VETH_NS:-}"
PROXY_HOST_IP="${PROXY_HOST_IP:-172.31.253.1}"
PROXY_NS_IP="${PROXY_NS_IP:-172.31.253.2}"
FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"
LOW_ALLOC_REPEATS="${LOW_ALLOC_REPEATS:-2}"

fail() { echo "==> FAIL: $*" >&2; exit 1; }
skip() { fail "$*"; }

. "$SCRIPT_DIR/execute_contract.sh"
RECOVERY_PENDING=0
execute_state_path_absent "$(execute_state_path)" || RECOVERY_PENDING=1
recovery_prerequisite() {
    [ "$RECOVERY_PENDING" = 0 ] && skip "$1"
    fail "$1; interrupted execute state requires recovery"
}

case "$BIN" in
    /*) ;;
    *) BIN="$(cd "$BIN" 2>/dev/null && pwd)" || recovery_prerequisite "BIN directory not found" ;;
esac

command -v flock >/dev/null 2>&1 || recovery_prerequisite "flock not found"
[ -d /run/systemd/system ] || recovery_prerequisite "systemd not PID1"
if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if [ "${KUASAR_EXECUTE_LOCK_HELD:-0}" != 1 ]; then
    run_host_serialized /run/systemd/system "$0" "$@"
    exit "$?"
fi

# Recover only a topology whose names were durably reserved by an earlier
# lifecycle/exec/Proxy/MMDS case interrupted before cleanup completed.
if [ "$RECOVERY_PENDING" = 1 ]; then
    [ -x "$BIN/connector-ctl" ] || fail "missing $BIN/connector-ctl; interrupted execute state requires recovery"
    for command in ip iptables systemctl stat sysctl awk grep seq sleep rm rmdir; do
        command -v "$command" >/dev/null 2>&1 \
            || fail "$command not found; interrupted execute state requires recovery"
    done
fi
execute_state_recover "$BIN"

for b in node-ctl sandbox-ctl flatten-ctl manifest-ctl store-ctl e2b-key-ctl connector-ctl cloud-hypervisor mkfs.erofs; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.bundle" ] || skip "missing $BIN/sandbox-runtime.bundle"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN to the prepared registry)"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
command -v iptables >/dev/null 2>&1 || skip "iptables not found"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || fail "prepared execute image is missing: $E2E_IMAGE"
export PATH="$BIN:$PATH"

# Preserve the existing Build admission vector and its derived A/B CPU.
# Target Sandbox CPU resolves independently from the configuration below;
# parent Builder services/slices do not add a CPU policy.
BUILDER_CPU="$(nproc)"

: "${WORK:?WORK must be supplied by the common runner}"
CASE_OUT="${OUT:-$WORK}"
WORK="$(mktemp -d /tmp/e-XXXXXX)"
RUN_KEY="${WORK##*/}"
SWITCH="${SWITCH:-x${RUN_KEY#e-}}"
SW_NETNS="${SW_NETNS:-${RUN_KEY}-sw}"
PROXY_NETNS="${PROXY_NETNS:-${RUN_KEY}-proxy}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-${RUN_KEY}h}"
PROXY_VETH_NS="${PROXY_VETH_NS:-${RUN_KEY}p}"
for name in "$SWITCH" "$SW_NETNS" "$PROXY_NETNS" "$PROXY_VETH_HOST" "$PROXY_VETH_NS"; do
    [[ "$name" =~ ^[A-Za-z0-9_-]{1,15}$ ]] || fail "invalid E2E network resource name"
done
RUNNER_PREFIX="sandbox-runner-${RUN_KEY}@"
BUILDER_PREFIX="sandbox-builder-${RUN_KEY}@"
TAPFD_SOCKET="$WORK/tapfd.sock"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=("${RUNNER_PREFIX}.service" "${BUILDER_PREFIX}.service" sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    if ! execute_state_path_absent "$UNIT_DIR/$u"; then skip "$UNIT_DIR/$u exists; refusing to clobber"; fi
    OURS+=("$UNIT_DIR/$u")
done
for prefix in "$RUNNER_PREFIX" "$BUILDER_PREFIX"; do
    execute_state_path_absent "$UNIT_DIR/${prefix}.service.d" || skip "$UNIT_DIR/${prefix}.service.d exists; refusing to clobber"
done
execute_state_assert_targets_absent "$BIN" "$SWITCH" "$SW_NETNS" "$PROXY_NETNS" "$PROXY_VETH_HOST" "$PROXY_VETH_NS"
execute_state_assert_units_absent "$RUNNER_PREFIX" "$BUILDER_PREFIX"
execute_state_reserve "$RUN_KEY" "$WORK" "$SWITCH" "$SW_NETNS" "$PROXY_NETNS" "$PROXY_VETH_HOST" "$PROXY_VETH_NS" -
EXECUTE_STATE_EXPECTED="$(execute_state_record "$RUN_KEY" "$WORK" "$SWITCH" "$SW_NETNS" "$PROXY_NETNS" "$PROXY_VETH_HOST" "$PROXY_VETH_NS" -)"
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"

declare -a PIDS=()
declare -a TAGS=()
IMMEDIATE_DATA_PID=""
PROXY_PID=""
SW_STARTED=""
SW_NETNS_OWNED=0
PROXY_NETNS_OWNED=0
PROXY_VETH_OWNED=0
FORWARD_TO_SWITCH_OWNED=0
FORWARD_FROM_SWITCH_OWNED=0
ORIG_IP_FORWARD=""
trap cleanup EXIT
for helper in execute_network execute_client execute_resource execute_artifact execute_sandbox execute_process; do
    . "$SCRIPT_DIR/$helper.sh"
done
[ -n "$PORT" ] || PORT="$(free_port)"
[ -n "$PROXY_PORT" ] || PROXY_PORT="$(free_port)"
. "$SCRIPT_DIR/execute_instrument.sh"
MMDS_ROUTES_CONFIG=""
MMDS_SERVICES_CONFIG=""
if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$SCRIPT_DIR/mmds_service_guest.sh"
    start_mmds_service_backend "$WORK" || fail "start MMDS service UDS backend"
    MMDS_ROUTES_CONFIG="  routes:
    enabled: true"
    MMDS_SERVICES_CONFIG="  services:
    $MMDS_SERVICE_NAME:
      endpoint: unix://$MMDS_SERVICE_SOCKET"
fi

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
docker image inspect "$ORCHESTRATOR_EXECUTE_IMAGE" >/dev/null || fail "prepared execute image is missing"
docker tag "$ORCHESTRATOR_EXECUTE_IMAGE" "$REF"
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; seeded prepared $REF (user + ionice/nice shims)"

# ---- vswitch up (ip netns + start) -----------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP. Use this run's names;
# stale switch metadata must never delete a new or foreign interface by index.
MGMT_VIP="169.254.169.254"
MMDS_PORT="$(free_port)"
switch_status=0
"$BIN/connector-ctl" vswitch status "$SWITCH" >/dev/null 2>&1 || switch_status=$?
[ "$switch_status" -eq 3 ] || fail "vSwitch is not absent; refusing foreign or inconsistent resource: $SWITCH"
if [ -e "/var/run/netns/$SW_NETNS" ] || [ -L "/var/run/netns/$SW_NETNS" ]; then
    fail "switch netns already exists; refusing foreign resource: $SW_NETNS"
fi
ip netns add "$SW_NETNS"
SW_NETNS_OWNED=1
setup_proxy_netns
check_proxy_netns_address before-vswitch
echo "==> starting vswitch $SWITCH (netns=$SW_NETNS)"
"$BIN/connector-ctl" vswitch serve "$SWITCH" \
    --netns="$SW_NETNS" \
    --ports=64 \
    --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 \
    --mode=tap \
    "--mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0" \
    "--mgmt-service=$MGMT_VIP:80:$PROXY_NS_IP:$MMDS_PORT" \
    "--mgmt-service=$MGMT_VIP:4317:$PROXY_NS_IP:4317" \
    "--mgmt-service=$MGMT_VIP:4318:$PROXY_NS_IP:4318" \
    --tapfd-listen="$TAPFD_SOCKET" --watch-interval=2s >"$WORK/vswitch-start.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
    if [ -S "$TAPFD_SOCKET" ] && "$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1; then
        break
    fi
    kill -0 "${PIDS[-1]}" 2>/dev/null || { echo "vswitch serve failed:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch serve exited"; }
    sleep 0.2
done
"$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1 \
    || { echo "vswitch not ready:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch not ready"; }
SW_STARTED=1
ip addr replace "$MGMT_VIP/32" dev "${SWITCH}m0" \
    || fail "configure management VIP on ${SWITCH}m0"
allow_proxy_forwarding
check_proxy_netns_address after-vswitch
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF; tapfd_socket=$TAPFD_SOCKET; Proxy netns=$PROXY_NETNS reaches $FIP_CIDR)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper
# (deployment-provided in prod; created inline here). mkfs.ext4 may live in /sbin.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
echo "==> overlay diff_template: $OVL ($(du -h "$OVL" | cut -f1) on disk)"
BLD="$WORK/builder-2G.ext4"   # build sandbox writable disk (pull cache + export scratch)
truncate -s 2G "$BLD"
"$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || { cat "$WORK/mkfs-bld.log"; fail "mkfs.ext4 builder template"; }

CHECKPOINT_ROOT="$WORK/lib/sandboxes"
cp "$SCRIPT_DIR/envd_exec.py" "$SCRIPT_DIR/envd_start.py" "$WORK/"
