#!/usr/bin/env bash
# Prepared inputs and task-owned fixture processes for Builder cases.
SCRIPT_DIR="${E2E_LIB:?E2E_LIB must point to prepared helpers}/orchestrator"
. "$SCRIPT_DIR/proxy.sh"
. "$SCRIPT_DIR/build_fixture_units.sh"
. "$SCRIPT_DIR/vmm_cgroup.sh"
: "${BIN:?BIN must point to prepared products}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
# Builds with steps/startCmd carry the e2b contract: envd runs them as
# `/bin/bash -l -c` — the image must have bash (python:3.12-slim does).
: "${ORCHESTRATOR_BASE_IMAGE:?ORCHESTRATOR_BASE_IMAGE must name the prepared builder image}"
E2E_IMAGE="$ORCHESTRATOR_BASE_IMAGE"
: "${ZOT_BIN:?ZOT_BIN must point to the prepared registry binary}"
SWITCH="${SWITCH:-b${BASHPID}}"; SW_NETNS="${SW_NETNS:-e2ebld_${BASHPID}}"; SW_MGMT="${SW_MGMT:-${SWITCH}m0}"
MGMT_VIP="169.254.169.254"                   # host-side mgmt NIC IP; guests route 0/0 here

fail() { echo "==> FAIL: $*" >&2; exit 1; }

# ---- prerequisite checks --------------------------------------------------
for b in node-ctl sandbox-ctl e2b-key-ctl connector-ctl cloud-hypervisor flatten-ctl manifest-ctl store-ctl; do
    [ -x "$BIN/$b" ] || fail "missing $BIN/$b; prepare the required products"
done
for f in vmlinux sandbox-runtime.bundle; do
    [ -f "$BIN/$f" ] || fail "missing $BIN/$f; prepare the required products"
done
command -v curl >/dev/null 2>&1 || fail "curl not on PATH"
command -v python3 >/dev/null 2>&1 || fail "python3 not on PATH"
command -v docker >/dev/null 2>&1 || fail "docker not on PATH"
docker info >/dev/null 2>&1 || fail "docker daemon not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || fail "zot not found (set ZOT_BIN to the prepared registry)"
command -v mkfs.ext4 >/dev/null 2>&1 || [ -x /sbin/mkfs.ext4 ] || fail "mkfs.ext4 not found"
[ -d /run/systemd/system ] || fail "systemd is not PID1 (orchestrator drives units over D-Bus)"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || fail "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || fail "prepared builder image is missing: $E2E_IMAGE"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

# Keep the existing Build admission and A/B resource vectors. Runtime limits
# belong to sandbox-ctl at the VMM leaf; no parent service/slice quota applies.
HOST_CPU="$(nproc)"
BUILDER_CPU=$((HOST_CPU > 2 ? 2 : 1))
BUILDER_CPU_MILLI=$((BUILDER_CPU * 1000))
BUILDER_EXECUTION_CPU=$((BUILDER_CPU * 2))
BUILDER_EXECUTION_CPU_MILLI=$((BUILDER_EXECUTION_CPU * 1000))
BUILDER_REGISTRATION_CPU=$((BUILDER_CPU * 16))
BUILDER_REGISTRATION_CPU_MILLI=$((BUILDER_REGISTRATION_CPU * 1000))

CASE_OUT="${OUT:-${WORK:?WORK must be supplied by the public runner}}"
mkdir -p "$CASE_OUT"
WORK="$(mktemp -d /tmp/e-XXXXXX)"
TAPFD_SOCKET="$WORK/tapfd.sock"
# Units must live in a real systemd load path; we only remove what we created.
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && fail "$UNIT_DIR/$u already exists (real deployment?); refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot" "$WORK/vgw" "$WORK/ref-locations"
REF_LOCATION_PARENT="file://$WORK/ref-locations"
declare -a PIDS=() TAGS=()
SW_STARTED=""
NETNS_CREATED=""
cleanup() {
    set +e
    stop_build_fixture_units "$WORK" 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    [ -n "$NETNS_CREATED" ] && ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    mkdir -p "$CASE_OUT/runtime"
    find "$WORK" -maxdepth 1 -type f \( -name '*.log' -o -name '*.out' -o -name '*.journal' -o -name '*actions.json' -o -name '*timeout.json' \) -exec cp {} "$CASE_OUT/runtime/" \;
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { # host port name
    for _ in $(seq 1 60); do
        (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
        sleep 0.5
    done
    fail "$3 did not open $1:$2"
}

# ---- store-ctl (fs backend, generation G1) --------------------------------

for helper in client journal ownership artifact process network storage config; do
    . "$SCRIPT_DIR/builder_$helper.sh"
done
start_builder_store_registry
start_builder_network
# ---- manifest config + diff templates ---------------------------------------
KEYLESS_CACHE=""   # no cache-ctl: empty endpoint falls back to store-as-cache
cat > "$WORK/manifest.yaml" <<EOF
manifest:
  key: ""
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 30s
cache:
  endpoint: "$KEYLESS_CACHE"
chunker:
  mode: cdc
  cdc: { min: 128KiB, avg: 512KiB, max: 1MiB }
crypto:
  chunk: aes
  manifest: aes
EOF
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
OVL="$WORK/overlay-1G.ext4"             # cold-boot overlay upper (template VMs)
truncate -s 1G "$OVL" && "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL"
BLDDIFF="$WORK/builder-2G.ext4"         # build VM writable disk (pull cache + steps delta + export scratch)
truncate -s 2G "$BLDDIFF" && "$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$BLDDIFF"

MK="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC="$("$BIN/e2b-key-ctl" gen-key)"
PORT="$(free_port)"
PROXY_PORT="$(free_port)"

FILES_STORAGE_YAML=""
