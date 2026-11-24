#!/usr/bin/env bash
# Process, configuration and request primitives for real cluster cases.
umask 077
SCRIPT_DIR="${E2E_LIB:?E2E_LIB must point to prepared helpers}/orchestrator"
. "$SCRIPT_DIR/proxy.sh"
. "$SCRIPT_DIR/build_fixture_units.sh"
: "${BIN:?BIN must point to prepared products}"
DOMAIN="${DOMAIN:-cluster.real.local}"
SWITCH="${SWITCH:-c${BASHPID}}"
: "${ORCHESTRATOR_BASE_IMAGE:?ORCHESTRATOR_BASE_IMAGE must name the prepared base image}"
E2E_IMAGE="$ORCHESTRATOR_BASE_IMAGE"
: "${ZOT_BIN:?ZOT_BIN must point to the prepared registry binary}"
CLUSTER_STORAGE_BACKEND="${CLUSTER_STORAGE_BACKEND:-foundationdb}"
CLUSTER_USE_PLACER="${CLUSTER_USE_PLACER:-1}"
SW_NETNS="${SW_NETNS:-e2ec_${BASHPID}}"

step() { echo "==> $*" >&2; }
# The shared Proxy helper may print its complete log on startup failure. Keep
# those bytes private for cluster fixtures, consistent with the failure hook.
collect_proxy_log() { printf 'Cluster Proxy did not become ready; private log retained until cleanup.\n' >&2; }

fail() {
    # Callers may hold HTTP bodies or live capabilities in their arguments.
    # Only the fixed failure location is public; the platform collector emits
    # a closed-vocabulary summary from this fixture's private observations.
    export CLUSTER_FAILURE_SOURCE="${BASH_SOURCE[1]##*/}" CLUSTER_FAILURE_LINE="${BASH_LINENO[0]}"
    printf 'FAIL: cluster fixture at %s:%s (private response details withheld)\n' \
        "${BASH_SOURCE[1]##*/}" "${BASH_LINENO[0]}" >&2
    exit 1
}


for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cluster-ctl run-cluster-ctl cloud-hypervisor; do
    [ -x "$BIN/$b" ] || fail "missing $BIN/$b"
done
[ -f "$BIN/vmlinux" ] || fail "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.bundle" ] || fail "missing $BIN/sandbox-runtime.bundle"
command -v python3 >/dev/null 2>&1 || fail "python3 not on PATH"
command -v curl >/dev/null 2>&1 || fail "curl not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || fail "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || fail "prepared registry is missing: $ZOT_BIN"
[ -x "$(dirname "$BIN")/lib/cluster-ctl/ld.so" ] || fail "prepared cluster native runtime is missing"
command -v ip >/dev/null 2>&1 || fail "iproute2 (ip) not found"
[ -d /run/systemd/system ] || fail "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || fail "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || fail "prepared base image is missing: $E2E_IMAGE"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi
export PATH="$BIN:$PATH"

: "${WORK:?WORK must be supplied by the common runner}"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && fail "$UNIT_DIR/$u exists; refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
# Full case IDs and node-local sandbox IDs exceed sun_path under CI's run root.
# Keep runtime sockets short and retain observations in the runner's output root.
CASE_OUT="${OUT:-$WORK}"
mkdir -p "$CASE_OUT"
WORK="$(mktemp -d /tmp/c-XXXXXX)"
mkdir -p "$WORK/r" "$WORK/l" "$WORK/s" "$WORK/z/d" "$WORK/g" "$WORK/br" "$WORK/bl" "$WORK/cr" "$WORK/cl"
declare -a PIDS=()
declare -a REGISTRY_PIDS=()
declare -a TAGS=()
SW_STARTED=""
NETNS_CREATED=""
CLUSTER_EXEC_PID=""
CLUSTER_FDB_PID=""
TEST_POSTGRES_CONTAINER=""
TEST_POSTGRES_NETWORK=""

cleanup() {
    local result=$?
    set +e
    rm -f "$WORK/create.credentials"
    if declare -F cluster_extra_cleanup >/dev/null; then cluster_extra_cleanup; fi
    stop_build_fixture_units "$WORK" 2>/dev/null
    stop_test_postgres 2>/dev/null || { [ "$result" != 0 ] || result=1; }
    [ -n "$CLUSTER_EXEC_PID" ] && kill "$CLUSTER_EXEC_PID" 2>/dev/null
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        p="${PIDS[$i]}"
        [ -n "$p" ] && kill "$p" 2>/dev/null
        [ -n "$p" ] && wait "$p" 2>/dev/null
    done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    [ -n "$NETNS_CREATED" ] && ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    mkdir -p "$CASE_OUT/runtime"
    # Raw logs/config/response bodies remain private; never upload capabilities.
    [ -n "${E2E_KEEP:-}" ] && step "kept runtime directory: $WORK" || rm -rf "$WORK"
    exit "$result"
}
trap cleanup EXIT


. "$SCRIPT_DIR/cluster_http.sh"
. "$SCRIPT_DIR/cluster_process.sh"
. "$SCRIPT_DIR/postgres_fixture.sh"
