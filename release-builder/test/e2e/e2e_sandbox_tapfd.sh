#!/usr/bin/env bash
#
# e2e_sandbox_tapfd.sh — boot a sandbox whose network comes from the tapfd
# handoff protocol (docs/tapfd.md) instead of a pre-created host tap, and
# prove real connectivity over the handed-off vnet_hdr fd, then restore it
# with a fresh network identity.
#
# Stage 1 (cold + connectivity):
#   sandbox.yaml network.tapfd.exec = `connector-ctl tapfd get --new <tap>`, which CREATES the
#   tap (host-side IP, up) and OPENS an IFF_VNET_HDR queue fd, handing it to
#   sandbox-ctl over SCM_RIGHTS. CH is driven with --net fd=<N>,mac=,id=_net0.
#   The guest gets a per-run link-local /31 and an app prints NETUP then sleeps.
#   The host binds its ping to that per-run tap and reaches the guest — success
#   proves the handed-off fd carries traffic with correct vnet_hdr framing.
#
# Stage 2 (restore with new identity):
#   snapshot the running VM, then `run --restore` with a NEW per-run /31.
#   The guest re-applies the IP flush-and-replace and CH
#   re-binds the fresh fd via net_fds; the host pings the NEW address.
#
# Skips (exit 0) on missing prerequisites; REQUIRE_KVM=1 to fail hard.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
TAP_NAME="$(printf 'etf%x' "$$")"

skip() {
    echo; echo "==> e2e_sandbox_tapfd: skipping ($*)"
    [ "${REQUIRE_KVM:-0}" = "1" ] && { echo "REQUIRE_KVM=1; failing" >&2; exit 1; }
    exit 0
}

[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle connector-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
command -v ip >/dev/null 2>&1 || skip "ip not on PATH"

BLK0_IMAGE="${BLK0_IMAGE:-}"
[ -z "$BLK0_IMAGE" ] && [ -f "$REPO_ROOT/build/python-312.erofs" ] && BLK0_IMAGE="$REPO_ROOT/build/python-312.erofs"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

# The cold and restore ranges are intentionally disjoint so restore always
# changes the guest identity. Start from a PID-derived slot, but scan the host
# state so PID reuse cannot select a /31 still owned by an interrupted run.
declare -A HOST_IPV4_ADDRESSES=() HOST_IPV4_ROUTES=()
while read -r address; do
    HOST_IPV4_ADDRESSES["${address%/*}"]=1
done < <(ip -o -4 addr show | awk '{print $4}')
while read -r destination _; do
    [[ "$destination" == */* ]] && HOST_IPV4_ROUTES["$destination"]=1
done < <(ip -4 route show table all type unicast)

network_in_use() { # <cidr> <host_ip> <guest_ip>
    [ -n "${HOST_IPV4_ROUTES[$1]:-}" ] || \
        [ -n "${HOST_IPV4_ADDRESSES[$2]:-}" ] || \
        [ -n "${HOST_IPV4_ADDRESSES[$3]:-}" ]
}

allocate_networks() {
    local start=$(( $$ % 8192 )) offset slot block host_byte
    local cold_host_ip cold_guest_ip cold_cidr restore_host_ip restore_guest_ip restore_cidr
    for ((offset = 0; offset < 8192; offset++)); do
        slot=$(( (start + offset) % 8192 ))
        block=$(( slot / 128 ))
        host_byte=$(( (slot % 128) * 2 ))
        cold_host_ip="169.254.$((64 + block)).$host_byte"
        cold_guest_ip="169.254.$((64 + block)).$((host_byte + 1))"
        cold_cidr="$cold_host_ip/31"
        restore_host_ip="169.254.$((128 + block)).$host_byte"
        restore_guest_ip="169.254.$((128 + block)).$((host_byte + 1))"
        restore_cidr="$restore_host_ip/31"
        if ! network_in_use "$cold_cidr" "$cold_host_ip" "$cold_guest_ip" && \
            ! network_in_use "$restore_cidr" "$restore_host_ip" "$restore_guest_ip"; then
            COLD_HOST_CIDR="$cold_cidr"
            COLD_GUEST_IP="$cold_guest_ip"
            RESTORE_HOST_CIDR="$restore_cidr"
            RESTORE_GUEST_IP="$restore_guest_ip"
            return 0
        fi
    done
    echo "e2e_sandbox_tapfd: no unused per-run network pair is available" >&2
    exit 1
}
allocate_networks

if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "no prebuilt blk0 and docker unavailable"
    docker image inspect python:3.12-slim >/dev/null 2>&1 || docker pull python:3.12-slim >/dev/null
fi

WORK="$(mktemp -d /tmp/e2e-tapfd-XXXXXX)"
# No tap cleanup needed: connector-ctl creates a non-persistent, per-run tap that
# vanishes when the consuming VM (CH) exits.
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; true' EXIT

if [ -z "$BLK0_IMAGE" ]; then
    BLK0_IMAGE="$WORK/blk0.img"
    docker save python:3.12-slim | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"
echo "==> blk0: $BLK0_IMAGE"

GUEST_MAC="02:00:00:00:80:01"
mkdiff() { truncate -s 1G "$1"; mkfs.ext4 -q -F "$1"; }

# write_yaml <out> <guest_ip> <diff> <host_cidr> <with_launch:0|1>
write_yaml() {
    local out="$1" gip="$2" diff="$3" hcidr="$4" launch="$5"
    cat > "$out" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tapfd:
    exec: ["$BIN/connector-ctl", "tapfd", "get", "--new", "--host-cidr", "$hcidr",
           "--mac", "$GUEST_MAC", "--ip", "$gip", "$TAP_NAME"]
  ip: $gip/31          # mask source; provider sends bare ip → keeps /31
  mtu: 1400            # guest MTU is sandbox config, not tapfd metadata
  hostname: e2e-tapfd
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root:
    base: $BLK0_REF
    overlay: { diff: file://$diff, size: 1GiB }
EOF
    if [ "$launch" = "1" ]; then
        cat >> "$out" <<EOF
launch:
  args: ["-c", "import time; print('MTU='+open('/sys/class/net/eth0/mtu').read().strip(), flush=True); print('NETUP', flush=True); time.sleep(60)"]
  restart: never
EOF
    fi
}

wait_marker() { # <regex> <log> <pid>
    for _ in $(seq 1 600); do
        grep -qE "$1" "$2" 2>/dev/null && return 0
        kill -0 "$3" 2>/dev/null || return 1
        sleep 0.05
    done
    return 1
}
ping_guest() { for _ in $(seq 1 24); do ping -I "$TAP_NAME" -c1 -W1 "$1" >/dev/null 2>&1 && return 0; sleep 0.25; done; return 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# ===================== Stage 1: cold boot + connectivity ===================
echo "==> Stage 1: cold boot via tapfd + host<->guest connectivity ($COLD_HOST_CIDR)"
DIFF0="$WORK/blk1.diff"; mkdiff "$DIFF0"
write_yaml "$WORK/cold.yaml" "$COLD_GUEST_IP" "$DIFF0" "$COLD_HOST_CIDR" 1
SID="tapfd-e2e"
LOG="$WORK/cold.log"
mkdir -p "$WORK/runtime/$SID"
timeout -k 10s 120 "$BIN/sandbox-ctl" run --config "$WORK/cold.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID" \
    > "$LOG" 2>&1 &
RUNPID=$!

if wait_marker "^NETUP$" "$LOG" "$RUNPID"; then
    ok "guest booted via tapfd handoff (NETUP)"
else
    echo "--- cold.log tail ---"; tail -40 "$LOG"
    kill "$RUNPID" 2>/dev/null || true
    echo "==> FAIL: guest did not reach NETUP"
    exit 1
fi
grep -q "tapfd: received tap fd" "$LOG" && ok "tapfd handoff engaged in sandbox-ctl" || bad "no tapfd handoff log"
grep -qE "net fd=[0-9]+,mac=$GUEST_MAC,id=_net0" "$LOG" && ok "CH driven with --net fd=,mac=,id=_net0" || bad "fd-mode --net not in log"
grep -q "^MTU=1400$" "$LOG" && ok "guest MTU came from sandbox network config" || bad "guest MTU was not 1400"
ping_guest "$COLD_GUEST_IP" && ok "host pinged guest $COLD_GUEST_IP through $TAP_NAME over the vnet_hdr fd" \
    || { echo "--- log ---"; tail -25 "$LOG"; ip -br addr || true; bad "ping $COLD_GUEST_IP failed"; }

# snapshot the running VM (default --resume=false shuts it down → run exits)
SNAP="$WORK/snap"; mkdir -p "$SNAP"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID" --output "$SNAP" --run-root "$WORK/runtime" \
    >"$WORK/snap.log" 2>&1 && ok "snapshot taken" || { echo "--- snap.log ---"; cat "$WORK/snap.log"; bad "snapshot failed"; }
wait "$RUNPID" 2>/dev/null || true
SNAP_FILE="$SNAP/$SID.snapshot"

# ===================== Stage 2: restore with NEW identity ==================
if [ -f "$SNAP_FILE" ]; then
    echo "==> Stage 2: restore with fresh identity $RESTORE_GUEST_IP (flush-and-replace)"
    # The fake provider recreates the named, non-persistent tap on the restore
    # handoff and assigns the new host /31 via --host-cidr — no manual setup.
    DIFF1="$WORK/blk1.restore.diff"; mkdiff "$DIFF1"
    write_yaml "$WORK/restore.yaml" "$RESTORE_GUEST_IP" "$DIFF1" "$RESTORE_HOST_CIDR" 0
    SIDR="tapfd-e2e-r"; RLOG="$WORK/restore.log"; mkdir -p "$WORK/runtime2/$SIDR"
    timeout -k 10s 120 "$BIN/sandbox-ctl" run --restore "$SNAP_FILE" --config "$WORK/restore.yaml" \
        --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime2" --sandbox-id "$SIDR" \
        > "$RLOG" 2>&1 &
    RPID=$!
    wait_marker "restore network re-applied|restore notify acked|VM resumed" "$RLOG" "$RPID" \
        && ok "restore completed" || { echo "--- restore.log tail ---"; tail -40 "$RLOG"; bad "restore did not complete"; }
    grep -q "tapfd: received tap fd for restore" "$RLOG" && ok "tapfd re-handoff on restore" || bad "no restore re-handoff log"
    grep -q "net_fds=\[_net0@\[" "$RLOG" && ok "CH restore re-bound fd via net_fds" || bad "no net_fds in restore log"
    ping_guest "$RESTORE_GUEST_IP" && ok "host pinged restored guest at NEW ip $RESTORE_GUEST_IP through $TAP_NAME (re-config worked)" \
        || { echo "--- restore.log tail ---"; tail -30 "$RLOG"; bad "ping restored $RESTORE_GUEST_IP failed"; }
    kill -TERM "$RPID" 2>/dev/null || true; wait "$RPID" 2>/dev/null || true
else
    echo "==> Stage 2 skipped (no snapshot at $SNAP_FILE)"
fi

echo; echo "========================================="
echo "  e2e_sandbox_tapfd: $PASS passed, $FAIL failed"
echo "========================================="
[ "$FAIL" -eq 0 ] && echo "==> e2e_sandbox_tapfd: OK" || exit 1
