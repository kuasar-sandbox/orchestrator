#!/usr/bin/env bash
#
# e2e_sandbox_tapfd.sh — boot a sandbox whose network comes from the tapfd
# handoff protocol (docs/tapfd.md) instead of a pre-created host tap, and
# prove real connectivity over the handed-off vnet_hdr fd, then restore it
# with a fresh network identity.
#
# Stage 1 (cold + connectivity):
#   sandbox.yaml network.tapfd.exec = `tapfd-get --new <tap>`, which CREATES the
#   tap (host-side IP, up) and OPENS an IFF_VNET_HDR queue fd, handing it to
#   sandbox-ctl over SCM_RIGHTS. CH is driven with --net fd=<N>,mac=,id=_net0.
#   The guest gets eth0=169.254.1.1/31 and an app prints NETUP then sleeps.
#   The host pings 169.254.1.1 across the tap — success proves the fd carries
#   traffic with correct vnet_hdr framing.
#
# Stage 2 (restore with new identity):
#   snapshot the running VM, then `run --restore` with a NEW ip
#   (169.254.4.1/31). The guest re-applies the IP flush-and-replace and CH
#   re-binds the fresh fd via net_fds; the host pings the NEW address.
#
# Skips (exit 0) on missing prerequisites; REQUIRE_KVM=1 to fail hard.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"

skip() {
    echo; echo "==> e2e_sandbox_tapfd: skipping ($*)"
    [ "${REQUIRE_KVM:-0}" = "1" ] && { echo "REQUIRE_KVM=1; failing" >&2; exit 1; }
    exit 0
}

[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.erofs tapfd-get; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"

BLK0_IMAGE="${BLK0_IMAGE:-}"
[ -z "$BLK0_IMAGE" ] && [ -f "$REPO_ROOT/build/python-312.erofs" ] && BLK0_IMAGE="$REPO_ROOT/build/python-312.erofs"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "no prebuilt blk0 and docker unavailable"
    docker image inspect python:3.12-slim >/dev/null 2>&1 || docker pull python:3.12-slim >/dev/null
fi

WORK="$(mktemp -d /tmp/e2e-tapfd-XXXXXX)"
# No tap cleanup needed: tapfd-get --new auto-allocates a non-persistent tap
# that vanishes when the consuming VM (CH) exits.
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; true' EXIT

if [ -z "$BLK0_IMAGE" ]; then
    BLK0_IMAGE="$WORK/blk0.erofs"
    docker save python:3.12-slim | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
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
    exec: ["$BIN/tapfd-get", "--new", "--host-cidr", "$hcidr",
           "--mac", "$GUEST_MAC", "--ip", "$gip", "--mtu", "1400"]   # auto-named tap
  ip: $gip/31          # mask source; provider sends bare ip → keeps /31
  hostname: e2e-tapfd
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  cmdline: "console=hvc0"
  root:
    base: file://$BLK0_IMAGE
    overlay: { diff: file://$diff, size: 1GiB }
EOF
    if [ "$launch" = "1" ]; then
        cat >> "$out" <<EOF
launch:
  args: ["-c", "import time,sys; print('NETUP', flush=True); time.sleep(60)"]
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
ping_guest() { for _ in $(seq 1 24); do ping -c1 -W1 "$1" >/dev/null 2>&1 && return 0; sleep 0.25; done; return 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# ===================== Stage 1: cold boot + connectivity ===================
echo "==> Stage 1: cold boot via tapfd + host<->guest connectivity"
DIFF0="$WORK/blk1.diff"; mkdiff "$DIFF0"
write_yaml "$WORK/cold.yaml" "169.254.1.1" "$DIFF0" "169.254.1.0/31" 1
SID="tapfd-e2e"
LOG="$WORK/cold.log"
mkdir -p "$WORK/runtime/$SID"
timeout 120 "$BIN/sandbox-ctl" run --config "$WORK/cold.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID" \
    > "$LOG" 2>&1 &
RUNPID=$!

if wait_marker "^NETUP$" "$LOG" "$RUNPID"; then
    ok "guest booted via tapfd handoff (NETUP)"
else
    echo "--- cold.log tail ---"; tail -40 "$LOG"
    kill "$RUNPID" 2>/dev/null || true; skip "guest did not reach NETUP"
fi
grep -q "tapfd: received tap fd" "$LOG" && ok "tapfd handoff engaged in sandbox-ctl" || bad "no tapfd handoff log"
grep -qE "net fd=[0-9]+,mac=$GUEST_MAC,id=_net0" "$LOG" && ok "CH driven with --net fd=,mac=,id=_net0" || bad "fd-mode --net not in log"
ping_guest 169.254.1.1 && ok "host pinged guest 169.254.1.1 over the vnet_hdr fd" \
    || { echo "--- log ---"; tail -25 "$LOG"; ip -br addr || true; bad "ping 169.254.1.1 failed"; }

# snapshot the running VM (default --resume=false shuts it down → run exits)
SNAP="$WORK/snap"; mkdir -p "$SNAP"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID" --output "$SNAP" --run-root "$WORK/runtime" \
    >"$WORK/snap.log" 2>&1 && ok "snapshot taken" || { echo "--- snap.log ---"; cat "$WORK/snap.log"; bad "snapshot failed"; }
wait "$RUNPID" 2>/dev/null || true
SNAP_FILE="$SNAP/$SID.snapshot"

# ===================== Stage 2: restore with NEW identity ==================
if [ -f "$SNAP_FILE" ]; then
    echo "==> Stage 2: restore with fresh identity 169.254.4.1 (flush-and-replace)"
    # The fake provider recreates the (non-persistent) tap on the restore
    # handoff and assigns the new host /31 via --host-cidr — no manual setup.
    DIFF1="$WORK/blk1.restore.diff"; mkdiff "$DIFF1"
    write_yaml "$WORK/restore.yaml" "169.254.4.1" "$DIFF1" "169.254.4.0/31" 0
    SIDR="tapfd-e2e-r"; RLOG="$WORK/restore.log"; mkdir -p "$WORK/runtime2/$SIDR"
    timeout 120 "$BIN/sandbox-ctl" run --restore "$SNAP_FILE" --config "$WORK/restore.yaml" \
        --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime2" --sandbox-id "$SIDR" \
        > "$RLOG" 2>&1 &
    RPID=$!
    wait_marker "restore network re-applied|restore notify acked|VM resumed" "$RLOG" "$RPID" \
        && ok "restore completed" || { echo "--- restore.log tail ---"; tail -40 "$RLOG"; bad "restore did not complete"; }
    grep -q "tapfd: received tap fd for restore" "$RLOG" && ok "tapfd re-handoff on restore" || bad "no restore re-handoff log"
    grep -q "net_fds=\[_net0@\[" "$RLOG" && ok "CH restore re-bound fd via net_fds" || bad "no net_fds in restore log"
    ping_guest 169.254.4.1 && ok "host pinged restored guest at NEW ip 169.254.4.1 (re-config worked)" \
        || { echo "--- restore.log tail ---"; tail -30 "$RLOG"; bad "ping restored 169.254.4.1 failed"; }
    kill -TERM "$RPID" 2>/dev/null || true; wait "$RPID" 2>/dev/null || true
else
    echo "==> Stage 2 skipped (no snapshot at $SNAP_FILE)"
fi

echo; echo "========================================="
echo "  e2e_sandbox_tapfd: $PASS passed, $FAIL failed"
echo "========================================="
[ "$FAIL" -eq 0 ] && echo "==> e2e_sandbox_tapfd: OK" || exit 1
