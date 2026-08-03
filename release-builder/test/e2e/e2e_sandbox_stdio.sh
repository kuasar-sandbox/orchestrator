#!/usr/bin/env bash
#
# e2e_sandbox_stdio.sh — verify the sandbox-ctl stdio model:
#
#   case 1: --stdout-to FILE   → guest prints land in FILE
#   case 2: --stdout=false     → guest prints discarded (file empty)
#   case 3: default            → guest prints land in sandbox-ctl stdout
#
# Each case runs an exit-fast python launch (prints a unique marker, then
# exits) so we don't need to manage a long-running sandbox.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"

skip() {
    echo
    echo "==> e2e_sandbox_stdio: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    [ "$(id -u)" -eq 0 ] || { echo "$0: must run as root to create $TAP_NAME" >&2; exit 1; }
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi
[ "$(id -u)" -eq 0 ] || skip "must run as root"

WORK="$(mktemp -d /tmp/e2e-stdio-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

IMAGE="${IMAGE:-python:3.12-slim}"
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

mkdir -p "$WORK/runtime"

write_yaml() {
    local out="$1"
    local diff="$2"
    truncate -s 1G "$diff"
    mkfs.ext4 -q -F "$diff"
    cat > "$out" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-stdio
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$diff
      size: 1GiB
launch:
  args: ["-c", "print('STDIO-MARKER-12345', flush=True)"]
  restart: never
EOF
}

# ---- case 1: --stdout-to <file> -------------------------------------------
echo "==> case 1: --stdout-to FILE"
write_yaml "$WORK/c1.yaml" "$WORK/runtime/c1.diff"
OUT1="$WORK/c1.stdout"
LOG1="$WORK/c1.sandbox.log"
timeout -k 10s 30 "$BIN/sandbox-ctl" run \
    --config "$WORK/c1.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "stdio-c1-$$" \
    --stdout-to "$OUT1" \
    > "$LOG1" 2>&1 || true

if grep -q "STDIO-MARKER-12345" "$OUT1" 2>/dev/null; then
    echo "    PASS: marker landed in --stdout-to file"
else
    echo "    FAIL: marker NOT in $OUT1"
    echo "    --- file contents ---"; cat "$OUT1" 2>/dev/null | head -10
    echo "    --- sandbox log ---"; tail -20 "$LOG1"
    exit 1
fi
if grep -q "STDIO-MARKER-12345" "$LOG1" 2>/dev/null; then
    echo "    FAIL: marker leaked to sandbox-ctl stdout (should have been redirected)"
    exit 1
fi

# ---- case 2: --stdout=false (drop) ----------------------------------------
echo "==> case 2: --stdout=false"
write_yaml "$WORK/c2.yaml" "$WORK/runtime/c2.diff"
OUT2="$WORK/c2.stdout"
timeout -k 10s 30 "$BIN/sandbox-ctl" run \
    --config "$WORK/c2.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "stdio-c2-$$" \
    --stdout=false \
    > "$OUT2" 2>&1 || true

if grep -q "STDIO-MARKER-12345" "$OUT2" 2>/dev/null; then
    echo "    FAIL: marker present in stdout despite --stdout=false"
    head -20 "$OUT2"
    exit 1
fi
echo "    PASS: marker absent (silenced)"

# ---- case 3: default (inherit sandbox-ctl stdout) ------------------------
echo "==> case 3: default stdout"
write_yaml "$WORK/c3.yaml" "$WORK/runtime/c3.diff"
OUT3="$WORK/c3.stdout"
timeout -k 10s 30 "$BIN/sandbox-ctl" run \
    --config "$WORK/c3.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "stdio-c3-$$" \
    > "$OUT3" 2>&1 || true

if grep -q "STDIO-MARKER-12345" "$OUT3" 2>/dev/null; then
    echo "    PASS: marker in inherited stdout"
else
    echo "    FAIL: marker missing from default stdout"
    tail -30 "$OUT3"
    exit 1
fi

echo
echo "==> e2e_sandbox_stdio: OK"
