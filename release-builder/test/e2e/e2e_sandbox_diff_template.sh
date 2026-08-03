#!/usr/bin/env bash
#
# e2e_sandbox_diff_template.sh — cold-start with NO explicit overlay.diff,
# seeded from a diff_template. Exercises the new path (docs/sandbox.md §3.1):
#   - boot.root.overlay.diff omitted → auto-default under the on-disk base dir
#   - boot.root.overlay.diff_template → sparse-copy a pre-formatted ext4 as the
#     writable upper (no mkfs at boot)
#   - cold-boot ext4-source validation is satisfied by the template
#   - --base-root places the auto-created diff on disk (here: a temp dir)
#
# Asserts the app actually runs (proves the templated upper mounted).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_diff_template: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

WORK="$(mktemp -d /tmp/e2e-difftmpl-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker missing"
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE" >/dev/null
    BLK0_IMAGE="$WORK/blk0.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

# Pre-formatted ext4 template (sparse). diff is seeded from this; no mkfs at boot.
TEMPLATE="$WORK/basic.ext4"
truncate -s 512M "$TEMPLATE"
mkfs.ext4 -q -F "$TEMPLATE"

mkdir -p "$WORK/run" "$WORK/base"

# No `diff:` — it auto-defaults under --base-root/<sid>/. diff_template seeds it.
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-dt }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff_template: file://$TEMPLATE
launch:
  args: ["-c", "import sys; print('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor)"]
  restart: never
EOF

echo "==> sandbox.yaml:"; sed 's/^/    /' "$WORK/sandbox.yaml"

LOG="$WORK/run.log"
set +e
timeout -k 10s 60 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/run" \
    --base-root "$WORK/base" \
    > "$LOG" 2>&1
EXIT=$?
set -e

echo "==> last 30 lines:"; tail -30 "$LOG"
if [ "$EXIT" = "124" ] && ! grep -q "PYBOOT-OK" "$LOG"; then
    echo "==> FAIL: timed out; inspect $LOG"
    exit 1
fi
if grep -qE "PYBOOT-OK [0-9]+" "$LOG"; then
    echo "==> PASS: booted on a template-seeded auto-default diff ($(grep -oE 'PYBOOT-OK [0-9]+' "$LOG" | head -1))"
    echo "==> e2e_sandbox_diff_template: OK"
else
    echo "==> FAIL: app did not run (template-seeded diff path)"
    exit 1
fi
