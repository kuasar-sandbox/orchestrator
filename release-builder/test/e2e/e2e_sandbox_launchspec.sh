#!/usr/bin/env bash
#
# e2e_sandbox_launchspec.sh — cold-start a sandbox with a populated launch
# spec (mounts + files + init + non-root user) and verify each piece took
# effect inside the guest. Exercises docs/sandbox-init.md §3.1-§3.2:
#
#   - mounts: tmpfs /tmp + empty (volume) /var/log
#   - files:  /etc/resolv.conf injected (tmpfs+bind, memory-only)
#   - init:   one-shot command writes a marker before the app runs
#   - user:   the app runs as a non-root identity (resolved guest-side)
#
# The app is /bin/sh emitting markers the assertions below grep for. Same
# prerequisite / skip semantics as e2e_sandbox_cold.sh.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_launchspec: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible to current user"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX env var)"

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

WORK="$(mktemp -d /tmp/e2e-launchspec-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"; [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null; true' EXIT

BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE=path/to/prebuilt.erofs"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH (apt install e2fsprogs)"
mkfs.ext4 -q -F "$DIFF_FILE"

# The verification script is itself injected as a file (block scalar avoids
# YAML quoting pitfalls and exercises injecting an executable). It runs as the
# non-root launch.user and emits one uniquely-shaped marker per verified
# feature (so assertions don't match the launch.args echo in sandbox-ctl logs).
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:
    cpu: 1
    memory: 512MiB
  allocatable:
    cpu: 1
    memory: 512MiB
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-launchspec
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
mounts:
  - { target: /tmp,     type: tmpfs, options: "nosuid,nodev,mode=1777" }
  - { target: /var/log, type: empty }
files:
  - path: /etc/resolv.conf
    mode: "0644"
    content: |
      nameserver 169.254.169.253
  - path: /verify.sh
    mode: "0755"
    content: |
      #!/bin/sh
      echo "LS-WHOAMI \$(id -u):\$(id -g)"
      echo "LS-RESOLV \$(head -1 /etc/resolv.conf)"
      grep -q " /tmp tmpfs " /proc/self/mounts && echo LS-TMPFS-OK
      grep -qE " /var/log (ext4|overlay) " /proc/self/mounts && echo LS-VOLUME-OK
      [ -f /tmp/ls-init-ran ] && echo "LS-INIT-OK \$(cat /tmp/ls-init-ran)"
      echo LS-DONE
init:
  - exec: /bin/sh
    args: ["-c", "echo provisioned > /tmp/ls-init-ran"]
launch:
  exec: /bin/sh
  args: ["/verify.sh"]
  user: "nobody"
  stop_signal: SIGTERM
  stop_grace_period: 5s
  restart: never
EOF

echo "==> sandbox.yaml:"
sed 's/^/    /' "$WORK/sandbox.yaml"

LOG="$WORK/run.log"
echo "==> launching sandbox-ctl run (timeout 60s)"
set +e
timeout -k 10s 60 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    > "$LOG" 2>&1
EXIT=$?
set -e

echo "==> last 50 lines of log:"
tail -50 "$LOG"

if [ "$EXIT" = "124" ] && ! grep -q "LS-DONE" "$LOG"; then
    echo "==> FAIL: sandbox run timed out at 60s — guest didn't reach app. Inspect $LOG"
    exit 1
fi

echo
echo "==> assertions:"
fail=0
assert() { # marker, human description
    if grep -qE "$1" "$LOG"; then
        echo "    PASS: $2 ($(grep -oE "$1" "$LOG" | head -1))"
    else
        echo "    FAIL: $2 — marker /$1/ not found"
        fail=1
    fi
}

assert '^LS-DONE$'                      "app ran to completion"
assert '^LS-WHOAMI 65534:65534$'        "ran as non-root user nobody (uid:gid 65534)"
assert '^LS-RESOLV nameserver 169\.254\.169\.253$' "injected /etc/resolv.conf visible to app"
assert '^LS-TMPFS-OK$'                  "tmpfs mount at /tmp present"
assert '^LS-VOLUME-OK$'                 "empty volume mount at /var/log present"
assert '^LS-INIT-OK provisioned$'       "init command ran before app (marker readable)"

if [ "$fail" != "0" ]; then
    echo "==> FAIL: e2e_sandbox_launchspec (exit=$EXIT)"
    exit 1
fi
echo "==> e2e_sandbox_launchspec: OK"
