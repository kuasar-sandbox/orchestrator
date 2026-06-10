#!/usr/bin/env bash
#
# e2e_sandbox_placeholder.sh — boot a no-exec "placeholder" sandbox and verify
# the exec-driven anchor model (launch.placeholder, docs/sandbox.md launch §).
#
# A placeholder sandbox runs NO external program: sandbox-init forks the app
# child, which does its namespace/cgroup/stdio setup and then waits for a stop
# signal instead of execve. It is the sandbox's "anchor" — you drive everything
# via `sandbox-ctl exec`. Because killing the anchor would otherwise reboot the
# sandbox, a placeholder is forced to restart=always, so an exec session that
# kills PID 1 restarts the anchor in place rather than tearing the sandbox down.
#
# This exercises:
#   1. launch.placeholder boot (no launch.exec, image Cmd ignored)
#   2. the guest handshake + phase-2 fork accept a no-exec spec (no "kill init")
#   3. `sandbox-ctl exec` into the placeholder sandbox actually runs commands
#   4. PID 1 in the app ns is the placeholder (exec-child-placeholder argv)
#   5. SIGTERM the anchor from an exec session → in-place restart, NOT reboot
#   6. graceful stop (SIGTERM the run process) exits 0
#
# Prerequisites (checked; missing → skip with a message, exit 0):
#   /dev/kvm rw · bin/{cloud-hypervisor,sandbox-ctl,sandbox-init,
#   sandbox-runtime.erofs,flatten-ctl} · $VMLINUX · docker (or BLK0_IMAGE=) ·
#   mkfs.ext4 · root (tap/cgroup/vsock). Set REQUIRE_KVM=1 to fail hard.
#
# Any rootfs with /bin/sh works; default base is busybox (tiny, fast). Override
# IMAGE= or BLK0_IMAGE=path/to/prebuilt.erofs.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-busybox:latest}"
TAP_NAME="${TAP_NAME:-sb-tap0}"
SID="${SID:-ph1}"

skip() {
    echo
    echo "==> e2e_sandbox_placeholder: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible to current user"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.erofs flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build' and 'make cloud-hypervisor'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX)"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH (apt install e2fsprogs)"

# Self-elevate: tap creation, cgroup writes, vsock and (rootful) flatten all
# need root. After the cheap prereq checks so skips stay fast.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

WORK="$(mktemp -d /tmp/e2e-placeholder-XXXXXX)"
RUNROOT="$WORK/runtime"; mkdir -p "$RUNROOT"
RUNLOG="$WORK/run.log"
RUNPID=""
TAP_CREATED=0

cleanup() {
    set +e
    [ -n "$RUNPID" ] && kill -0 "$RUNPID" 2>/dev/null && kill -TERM "$RUNPID" 2>/dev/null
    sleep 1
    [ -n "$RUNPID" ] && kill -0 "$RUNPID" 2>/dev/null && kill -KILL "$RUNPID" 2>/dev/null
    pkill -f "cloud-hypervisor.*$SID" 2>/dev/null
    [ "$TAP_CREATED" = 1 ] && ip link del "$TAP_NAME" 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

# ---- TAP ------------------------------------------------------------------
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED=1
fi

# ---- blk0 base (any /bin/sh rootfs) ---------------------------------------
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE=path/to/prebuilt.erofs"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.erofs"
    echo "==> docker save $IMAGE | flatten-ctl export"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
    echo "==> blk0 erofs ready ($(du -h "$BLK0_IMAGE" | cut -f1))"
fi

# ---- blk1 overlay diff (fresh ext4 upper) ---------------------------------
DIFF="$RUNROOT/blk1.diff"
truncate -s 1G "$DIFF"
mkfs.ext4 -q -F "$DIFF"

# ---- placeholder config ---------------------------------------------------
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-placeholder
boot:
  kernel:  file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: file://$BLK0_IMAGE
    overlay:
      diff: file://$DIFF
      size: 1GiB
launch:
  placeholder: true        # no exec — anchor app; forced restart=always
EOF
echo "==> sandbox.yaml:"; sed 's/^/    /' "$WORK/sandbox.yaml"

# ---- boot (background; a placeholder never exits on its own) ---------------
echo "==> launching placeholder sandbox (background)"
timeout 120 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" --sandbox-id "$SID" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUNROOT" \
    > "$RUNLOG" 2>&1 &
RUNPID=$!

# Readiness: ctl.sock is created early (host listener), so it is NOT a boot
# signal — a working exec is. Poll exec until the guest answers.
exec1() { "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$RUNROOT" "$@"; }
echo "==> waiting for the sandbox to become exec-ready"
READY=0
for _ in $(seq 1 90); do
    if timeout 6 "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$RUNROOT" \
         -- /bin/sh -c 'echo READY' >"$WORK/ready.out" 2>/dev/null && grep -q READY "$WORK/ready.out"; then
        READY=1; break
    fi
    kill -0 "$RUNPID" 2>/dev/null || { echo "==> FAIL: run exited during boot"; tail -60 "$RUNLOG"; exit 1; }
    sleep 1
done
[ "$READY" = 1 ] || { echo "==> FAIL: sandbox never became exec-ready"; tail -60 "$RUNLOG"; exit 1; }
echo "==> PASS: booted no-exec placeholder, sandbox is exec-ready"
grep -q "placeholder app (no exec)" "$RUNLOG" \
    && echo "==> PASS: guest log confirms the placeholder is waiting" \
    || echo "==> INFO: placeholder console line not captured (lag); proven via exec below"

# ---- 1. exec a command inside the placeholder sandbox ---------------------
echo "==> [1] exec a command"
exec1 -- /bin/sh -c 'echo EXEC-OK-1; id; uname -sr' >"$WORK/x1.out" 2>&1 || true
sed 's/^/    /' "$WORK/x1.out"
grep -q EXEC-OK-1 "$WORK/x1.out" || { echo "==> FAIL: exec produced no marker"; tail -60 "$RUNLOG"; exit 1; }
echo "==> PASS: exec ran inside the placeholder sandbox"

# ---- 2. PID 1 in the app ns is the placeholder ----------------------------
echo "==> [2] verify PID 1 is the placeholder"
exec1 -- cat /proc/1/cmdline >"$WORK/pid1" 2>/dev/null || true
if grep -aq "exec-child-placeholder" "$WORK/pid1"; then
    echo "==> PASS: PID 1 cmdline = $(tr '\0' ' ' < "$WORK/pid1")"
else
    echo "==> FAIL: PID 1 is not the placeholder: $(tr '\0' ' ' < "$WORK/pid1")"; exit 1
fi

# ---- 3. kill the anchor (PID 1) → in-place restart, NOT reboot ------------
echo "==> [3] SIGTERM PID 1 from an exec session → expect restart, not reboot"
exec1 -- /bin/sh -c 'kill -TERM 1' >/dev/null 2>&1 || true   # ns teardown may kill the exec cmd; ignore
sleep 3
kill -0 "$RUNPID" 2>/dev/null \
    || { echo "==> FAIL: run exited — the kill rebooted the sandbox"; tail -80 "$RUNLOG"; exit 1; }
echo "==> PASS: run process still alive (sandbox did NOT reboot)"
exec1 -- /bin/sh -c 'echo EXEC-OK-AFTER-RESTART' >"$WORK/x2.out" 2>&1 || true
grep -q EXEC-OK-AFTER-RESTART "$WORK/x2.out" \
    || { echo "==> FAIL: exec failed after restart"; tail -80 "$RUNLOG"; exit 1; }
echo "==> PASS: exec works again after the anchor restarted in place"
grep -q "restarting in" "$RUNLOG" \
    && echo "==> PASS: guest log shows in-place restart" \
    || echo "==> INFO: 'restarting in' console line not captured (lag)"
if grep -q "rebooting" "$RUNLOG"; then
    echo "==> FAIL: guest log shows a reboot — kill must restart, not reboot"; exit 1
fi

# ---- 4. graceful stop -----------------------------------------------------
echo "==> [4] graceful stop (SIGTERM the run process)"
kill -TERM "$RUNPID" 2>/dev/null || true
set +e; wait "$RUNPID"; RC=$?; set -e
RUNPID=""
echo "==> run exit code: $RC"
[ "$RC" = 0 ] || { echo "==> FAIL: graceful stop exit=$RC (want 0)"; tail -40 "$RUNLOG"; exit 1; }

echo
echo "==> guest restart/reboot/placeholder log lines:"
grep -nE "placeholder app|restarting in|rebooting|app exited" "$RUNLOG" | sed 's/^/    /' || true
echo
echo "==> e2e_sandbox_placeholder: OK"
