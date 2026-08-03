#!/usr/bin/env bash
#
# e2e_sandbox_placeholder.sh — boot a no-exec "placeholder" sandbox without a
# virtio-net device and verify the exec-driven anchor model (launch.placeholder,
# docs/sandbox.md launch §).
#
# A placeholder sandbox runs NO external program: sandbox-init forks the app
# child, which does its namespace/cgroup/stdio setup and then waits for a stop
# signal instead of execve. It is the sandbox's "anchor" — you drive everything
# via `sandbox-ctl exec`. Because killing the anchor would otherwise reboot the
# sandbox, a placeholder is forced to restart=always, so an exec session that
# kills PID 1 restarts the anchor in place rather than tearing the sandbox down.
#
# This exercises:
#   1. launch.placeholder boot without a virtio-net device
#   2. the guest handshake + phase-2 fork accept a no-exec spec (no "kill init")
#   3. no CH --net, guest has only lo, and vsock-backed exec still works
#   4. PID 1 in the app ns is the placeholder (exec-child-placeholder argv)
#   5. SIGTERM the anchor from an exec session → in-place restart, NOT reboot
#   6. graceful stop (SIGTERM the run process) exits 0
#
# Prerequisites (checked; missing → skip with a message, exit 0):
#   /dev/kvm rw · bin/{cloud-hypervisor,sandbox-ctl,sandbox-init,
#   sandbox-runtime.bundle,flatten-ctl} · $VMLINUX · docker (or BLK0_IMAGE=) ·
#   mkfs.ext4 · root (cgroup/userfaultfd/rootful flatten). Set REQUIRE_KVM=1 to
#   fail hard.
#
# Any rootfs with /bin/sh works; default base is busybox (tiny, fast). Override
# IMAGE= or BLK0_IMAGE=path/to/prebuilt.erofs.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-busybox:latest}"
SID="${SID:-ph1}"
source "$REPO_ROOT/test/e2e/readiness_helpers.sh"

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
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH (needed to probe ctl.sock)"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX)"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH (apt install e2fsprogs)"

# Self-elevate: cgroup writes, userfaultfd and rootful flatten need root. After
# the cheap prereq checks so skips stay fast.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

WORK="$(mktemp -d /tmp/e2e-placeholder-XXXXXX)"
RUNROOT="$WORK/runtime"; mkdir -p "$RUNROOT"
RUNLOG="$WORK/run.log"
RUNPID=""
CTL_PID=""

cleanup() {
    set +e
    readiness_stop_watchdog
    if [ -n "$CTL_PID" ] && kill -0 "$CTL_PID" 2>/dev/null; then
        kill -TERM "$CTL_PID" 2>/dev/null
    elif [ -n "$RUNPID" ] && kill -0 "$RUNPID" 2>/dev/null; then
        kill -TERM "$RUNPID" 2>/dev/null
    fi
    sleep 1
    [ -n "$RUNPID" ] && readiness_kill_session KILL "$RUNPID"
    pkill -f "cloud-hypervisor.*$SID" 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

# ---- blk0 base (any /bin/sh rootfs) ---------------------------------------
BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE=path/to/prebuilt.erofs"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    echo "==> docker save $IMAGE | flatten-ctl export"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
    echo "==> blk0 erofs ready ($(du -h "$BLK0_IMAGE" | cut -f1))"
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

# ---- blk1 overlay diff (fresh ext4 upper) ---------------------------------
DIFF="$RUNROOT/blk1.diff"
truncate -s 1G "$DIFF"
mkfs.ext4 -q -F "$DIFF"

# ---- placeholder config ---------------------------------------------------
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
boot:
  kernel:  file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF
      size: 1GiB
launch:
  placeholder: true        # no exec — anchor app; forced restart=always
EOF
echo "==> sandbox.yaml:"; sed 's/^/    /' "$WORK/sandbox.yaml"

# ---- boot (background; a placeholder never exits on its own) ---------------
echo "==> launching placeholder sandbox (background)"
readiness_begin_capture "$WORK/placeholder.ready"
PLACEHOLDER_READER_PID=$READY_READER_PID
readiness_exec_in_new_session "$BIN/sandbox-ctl" run \
    --ready-fd="$READY_WRITE_FD" \
    --config "$WORK/sandbox.yaml" --sandbox-id "$SID" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUNROOT" \
    > "$RUNLOG" 2>&1 &
RUNPID=$!
readiness_close_parent_writer
readiness_start_watchdog "$RUNPID" 120 10
# sandbox-ctl is now the direct child. Signalling this PID exercises its own
# graceful CH shutdown path without a timeout wrapper duplicating the signal.
CTL_PID=$RUNPID

exec1() { "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$RUNROOT" "$@"; }
readiness_wait_event "$WORK/placeholder.ready" 1 control_ready "$RUNPID" \
    || { tail -60 "$RUNLOG"; exit 1; }
readiness_connect_ctl "$RUNROOT/$SID/ctl.sock" \
    || { echo "==> FAIL: ctl.sock was not connectable at control_ready"; exit 1; }
readiness_wait_event "$WORK/placeholder.ready" 2 ready "$RUNPID" \
    || { tail -60 "$RUNLOG"; exit 1; }
exec1 -- /bin/true \
    || { echo "==> FAIL: immediate exec after placeholder ready failed"; tail -60 "$RUNLOG"; exit 1; }
readiness_assert_wire "$WORK/placeholder.ready" "$PLACEHOLDER_READER_PID" $'control_ready\nready\n' \
    || { echo "==> FAIL: placeholder readiness wire was not exact"; exit 1; }
echo "==> PASS: placeholder emitted exact readiness wire and immediate exec succeeded"

# The no-network contract has two independent observable sides: CH receives no
# virtio-net argument, while the guest retains loopback and the vsock control
# plane used by this exec check.
CH_ARGS="$(grep -m1 '\[sandbox-ctl\] CH args:' "$RUNLOG" || true)"
[ -n "$CH_ARGS" ] || { echo "==> FAIL: CH args line missing"; tail -60 "$RUNLOG"; exit 1; }
if grep -Eq '(^|[[:space:]])--net([[:space:]]|$)' <<<"$CH_ARGS"; then
    echo "==> FAIL: no-network sandbox received --net: $CH_ARGS"
    exit 1
fi
if ! exec1 -- /bin/sh -c \
    'for path in /sys/class/net/*; do printf "%s\n" "${path##*/}"; done' \
    >"$WORK/netdevs" 2>"$WORK/netdevs.err"; then
    echo "==> FAIL: could not inspect guest network devices"
    sed 's/^/    /' "$WORK/netdevs.err"
    tail -60 "$RUNLOG"
    exit 1
fi
NETDEVS="$(tr -d '\r' <"$WORK/netdevs")"
[ "$NETDEVS" = "lo" ] || {
    echo "==> FAIL: expected guest network devices to be exactly 'lo', got:"
    sed 's/^/    /' "$WORK/netdevs"
    exit 1
}
echo "==> PASS: CH omitted --net, guest exposes only lo, and vsock exec works"

# The trailing digest ZIP must not change the offset-zero EROFS/PMEM contract.
# Verify the runtime remains mounted with fs-DAX inside the switched root.
if ! exec1 -- cat /proc/mounts >"$WORK/mounts" 2>"$WORK/mounts.err"; then
    echo "==> FAIL: could not inspect guest mounts"
    sed 's/^/    /' "$WORK/mounts.err"
    exit 1
fi
if ! grep -Eq '^/dev/root /opt/sandbox-runtime erofs .*dax=always' "$WORK/mounts"; then
    echo "==> FAIL: runtime EROFS is not mounted with dax=always"
    grep -E ' /opt/sandbox-runtime ' "$WORK/mounts" | sed 's/^/    /' || true
    exit 1
fi
echo "==> PASS: runtime bundle remains offset-zero EROFS mounted with dax=always"

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
readiness_assert_wire "$WORK/placeholder.ready" "$PLACEHOLDER_READER_PID" $'control_ready\nready\n' \
    || { echo "==> FAIL: app restart changed the one-shot readiness stream"; exit 1; }
echo "==> PASS: in-place app restart emitted no second ready"
grep -q "restarting in" "$RUNLOG" \
    && echo "==> PASS: guest log shows in-place restart" \
    || echo "==> INFO: 'restarting in' console line not captured (lag)"
if grep -q "rebooting" "$RUNLOG"; then
    echo "==> FAIL: guest log shows a reboot — kill must restart, not reboot"; exit 1
fi

# ---- 4. graceful stop -----------------------------------------------------
echo "==> [4] graceful stop (SIGTERM sandbox-ctl directly)"
kill -TERM "$CTL_PID"
set +e; wait "$RUNPID"; RC=$?; set -e
readiness_stop_watchdog
RUNPID=""
CTL_PID=""
echo "==> run exit code: $RC"
[ "$RC" = 0 ] || { echo "==> FAIL: graceful stop exit=$RC (want 0)"; tail -40 "$RUNLOG"; exit 1; }
SIGNAL_EVENTS="$(grep -c "received terminated" "$RUNLOG" || true)"
[ "$SIGNAL_EVENTS" = 1 ] \
    || { echo "==> FAIL: graceful stop delivered $SIGNAL_EVENTS SIGTERM events (want 1)"; tail -40 "$RUNLOG"; exit 1; }
grep -q "while shutdown in progress" "$RUNLOG" \
    && { echo "==> FAIL: graceful stop triggered second-signal escalation"; tail -40 "$RUNLOG"; exit 1; }
echo "==> PASS: graceful stop delivered exactly one SIGTERM"

echo
echo "==> guest restart/reboot/placeholder log lines:"
grep -nE "placeholder app|restarting in|rebooting|app exited" "$RUNLOG" | sed 's/^/    /' || true
echo
echo "==> e2e_sandbox_placeholder: OK"
