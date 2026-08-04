#!/usr/bin/env bash
#
# e2e_sandbox_cold.sh — cold-start a single sandbox VM with python:3.12-slim
# as the rootfs base and verify the user app exits cleanly.
#
# This test exercises the §3.2 image-config auto-extraction path:
# flatten-ctl appends config.json (Cmd=["python3"]) to the erofs image,
# sandbox-ctl reads it on the host, merges with sandbox.yaml `launch:`,
# sends the resolved spec to sandbox-init via vsock, sandbox-init execs
# python3 with the merged args.
#
# Prerequisites (the script checks each and prints what's missing):
#   1. KVM accessible:                /dev/kvm exists, current user has rw
#   2. cloud-hypervisor built:        bin/cloud-hypervisor (make build)
#   3. sandbox-* binaries built:      bin/sandbox-ctl, bin/sandbox-init,
#                                      bin/sandbox-runtime.bundle, bin/flatten-ctl
#   4. Kernel image:                  $VMLINUX (default $REPO/bin/vmlinux)
#   5. Pre-existing TAP up:           $TAP_NAME (default sb-tap0)
#   6. Docker for image fetch:        `docker pull python:3.12-slim`
#
# Override BLK0_IMAGE=path/to/prebuilt.erofs to skip the docker+flatten step
# (e.g. on hosts without docker but with a prebuilt image cached locally).
#
# When prerequisites are missing the script exits 0 with a clear message
# (treating it as "skipped on this host") so make build doesn't fail in
# CI. Set REQUIRE_KVM=1 to fail hard on missing prerequisites instead.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
# BIN overridable so perf bench can point at a /tmp-cached copy of the
# binaries (some filesystems, e.g. WSL2 drvfs, add ~500ms per exec).
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"
SID="${SID:-cold1}"
source "$REPO_ROOT/test/e2e/readiness_helpers.sh"

# ---- prerequisite checks --------------------------------------------------

skip() {
    echo
    echo "==> e2e_sandbox_cold: skipping ($*)"
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
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX env var)"

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

# ---- prepare blk0 (python:3.12-slim → erofs with appended config.json) ----
WORK="$(mktemp -d /tmp/e2e-sandbox-XXXXXX)"
SBPID=""
POST_PID=""
cleanup() {
    set +e
    readiness_stop_watchdog
    for pid in "$SBPID" "$POST_PID"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    sleep 1
    [ -n "$SBPID" ] && readiness_kill_session KILL "$SBPID"
    [ -n "$POST_PID" ] && kill -0 "$POST_PID" 2>/dev/null && kill -KILL "$POST_PID" 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null
}
trap cleanup EXIT

BLK0_IMAGE="${BLK0_IMAGE:-}"
if [ -z "$BLK0_IMAGE" ]; then
    command -v docker >/dev/null 2>&1 || skip "docker not available; provide BLK0_IMAGE=path/to/prebuilt.erofs"
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "==> docker pull $IMAGE"
        docker pull "$IMAGE" >/dev/null
    fi
    BLK0_IMAGE="$WORK/blk0.img"
    echo "==> docker save $IMAGE | flatten-ctl export --output $BLK0_IMAGE"
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_IMAGE" --no-progress
    echo "==> blk0 erofs ready ($(du -h "$BLK0_IMAGE" | cut -f1))"
    echo "==> appended config preview:"
    "$BIN/flatten-ctl" info "$BLK0_IMAGE" 2>&1 | sed 's/^/    /' | head -20 || true
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_IMAGE")"

# ---- prepare sandbox.yaml -------------------------------------------------
mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
# Pre-format the diff as ext4 — sandbox-init mounts /dev/vdb as ext4 to
# build the rootfs overlay's upper layer. Without a filesystem header,
# mount returns EINVAL and sandbox-init aborts. Real production diffs
# carry a snapshot's ext4 from the previous run; here we make a fresh one.
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH (apt install e2fsprogs)"
mkfs.ext4 -q -F "$DIFF_FILE"

# Override only `launch.args`: image config supplies exec=python3 +
# default Env (PATH for /usr/local/bin/python3 lookup). Args make the
# interpreter print + exit, so the VM can shut down cleanly within the
# 60s e2e budget.
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
  hostname: e2e-cold
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  # Args use a uniquely-shaped python expression: the script computes
  # version components and emits a marker the e2e grep can distinguish
  # from the literal launch.args text appearing in sandbox-ctl logs. The app
  # waits on a test sentinel so readiness can be exercised without adding a
  # fixed delay to the cold-start performance measurement.
  args:
    - "-c"
    - |
      import os, sys, time
      print('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor, flush=True)
      while not os.path.exists('/tmp/e2e-cold-exit'):
          time.sleep(0.01)
  restart: never
EOF

echo "==> sandbox.yaml:"
sed 's/^/    /' "$WORK/sandbox.yaml"

# ---- readiness failure semantics -----------------------------------------
echo "==> readiness: failure before control_ready produces immediate EOF"
readiness_begin_capture "$WORK/pre-control.ready"
PRE_READER_PID=$READY_READER_PID
set +e
"$BIN/sandbox-ctl" run --ready-fd="$READY_WRITE_FD" \
    --config "$WORK/does-not-exist.yaml" --sandbox-id cold-pre-fail \
    --run-root "$WORK/runtime" >"$WORK/pre-control.log" 2>&1
PRE_RC=$?
set -e
readiness_close_parent_writer
[ "$PRE_RC" -ne 0 ] || { echo "==> FAIL: invalid config unexpectedly started"; exit 1; }
readiness_assert_wire "$WORK/pre-control.ready" "$PRE_READER_PID" "" \
    || { echo "==> FAIL: pre-control failure emitted readiness data"; exit 1; }

echo "==> readiness: failure after control_ready closes without ready"
readiness_begin_capture "$WORK/post-control.ready"
POST_READER_PID=$READY_READER_PID
"$BIN/sandbox-ctl" run --ready-fd="$READY_WRITE_FD" \
    --config "$WORK/sandbox.yaml" --sandbox-id cold-post-fail \
    --ch-binary "$WORK/missing-cloud-hypervisor" --run-root "$WORK/runtime" \
    >"$WORK/post-control.log" 2>&1 &
POST_PID=$!
readiness_close_parent_writer
readiness_wait_event "$WORK/post-control.ready" 1 control_ready "$POST_PID" \
    || { tail -60 "$WORK/post-control.log"; exit 1; }
set +e; wait "$POST_PID"; POST_RC=$?; set -e
POST_PID=""
[ "$POST_RC" -ne 0 ] || { echo "==> FAIL: missing CH binary unexpectedly succeeded"; exit 1; }
readiness_assert_wire "$WORK/post-control.ready" "$POST_READER_PID" $'control_ready\n' \
    || { echo "==> FAIL: post-control failure emitted an invalid wire"; exit 1; }
echo "==> PASS: both pre-ready failure paths closed readiness exactly"

# ---- run sandbox-ctl with timing ----------------------------------------
echo "==> launching sandbox-ctl run (timeout 60s)"
LOG="$WORK/run.log"

# T0 = wallclock just before invoking the binary. T_app = wallclock when
# the user app's first stdout (PYBOOT-OK marker) becomes visible. T_end =
# wallclock when sandbox-ctl exits. The poll interval bounds T_app
# resolution; 5ms keeps it tight without burning CPU.
T0_NS=$(date +%s%N)
set +e
# PERF_STATS_JSON: external stats.json sink (used by test/perf/sandbox-perf.sh
# to harvest runtime metrics). Defaults to a path inside $WORK (cleaned on exit).
STATS_JSON="${PERF_STATS_JSON:-$WORK/stats.json}"
readiness_begin_capture "$WORK/cold.ready"
COLD_READER_PID=$READY_READER_PID
readiness_exec_in_new_session "$BIN/sandbox-ctl" run \
    --ready-fd="$READY_WRITE_FD" \
    --config "$WORK/sandbox.yaml" \
    --sandbox-id "$SID" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --stats-json "$STATS_JSON" \
    > "$LOG" 2>&1 &
SBPID=$!
readiness_close_parent_writer
readiness_start_watchdog "$SBPID" 60 10

readiness_wait_event "$WORK/cold.ready" 1 control_ready "$SBPID" \
    || { tail -60 "$LOG"; exit 1; }
readiness_connect_ctl "$WORK/runtime/$SID/ctl.sock" \
    || { echo "==> FAIL: ctl.sock was not connectable at control_ready"; exit 1; }
echo "==> PASS: control_ready observed only after ctl.sock accepted a connection"
readiness_wait_event "$WORK/cold.ready" 2 ready "$SBPID" \
    || { tail -60 "$LOG"; exit 1; }
if ! "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/runtime" -- /bin/true; then
    echo "==> FAIL: immediate exec after ready failed"; tail -60 "$LOG"; exit 1
fi
readiness_assert_wire "$WORK/cold.ready" "$COLD_READER_PID" $'control_ready\nready\n' \
    || { echo "==> FAIL: cold readiness wire was not exact"; exit 1; }
echo "==> PASS: exact control_ready -> ready -> EOF; immediate exec succeeded"

T_APP_NS=""
# Match python's actual stdout line ("^PYBOOT-OK <int>$"), not the
# sandbox-ctl log dump that contains the literal string 'PYBOOT-OK'
# inside the launch.args echo at startup. Anchoring on ^ + trailing
# digit ensures we measure when python truly emitted output.
while kill -0 "$SBPID" 2>/dev/null; do
    if grep -qE "^PYBOOT-OK [0-9]+$" "$LOG" 2>/dev/null; then
        T_APP_NS=$(date +%s%N)
        break
    fi
    sleep 0.005
done
if ! "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/runtime" \
    -- /bin/sh -c 'touch /tmp/e2e-cold-exit'; then
    echo "==> FAIL: could not release cold readiness test app"; tail -60 "$LOG"; exit 1
fi
wait "$SBPID"
EXIT=$?
T_END_NS=$(date +%s%N)
readiness_stop_watchdog
SBPID=""
set -e

# Final fallback: if poll missed PYBOOT-OK between SBPID exit and the
# next sleep tick, scan the now-complete log.
if [ -z "$T_APP_NS" ] && grep -q "PYBOOT-OK" "$LOG" 2>/dev/null; then
    T_APP_NS=$T_END_NS  # approximate (within poll interval)
fi

ms_delta() { awk "BEGIN{printf \"%.1f\", ($2 - $1) / 1000000.0}"; }

echo "==> sandbox-ctl exit code: $EXIT"
echo "==> last 60 lines of log:"
tail -60 "$LOG"

echo
echo "==> cold-start timing (wallclock):"
if [ -n "$T_APP_NS" ]; then
    APP_MS=$(ms_delta "$T0_NS" "$T_APP_NS")
    echo "    T0 → app first stdout (PYBOOT-OK):  ${APP_MS} ms"
fi
END_MS=$(ms_delta "$T0_NS" "$T_END_NS")
echo "    T0 → sandbox-ctl exit:               ${END_MS} ms"

# Sub-stage breakdown extracted from sandbox-ctl logs (microsecond
# precision since we set log.Lmicroseconds in cmd/sandbox-ctl/main.go).
echo
echo "==> sub-stage milestones (from sandbox-ctl logs):"
awk '
    /sandbox-ctl run --config/ { stage="invoke"; next }
    /image config:/             { print "    " $1, $2, "image config extracted"; next }
    /spawning .* cloud-hypervisor with/    { print "    " $1, $2, "spawn CH"; next }
    /CH started pid=/                       { print "    " $1, $2, "CH process up"; next }
    /vhost: master connected.*blk0/         { print "    " $1, $2, "blk0 backend connected"; next }
    /vhost: master connected.*blk1/         { print "    " $1, $2, "blk1 backend connected"; next }
    /launch: guest connected/               { print "    " $1, $2, "vsock guest connected (sandbox-init reached phase 2)"; next }
    /launch: launch spec sent/              { print "    " $1, $2, "launch spec delivered"; next }
    /CH exited code=/                       { print "    " $1, $2, "CH exited"; next }
' "$LOG" || true

echo
echo "==> guest-side milestones (from sandbox-init [T+Xms] prefix):"
grep -oE '\[sandbox-init\]\[T\+[0-9]+ms\][^"]*' "$LOG" | head -10 | sed 's/^/    /' || true

if [ "$EXIT" = "124" ] && ! grep -q "PYBOOT-OK" "$LOG"; then
    echo "==> FAIL: sandbox run timed out at 60s — guest didn't reach app. Inspect $LOG"
    exit 1
fi

# Validate the guest produced its output. Marker "PYBOOT-OK 312" can
# only come from python's stdout (not from the launch.args literal in
# logs — which has the unevaluated expression "sys.version_info.major*100+...").
if grep -qE "PYBOOT-OK [0-9]+" "$LOG"; then
    marker=$(grep -oE "PYBOOT-OK [0-9]+" "$LOG" | head -1)
    echo "==> PASS: python actually executed in sandbox: $marker"
else
    echo "==> FAIL: sandbox-ctl exit=$EXIT and PYBOOT-OK marker not seen"
    exit 1
fi

# Verify image-config extraction actually engaged: sandbox-ctl logs
# "image config: cmd=[python3] entrypoint=[] workdir=..." just before
# spawning CH. If this is missing, the test passed by accident (e.g.
# someone added launch.exec).
if grep -q "image config: cmd=\[python3\]" "$LOG"; then
    echo "==> PASS: image-config auto-extraction engaged (cmd=[python3] from blk0 ZIP)"
else
    echo "==> FAIL: image-config extraction log line missing — test may not have exercised §3.2"
    exit 1
fi

# Verify cleanup: runtime dir should be empty.
if compgen -G "$WORK/runtime/sb-*" >/dev/null; then
    echo "==> WARN: $WORK/runtime/sb-* still exists after run; should have been cleaned"
    ls -la "$WORK/runtime/"
fi

# Verify --stats-json wrote a parseable file with both backends and the
# expected coverage shape (blk0 loaded > 0 since the guest read at least
# the rootfs metadata).
if [ ! -s "$STATS_JSON" ]; then
    echo "==> FAIL: --stats-json output $STATS_JSON missing or empty"
    exit 1
fi
if command -v python3 >/dev/null 2>&1; then
    python3 - <<PY
import json, sys
with open("$STATS_JSON") as f:
    rep = json.load(f)
backs = {b["name"]: b for b in rep["backends"]}
assert {"blk0", "blk1"} <= backs.keys(), f"missing backends: {list(backs)}"
b0 = backs["blk0"]
assert b0["read"]["count"] > 0, "blk0 had no reads"
assert b0["loaded_blocks"] > 0, "blk0 load coverage zero"
assert b0["read"]["p50_ns"] <= b0["read"]["lat_max_ns"], "p50 > max"
assert b0["read"]["p99_ns"] <= b0["read"]["lat_max_ns"], "p99 > max"
print(f"==> PASS: stats-json {b0['name']} reqs={b0['read']['count']} "
      f"loaded={b0['loaded_blocks']}/{b0['total_blocks']} "
      f"p50={b0['read']['p50_ns']/1000:.1f}us p99={b0['read']['p99_ns']/1000:.1f}us")
PY
else
    echo "==> FAIL: python3 missing for stats-json validation; file size $(stat -c%s "$STATS_JSON") bytes"
    exit 1
fi

echo "==> e2e_sandbox_cold: OK"
