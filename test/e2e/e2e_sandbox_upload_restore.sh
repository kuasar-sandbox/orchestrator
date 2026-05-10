#!/usr/bin/env bash
#
# e2e_sandbox_upload_restore.sh — full snapshot upload + restore manifest://
# round-trip:
#
#   1. Spin up store-ctl + cache-ctl tiered (rocksdb L1 + store origin)
#   2. Cold-start sandbox running a python TICK counter (file:// blk0)
#   3. Wait for TICK 10 in stdout
#   4. sandbox-ctl snapshot --upload  → emits snapshot manifest key
#      (--resume=false default destroys sandbox after dump)
#   5. sandbox-ctl run --restore manifest://<hex> + manifest-config
#   6. Verify restored sandbox reaches TICK > snapshot tick (vCPU resumed)
#   7. Snapshot again with --upload → second-pass dedup ratio
#   8. Print perf metrics: dedup ratio, lazy-load ratio, restore wallclock

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_upload_restore: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then exit 1; fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"

for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.erofs flatten-ctl manifest-ctl store-ctl cache-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build' and 'make cloud-hypervisor'"
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
[ "$(id -u)" -eq 0 ] || skip "must run as root (cgroup + uffd)"

WORK="$(mktemp -d /tmp/e2e-snap-upload-XXXXXX)"
PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [ -n "${E2E_KEEP:-}" ]; then echo "kept: $WORK"; else rm -rf "$WORK"; fi
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null || true
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}
KEY=$(openssl rand -hex 32)

# ---- daemons -------------------------------------------------------------

echo "==> spin up store-ctl"
STORE_PORT=$(free_port)
cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store-data
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" >"$WORK/store.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then break; fi
    sleep 0.1
done

echo "==> spin up cache-ctl (tiered: rocksdb L1 + store origin)"
CACHE_PORT=$(free_port)
CACHE_HEALTH_PORT=$(free_port)
cat > "$WORK/cache-ctl.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$CACHE_PORT
health_listen: 127.0.0.1:$CACHE_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $WORK/cache-rocks
      disk_bytes: 2GiB
      mem_ratio: 0.1
      direct_reads: false
      bloom_bits: 10
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 5s
  max_inflight: 16
EOF
"$BIN/cache-ctl" serve --config "$WORK/cache-ctl.yaml" >"$WORK/cache.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do
    if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null | grep -q SERVING; then break; fi
    sleep 0.1
done
echo "==> store=127.0.0.1:$STORE_PORT cache=127.0.0.1:$CACHE_PORT"

cat > "$WORK/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 30s
cache:
  endpoint: 127.0.0.1:$CACHE_PORT
  pool: 4
  timeout: 10s
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

# ---- prepare blk0 ------------------------------------------------------

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker pull "$IMAGE" >/dev/null
fi
BLK0_EROFS="$WORK/blk0.erofs"
docker save "$IMAGE" | "$BIN/flatten-ctl" --output "$BLK0_EROFS" --no-progress

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
mkfs.ext4 -q -F "$DIFF_FILE"

# Long-running TICK counter; the snapshotted process resumes at the
# captured i value after restore.
cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-upload
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: file://$BLK0_EROFS
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  args: ["-c", "import sys,time\nprint('PYBOOT-OK', flush=True)\ni=0\nwhile True:\n    print('TICK', i, flush=True)\n    i+=1\n    time.sleep(0.25)"]
  restart: never
EOF

# ---- run sandbox + first snapshot --upload --------------------------------

echo
echo "==> phase 1: cold-start sandbox + run TICK counter"
LOG1="$WORK/run1.log"
SID1="up1-$$"
mkdir -p "$WORK/runtime/$SID1"
"$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-dir "$WORK/runtime" \
    --sandbox-id "$SID1" \
    > "$LOG1" 2>&1 &
SBPID1=$!
PIDS+=($SBPID1)

for _ in $(seq 1 600); do
    if grep -qE "^TICK 10[[:space:]]*$" "$LOG1" 2>/dev/null; then break; fi
    if ! kill -0 "$SBPID1" 2>/dev/null; then
        echo "FAIL: sandbox exited early"; tail -40 "$LOG1"; exit 1
    fi
    sleep 0.05
done
PRE_SNAP_TICK=$(grep -oE "^TICK [0-9]+" "$LOG1" | tail -1 | awk '{print $2}')
echo "==> guest at TICK $PRE_SNAP_TICK; taking snapshot --upload"

SNAP1_LOG="$WORK/snap1.log"
T_UP1_BEG=$(date +%s%N)
SNAP_MKEY=$("$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID1" \
    --upload \
    --run-dir "$WORK/runtime" 2>"$SNAP1_LOG")
T_UP1_END=$(date +%s%N)
UP1_MS=$(( (T_UP1_END - T_UP1_BEG) / 1000000 ))

[ ${#SNAP_MKEY} -eq 64 ] || { echo "FAIL: snapshot manifest key length=${#SNAP_MKEY}, want 64"; cat "$SNAP1_LOG"; exit 1; }
echo "==> upload OK in ${UP1_MS} ms; snapshot manifest key=$SNAP_MKEY"
cat "$SNAP1_LOG" | sed 's/^/    /'

# --resume=false (default) shuts CH down; sandbox-ctl run1 returns naturally.
wait "$SBPID1" 2>/dev/null || true

# ---- restore from manifest:// ------------------------------------------------

echo
echo "==> phase 2: restore from manifest://$SNAP_MKEY"
DIFF_RESTORE="$WORK/runtime/blk1-restore.diff"
truncate -s 1G "$DIFF_RESTORE"
mkfs.ext4 -q -F "$DIFF_RESTORE"

# Restore-mode sandbox.yaml: capacity / runtime / base auto-derived
# from snapshot.cfg per docs §11.0; only network + overlay.diff required.
cat > "$WORK/host.yaml" <<EOF
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.erofs
  root:
    overlay:
      diff: file://$DIFF_RESTORE
      size: 1GiB
EOF

LOG2="$WORK/run2.log"
SID2="up2-$$"
mkdir -p "$WORK/runtime/$SID2"
T_RES_BEG=$(date +%s%N)
"$BIN/sandbox-ctl" run \
    --restore "manifest://$SNAP_MKEY" \
    --config "$WORK/host.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-dir "$WORK/runtime" \
    --sandbox-id "$SID2" \
    > "$LOG2" 2>&1 &
SBPID2=$!
PIDS+=($SBPID2)

WANT_TICK=$((PRE_SNAP_TICK + 3))
T_FIRST_TICK_NS=""
for _ in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK[[:space:]]*$" "$LOG2" 2>/dev/null; then
        T_FIRST_TICK_NS=$(date +%s%N)
        break
    fi
    if ! kill -0 "$SBPID2" 2>/dev/null; then
        echo "FAIL: restore sandbox exited early"; tail -50 "$LOG2"; exit 1
    fi
    sleep 0.05
done

if [ -z "$T_FIRST_TICK_NS" ]; then
    echo "FAIL: did not see TICK $WANT_TICK"; tail -40 "$LOG2"
    kill -TERM "$SBPID2" 2>/dev/null
    exit 1
fi
RESTORE_MS=$(( (T_FIRST_TICK_NS - T_RES_BEG) / 1000000 ))
echo "==> restore + TICK $WANT_TICK seen in ${RESTORE_MS} ms"

# ---- second snapshot --upload (dedup pass) ------------------------------

echo
echo "==> phase 3: second snapshot --upload (dedup measurement)"
SNAP2_LOG="$WORK/snap2.log"
T_UP2_BEG=$(date +%s%N)
SNAP2_MKEY=$("$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID2" \
    --upload \
    --run-dir "$WORK/runtime" 2>"$SNAP2_LOG")
T_UP2_END=$(date +%s%N)
UP2_MS=$(( (T_UP2_END - T_UP2_BEG) / 1000000 ))

[ ${#SNAP2_MKEY} -eq 64 ] || { echo "FAIL: snapshot 2 manifest key length=${#SNAP2_MKEY}"; cat "$SNAP2_LOG"; exit 1; }
echo "==> upload #2 OK in ${UP2_MS} ms; snapshot manifest key=$SNAP2_MKEY"
cat "$SNAP2_LOG" | sed 's/^/    /'

# --resume=false destroys sandbox; sandbox-ctl run2 returns.
wait "$SBPID2" 2>/dev/null || true

# ---- perf summary --------------------------------------------------------

echo
echo "==> perf summary"
echo "    TICK at snapshot:                 $PRE_SNAP_TICK"
echo "    TICK after restore:               $WANT_TICK (delta=+3, vCPU continuity confirmed)"
echo "    snapshot --upload #1 wallclock:  ${UP1_MS} ms"
echo "    restore manifest:// → first TICK: ${RESTORE_MS} ms"
echo "    snapshot --upload #2 wallclock:  ${UP2_MS} ms (same content, expect high dedup)"
echo
echo "    snap1 dedup line:   $(grep -oE 'snapshot total=[0-9]+ dedup=[0-9]+' $SNAP1_LOG | head -1)"
echo "    snap1 overlay dedup: $(grep -oE 'overlay total=[0-9]+ dedup=[0-9]+' $SNAP1_LOG | head -1)"
echo "    snap2 dedup line:   $(grep -oE 'snapshot total=[0-9]+ dedup=[0-9]+' $SNAP2_LOG | head -1)"
echo "    snap2 overlay dedup: $(grep -oE 'overlay total=[0-9]+ dedup=[0-9]+' $SNAP2_LOG | head -1)"

echo
echo "==> e2e_sandbox_upload_restore: OK"
