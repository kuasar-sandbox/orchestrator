#!/usr/bin/env bash
#
# e2e_sandbox_upload_restore.sh — full snapshot upload + restore manifest://
# round-trip:
#
#   1. Spin up store-ctl + cache-ctl tiered (rocksdb L1 + store origin)
#   2. Cold-start sandbox running a python TICK counter that also writes a
#      fresh 4 KiB block per tick to /ticks.dat (blk1 overlay) and reads back
#      block 0 → "TICK <i> DISK blk0=<marker>"
#   3. Wait for TICK 10 (with blk0=cold marker) in stdout, snapshot --upload
#   5. run --restore manifest://<hex>; verify TICK > snapshot tick (vCPU
#      resumed) AND blk0 still reads the cold marker (disk fall-through)
#   7. Snapshot the restored sandbox again → snap#2 (from_refs=[snap#1]),
#      restore it: the incremental 2-layer chain (mem + disk overlay)
#   8. Snapshot THAT chained-restored sandbox → snap#3 (3-layer chain),
#      restore it: validates the recurring quiesce teardown + deep-chain
#      mem/disk fall-through (asserts restore builds 3 memory layers)
#   9. Print perf metrics: dedup ratio, lazy-load ratio, restore wallclock

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
. "$REPO_ROOT/test/lib/uffd_performance_gate.sh"
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

for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl cache-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
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
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

# ---- prepare blk0 ------------------------------------------------------

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker pull "$IMAGE" >/dev/null
fi
BLK0_EROFS="$WORK/blk0.img"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_EROFS" --no-progress
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_EROFS")"

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
mkfs.ext4 -q -F "$DIFF_FILE"

# Long-running TICK counter that also exercises the disk overlay layer:
# each tick writes a fresh 4 KiB block at offset i*4096 of /ticks.dat (on the
# blk1 ext4 upper → CoW diff, captured into each snapshot's overlay) and reads
# back block 0 (written at cold tick 0). Per tick it prints
# "TICK <i> DISK blk0=<marker>". Different runs touch different block ranges,
# so each snapshot's overlay is non-empty and holds distinct blocks; reading
# block 0 after a chained restore must fall through every overlay layer to the
# deepest (snap#1) — blk0=TICK00000000 proves the layered disk read + vCPU
# continuity together. quiesce drop_caches forces post-restore reads off disk.
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
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  args: ["-c", "import os,time\nprint('PYBOOT-OK', flush=True)\nfd=os.open('/ticks.dat', os.O_RDWR|os.O_CREAT, 0o644)\ni=0\nwhile True:\n    os.pwrite(fd, ('TICK%08d' % i).encode().ljust(4096, b'.'), i*4096)\n    os.fsync(fd)\n    blk0=os.pread(fd, 12, 0).decode()\n    print('TICK %d DISK blk0=%s' % (i, blk0), flush=True)\n    i+=1\n    time.sleep(0.25)"]
  restart: never
EOF

# ---- run sandbox + first snapshot --upload --------------------------------

echo
# block 0 of /ticks.dat (written at cold tick 0) must read back as this through
# every restore — asserting the disk overlay layered read (fall-through to the
# deepest layer) alongside vCPU continuity. Matched on the same "TICK <n>" line.
BLK0_OK="DISK blk0=TICK00000000"
echo "==> phase 1: cold-start sandbox + run TICK counter (writes /ticks.dat)"
LOG1="$WORK/run1.log"
SID1="up1-$$"
mkdir -p "$WORK/runtime/$SID1"
"$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "$SID1" \
    > "$LOG1" 2>&1 &
SBPID1=$!
PIDS+=($SBPID1)

for _ in $(seq 1 600); do
    if grep -qE "^TICK 10 $BLK0_OK$" "$LOG1" 2>/dev/null; then break; fi
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
    --run-root "$WORK/runtime" 2>"$SNAP1_LOG")
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
# Restore diff is an EMPTY CoW upper — the filesystem comes from the snapshot's
# overlay base; do NOT mkfs (a fresh ext4 superblock/UUID would shadow the base
# and diverge from the guest's in-memory ext4 state).
truncate -s 1G "$DIFF_RESTORE"

# Restore-mode sandbox.yaml: capacity must match snapshot.cfg (declared
# explicitly, same as the cold yaml). The bundle was manifest:// loaded
# (snapshotPath=""), so ApplyRules requires host yaml to provide every
# file:// ref from snapshot.cfg explicitly — here runtime and root.base.
cat > "$WORK/host.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
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
    --run-root "$WORK/runtime" \
    --sandbox-id "$SID2" \
    --stats-json "$WORK/stats2.json" \
    > "$LOG2" 2>&1 &
SBPID2=$!
PIDS+=($SBPID2)

WANT_TICK=$((PRE_SNAP_TICK + 3))
T_FIRST_TICK_NS=""
for _ in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK $BLK0_OK$" "$LOG2" 2>/dev/null; then
        T_FIRST_TICK_NS=$(date +%s%N)
        break
    fi
    if ! kill -0 "$SBPID2" 2>/dev/null; then
        echo "FAIL: restore sandbox exited early"; tail -50 "$LOG2"; exit 1
    fi
    sleep 0.05
done

if [ -z "$T_FIRST_TICK_NS" ]; then
    echo "FAIL: did not see 'TICK $WANT_TICK $BLK0_OK' (vCPU continuity or disk fall-through broken)"; tail -40 "$LOG2"
    kill -TERM "$SBPID2" 2>/dev/null
    exit 1
fi
RESTORE_MS=$(( (T_FIRST_TICK_NS - T_RES_BEG) / 1000000 ))
echo "==> restore + TICK $WANT_TICK (disk blk0 OK) seen in ${RESTORE_MS} ms"

# ---- second snapshot --upload (dedup pass) ------------------------------

echo
echo "==> phase 3: second snapshot --upload (dedup measurement)"
SNAP2_LOG="$WORK/snap2.log"
T_UP2_BEG=$(date +%s%N)
SNAP2_MKEY=$("$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID2" \
    --upload \
    --run-root "$WORK/runtime" 2>"$SNAP2_LOG")
T_UP2_END=$(date +%s%N)
UP2_MS=$(( (T_UP2_END - T_UP2_BEG) / 1000000 ))

[ ${#SNAP2_MKEY} -eq 64 ] || { echo "FAIL: snapshot 2 manifest key length=${#SNAP2_MKEY}"; cat "$SNAP2_LOG"; exit 1; }
echo "==> upload #2 OK in ${UP2_MS} ms; snapshot manifest key=$SNAP2_MKEY"
cat "$SNAP2_LOG" | sed 's/^/    /'

# Capture the tick frozen into snap#2 (last TICK before the vCPU paused).
SNAP2_TICK=$(grep -oE "^TICK [0-9]+" "$LOG2" | tail -1 | awk '{print $2}')

# --resume=false destroys sandbox; sandbox-ctl run2 returns.
wait "$SBPID2" 2>/dev/null || true
uffd_performance_gate "manifest-restore-1-layer" "$RESTORE_MS" 3000 \
    "$WORK/stats2.json" buffered

# ---- phase 4: restore from the CHAINED snapshot -------------------------
# snap#2 was taken from the restored sandbox, so its snapshot.cfg has
# from_refs=[snap#1] / base_from_refs=[snap#1.overlay] (incremental layered
# chain, docs/sandbox.md §3.5). Restoring it builds layeredStream(snap#2,
# snap#1): pages snap#2 left as holes (not touched during phase-2's run) must
# fall through to snap#1. If the guest resumes and keeps ticking, the
# fall-through is correct end-to-end.

echo
echo "==> phase 4: restore from chained manifest://$SNAP2_MKEY (snap2 over snap1; snap2 frozen at TICK $SNAP2_TICK)"
DIFF_RESTORE2="$WORK/runtime/blk1-restore2.diff"
# Empty CoW upper (see phase 2): no mkfs — fs comes from the layered overlay base.
truncate -s 1G "$DIFF_RESTORE2"
cat > "$WORK/host2.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore2
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_RESTORE2
      size: 1GiB
EOF

LOG3="$WORK/run3.log"
SID3="up3-$$"
mkdir -p "$WORK/runtime/$SID3"
T_RES2_BEG=$(date +%s%N)
"$BIN/sandbox-ctl" run \
    --restore "manifest://$SNAP2_MKEY" \
    --config "$WORK/host2.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "$SID3" \
    --stats-json "$WORK/stats3.json" \
    > "$LOG3" 2>&1 &
SBPID3=$!
PIDS+=($SBPID3)

WANT_TICK2=$((SNAP2_TICK + 3))
T2_NS=""
for _ in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK2 $BLK0_OK$" "$LOG3" 2>/dev/null; then
        T2_NS=$(date +%s%N)
        break
    fi
    if ! kill -0 "$SBPID3" 2>/dev/null; then
        echo "FAIL: chained-restore sandbox exited early"; tail -50 "$LOG3"; exit 1
    fi
    sleep 0.05
done
if [ -z "$T2_NS" ]; then
    echo "FAIL: chained restore did not reach 'TICK $WANT_TICK2 $BLK0_OK' (mem or disk fall-through broken?)"; tail -40 "$LOG3"
    kill -TERM "$SBPID3" 2>/dev/null
    exit 1
fi
# Chain depth: snap#2 carries from_refs=[snap#1], so the restore builds a
# 2-layer memory source (asserts the chain is used, not a flattened bundle).
grep -qE "snapshot source: 2 memory layer\(s\)" "$LOG3" || {
    echo "FAIL: phase-4 restore was not 2-layer"; grep -E 'memory layer' "$LOG3" | grep -ivE faulty; kill -TERM "$SBPID3" 2>/dev/null; exit 1; }
RESTORE2_MS=$(( (T2_NS - T_RES2_BEG) / 1000000 ))
echo "==> PASS: chained restore reached TICK $WANT_TICK2 in ${RESTORE2_MS} ms (2-layer chain; disk blk0 fall-through OK)"
# Leave SID3 running — phase 5 snapshots it into a 3-layer chain.

# ---- phase 5: snapshot the chained-restored sandbox → 3-layer chain ------
# Snapshotting SID3 (itself a chained restore) re-exercises the deterministic
# quiesce teardown (§4.6) on a chained-restored VM, and produces snap#3 with
# from_refs=[snap#2, snap#1] / base_from_refs=[snap#2.overlay, snap#1.overlay].
# Restoring it must build a 3-layer memory+disk source and stay byte-correct.
echo
echo "==> phase 5: snapshot chained-restored SID3 → snap#3, then restore the 3-layer chain"
SNAP3_LOG="$WORK/snap3.log"
SNAP3_MKEY=$("$BIN/sandbox-ctl" snapshot \
    --sandbox-id "$SID3" \
    --upload \
    --run-root "$WORK/runtime" 2>"$SNAP3_LOG")
[ ${#SNAP3_MKEY} -eq 64 ] || { echo "FAIL: snapshot 3 manifest key length=${#SNAP3_MKEY}"; cat "$SNAP3_LOG"; exit 1; }
SNAP3_TICK=$(grep -oE "^TICK [0-9]+" "$LOG3" | tail -1 | awk '{print $2}')
echo "==> upload #3 OK; snap#3 key=$SNAP3_MKEY (frozen at TICK $SNAP3_TICK)"
cat "$SNAP3_LOG" | sed 's/^/    /'
wait "$SBPID3" 2>/dev/null || true  # snapshot --resume=false destroyed SID3
uffd_performance_gate "manifest-restore-2-layer" "$RESTORE2_MS" 3000 \
    "$WORK/stats3.json" buffered

DIFF_RESTORE3="$WORK/runtime/blk1-restore3.diff"
truncate -s 1G "$DIFF_RESTORE3"     # empty CoW upper; fs comes from the 3-layer overlay base
cat > "$WORK/host3.yaml" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-restore3
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_RESTORE3
      size: 1GiB
EOF
LOG4="$WORK/run4.log"
SID4="up4-$$"
mkdir -p "$WORK/runtime/$SID4"
T_RES3_BEG=$(date +%s%N)
"$BIN/sandbox-ctl" run \
    --restore "manifest://$SNAP3_MKEY" \
    --config "$WORK/host3.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/runtime" \
    --sandbox-id "$SID4" \
    --stats-json "$WORK/stats4.json" \
    > "$LOG4" 2>&1 &
SBPID4=$!
PIDS+=($SBPID4)
WANT_TICK3=$((SNAP3_TICK + 3))
T3_NS=""
for _ in $(seq 1 600); do
    if grep -qE "^TICK $WANT_TICK3 $BLK0_OK$" "$LOG4" 2>/dev/null; then T3_NS=$(date +%s%N); break; fi
    if ! kill -0 "$SBPID4" 2>/dev/null; then echo "FAIL: 3-layer restore sandbox exited early"; tail -50 "$LOG4"; exit 1; fi
    sleep 0.05
done
if [ -z "$T3_NS" ]; then
    echo "FAIL: 3-layer chain did not reach 'TICK $WANT_TICK3 $BLK0_OK' (deep-chain mem/disk fall-through broken?)"; tail -40 "$LOG4"
    kill -TERM "$SBPID4" 2>/dev/null
    exit 1
fi
# The chain must have ACCUMULATED to depth 3 (snap#3→snap#2→snap#1), not flattened.
grep -qE "snapshot source: 3 memory layer\(s\)" "$LOG4" || {
    echo "FAIL: snap#3 restore was not 3-layer (chain flattened/lost?)"; grep -E 'memory layer' "$LOG4" | grep -ivE faulty; kill -TERM "$SBPID4" 2>/dev/null; exit 1; }
RESTORE3_MS=$(( (T3_NS - T_RES3_BEG) / 1000000 ))
echo "==> PASS: 3-layer chained restore reached TICK $WANT_TICK3 in ${RESTORE3_MS} ms (snap3→snap2→snap1; disk blk0 fall-through through 3 layers)"
kill -TERM "$SBPID4" 2>/dev/null
wait "$SBPID4" 2>/dev/null || true
uffd_performance_gate "manifest-restore-3-layer" "$RESTORE3_MS" 3000 \
    "$WORK/stats4.json" buffered

# ---- perf summary --------------------------------------------------------

echo
echo "==> perf summary"
echo "    TICK at snapshot:                 $PRE_SNAP_TICK"
echo "    TICK after restore:               $WANT_TICK (delta=+3, vCPU continuity confirmed)"
echo "    snapshot --upload #1 wallclock:  ${UP1_MS} ms"
echo "    restore manifest:// → first TICK: ${RESTORE_MS} ms"
echo "    snapshot --upload #2 wallclock:  ${UP2_MS} ms (same content, expect high dedup)"
echo "    chained restore (snap2→snap1):    TICK $WANT_TICK2 in ${RESTORE2_MS} ms (2-layer; disk blk0 fall-through OK)"
echo "    chained restore (snap3→snap2→snap1): TICK $WANT_TICK3 in ${RESTORE3_MS} ms (3-layer; disk blk0 fall-through OK)"
echo
echo "    snap1 snapshot dedup: $(grep -oE 'snapshot stored=[0-9]+ dedup=[0-9]+' "$SNAP1_LOG" | head -1)"
echo "    snap1 overlay dedup:  $(grep -oE 'overlay stored=[0-9]+ dedup=[0-9]+' "$SNAP1_LOG" | head -1)"
echo "    snap2 snapshot dedup: $(grep -oE 'snapshot stored=[0-9]+ dedup=[0-9]+' "$SNAP2_LOG" | head -1)"
echo "    snap2 overlay dedup:  $(grep -oE 'overlay stored=[0-9]+ dedup=[0-9]+' "$SNAP2_LOG" | head -1)"
echo "    snap3 snapshot dedup: $(grep -oE 'snapshot stored=[0-9]+ dedup=[0-9]+' "$SNAP3_LOG" | head -1)"
echo "    snap3 overlay dedup:  $(grep -oE 'overlay stored=[0-9]+ dedup=[0-9]+' "$SNAP3_LOG" | head -1)"

echo
echo "==> e2e_sandbox_upload_restore: OK"
