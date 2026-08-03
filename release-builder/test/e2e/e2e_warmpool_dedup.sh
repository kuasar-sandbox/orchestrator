#!/usr/bin/env bash
#
# e2e_warmpool_dedup.sh — measure cross-sandbox dedup ratios for the
# warm-pool startup model.
#
# Workload: cold-start N independent sandboxes off the SAME container
# image (python:3.12-slim by default), each running the same TICK
# counter, snapshot each at steady state, then ingest blk1 overlays
# into the same store. Pairwise-diff the resulting manifests and
# report median/min/max chunk-bytes overlap separately for:
#
#   blk0 (image)  — single ingest, trivially shared (sanity check only)
#   blk1 (overlay diff) — one per sandbox, tests how similar the
#                          sandbox-init + EROFS overlay writes look
#                          across independent boots
#   memory snapshot     — one per sandbox, tests how similar the
#                          guest RAM looks at the same TICK count
#
# Why this matters: kuasar-sandbox.md §7.3 targets >85% cross-image
# dedup. Same-image / same-app dedup should comfortably exceed that;
# this test exposes the actual number on real workloads.
#
# Defaults:
#   WARMPOOL_N=5        sandboxes to spin up
#   WARMPOOL_TICKS=10   TICK to wait for before snapshot
#   IMAGE=python:3.12-slim
#
# Same skip semantics as the other sandbox e2e (no /dev/kvm, no TAP, no
# docker → exit 0; REQUIRE_KVM=1 to fail hard).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"
N="${WARMPOOL_N:-5}"
TICKS="${WARMPOOL_TICKS:-10}"

skip() {
    echo
    echo "==> e2e_warmpool_dedup: skipping ($*)"
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
command -v docker >/dev/null 2>&1 || skip "docker not on PATH"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"

WORK="$(mktemp -d /tmp/e2e-warmpool-XXXXXX)"
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

# ---- spin up store-ctl + cache-ctl (tiered) ------------------------------

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
    ping_out=$("$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null || true)
    if [[ "$ping_out" == *SERVING* ]]; then break; fi
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

# ---- prepare blk0 (shared base) ------------------------------------------

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> docker pull $IMAGE"
    docker pull "$IMAGE" >/dev/null
fi
BLK0_EROFS="$WORK/blk0.img"
echo "==> docker save $IMAGE | flatten-ctl export --output blk0.img"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_EROFS" --no-progress
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_EROFS")"

# Single ingest of blk0 → manifest. Each sandbox references this same
# manifest at boot via boot.root.base = manifest://<BLK0_MKEY>.
echo "==> ingest blk0 → store"
BLK0_MKEY=$("$BIN/manifest-ctl" store \
    --manifest-config "$WORK/accelerator.yaml" \
    --no-progress \
    "$BLK0_EROFS")
[ ${#BLK0_MKEY} -eq 64 ] || { echo "FAIL: bad BLK0_MKEY length=${#BLK0_MKEY}"; exit 1; }
echo "    blk0 manifest key: $BLK0_MKEY"

mkdir -p "$WORK/runtime"

# ---- prepare blk1 base (deterministic mkfs, copied per sandbox) ----------
#
# Naive per-sandbox mkfs.ext4 produces a fresh UUID + timestamp + lazy-init
# tail every run, so the resulting superblock + journal chunks are unique
# per sandbox and cross-sandbox dedup of blk1 collapses to ~0%.
#
# Production warm-pool model: blk1 is a CoW copy of a pre-built base diff
# (one mkfs upstream, N copies downstream). Mirror that here:
#   1. mkfs.ext4 once into base.diff with deterministic flags:
#        -U <fixed UUID>             — same FS UUID across sandboxes
#        -E nodiscard                — skip TRIM (which is also non-deterministic)
#        -E hash_seed=<fixed>        — htree hashing seed (else random per-FS)
#        -M /                        — last-mount-point (placeholder)
#        --offset 0 + -F             — force, no interactive
#        SOURCE_DATE_EPOCH=<fixed>   — pin all timestamps including superblock
#                                      mkfs_time, and lazy-init disabled to
#                                      avoid post-mount metadata writes.
#      Disable lazy-init so the FS is fully formatted up-front (later "mount"
#      writes won't differ across sandboxes).
#   2. cp base.diff blk1-i.diff per sandbox (each sandbox gets an
#      independent copy to write into).
echo "==> prepare base blk1 diff (deterministic mkfs)"
BLK1_BASE="$WORK/runtime/blk1-base.diff"
truncate -s 1G "$BLK1_BASE"
SOURCE_DATE_EPOCH=1577836800 mkfs.ext4 -q -F \
    -U 11111111-2222-3333-4444-555555555555 \
    -E nodiscard,lazy_itable_init=0,lazy_journal_init=0,hash_seed=00000000-0000-0000-0000-000000000000 \
    -M / \
    "$BLK1_BASE"
echo "    base diff sha256: $(sha256sum "$BLK1_BASE" | cut -d' ' -f1)"

# ---- launch N sandboxes in series, snapshot each --------------------------

# Long-running TICK counter app. Snapshot at TICK $TICKS captures a
# steady-state guest where the python interpreter has its loaded modules
# and the kernel has stable page cache.
SANDBOX_LAUNCH='import sys,time
print("PYBOOT-OK", flush=True)
i=0
while True:
    print("TICK", i, flush=True)
    i+=1
    time.sleep(0.25)'

declare -a SNAP_MKEYS=()
declare -a BLK1_MKEYS=()

for i in $(seq 1 "$N"); do
    echo
    echo "==> sandbox $i/$N"
    DIFF_FILE="$WORK/runtime/blk1-$i.diff"
    # Copy the deterministic base — same content across sandboxes, each
    # gets an independent file the sandbox can write into. Sparse copy
    # is fine because base is mostly zeros (fresh ext4).
    cp --sparse=always "$BLK1_BASE" "$DIFF_FILE"
    SID="warm-$$-$i"
    mkdir -p "$WORK/runtime/$SID"

    cat > "$WORK/sb-$i.yaml" <<EOF
resources:
  capacity:    { cpu: 2, memory: 8GiB }
  # 1 GiB allocatable (overridable via WARMPOOL_ALLOCATABLE_MEM) gives
  # python:3.12-slim and the host-side cgroup enough headroom on dev
  # hosts. Production target spec (128 MiB) lives in
  # e2e_sandbox_cold_target.sh; this test focuses on dedup quality and
  # does not exercise the resource-tight path.
  # cpu must equal capacity.cpu unless a cgroup_path is set; the dedup
  # test exercises the snapshot/quiesce path, not CPU-resource control,
  # so setting fractional cpu here just requires extra cgroup wiring
  # (provisioned in e2e_sandbox_cold_target.sh). Match capacity here.
  allocatable: { cpu: 2, memory: ${WARMPOOL_ALLOCATABLE_MEM:-1GiB} }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: warm-$i
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  # nokaslr + norandmaps disable kernel/user ASLR. Required for the
  # kuasar-sandbox.md §4.6 ">90% dedup" target — without them the kernel image
  # base + user mmap layout differ per boot, defeating chunk-level
  # cross-instance dedup.
  cmdline: "console=hvc0 printk.time=1 nokaslr norandmaps"
  root:
    base: $BLK0_REF
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  args: ["-c", $(printf '%s' "$SANDBOX_LAUNCH" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')]
  restart: never
EOF

    LOG="$WORK/run-$i.log"
    echo "    cold-starting (sid=$SID)"
    "$BIN/sandbox-ctl" run \
        --config "$WORK/sb-$i.yaml" \
        --manifest-config "$WORK/accelerator.yaml" \
        --ch-binary "$BIN/cloud-hypervisor" \
        --run-root "$WORK/runtime" \
        --sandbox-id "$SID" \
        > "$LOG" 2>&1 &
    SBPID=$!
    PIDS+=($SBPID)

    # Wait for the guest to reach the target TICK.
    for _ in $(seq 1 600); do
        if grep -qE "^TICK $TICKS[[:space:]]*\$" "$LOG" 2>/dev/null; then break; fi
        if ! kill -0 "$SBPID" 2>/dev/null; then
            echo "FAIL: sandbox $i exited before TICK $TICKS"
            tail -40 "$LOG"; exit 1
        fi
        sleep 0.05
    done
    if ! grep -qE "^TICK $TICKS[[:space:]]*\$" "$LOG" 2>/dev/null; then
        echo "FAIL: sandbox $i timed out before TICK $TICKS"
        tail -40 "$LOG"; exit 1
    fi
    echo "    reached TICK $TICKS, taking snapshot"

    # snapshot --upload returns the 64-hex manifest key on stdout.
    # --output and --upload are mutually exclusive; we only need the key,
    # so --upload only. --resume=false destroys the sandbox after the
    # snapshot (sandbox-ctl run exits on its own; the kill below is a no-op
    # backstop).
    SNAP_LOG="$WORK/snap-$i.log"
    SNAP_MKEY=$("$BIN/sandbox-ctl" snapshot \
        --sandbox-id "$SID" \
        --upload \
        --run-root "$WORK/runtime" \
        --resume=false 2>"$SNAP_LOG")
    [ ${#SNAP_MKEY} -eq 64 ] || { echo "FAIL: bad SNAP_MKEY for sandbox $i: '$SNAP_MKEY'"; cat "$SNAP_LOG"; exit 1; }
    SNAP_MKEYS[i]="$SNAP_MKEY"
    echo "    snapshot manifest: $SNAP_MKEY"

    # Tear down sandbox; blk1 diff is now flushed (snapshot --resume=false
    # paused vCPUs + flushed vhost workers).
    kill -TERM "$SBPID" 2>/dev/null || true
    wait "$SBPID" 2>/dev/null || true

    # Ingest blk1 diff (the overlay's actual disk content) into the store
    # so we can manifest-diff it against other sandboxes' overlays. store
    # consumes tarstream artifacts (ff88f5f), so wrap the raw diff first;
    # the envelope is a deterministic constant prefix (entry "image", zero
    # mtime/uid/gid, identical size across CoW copies) that dedups away, so
    # the cross-sandbox content dedup measured below is unaffected.
    echo "    ingest blk1 overlay → store"
    "$BIN/flatten-ctl" tar stream -f "$DIFF_FILE.tar" "image:$DIFF_FILE"
    BLK1_MKEY=$("$BIN/manifest-ctl" store \
        --manifest-config "$WORK/accelerator.yaml" \
        --no-progress \
        "$DIFF_FILE.tar")
    [ ${#BLK1_MKEY} -eq 64 ] || { echo "FAIL: bad BLK1_MKEY for sandbox $i: '$BLK1_MKEY'"; exit 1; }
    BLK1_MKEYS[i]="$BLK1_MKEY"
    echo "    blk1 manifest:     $BLK1_MKEY"
done

# ---- pull manifests back to local files for diff ------------------------

echo
echo "==> pulling manifests from store to local files"
LOCAL="$WORK/manifests"
mkdir -p "$LOCAL"
"$BIN/manifest-ctl" get-manifest \
    --manifest-config "$WORK/accelerator.yaml" \
    --output "$LOCAL/blk0.manifest" "$BLK0_MKEY"
for i in $(seq 1 "$N"); do
    "$BIN/manifest-ctl" get-manifest \
        --manifest-config "$WORK/accelerator.yaml" \
        --output "$LOCAL/snap-$i.manifest" "${SNAP_MKEYS[i]}"
    "$BIN/manifest-ctl" get-manifest \
        --manifest-config "$WORK/accelerator.yaml" \
        --output "$LOCAL/blk1-$i.manifest" "${BLK1_MKEYS[i]}"
done

# ---- pairwise diff matrices --------------------------------------------

# Run manifest-ctl diff for every (i,j) i<j pair within a category and
# extract the "shared" / "merged" line from the human-readable output.
# manifest-ctl diff prints something like:
#   shared: 12345 chunks (123.4 MiB)
#   only A: 100 chunks (1.0 MiB)
#   only B: 200 chunks (2.0 MiB)
#   merged unique: 12645 chunks (126.4 MiB)
# We parse "shared" + "merged unique" bytes and compute % = shared / merged.

# Helper: run diff, return "<shared_bytes> <merged_bytes>" on stdout.
# Parse manifest-ctl diff output. The tool prints (one per line):
#   shared:      <N> chunks (X.X UNIT)
#   only in A:   <N> chunks (X.X UNIT)
#   only in B:   <N> chunks (X.X UNIT)
# Returns "<shared_chunks> <merged_unique_chunks>" on stdout, where
# merged_unique = shared + onlyA + onlyB (total distinct chunk hashes
# across the two manifests, excluding zero-runs).
diff_pair() {
    local a="$1" b="$2"
    "$BIN/manifest-ctl" diff "$a" "$b" 2>/dev/null | awk '
        /^shared:[[:space:]]+[0-9]+/    { shared = $2 }
        /^only in A:[[:space:]]+[0-9]+/ { onlyA  = $4 }
        /^only in B:[[:space:]]+[0-9]+/ { onlyB  = $4 }
        END {
            shared = shared + 0; onlyA = onlyA + 0; onlyB = onlyB + 0
            print shared, shared + onlyA + onlyB
        }'
}

# Compute pairwise dedup % for a category.
compute_matrix() {
    local label="$1"
    shift
    local files=("$@")
    local n=${#files[@]}
    local pairs=()
    for ((a=0; a<n; a++)); do
        for ((b=a+1; b<n; b++)); do
            local r
            r=$(diff_pair "${files[a]}" "${files[b]}")
            local shared merged
            shared=$(echo "$r" | awk '{print $1}')
            merged=$(echo "$r" | awk '{print $2}')
            local pct
            if [ "$merged" -gt 0 ]; then
                pct=$(awk -v s="$shared" -v m="$merged" 'BEGIN{printf "%.2f", 100.0*s/m}')
            else
                pct="0.00"
            fi
            pairs+=("$pct")
        done
    done

    if [ ${#pairs[@]} -eq 0 ]; then
        echo "  [$label] no pairs (n=$n)"; return
    fi
    local sorted
    sorted=$(printf '%s\n' "${pairs[@]}" | sort -n)
    local count=${#pairs[@]}
    local mid=$((count / 2))
    local median min max
    if [ $((count % 2)) -eq 1 ]; then
        median=$(echo "$sorted" | sed -n "$((mid+1))p")
    else
        local m1 m2
        m1=$(echo "$sorted" | sed -n "${mid}p")
        m2=$(echo "$sorted" | sed -n "$((mid+1))p")
        median=$(awk -v a="$m1" -v b="$m2" 'BEGIN{printf "%.2f", (a+b)/2}')
    fi
    min=$(printf '%s\n' "$sorted" | awk 'NR == 1 { first = $0 } END { print first }')
    max=$(printf '%s\n' "$sorted" | awk 'NF { last = $0 } END { print last }')
    echo "  [$label] n=$n pairs=$count  median=${median}%  min=${min}%  max=${max}%"
}

count_files() {
    local dir="$1"
    shift
    if [ ! -d "$dir" ]; then
        echo 0
        return
    fi
    python3 - "$dir" "$@" <<'PY'
import fnmatch
import os
import sys

root = sys.argv[1]
patterns = []
args = sys.argv[2:]
i = 0
while i < len(args):
    if args[i] == "-name" and i + 1 < len(args):
        patterns.append(args[i + 1])
        i += 2
    else:
        i += 1

count = 0
for base, _, files in os.walk(root):
    for name in files:
        if patterns and not any(fnmatch.fnmatch(name, pat) for pat in patterns):
            continue
        count += 1
print(count)
PY
}

echo
echo "==> dedup matrix (pair-wise shared / total-distinct chunk count)"
echo "    overlap % = shared / (shared + only_in_A + only_in_B)"
echo "    100% = identical chunk set; 0% = no chunks in common"
echo
echo "  blk0 (image):  trivially shared — single ingest, key=$BLK0_MKEY"
echo

# blk1 overlay matrix
BLK1_FILES=()
for i in $(seq 1 "$N"); do BLK1_FILES+=("$LOCAL/blk1-$i.manifest"); done
compute_matrix "blk1 overlay   " "${BLK1_FILES[@]}"

# memory snapshot matrix
SNAP_FILES=()
for i in $(seq 1 "$N"); do SNAP_FILES+=("$LOCAL/snap-$i.manifest"); done
compute_matrix "memory snapshot" "${SNAP_FILES[@]}"

# ---- store-side aggregate stats ----------------------------------------

echo
echo "==> store-side: total chunks ingested vs pairwise sum"
total_chunks_in_store=$(count_files "$WORK/store-data" -name '*.chunk')
total_chunks_in_store_alt=$(count_files "$WORK/store-data/chunk")
[ "$total_chunks_in_store" -eq 0 ] && total_chunks_in_store="$total_chunks_in_store_alt"
sum_per_manifest=0
for f in "$LOCAL/blk0.manifest" "${BLK1_FILES[@]}" "${SNAP_FILES[@]}"; do
    n=$("$BIN/manifest-ctl" info "$f" 2>/dev/null | awk '/^chunk count:/ { n = $3 } END { if (n != "") print n }')
    if [ -n "$n" ] && [ "$n" -eq "$n" ] 2>/dev/null; then
        sum_per_manifest=$((sum_per_manifest + n))
    fi
done
echo "    unique chunks in store:    $total_chunks_in_store"
echo "    sum chunks across manifests: $sum_per_manifest"
if [ "$sum_per_manifest" -gt 0 ] && [ "$total_chunks_in_store" -gt 0 ]; then
    overall_dedup=$(awk -v u="$total_chunks_in_store" -v s="$sum_per_manifest" \
        'BEGIN{printf "%.2f", 100.0*(1.0 - u/s)}')
    echo "    overall dedup (1 - unique/sum):  ${overall_dedup}%"
fi

echo
echo "==> e2e_warmpool_dedup: OK"
