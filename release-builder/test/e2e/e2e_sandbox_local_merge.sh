#!/usr/bin/env bash
#
# e2e_sandbox_local_merge.sh — the local-layer invariant (docs/sandbox.md §3.5):
#
#   1. store-ctl + cache-ctl tiered, a TICK guest that writes /ticks.dat
#   2. cold-start → snapshot --output  → s1 (LOCAL file, from_refs=[])
#   3. restore from s1 (LOCAL path) → snapshot --output → s2 (LOCAL): the local
#      "replace the next-newest layer" MERGE — assert s2.snapshot.cfg from_refs
#      is EMPTY (the parent s1 was MERGED into s2's top, NOT stacked) and that
#      restoring s2 builds a SINGLE memory layer (not 2). TICK continuity + the
#      /ticks.dat blk0 cold marker (written before s1, merged into s2) prove the
#      merge preserved both layers' data while keeping local depth at 1.
#   4. upload-snapshot s2 (offline, no boot) → manifest://<key>, then
#      run --restore=manifest://<key>: the local→remote promotion round-trips.
#
# Self-skips without /dev/kvm + the built stack (same prereqs as the other
# sandbox e2e). REQUIRE_KVM=1 turns skips into failures.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> e2e_sandbox_local_merge: skipping ($*)"
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
if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

TAP_NAME="${TAP_NAME:-sb-tapm0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

WORK="$(mktemp -d /tmp/e2e-snap-merge-XXXXXX)"
PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
    if [ -n "${E2E_KEEP:-}" ]; then echo "kept: $WORK"; else rm -rf "$WORK"; fi
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null || true
}
trap cleanup EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'; }
KEY=$(openssl rand -hex 32)

# ---- daemons (store-ctl + cache-ctl tiered) ------------------------------
STORE_PORT=$(free_port)
cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store-data, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" >"$WORK/store.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null && break; sleep 0.1; done

CACHE_PORT=$(free_port); CACHE_HEALTH_PORT=$(free_port)
cat > "$WORK/cache-ctl.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$CACHE_PORT
health_listen: 127.0.0.1:$CACHE_HEALTH_PORT
rpc_timeout: 5s
freq: { counters: 1M, reset_after: 100K }
tiers:
  - type: embedded
    rocks: { path: $WORK/cache-rocks, disk_bytes: 2GiB, mem_ratio: 0.1, direct_reads: false, bloom_bits: 10 }
origin:
  type: store
  store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 2, timeout: 5s }
  max_inflight: 16
EOF
"$BIN/cache-ctl" serve --config "$WORK/cache-ctl.yaml" >"$WORK/cache.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null | grep -q SERVING && break; sleep 0.1; done

cat > "$WORK/accelerator.yaml" <<EOF
manifest: { key: "$KEY" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: 127.0.0.1:$CACHE_PORT, pool: 4, timeout: 10s }
chunker: { mode: cdc }
crypto: { chunk: aes, manifest: aes }
EOF

# ---- guest base + TICK workload (writes /ticks.dat blk0 cold marker) ------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then docker pull "$IMAGE" >/dev/null; fi
BLK0_EROFS="$WORK/blk0.img"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_EROFS" --no-progress
BLK0_REF="$(plaintext_tarstream_ref "$BLK0_EROFS")"
mkdir -p "$WORK/runtime"; DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"; mkfs.ext4 -q -F "$DIFF_FILE"

PYTICK='import os,time
print("PYBOOT-OK", flush=True)
fd=os.open("/ticks.dat", os.O_RDWR|os.O_CREAT, 0o644)
i=0
while True:
    os.pwrite(fd, ("TICK%08d" % i).encode().ljust(4096, b"."), i*4096)
    os.fsync(fd)
    blk0=os.pread(fd, 12, 0).decode()
    print("TICK %d DISK blk0=%s" % (i, blk0), flush=True)
    i+=1
    time.sleep(0.25)'
write_yaml() { # $1=out $2=hostname [$3=diff override] [$4=base override; "none" omits]
    local base_line="    base: $BLK0_REF"
    case "${4:-}" in
    none) base_line="" ;;       # manifest:// snapshots carry their own base ref
    "") ;;
    *) base_line="    base: $4" ;;
    esac
    cat > "$1" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: $2 }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
$base_line
    overlay: { diff: file://${3:-$DIFF_FILE}, size: 1GiB }
launch:
  args: ["-c", $(python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))" <<<"$PYTICK")]
  restart: never
EOF
}
BLK0_OK="DISK blk0=TICK00000000"

run_until() { # $1=logfile $2=pidvar $3=want-tick-regex ; sets the pid into $2
    local log="$1" want="$3"
    for _ in $(seq 1 600); do
        grep -qE "$want" "$log" 2>/dev/null && return 0
        kill -0 "${!2}" 2>/dev/null || { echo "FAIL: sandbox exited early"; tail -40 "$log"; exit 1; }
        sleep 0.05
    done
    echo "FAIL: never saw /$want/"; tail -40 "$log"; exit 1
}

# ---- phase 1: cold-start → snapshot --output (s1, LOCAL) -----------------
echo "==> phase 1: cold-start → snapshot --output (s1 local)"
write_yaml "$WORK/sandbox.yaml" e2e-merge
SNAPDIR="$WORK/snaps"; mkdir -p "$SNAPDIR"
LOG1="$WORK/run1.log"; SID1="m1-$$"; mkdir -p "$WORK/runtime/$SID1"
"$BIN/sandbox-ctl" run --config "$WORK/sandbox.yaml" --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID1" >"$LOG1" 2>&1 &
SBPID1=$!; PIDS+=($SBPID1)
run_until "$LOG1" SBPID1 "^TICK 10 $BLK0_OK\$"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID1" --output "$SNAPDIR" --run-root "$WORK/runtime" 2>"$WORK/snap1.log"
wait "$SBPID1" 2>/dev/null || true
S1="$SNAPDIR/$SID1.snapshot"
[ -e "$S1" ] || { echo "FAIL: no $S1"; cat "$WORK/snap1.log"; exit 1; }
echo "    s1 from_refs: $("$BIN/sandbox-ctl" info --json "$S1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["FromRefs"] or [])')"

# ---- phase 2: restore s1 (LOCAL) → snapshot --output (s2 = MERGE) --------
echo "==> phase 2: restore s1 (local) → snapshot --output (s2, merge replaces s1)"
write_yaml "$WORK/host2.yaml" e2e-merge2 "$WORK/runtime/blk1-r2.diff"
truncate -s 1G "$WORK/runtime/blk1-r2.diff"
LOG2="$WORK/run2.log"; SID2="m2-$$"; mkdir -p "$WORK/runtime/$SID2"
"$BIN/sandbox-ctl" run --restore "$S1" --config "$WORK/host2.yaml" --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID2" >"$LOG2" 2>&1 &
SBPID2=$!; PIDS+=($SBPID2)
S1_TICK=$(grep -oE "^TICK [0-9]+" "$LOG1" | tail -1 | awk '{print $2}')
run_until "$LOG2" SBPID2 "^TICK $((S1_TICK+3)) $BLK0_OK\$"
"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID2" --output "$SNAPDIR" --run-root "$WORK/runtime" 2>"$WORK/snap2.log"
wait "$SBPID2" 2>/dev/null || true
S2="$SNAPDIR/$SID2.snapshot"
# THE KEY ASSERTION: s2 MERGED s1, so its from_refs is EMPTY (not [s1.snapshot]).
S2_FROMREFS=$("$BIN/sandbox-ctl" info --json "$S2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["FromRefs"] or [])')
echo "    s2 from_refs: $S2_FROMREFS"
[ "$S2_FROMREFS" = "[]" ] || { echo "FAIL: s2 from_refs=$S2_FROMREFS, want [] (merge should REPLACE s1, not stack it)"; exit 1; }

# ---- phase 3: restore s2 (LOCAL) — must be 1-layer + data intact ----------
echo "==> phase 3: restore s2 (local) — single layer (merged), blk0 fall-through intact"
write_yaml "$WORK/host3.yaml" e2e-merge3 "$WORK/runtime/blk1-r3.diff"
truncate -s 1G "$WORK/runtime/blk1-r3.diff"
LOG3="$WORK/run3.log"; SID3="m3-$$"; mkdir -p "$WORK/runtime/$SID3"
"$BIN/sandbox-ctl" run --restore "$S2" --config "$WORK/host3.yaml" --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID3" >"$LOG3" 2>&1 &
SBPID3=$!; PIDS+=($SBPID3)
S2_TICK=$(grep -oE "^TICK [0-9]+" "$LOG2" | tail -1 | awk '{print $2}')
run_until "$LOG3" SBPID3 "^TICK $((S2_TICK+3)) $BLK0_OK\$"
grep -qE "snapshot source: 1 memory layer" "$LOG3" || { echo "FAIL: s2 restore not 1-layer (merge didn't flatten?)"; grep -E 'memory layer' "$LOG3"; exit 1; }
echo "    PASS: s2 restored as 1 memory layer; blk0 cold marker intact (merge preserved s1's data)"
kill -TERM "$SBPID3" 2>/dev/null; wait "$SBPID3" 2>/dev/null || true   # infinite TICK loop — kill, don't wait

# ---- phase 4: upload-snapshot s2 (offline) → restore manifest:// ----------
echo "==> phase 4: upload-snapshot s2 (offline, no boot) → restore from manifest://"
# upload-snapshot auto-uploads every local artifact the cfg references;
# file:// refs resolve as bundle-dir siblings, so co-locate the base image.
ln -f "$BLK0_EROFS" "$SNAPDIR/$(basename "$BLK0_EROFS")"
MKEY=$("$BIN/sandbox-ctl" upload-snapshot --manifest-config "$WORK/accelerator.yaml" --quiet "$S2")
MKEY=${MKEY#manifest://}
[ ${#MKEY} -eq 64 ] || { echo "FAIL: upload-snapshot key len=${#MKEY}, want 64"; exit 1; }
echo "    uploaded s2 → manifest://$MKEY"
write_yaml "$WORK/host4.yaml" e2e-merge4 "$WORK/runtime/blk1-r4.diff" none
truncate -s 1G "$WORK/runtime/blk1-r4.diff"
LOG4="$WORK/run4.log"; SID4="m4-$$"; mkdir -p "$WORK/runtime/$SID4"
"$BIN/sandbox-ctl" run --restore "manifest://$MKEY" --config "$WORK/host4.yaml" --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/runtime" --sandbox-id "$SID4" >"$LOG4" 2>&1 &
SBPID4=$!; PIDS+=($SBPID4)
run_until "$LOG4" SBPID4 "^TICK $((S2_TICK+3)) $BLK0_OK\$"
echo "    PASS: restore from offline-uploaded manifest://$MKEY reached TICK $((S2_TICK+3)); blk0 intact"
kill -TERM "$SBPID4" 2>/dev/null; wait "$SBPID4" 2>/dev/null || true

echo
echo "==> e2e_sandbox_local_merge: OK (local merge keeps depth 1; offline upload round-trips)"
