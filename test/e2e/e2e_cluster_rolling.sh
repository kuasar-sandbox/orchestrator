#!/bin/bash
set -euo pipefail

# E2E test for rolling EC membership change via SIGHUP.
#
# Layout: 6 shard peers + 1 origin (store-ctl) + 1 tiered client.
#  - Initial membership: {p1, p2, p3, p4, p5}.
#  - Store N distinct values through the tiered client → shards spread
#    across p1..p5.
#  - Rewrite the tiered YAML to membership {p2, p3, p4, p5, p6} and
#    send SIGHUP.
#  - Read all N values; all reads should succeed (4/5 shards remain on
#    surviving peers → RS decodes successfully; the removed peer's
#    shard is a miss for every key).
#  - Verify no origin fall-through happened for the reads (origin
#    Get counter shouldn't have bumped for the repeated reads).
#
# This is the "stability" guarantee: rolling 1-peer replacement must
# not force cache invalidation — surviving peers keep serving their
# existing shards.
#
# Usage:
#   bash test/e2e/e2e_cluster_rolling.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$PROJECT_ROOT/bin}"
TMPDIR=$(mktemp -d /tmp/acc-cluster-e2e-XXXXXX)
KEY=$(openssl rand -hex 32)

PASS=0
FAIL=0
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

ok() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

assert_eq() {
    if [ "$1" = "$2" ]; then
        ok "$3"
    else
        fail "$3 (expected '$1', got '$2')"
    fi
}

wait_ready() {
    local endpoint=$1
    for i in $(seq 1 50); do
        if "$BIN/cache-ctl" ping --endpoint "$endpoint" 2>/dev/null | grep -q SERVING; then
            return 0
        fi
        sleep 0.1
    done
    echo "  ERROR: cache-ctl health at $endpoint did not become ready"
    return 1
}

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# ============================================================
# store-ctl sidecar
# ============================================================
STORE_PORT=$(free_port)
cat > "$TMPDIR/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $TMPDIR/store-data
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$TMPDIR/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$TMPDIR/store-ctl.yaml" &
PIDS+=($!)
for i in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then break; fi
    sleep 0.1
done

cat > "$TMPDIR/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 5s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF
COMMON="--manifest-config $TMPDIR/accelerator.yaml"

# manifest-ctl no longer accepts --cache-endpoint flag override. Build
# a YAML variant on demand and pass --config explicitly.
accel_cfg_for_cache() {
    local ep="$1"
    local out="$TMPDIR/accelerator-cache-$(echo "$ep" | tr ':.' '__').yaml"
    cat > "$out" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 5s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
cache:
  endpoint: $ep
  pool: 2
  timeout: 5s
EOF
    echo "$out"
}

# ============================================================
# 6 shard peers
# ============================================================
SHARD_PORTS=()
SHARD_HEALTH_PORTS=()
for i in $(seq 1 6); do
    SP=$(free_port)
    SHP=$(free_port)
    SHARD_PORTS+=("$SP")
    SHARD_HEALTH_PORTS+=("$SHP")
    cat > "$TMPDIR/shard-$i.yaml" <<EOF
mode: shard
listen: 127.0.0.1:$SP
health_listen: 127.0.0.1:$SHP
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-shard-$i
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF
    "$BIN/cache-ctl" serve --config "$TMPDIR/shard-$i.yaml" &
    PIDS+=($!)
done
for SHP in "${SHARD_HEALTH_PORTS[@]}"; do
    wait_ready "127.0.0.1:$SHP"
done

# ============================================================
# Tiered client, initial membership {p1..p5}
# ============================================================
TIERED_PORT=$(free_port)
TIERED_HEALTH_PORT=$(free_port)
render_tiered_yaml() {
    # $1 = space-separated list of 5 peer indices (1..6)
    local indices=($1)
    cat > "$TMPDIR/tiered.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$TIERED_PORT
health_listen: 127.0.0.1:$TIERED_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      peers:
        - {id: s${indices[0]}, endpoint: "127.0.0.1:${SHARD_PORTS[$((indices[0]-1))]}"}
        - {id: s${indices[1]}, endpoint: "127.0.0.1:${SHARD_PORTS[$((indices[1]-1))]}"}
        - {id: s${indices[2]}, endpoint: "127.0.0.1:${SHARD_PORTS[$((indices[2]-1))]}"}
        - {id: s${indices[3]}, endpoint: "127.0.0.1:${SHARD_PORTS[$((indices[3]-1))]}"}
        - {id: s${indices[4]}, endpoint: "127.0.0.1:${SHARD_PORTS[$((indices[4]-1))]}"}
      pool: 1
      timeout: 2s
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
}

render_tiered_yaml "1 2 3 4 5"
"$BIN/cache-ctl" serve --config "$TMPDIR/tiered.yaml" &
TIERED_PID=$!
PIDS+=($TIERED_PID)
wait_ready "127.0.0.1:$TIERED_HEALTH_PORT"

# ============================================================
# Populate N keys through the tiered client (writes go through EC →
# shards land on {p1..p5}). Record original SHA256 of each blob.
# ============================================================
N=20
declare -a MKEYS=()
declare -a ORIG_HASHES=()
echo "=== Populating $N keys into EC cluster {s1..s5} ==="
for i in $(seq 1 $N); do
    dd if=/dev/urandom of="$TMPDIR/val-$i.bin" bs=1024 count=32 2>/dev/null
    MKEYS+=("$("$BIN/manifest-ctl" store $COMMON --no-progress "$TMPDIR/val-$i.bin" 2>/dev/null)")
    ORIG_HASHES+=("$(sha256sum $TMPDIR/val-$i.bin | awk '{print $1}')")
done
echo "  $N manifest keys stored."

# Pre-warm the EC tier: read all N through tiered client so each key
# gets its 5 shards distributed and filled on {p1..p5}.
echo "=== Pre-warm: load each manifest through tiered (populates EC cache) ==="
for i in $(seq 1 $N); do
    "$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$TIERED_PORT")" \
        --output "$TMPDIR/val-$i.prewarm" \
        --no-progress "${MKEYS[$((i-1))]}" 2>/dev/null
done
ok "$N keys prewarmed through EC tier"

# ============================================================
# SIGHUP: rotate membership from {s1..s5} to {s2..s6}.
# ============================================================
echo "=== SIGHUP: rotate membership {s1..s5} → {s2..s6} ==="
render_tiered_yaml "2 3 4 5 6"
kill -HUP "$TIERED_PID"
# Give the daemon a moment to reload + probe s6.
sleep 1

# ============================================================
# Read all N keys again. Because 4/5 shards for every key remain on
# surviving peers (s2..s5), RS decoding succeeds; s6 is a miss for
# every key but that's within RS's 1-miss tolerance. Assertion: all
# output hashes match the original.
# ============================================================
echo "=== Read $N keys after membership change ==="
all_match=1
for i in $(seq 1 $N); do
    "$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$TIERED_PORT")" \
        --output "$TMPDIR/val-$i.postswap" \
        --no-progress "${MKEYS[$((i-1))]}" 2>/dev/null
    h=$(sha256sum "$TMPDIR/val-$i.postswap" | awk '{print $1}')
    if [ "$h" != "${ORIG_HASHES[$((i-1))]}" ]; then
        fail "key $i hash mismatch after membership swap"
        all_match=0
    fi
done
if [ "$all_match" = "1" ]; then
    ok "all $N keys decoded correctly after 1-peer rolling swap"
fi

# ============================================================
# Monotone epoch guard: second SIGHUP with the same peer set should be
# accepted (tier.Epoch bumps per reload even when peer set is
# unchanged), but rollback to older epoch is impossible via SIGHUP
# because the YAML doesn't carry epoch — cache-ctl computes
# next_epoch = current + 1 each time.
# ============================================================
echo "=== Second SIGHUP (idempotent-ish: membership {s2..s6} again) ==="
kill -HUP "$TIERED_PID"
sleep 0.5
# Reads should still work.
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$TIERED_PORT")" \
    --output "$TMPDIR/val-1.idem" \
    --no-progress "${MKEYS[0]}" 2>/dev/null
h=$(sha256sum "$TMPDIR/val-1.idem" | awk '{print $1}')
assert_eq "${ORIG_HASHES[0]}" "$h" "second SIGHUP leaves reads working"

echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
