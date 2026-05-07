#!/bin/bash
set -euo pipefail

# E2E test for cache-ctl + manifest-ctl integration.
# Tests local mode, shard mode, and tiered (embedded + EC) mode.
#
# Usage:
#   bash test/e2e/e2e_cache.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$PROJECT_ROOT/bin}"
TMPDIR=$(mktemp -d /tmp/acc-cache-e2e-XXXXXX)
KEY=$(openssl rand -hex 32)

PASS=0
FAIL=0
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

ok() {
    PASS=$((PASS + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    echo "  FAIL: $1"
}

assert_eq() {
    if [ "$1" = "$2" ]; then
        ok "$3"
    else
        fail "$3 (expected '$1', got '$2')"
    fi
}

# Wait for a cache-ctl health endpoint to become healthy.
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

# Allocate a free port.
free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# Prepare a small test file (deterministic, ~512 KiB).
dd if=/dev/urandom of="$TMPDIR/test.bin" bs=1024 count=512 2>/dev/null

# ============================================================
# Spin up store-ctl sidecar. All subsequent manifest-ctl / cache-ctl
# tests talk to the store only through this daemon's gRPC endpoint.
# ============================================================
echo ""
echo "=== Spin up store-ctl sidecar ==="
STORE_PORT=$(free_port)
STORE_ROOT="$TMPDIR/store-data"
cat > "$TMPDIR/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $STORE_ROOT
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$TMPDIR/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$TMPDIR/store-ctl.yaml" &
STORE_PID=$!
PIDS+=($STORE_PID)
# Poll for the gRPC port to be accepting connections.
for i in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then
        break
    fi
    sleep 0.1
done
echo "  store-ctl listen=127.0.0.1:$STORE_PORT root=$STORE_ROOT"

# Write config for manifest-ctl. The store endpoint points at the
# store-ctl daemon above; there is no `backend: fs` notion anymore.
cat > "$TMPDIR/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 5s
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

COMMON="--config $TMPDIR/accelerator.yaml"

# Store test data via store-ctl.
echo ""
echo "=== Ingest test data (via store-ctl) ==="
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/test.bin" --manifest "$TMPDIR/test.manifest" --no-progress 2>&1
ORIG_HASH=$(sha256sum "$TMPDIR/test.bin" | awk '{print $1}')
echo "  Stored. SHA256=$ORIG_HASH"

# ============================================================
echo ""
echo "=== Test 0: store-ctl standalone roundtrip ==="
# Write + read a distinct manifest through manifest-ctl → store-ctl
# to prove the standalone path is healthy before any cache-ctl
# tests run. Uses a fresh 64 KiB payload so it doesn't collide with
# the main 512 KiB blob's chunks.
dd if=/dev/urandom of="$TMPDIR/store0.bin" bs=1024 count=64 2>/dev/null
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/store0.bin" --manifest "$TMPDIR/store0.manifest" --no-progress 2>&1
MKEY0=$("$BIN/manifest-ctl" put-manifest $COMMON --input "$TMPDIR/store0.manifest")
"$BIN/manifest-ctl" get-manifest $COMMON --key "$MKEY0" --output "$TMPDIR/store0.manifest.rt" 2>&1
"$BIN/manifest-ctl" load $COMMON --manifest "$TMPDIR/store0.manifest" --output "$TMPDIR/store0.rt" --no-progress 2>&1
H_ORIG=$(sha256sum "$TMPDIR/store0.bin" | awk '{print $1}')
H_RT=$(sha256sum "$TMPDIR/store0.rt" | awk '{print $1}')
assert_eq "$H_ORIG" "$H_RT" "store-ctl standalone store + put-manifest + load roundtrip"

# ============================================================
echo ""
echo "=== Test 1: local mode — object put/get roundtrip ==="
LOCAL_PORT=$(free_port)
LOCAL_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/local.yaml" <<EOF
mode: local
listen: 127.0.0.1:$LOCAL_PORT
health_listen: 127.0.0.1:$LOCAL_HEALTH_PORT
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-local
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/local.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$LOCAL_HEALTH_PORT"

TEST_HASH="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
echo -n "test-value-local" | "$BIN/cache-ctl" object put --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" --value -
GOT=$("$BIN/cache-ctl" object get --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH")
assert_eq "test-value-local" "$GOT" "local object put/get roundtrip"

# ============================================================
echo ""
echo "=== Test 2: local mode — shard put/get also works (unified handler) ==="
# shard put attaches [idx][total] prefix to the raw data before sending;
# shard get parses the prefix off and echoes only the raw body to stdout,
# so the assertion matches the input verbatim.
echo -n "shard-on-local" | "$BIN/cache-ctl" shard put --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" --idx 0 --total 5 --value -
GOT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" 2>/dev/null)
assert_eq "shard-on-local" "$GOT" "shard put/get on local mode (unified handler)"

# ============================================================
echo ""
echo "=== Test 3: shard mode — shard put/get roundtrip ==="
SHARD_PORT=$(free_port)
SHARD_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/shard.yaml" <<EOF
mode: shard
listen: 127.0.0.1:$SHARD_PORT
health_listen: 127.0.0.1:$SHARD_HEALTH_PORT
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-shard
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/shard.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$SHARD_HEALTH_PORT"

echo -n "shard-data-idx3" | "$BIN/cache-ctl" shard put --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" --idx 3 --total 5 --value -
GOT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" 2>/dev/null)
assert_eq "shard-data-idx3" "$GOT" "shard put/get roundtrip"

# ============================================================
echo ""
echo "=== Test 4: shard mode — object put/get also works (unified handler) ==="
echo -n "obj-on-shard" | "$BIN/cache-ctl" object put --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" --value -
GOT=$("$BIN/cache-ctl" object get --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH")
assert_eq "obj-on-shard" "$GOT" "object put/get on shard mode (unified handler)"

# ============================================================
echo ""
echo "=== Test 5: tiered mode (embedded only) — manifest-ctl load through cache ==="
TIERED_PORT=$(free_port)
TIERED_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/tiered.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$TIERED_PORT
health_listen: 127.0.0.1:$TIERED_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $TMPDIR/rocks-tiered
      disk_bytes: 1GiB
      mem_ratio: 0.05
      direct_reads: false
      bloom_bits: 10
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/tiered.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$TIERED_HEALTH_PORT"

# Load via cache (first read — cold, fills embedded from origin).
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$TIERED_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-cached.bin" --no-progress 2>&1
CACHED_HASH=$(sha256sum "$TMPDIR/test-cached.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$CACHED_HASH" "tiered load roundtrip (cold)"

# ============================================================
echo ""
echo "=== Test 6: tiered mode — warm read (second load hits embedded cache) ==="
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$TIERED_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-warm.bin" --no-progress 2>&1
WARM_HASH=$(sha256sum "$TMPDIR/test-warm.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$WARM_HASH" "tiered load roundtrip (warm)"

# ============================================================
echo ""
echo "=== Test 7: tiered mode — object put returns error (writes not supported) ==="
PUT_OUT=$("$BIN/cache-ctl" object put --endpoint "127.0.0.1:$TIERED_PORT" --namespace chunk --hash "$TEST_HASH" --value /dev/null 2>&1 || true)
if echo "$PUT_OUT" | grep -qi "not supported\|error"; then
    ok "object put on tiered mode returns error"
else
    fail "object put on tiered mode should return error (got: $PUT_OUT)"
fi

# ============================================================
echo ""
echo "=== Test 8: 5-node shard cluster + tiered EC mode ==="

SHARD_PORTS=()
SHARD_HEALTH_PORTS=()
for i in $(seq 1 5); do
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

# Wait for all shard nodes.
for SHP in "${SHARD_HEALTH_PORTS[@]}"; do
    wait_ready "127.0.0.1:$SHP"
done

# Tiered with EC.
EC_TIERED_PORT=$(free_port)
EC_TIERED_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/tiered-ec.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$EC_TIERED_PORT
health_listen: 127.0.0.1:$EC_TIERED_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $TMPDIR/rocks-tiered-ec
      disk_bytes: 1GiB
      mem_ratio: 0.05
      direct_reads: false
      bloom_bits: 10
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      peers:
        - {id: s1, endpoint: "127.0.0.1:${SHARD_PORTS[0]}"}
        - {id: s2, endpoint: "127.0.0.1:${SHARD_PORTS[1]}"}
        - {id: s3, endpoint: "127.0.0.1:${SHARD_PORTS[2]}"}
        - {id: s4, endpoint: "127.0.0.1:${SHARD_PORTS[3]}"}
        - {id: s5, endpoint: "127.0.0.1:${SHARD_PORTS[4]}"}
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

"$BIN/cache-ctl" serve --config "$TMPDIR/tiered-ec.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$EC_TIERED_HEALTH_PORT"

# Load through EC tiered cache.
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$EC_TIERED_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-ec.bin" --no-progress 2>&1
EC_HASH=$(sha256sum "$TMPDIR/test-ec.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$EC_HASH" "tiered+EC load roundtrip"

# ============================================================
echo ""
echo "=== Test 9: EC failure injection — kill 1 shard node ==="
# Kill shard node 1 (index 3 in PIDS: 0=local, 1=shard, 2=tiered, 3..7=shard-1..5).
SHARD1_PID_IDX=3
kill "${PIDS[$SHARD1_PID_IDX]}" 2>/dev/null; wait "${PIDS[$SHARD1_PID_IDX]}" 2>/dev/null || true

# Second load (warm from embedded + EC with 1 node down — should still work).
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$EC_TIERED_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-ec-1down.bin" --no-progress 2>&1
DOWN1_HASH=$(sha256sum "$TMPDIR/test-ec-1down.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$DOWN1_HASH" "tiered+EC load with 1 shard down"

# ============================================================
echo ""
echo "=== Test 10: tiered + upstream — read-through + writeback ==="
# Two-process layout:
#   remote = local mode (terminal; its own RocksDB is the backing store)
#   front  = tiered mode with a single upstream tier pointing at remote
#            (NO embedded — otherwise reads would never reach upstream)
# Cold load through front:
#   front.Get(chunk) → upstream.Get → miss → origin.Get → HIT (fs backend)
#                    → TieredCache.startFill writes back into upstream
#                    → client.Tier.Fill → writer.Put → remote.Put → rocks
# Then kill front and read the same manifest directly from remote.
# remote is local mode, no origin — it can only serve chunks that were
# written during the writeback phase. Hash match ⇒ writeback works.
REMOTE_PORT=$(free_port)
REMOTE_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/upstream-remote.yaml" <<EOF
mode: local
listen: 127.0.0.1:$REMOTE_PORT
health_listen: 127.0.0.1:$REMOTE_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-upstream-remote
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/upstream-remote.yaml" &
REMOTE_PID=$!
PIDS+=($REMOTE_PID)
wait_ready "127.0.0.1:$REMOTE_HEALTH_PORT"

FRONT_PORT=$(free_port)
FRONT_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/upstream-front.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$FRONT_PORT
health_listen: 127.0.0.1:$FRONT_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: upstream
    endpoint: 127.0.0.1:$REMOTE_PORT
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
"$BIN/cache-ctl" serve --config "$TMPDIR/upstream-front.yaml" &
FRONT_PID=$!
PIDS+=($FRONT_PID)
wait_ready "127.0.0.1:$FRONT_HEALTH_PORT"

# Cold read through front: fill chain runs all the way to origin and
# writes back through upstream into remote.
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$FRONT_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-upstream.bin" --no-progress 2>&1
UP_HASH=$(sha256sum "$TMPDIR/test-upstream.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$UP_HASH" "upstream tier read-through (cold)"

# Kill front; remote must now serve the same manifest on its own.
# makeCacheReader has no local-store fall-through — if writeback did
# not populate remote, the next load fails with "chunk not found".
kill $FRONT_PID 2>/dev/null; wait $FRONT_PID 2>/dev/null || true
"$BIN/manifest-ctl" load $COMMON --cache-endpoint "127.0.0.1:$REMOTE_PORT" \
    --manifest "$TMPDIR/test.manifest" --output "$TMPDIR/test-from-remote.bin" --no-progress 2>&1
REM_HASH=$(sha256sum "$TMPDIR/test-from-remote.bin" | awk '{print $1}')
assert_eq "$ORIG_HASH" "$REM_HASH" "upstream tier writeback populated remote"

# ============================================================
echo ""
echo "=== Test 11: upstream dial failure rejects cache-ctl startup ==="
# 127.0.0.1:1 is RFC-reserved and refuses connections immediately, so
# DialConnPool fails at the first dial and buildTieredChain rolls back.
# serve then calls fatal → os.Exit(1). Verify the process exits non-zero
# and the error message names the failure site.
BAD_PORT=$(free_port)
BAD_HEALTH_PORT=$(free_port)
cat > "$TMPDIR/upstream-bad.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$BAD_PORT
health_listen: 127.0.0.1:$BAD_HEALTH_PORT
rpc_timeout: 2s
tiers:
  - type: upstream
    endpoint: 127.0.0.1:1
    pool: 1
    timeout: 500ms
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
# Run in foreground; do NOT push PID to PIDS (process is expected to
# exit on its own).
if ERR_OUT=$("$BIN/cache-ctl" serve --config "$TMPDIR/upstream-bad.yaml" 2>&1); then
    fail "upstream bad endpoint should have rejected startup (exited 0; output: $ERR_OUT)"
else
    if echo "$ERR_OUT" | grep -qiE "dial reader|dial writer|upstream.*connection refused|connection refused"; then
        ok "upstream bad endpoint rejected at startup"
    else
        fail "upstream bad endpoint error should mention dial/connection refused (got: $ERR_OUT)"
    fi
fi

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
