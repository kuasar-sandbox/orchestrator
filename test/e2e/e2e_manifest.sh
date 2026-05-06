#!/bin/bash
set -euo pipefail

# E2E test for manifest-ctl and flatten-ctl using real Docker images.
# Usage:
#   bash test/e2e/e2e_manifest.sh
#   IMAGE_A=alpine:3.19 IMAGE_B=alpine:3.20 bash test/e2e/e2e_manifest.sh

IMAGE_A="${IMAGE_A:-python:3.12-slim}"
IMAGE_B="${IMAGE_B:-python:3.12-alpine}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin"
TMPDIR=$(mktemp -d /tmp/acc-e2e-XXXXXX)
KEY=$(openssl rand -hex 32)

PASS=0
FAIL=0
STORE_PID=""

cleanup() {
    if [ -n "$STORE_PID" ]; then
        kill "$STORE_PID" 2>/dev/null || true
        wait "$STORE_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

# Allocate a free TCP port via Python (same helper as e2e_cache.sh).
free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

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

# Ensure a Docker image is available locally; pull on miss. Exits
# non-zero with a clear message if the pull fails so that callers
# don't get a silent empty `docker save` stream feeding flatten-ctl.
ensure_image() {
    local img=$1
    if docker image inspect "$img" >/dev/null 2>&1; then
        return 0
    fi
    echo "  pulling $img ..." >&2
    if ! docker pull "$img"; then
        echo "  ERROR: failed to pull $img" >&2
        exit 1
    fi
}

# ============================================================
# Spin up store-ctl sidecar. manifest-ctl no longer touches the
# filesystem directly — every chunk / manifest I/O goes through
# this daemon's gRPC endpoint.
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
# Poll the gRPC port for readiness (5x100ms).
for i in 1 2 3 4 5; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then
        break
    fi
    sleep 0.1
done
echo "  store-ctl listen=127.0.0.1:$STORE_PORT root=$STORE_ROOT"

# Write manifest-ctl config (YAML, points at store-ctl's gRPC endpoint)
cat > "$TMPDIR/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 10s
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

COMMON="--config $TMPDIR/accelerator.yaml"

# Ensure both images are available locally before any test starts;
# eager-pull avoids dragging a multi-minute `docker pull` progress
# bar through the middle of a numbered test, and lets the script
# fail fast with a clear error if a tag is wrong.
echo ""
echo "=== Ensure docker images ==="
ensure_image "$IMAGE_A"
ensure_image "$IMAGE_B"
echo "  $IMAGE_A and $IMAGE_B available locally"

# ============================================================
echo ""
echo "=== Test 1: Flatten $IMAGE_A ==="
docker save "$IMAGE_A" | "$BIN/flatten-ctl" --output "$TMPDIR/image-a.erofs" --no-progress 2>&1
SIZE=$(stat --printf="%s" "$TMPDIR/image-a.erofs" 2>/dev/null || stat -f "%z" "$TMPDIR/image-a.erofs")
if [ "$SIZE" -gt 0 ]; then
    ok "flatten produced $SIZE bytes"
else
    fail "flatten produced empty output"
fi

# Trailing ZIP must contain config.json (standard unzip works because
# ZIP EOCD scan tolerates the EROFS prefix). Note: unzip exits 1 with
# a warning about the prefix data but still produces a valid listing —
# capture into a variable to neutralise pipefail interaction.
ZIP_LIST=$(unzip -l "$TMPDIR/image-a.erofs" 2>/dev/null || true)
if echo "$ZIP_LIST" | grep -q '\bconfig\.json\b'; then
    ok "trailing ZIP contains config.json"
else
    fail "trailing ZIP missing or no config.json entry"
fi

# `flatten-ctl info --json` should report a valid erofs_size and a
# non-null Architecture (Docker images we test with always carry it).
INFO_JSON=$("$BIN/flatten-ctl" info --json "$TMPDIR/image-a.erofs")
EROFS_SIZE=$(echo "$INFO_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["erofs_size"])')
ARCH=$(echo "$INFO_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["config"].get("Architecture",""))')
if [ "$EROFS_SIZE" -gt 0 ] && [ "$EROFS_SIZE" -lt "$SIZE" ]; then
    ok "info --json: erofs_size=$EROFS_SIZE < total file size $SIZE (ZIP appended after EROFS)"
else
    fail "info --json: erofs_size=$EROFS_SIZE inconsistent with file size $SIZE"
fi
if [ -n "$ARCH" ]; then
    ok "info --json: Architecture=$ARCH"
else
    fail "info --json: Architecture is empty"
fi

# Truncating to erofs_size yields a pure EROFS file that still mounts
# (we don't actually mount in CI — just verify the byte invariant
# that EROFS magic remains at offset 1024).
cp "$TMPDIR/image-a.erofs" "$TMPDIR/image-a.pure.erofs"
truncate -s "$EROFS_SIZE" "$TMPDIR/image-a.pure.erofs"
MAGIC_HEX=$(dd if="$TMPDIR/image-a.pure.erofs" bs=1 count=4 skip=1024 2>/dev/null | od -An -tx1 | tr -d ' \n')
if [ "$MAGIC_HEX" = "e2e1f5e0" ]; then
    ok "post-truncate file retains EROFS magic at offset 1024"
else
    fail "EROFS magic missing after truncate (got: $MAGIC_HEX)"
fi

# ============================================================
echo ""
echo "=== Test 2: Store + roundtrip ==="
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/image-a.erofs" --manifest "$TMPDIR/image-a.manifest" --no-progress 2>&1
"$BIN/manifest-ctl" load $COMMON --manifest "$TMPDIR/image-a.manifest" --output "$TMPDIR/image-a-restored.erofs" --no-progress 2>&1

H1=$(sha256sum "$TMPDIR/image-a.erofs" | awk '{print $1}')
H2=$(sha256sum "$TMPDIR/image-a-restored.erofs" | awk '{print $1}')
assert_eq "$H1" "$H2" "store → load roundtrip matches"

# ============================================================
echo ""
echo "=== Test 3: Dedup (same image stored twice) ==="
OUTPUT=$("$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/image-a.erofs" --manifest "$TMPDIR/image-a-dup.manifest" --no-progress 2>&1)
STORED=$(echo "$OUTPUT" | grep "stored" | grep -oP 'stored \K[0-9]+')
if [ "$STORED" = "0" ]; then
    ok "100% dedup on second store (0 new chunks)"
else
    fail "expected 0 stored chunks on second store, got $STORED"
fi

# ============================================================
echo ""
echo "=== Test 4: Info ==="
OUTPUT=$("$BIN/manifest-ctl" info --manifest "$TMPDIR/image-a.manifest" 2>&1)
if echo "$OUTPUT" | grep -q "chunk mode"; then
    ok "info shows chunk mode"
else
    fail "info missing chunk mode"
fi
if echo "$OUTPUT" | grep -q "chunk count"; then
    ok "info shows chunk count"
else
    fail "info missing chunk count"
fi

# ============================================================
echo ""
echo "=== Test 5: Verify ==="
OUTPUT=$("$BIN/manifest-ctl" verify $COMMON --manifest "$TMPDIR/image-a.manifest" --no-progress 2>&1)
if echo "$OUTPUT" | grep -q "failed: 0"; then
    ok "verify passed with 0 failures"
else
    fail "verify reported failures: $OUTPUT"
fi

# ============================================================
echo ""
echo "=== Test 6: Put-manifest + get-manifest roundtrip ==="
MKEY=$("$BIN/manifest-ctl" put-manifest $COMMON --input "$TMPDIR/image-a.manifest")
echo "  Manifest content key: $MKEY"
"$BIN/manifest-ctl" get-manifest $COMMON --key "$MKEY" --output "$TMPDIR/image-a-from-store.manifest"

HM1=$(sha256sum "$TMPDIR/image-a.manifest" | awk '{print $1}')
HM2=$(sha256sum "$TMPDIR/image-a-from-store.manifest" | awk '{print $1}')
assert_eq "$HM1" "$HM2" "put-manifest → get-manifest roundtrip matches"

# ============================================================
echo ""
echo "=== Test 7: Get-manifest → load pipeline ==="
"$BIN/manifest-ctl" get-manifest $COMMON --key "$MKEY" | \
    "$BIN/manifest-ctl" load $COMMON --output "$TMPDIR/image-a-pipe.erofs" --no-progress 2>&1

H3=$(sha256sum "$TMPDIR/image-a-pipe.erofs" | awk '{print $1}')
assert_eq "$H1" "$H3" "get-manifest | load pipeline matches original"

# ============================================================
echo ""
echo "=== Test 8: Cross-image diff ($IMAGE_A vs $IMAGE_B) ==="
docker save "$IMAGE_B" | "$BIN/flatten-ctl" --output "$TMPDIR/image-b.erofs" --no-progress 2>&1
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/image-b.erofs" --manifest "$TMPDIR/image-b.manifest" --no-progress 2>&1
OUTPUT=$("$BIN/manifest-ctl" diff "$TMPDIR/image-a.manifest" "$TMPDIR/image-b.manifest" 2>&1)
echo "  $OUTPUT" | head -5
if echo "$OUTPUT" | grep -q "shared"; then
    ok "diff shows shared chunks"
else
    fail "diff missing shared info"
fi

# ============================================================
echo ""
echo "=== Test 9: Fixed chunking vs CDC ==="
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/image-a.erofs" --manifest "$TMPDIR/image-a-fixed.manifest" --chunk-mode fixed --no-progress 2>&1
OUTPUT=$("$BIN/manifest-ctl" diff "$TMPDIR/image-a.manifest" "$TMPDIR/image-a-fixed.manifest" 2>&1)
echo "  $OUTPUT" | head -5
ok "CDC vs fixed diff completed"

# ============================================================
echo ""
echo "=== Test 10: Fake crypto roundtrip ==="
"$BIN/manifest-ctl" store $COMMON --input "$TMPDIR/image-a.erofs" --manifest "$TMPDIR/image-a-fake.manifest" \
    --crypto-chunk fake --crypto-manifest fake --crypto-fake --no-progress 2>&1
"$BIN/manifest-ctl" load $COMMON --manifest "$TMPDIR/image-a-fake.manifest" --output "$TMPDIR/image-a-fake-restored.erofs" \
    --crypto-chunk fake --crypto-manifest fake --crypto-fake --no-progress 2>&1

H4=$(sha256sum "$TMPDIR/image-a-fake-restored.erofs" | awk '{print $1}')
assert_eq "$H1" "$H4" "fake crypto store → load roundtrip matches"

# ============================================================
echo ""
echo "=== Test 11: Full pipeline (store → put-manifest → get-manifest → load) ==="
MKEY2=$(cat "$TMPDIR/image-a.erofs" | \
    "$BIN/manifest-ctl" store $COMMON --no-progress | \
    "$BIN/manifest-ctl" put-manifest $COMMON)
echo "  Pipeline manifest key: $MKEY2"
"$BIN/manifest-ctl" get-manifest $COMMON --key "$MKEY2" | \
    "$BIN/manifest-ctl" load $COMMON --output "$TMPDIR/image-a-fullpipe.erofs" --no-progress 2>&1

H5=$(sha256sum "$TMPDIR/image-a-fullpipe.erofs" | awk '{print $1}')
assert_eq "$H1" "$H5" "full pipeline roundtrip matches"

# ============================================================
echo ""
echo "=== Test 12: Short-form roundtrip (store --put-manifest + load --get-manifest) ==="
# store --put-manifest: Manifest 直接落 Store，stdout 是 hex key。
# 注意：Manifest 的 sealed key table 使用 AES-GCM，nonce 随机化，所以每次
# store 产出的 manifest bytes 都不同 → content key 也不同。这不是 bug，
# 只是说"两次 store 同一内容会得到两个不同的 manifest key"，语义等价即可。
MKEY3=$("$BIN/manifest-ctl" store $COMMON --no-progress \
    --input "$TMPDIR/image-a.erofs" --put-manifest)
echo "  Short-form manifest key: $MKEY3"
"$BIN/manifest-ctl" load $COMMON --no-progress \
    --get-manifest "$MKEY3" --output "$TMPDIR/image-a-shortform.erofs"
H6=$(sha256sum "$TMPDIR/image-a-shortform.erofs" | awk '{print $1}')
assert_eq "$H1" "$H6" "short-form roundtrip matches original"

# ============================================================
echo ""
echo "=== Test 13: Sparse file roundtrip (zero-block optimization) ==="
# 8 MiB sparse file: 1 MiB random data + 7 MiB zeros. Verify the
# all-zero optimization path: most chunks are IsZero, manifest size is
# small (compressed key table), and round-trip reconstructs identical
# bytes without ever encrypting/storing/fetching the zero chunks.
SPARSE="$TMPDIR/sparse.bin"
dd if=/dev/urandom of="$SPARSE" bs=1M count=1 status=none
dd if=/dev/zero    of="$SPARSE" bs=1M count=7 seek=1 conv=notrunc status=none
SPARSE_HASH=$(sha256sum "$SPARSE" | awk '{print $1}')

# Use fixed chunking so we can predict counts: 8 MiB / 64 KiB = 128 chunks total.
"$BIN/manifest-ctl" store $COMMON --no-progress \
    --chunk-mode fixed --chunk-fixed-size 64KiB \
    --input "$SPARSE" --manifest "$TMPDIR/sparse.manifest" 2>"$TMPDIR/sparse-store.stderr"

# Parse the "chunks: N (stored S, dedup D, zero W)" summary line.
SUMMARY=$(grep -E '^chunks:' "$TMPDIR/sparse-store.stderr" | head -1)
ZERO_W=$(echo "$SUMMARY" | sed -E 's/.*zero ([0-9]+).*/\1/')
STORED_S=$(echo "$SUMMARY" | sed -E 's/.*stored ([0-9]+).*/\1/')
echo "  sparse store summary: $SUMMARY"
if [ -z "$ZERO_W" ] || [ "$ZERO_W" -lt 100 ]; then
    fail "expected ≥100 zero chunks for 7/8 MiB sparse file (got: '$ZERO_W')"
else
    ok "sparse ingest: $ZERO_W zero chunks detected, $STORED_S stored"
fi

# info command must also report zero chunks count > 0.
"$BIN/manifest-ctl" info --manifest "$TMPDIR/sparse.manifest" > "$TMPDIR/sparse-info.txt"
INFO_ZERO=$(grep -E '^zero chunks:' "$TMPDIR/sparse-info.txt" | sed -E 's/zero chunks:\s+([0-9]+).*/\1/')
assert_eq "$ZERO_W" "$INFO_ZERO" "info reports same zero count as store summary"

# Round-trip: load and compare SHA256.
"$BIN/manifest-ctl" load $COMMON --no-progress \
    --manifest "$TMPDIR/sparse.manifest" --output "$TMPDIR/sparse.rt"
RT_HASH=$(sha256sum "$TMPDIR/sparse.rt" | awk '{print $1}')
assert_eq "$SPARSE_HASH" "$RT_HASH" "sparse file roundtrip matches"

# Manifest size sanity: with 128 chunks total but only ~16 non-zero
# (1 MiB / 64 KiB), the sealed key table holds ~16*32 bytes of key
# material instead of 128*32 — manifest is significantly smaller.
MANIFEST_SIZE=$(stat -c %s "$TMPDIR/sparse.manifest" 2>/dev/null || stat -f %z "$TMPDIR/sparse.manifest")
echo "  sparse manifest size: $MANIFEST_SIZE bytes (with key-table compression)"
# 128 * 56 (entry) + 64 (header) + 16 * 32 (compressed keys) + AEAD overhead ≈ 7800
# Without compression would be 128 * 32 = 4096 instead of 512 in keys,
# so ~3.5 KiB delta. We assert manifest < 12 KiB as a loose bound.
if [ "$MANIFEST_SIZE" -gt 12288 ]; then
    fail "sparse manifest unexpectedly large: $MANIFEST_SIZE bytes"
else
    ok "sparse manifest size within bound (≤12 KiB)"
fi

# ============================================================
echo ""
echo "=== Test 14: Sparse file with holes (--detect-holes + --hole=zero/punch) ==="
# 8 MiB sparse file: 1 MiB random + 7 MiB hole. truncate creates a
# real filesystem hole (not zero-fill); dd at offset 0 writes the
# leading data without touching the trailing hole region.
HOLED="$TMPDIR/holed.img"
truncate -s 8M "$HOLED"
dd if=/dev/urandom of="$HOLED" bs=1M count=1 conv=notrunc status=none
HOLED_HASH=$(sha256sum "$HOLED" | awk '{print $1}')

# Sanity: the source actually has a sparse tail (block count × 512 < apparent size).
HOLED_BLOCKS=$(stat -c '%b' "$HOLED")
HOLED_BSIZE=$(stat -c '%B' "$HOLED")
HOLED_ALLOC=$((HOLED_BLOCKS * HOLED_BSIZE))
echo "  source: apparent=8MiB allocated=$HOLED_ALLOC bytes"

# Store with --detect-holes; fixed chunking so we can predict counts.
"$BIN/manifest-ctl" store $COMMON --no-progress \
    --detect-holes --chunk-mode fixed --chunk-fixed-size 64KiB \
    --input "$HOLED" --manifest "$TMPDIR/holed.manifest" 2>"$TMPDIR/holed-store.stderr"

# Verify the manifest carries holes.
HOLE_LINE=$(grep -E '^holes:' "$TMPDIR/holed-store.stderr" | head -1)
if [ -z "$HOLE_LINE" ]; then
    fail "store summary missing 'holes:' line — --detect-holes ineffective"
else
    ok "store detected holes: $HOLE_LINE"
fi

# info also reports the hole.
"$BIN/manifest-ctl" info --manifest "$TMPDIR/holed.manifest" > "$TMPDIR/holed-info.txt"
INFO_HOLES=$(grep -E '^holes:' "$TMPDIR/holed-info.txt" | sed -E 's/holes:\s+([0-9]+).*/\1/')
if [ "$INFO_HOLES" -lt 1 ]; then
    fail "info shows holes=$INFO_HOLES, expected ≥1"
else
    ok "info reports $INFO_HOLES hole extent(s)"
fi

# Default --hole=error rejects the manifest.
if "$BIN/manifest-ctl" load $COMMON --no-progress \
        --manifest "$TMPDIR/holed.manifest" --output "$TMPDIR/holed-error.out" 2>"$TMPDIR/holed-load-err.stderr"; then
    fail "default --hole=error should have failed but did not"
else
    ok "default policy rejected manifest with hole"
fi

# --hole=zero: synthesize zeros for hole region, output bytes-equal to source.
"$BIN/manifest-ctl" load $COMMON --no-progress --hole=zero \
    --manifest "$TMPDIR/holed.manifest" --output "$TMPDIR/holed-zero.out" 2>&1 >/dev/null
ZERO_HASH=$(sha256sum "$TMPDIR/holed-zero.out" | awk '{print $1}')
assert_eq "$HOLED_HASH" "$ZERO_HASH" "--hole=zero output bytes-equal to sparse source"

# --hole=punch: output should be a sparse file (allocation < apparent size).
"$BIN/manifest-ctl" load $COMMON --no-progress --hole=punch \
    --manifest "$TMPDIR/holed.manifest" --output "$TMPDIR/holed-punch.out" 2>&1 >/dev/null
PUNCH_HASH=$(sha256sum "$TMPDIR/holed-punch.out" | awk '{print $1}')
assert_eq "$HOLED_HASH" "$PUNCH_HASH" "--hole=punch output bytes-equal to sparse source"
PUNCH_BLOCKS=$(stat -c '%b' "$TMPDIR/holed-punch.out")
PUNCH_BSIZE=$(stat -c '%B' "$TMPDIR/holed-punch.out")
PUNCH_ALLOC=$((PUNCH_BLOCKS * PUNCH_BSIZE))
echo "  --hole=punch output: apparent=8MiB allocated=$PUNCH_ALLOC bytes"
# Sparse if allocated < ~6 MiB (punching a 7 MiB hole leaves ≤ 1 MiB allocated).
if [ "$PUNCH_ALLOC" -lt 6291456 ]; then
    ok "--hole=punch produced sparse output (allocated $PUNCH_ALLOC < 6 MiB)"
else
    fail "--hole=punch did not punch (allocated $PUNCH_ALLOC ≥ 6 MiB)"
fi

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
