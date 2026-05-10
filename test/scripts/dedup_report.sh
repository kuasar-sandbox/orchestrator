#!/bin/bash
set -euo pipefail

# Dedup analysis report for N container images.
#
# Usage:
#   bash test/scripts/dedup_report.sh python:3.11-slim python:3.12-slim python:3.13-slim
#   bash test/scripts/dedup_report.sh alpine:3.{17..20}
#   bash test/scripts/dedup_report.sh node:20-slim node:22-slim python:3.12-slim

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$PROJECT_ROOT/bin}"
WORKDIR=$(mktemp -d /tmp/acc-dedup-XXXXXX)
STORE="$WORKDIR/store"
KEY=$(openssl rand -hex 32)
STORE_PID=""
STORE_LOG="$WORKDIR/store-ctl.log"

cleanup() {
    local rc=$?
    if [ -n "$STORE_PID" ]; then
        kill "$STORE_PID" 2>/dev/null || true
        wait "$STORE_PID" 2>/dev/null || true
    fi
    if [ "$rc" -ne 0 ] && [ -s "$STORE_LOG" ]; then
        echo "===== $STORE_LOG =====" >&2
        tail -50 "$STORE_LOG" >&2
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

if [ $# -lt 2 ]; then
    echo "Usage: $0 <image1> <image2> [image3 ...]"
    echo "Example: $0 python:3.11-slim python:3.12-slim python:3.13-slim"
    exit 1
fi

IMAGES=("$@")
N=${#IMAGES[@]}

# Allocate a free TCP port for store-ctl.
free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# Ensure a Docker image is available locally; pull on miss. Exits
# non-zero with a clear message if the pull fails so that the main
# loop doesn't get a silent empty `docker save` stream feeding
# flatten-ctl.
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

# Spin up store-ctl sidecar. manifest-ctl talks to store-ctl over
# gRPC for all chunk / manifest I/O — there is no direct fs path.
STORE_PORT=$(free_port)
cat > "$WORKDIR/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $STORE
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORKDIR/store-ctl.yaml" --generation G1 >>"$STORE_LOG" 2>&1
"$BIN/store-ctl" serve --config "$WORKDIR/store-ctl.yaml" >>"$STORE_LOG" 2>&1 &
STORE_PID=$!
# Poll the gRPC port for readiness.
for i in 1 2 3 4 5; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then
        break
    fi
    sleep 0.1
done

# manifest-ctl YAML config pointed at store-ctl.
cat > "$WORKDIR/config.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 30s
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF
CFG="--config $WORKDIR/config.yaml"

# ═══════════════════════════════════════════════════════════════
# Phase 0: Ensure all images exist locally (pull on miss)
# ═══════════════════════════════════════════════════════════════
# Eager pre-check so the main flatten loop never hits an empty
# `docker save` pipe. Pull progress is shown to the user; failures
# abort early with a clear error rather than producing a blank
# dedup report.
echo "Ensuring $N images are available locally ..."
for img in "${IMAGES[@]}"; do
    ensure_image "$img"
done
echo ""

# ═══════════════════════════════════════════════════════════════
# Phase 1: Flatten + Ingest all images
# ═══════════════════════════════════════════════════════════════
echo "═══════════════════════════════════════════════════════════"
echo " Dedup Report: $N images"
echo "═══════════════════════════════════════════════════════════"
echo ""

declare -a NAMES EROFS_SIZES MANIFEST_PATHS CHUNK_COUNTS STORED_COUNTS DEDUP_COUNTS ZERO_COUNTS STORED_BYTES

for i in $(seq 0 $((N-1))); do
    img="${IMAGES[$i]}"
    tag=$(echo "$img" | tr '/:' '--')
    echo -n "[$((i+1))/$N] $img ... "

    # Flatten. docker save writes the image tar to stdout; flatten-ctl
    # consumes it. Both stderr streams are left intact so any failure
    # (corrupted image, flatten crash, rocks I/O error) surfaces as
    # real output instead of as a silent erofs_size=0 downstream.
    docker save "$img" | "$BIN/flatten-ctl" --output "$WORKDIR/$tag.erofs" --no-progress
    erofs_size=$(stat --printf="%s" "$WORKDIR/$tag.erofs" 2>/dev/null || stat -f "%z" "$WORKDIR/$tag.erofs")

    # Ingest
    output=$("$BIN/manifest-ctl" store $CFG --input "$WORKDIR/$tag.erofs" --manifest "$WORKDIR/$tag.manifest" --no-progress 2>&1)
    "$BIN/manifest-ctl" info --manifest "$WORKDIR/$tag.manifest"

    chunks=$(echo "$output" | grep "chunks:" | grep -oP 'chunks:\s+\K[0-9]+')
    stored=$(echo "$output" | grep "stored" | grep -oP 'stored \K[0-9]+')
    dedup=$(echo "$output" | grep "dedup" | grep -oP 'dedup \K[0-9]+')
    zero=$(echo "$output" | grep "zero" | grep -oP 'zero \K[0-9]+')
    stored_bytes_line=$(echo "$output" | grep "stored bytes:")
    stored_b=$(echo "$stored_bytes_line" | grep -oP '[\d.]+\s+\w+iB' | head -1)

    NAMES[$i]="$img"
    EROFS_SIZES[$i]="$erofs_size"
    MANIFEST_PATHS[$i]="$WORKDIR/$tag.manifest"
    CHUNK_COUNTS[$i]="$chunks"
    STORED_COUNTS[$i]="$stored"
    DEDUP_COUNTS[$i]="$dedup"
    ZERO_COUNTS[$i]="$zero"
    STORED_BYTES[$i]="$stored_b"

    echo "$(numfmt --to=iec-i --suffix=B $erofs_size) EROFS, $chunks chunks ($stored new, $dedup dedup)"
done

# ═══════════════════════════════════════════════════════════════
# Phase 2: Summary table
# ═══════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Per-Image Summary"
echo "═══════════════════════════════════════════════════════════"
printf "%-30s %10s %8s %8s %8s %10s\n" "Image" "EROFS Size" "Chunks" "New" "Dedup" "Stored"
printf "%-30s %10s %8s %8s %8s %10s\n" "-----" "----------" "------" "---" "-----" "------"

total_erofs=0
total_chunks=0
total_stored=0
total_dedup=0

for i in $(seq 0 $((N-1))); do
    sz=$(numfmt --to=iec-i --suffix=B "${EROFS_SIZES[$i]}")
    printf "%-30s %10s %8s %8s %8s %10s\n" \
        "${NAMES[$i]}" "$sz" "${CHUNK_COUNTS[$i]}" "${STORED_COUNTS[$i]}" "${DEDUP_COUNTS[$i]}" "${STORED_BYTES[$i]}"
    total_erofs=$((total_erofs + ${EROFS_SIZES[$i]}))
    total_chunks=$((total_chunks + ${CHUNK_COUNTS[$i]}))
    total_stored=$((total_stored + ${STORED_COUNTS[$i]}))
    total_dedup=$((total_dedup + ${DEDUP_COUNTS[$i]}))
done

printf "%-30s %10s %8s %8s %8s\n" "-----" "----------" "------" "---" "-----"
printf "%-30s %10s %8s %8s %8s\n" \
    "TOTAL ($N images)" "$(numfmt --to=iec-i --suffix=B $total_erofs)" \
    "$total_chunks" "$total_stored" "$total_dedup"

# ═══════════════════════════════════════════════════════════════
# Phase 3: Storage efficiency
# ═══════════════════════════════════════════════════════════════
actual_store_size=$(du -sb "$STORE/chunk" 2>/dev/null | awk '{print $1}')
actual_store_size=${actual_store_size:-0}

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Storage Efficiency"
echo "═══════════════════════════════════════════════════════════"
echo "  Raw EROFS total:  $(numfmt --to=iec-i --suffix=B $total_erofs)"
echo "  Store actual:     $(numfmt --to=iec-i --suffix=B $actual_store_size)"
echo "  Unique chunks:    $total_stored"
echo "  Dedup chunks:     $total_dedup ($(( total_dedup * 100 / (total_stored + total_dedup) ))% of all chunks)"
if [ "$total_erofs" -gt 0 ]; then
    saving=$(( (total_erofs - actual_store_size) * 100 / total_erofs ))
    ratio=$(echo "scale=1; $total_erofs / $actual_store_size" | bc 2>/dev/null || echo "N/A")
    echo "  Space saving:     ${saving}%"
    echo "  Dedup ratio:      ${ratio}x"
fi

# ═══════════════════════════════════════════════════════════════
# Phase 4: Pairwise diff matrix
# ═══════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Pairwise Dedup Matrix (shared chunks / shared %)"
echo "═══════════════════════════════════════════════════════════"

# Header: short names
declare -a SHORT_NAMES
for i in $(seq 0 $((N-1))); do
    # Shorten: python:3.12-slim → py3.12-slim
    short=$(echo "${NAMES[$i]}" | sed 's/python/py/;s/alpine/alp/;s/node/nd/')
    SHORT_NAMES[$i]="$short"
done

# Compute pairwise results first, store in array
declare -A PAIR_SHARED PAIR_SIZE PAIR_PCT
for i in $(seq 0 $((N-1))); do
    for j in $(seq $((i+1)) $((N-1))); do
        diff_output=$("$BIN/manifest-ctl" diff "${MANIFEST_PATHS[$i]}" "${MANIFEST_PATHS[$j]}" 2>&1)
        shared_chunks=$(echo "$diff_output" | grep "shared:" | grep -oP 'shared:\s+\K[0-9]+')
        shared_size=$(echo "$diff_output" | grep "shared:" | grep -oP '\(([^)]+)\)' | tr -d '()')
        ci=${CHUNK_COUNTS[$i]}
        cj=${CHUNK_COUNTS[$j]}
        smaller=$(( ci < cj ? ci : cj ))
        pct=0
        [ "$smaller" -gt 0 ] && pct=$(( shared_chunks * 100 / smaller ))
        PAIR_SHARED[$i,$j]="$shared_chunks"
        PAIR_SIZE[$i,$j]="$shared_size"
        PAIR_PCT[$i,$j]="$pct"
    done
done

# Print table
COL=20
printf "%-20s" ""
for j in $(seq 0 $((N-1))); do
    printf "%-${COL}s" "${SHORT_NAMES[$j]}"
done
echo ""

for i in $(seq 0 $((N-1))); do
    printf "%-20s" "${SHORT_NAMES[$i]}"
    for j in $(seq 0 $((N-1))); do
        if [ "$i" = "$j" ]; then
            printf "%-${COL}s" "  —"
        elif [ "$i" -lt "$j" ]; then
            printf "%-${COL}s" "  ${PAIR_SHARED[$i,$j]} (${PAIR_PCT[$i,$j]}%)"
        else
            printf "%-${COL}s" "  ${PAIR_SHARED[$j,$i]} (${PAIR_PCT[$j,$i]}%)"
        fi
    done
    echo ""
done

# Detail lines below matrix
echo ""
echo "  (cell = shared chunks, % of smaller image's chunks)"
echo ""
echo "  Pairwise detail:"
for i in $(seq 0 $((N-1))); do
    for j in $(seq $((i+1)) $((N-1))); do
        echo "    ${SHORT_NAMES[$i]} ↔ ${SHORT_NAMES[$j]}: ${PAIR_SHARED[$i,$j]} chunks (${PAIR_SIZE[$i,$j]}), ${PAIR_PCT[$i,$j]}%"
    done
done

# ═══════════════════════════════════════════════════════════════
# Phase 5: Incremental cost (cost of adding each image)
# ═══════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Incremental Storage Cost (order of ingestion)"
echo "═══════════════════════════════════════════════════════════"
printf "%-30s %12s %12s %8s\n" "Image" "EROFS Size" "New Stored" "Marginal"
printf "%-30s %12s %12s %8s\n" "-----" "----------" "----------" "--------"

cumulative=0
for i in $(seq 0 $((N-1))); do
    new_bytes=0
    # Estimate new stored bytes from STORED_COUNTS * avg chunk size
    if [ "${STORED_COUNTS[$i]}" -gt 0 ] && [ "${CHUNK_COUNTS[$i]}" -gt 0 ]; then
        avg_chunk=$(( ${EROFS_SIZES[$i]} / ${CHUNK_COUNTS[$i]} ))
        new_bytes=$(( ${STORED_COUNTS[$i]} * avg_chunk ))
    fi
    cumulative=$((cumulative + new_bytes))
    if [ "${EROFS_SIZES[$i]}" -gt 0 ]; then
        marginal=$(( new_bytes * 100 / ${EROFS_SIZES[$i]} ))
    else
        marginal=0
    fi
    printf "%-30s %12s %12s %7d%%\n" \
        "${NAMES[$i]}" \
        "$(numfmt --to=iec-i --suffix=B ${EROFS_SIZES[$i]})" \
        "$(numfmt --to=iec-i --suffix=B $new_bytes)" \
        "$marginal"
done
echo ""
echo "  Cumulative store: $(numfmt --to=iec-i --suffix=B $cumulative)"
echo "  vs raw total:     $(numfmt --to=iec-i --suffix=B $total_erofs)"
if [ "$total_erofs" -gt 0 ]; then
    echo "  Overall saving:   $(( (total_erofs - cumulative) * 100 / total_erofs ))%"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Done. Temp dir: $WORKDIR (will be cleaned up)"
echo "═══════════════════════════════════════════════════════════"
