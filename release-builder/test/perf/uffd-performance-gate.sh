#!/usr/bin/env bash
# Stable, same-process UFFD performance guard for BMS. Real KVM readiness and
# tail-path assertions live in e2e_sandbox_restore/upload_restore; this script
# catches critical-path and read-amplification regressions without KVM noise.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ORG="${ORG:-$(cd "$REPO_ROOT/../.." && pwd)}"
OUTPUT_DIR="${KUASAR_CI_DIR:-$(mktemp -d /tmp/e2e-uffd-performance-XXXXXX)}"
RAW="$OUTPUT_DIR/uffd-benchmark.txt"
JSON="$OUTPUT_DIR/uffd-performance-gate.json"
MARKDOWN="$OUTPUT_DIR/uffd-performance-gate.md"

mkdir -p "$OUTPUT_DIR"

echo "==> UFFD A/B/C performance gate"
echo "    raw:      $RAW"
echo "    json:     $JSON"
echo "    markdown: $MARKDOWN"

set +e
(
    cd "$ORG/sandboxer" || exit 125
    go test ./pkg/uffd \
        -run '^$' \
        -bench '^BenchmarkUFFDFaultStrategies/(OrdinaryData|ManifestHit|ManifestColdCopy|LocalPlaintextTar|LocalEncryptedTar|Zero)/(Sequential1VCPU|Random2VCPU)/(A_SyncFullBatch|B_FaultFirstNoTail|C_FaultFirstSerialTail)$' \
        -benchmem \
        -benchtime=200ms \
        -count=5
) 2>&1 | tee "$RAW"
pipeline_status=("${PIPESTATUS[@]}")
set -e
benchmark_status="${pipeline_status[0]}"
tee_status="${pipeline_status[1]}"

set +e
PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/test/perf/uffd_benchmark_gate.py" \
    --input "$RAW" \
    --json-output "$JSON" \
    --markdown-output "$MARKDOWN" \
    --benchmark-status "$benchmark_status"
gate_status=$?
set -e

if (( benchmark_status != 0 )); then
    echo "FAIL: UFFD benchmark exited with status $benchmark_status" >&2
    exit "$benchmark_status"
fi
if (( tee_status != 0 )); then
    echo "FAIL: could not retain UFFD benchmark output (tee status $tee_status)" >&2
    exit "$tee_status"
fi
if (( gate_status != 0 )); then
    exit "$gate_status"
fi

echo "==> e2e_uffd_performance: OK"
