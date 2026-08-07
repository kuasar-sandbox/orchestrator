#!/usr/bin/env bash
# Stable, same-process UFFD performance guard for BMS. Real KVM readiness and
# tail-path assertions live in e2e_sandbox_restore/upload_restore; this script
# catches critical-path and read-amplification regressions without KVM noise.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ORG="$(cd "$REPO_ROOT/../.." && pwd)"
OUTPUT_DIR="${KUASAR_CI_DIR:-$(mktemp -d /tmp/e2e-uffd-performance-XXXXXX)}"
RAW="$OUTPUT_DIR/uffd-benchmark.txt"
JSON="$OUTPUT_DIR/uffd-performance-gate.json"
MARKDOWN="$OUTPUT_DIR/uffd-performance-gate.md"

mkdir -p "$OUTPUT_DIR"

echo "==> UFFD A/B/C performance gate"
echo "    raw:      $RAW"
echo "    json:     $JSON"
echo "    markdown: $MARKDOWN"

(
    cd "$ORG/sandboxer"
    go test ./pkg/uffd \
        -run '^$' \
        -bench '^BenchmarkUFFDFaultStrategies/(OrdinaryData|ManifestHit|ManifestColdCopy|LocalPlaintextTar|LocalEncryptedTar|Zero)/(Sequential1VCPU|Random2VCPU)/(A_SyncFullBatch|B_FaultFirstNoTail|C_FaultFirstSerialTail)$' \
        -benchmem \
        -benchtime=200ms \
        -count=5
) 2>&1 | tee "$RAW"

PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/test/perf/uffd_benchmark_gate.py" \
    --input "$RAW" \
    --json-output "$JSON" \
    --markdown-output "$MARKDOWN"

echo "==> e2e_uffd_performance: OK"
