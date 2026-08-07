#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/uffd_performance_gate.sh"

WORK="$(mktemp -d /tmp/uffd-performance-gate-test-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
export KUASAR_CI_DIR="$WORK/artifact"
mkdir -p "$KUASAR_CI_DIR"

cat >"$WORK/good.json" <<'EOF'
{
  "uffd": {
    "errors": 0,
    "faults_absent": 10,
    "source_read_calls": 8,
    "urgent_copy_calls": 8,
    "urgent_zero_calls": 2,
    "tail_buffered_data": 3,
    "tail_deferred_data": 4,
    "tail_zero": 2,
    "tail_pages_completed": 64,
    "tail_dropped_busy": 1,
    "tail_conflicts": 0
  }
}
EOF

uffd_performance_gate zero-pass 400 2000 "$WORK/good.json" zero
uffd_performance_gate deferred-pass 500 3000 "$WORK/good.json" deferred
uffd_performance_gate buffered-pass 600 3000 "$WORK/good.json" buffered

if uffd_performance_gate latency-fail 5001 5000 "$WORK/good.json" zero >/dev/null 2>&1; then
    echo "latency regression unexpectedly passed" >&2
    exit 1
fi

cat >"$WORK/incomplete.json" <<'EOF'
{"uffd":{"errors":0}}
EOF
if uffd_performance_gate incomplete 1 5000 "$WORK/incomplete.json" zero >/dev/null 2>&1; then
    echo "incomplete metrics unexpectedly passed" >&2
    exit 1
fi

grep -q $'^zero-pass\t400\t2000\t' "$KUASAR_CI_DIR/uffd-e2e-gate.tsv"
grep -q $'^latency-fail\t5001\t5000\t' "$KUASAR_CI_DIR/uffd-e2e-gate.tsv"
grep -q $'\tPASS$' "$KUASAR_CI_DIR/uffd-e2e-gate.tsv"
grep -q $'\tFAIL$' "$KUASAR_CI_DIR/uffd-e2e-gate.tsv"

mkdir -p "$WORK/bin" "$WORK/org/sandboxer" "$WORK/driver-artifact"
cat >"$WORK/bin/go" <<'EOF'
#!/usr/bin/env bash
if [ "$PWD" != "${EXPECTED_PWD:?}" ]; then
    echo "unexpected source root: $PWD" >&2
    exit 43
fi
echo "synthetic benchmark failure" >&2
exit 42
EOF
chmod +x "$WORK/bin/go"

set +e
PATH="$WORK/bin:$PATH" \
    ORG="$WORK/org" \
    EXPECTED_PWD="$WORK/org/sandboxer" \
    KUASAR_CI_DIR="$WORK/driver-artifact" \
    bash "$REPO_ROOT/test/perf/uffd-performance-gate.sh" \
    >"$WORK/driver.log" 2>&1
driver_status=$?
set -e
if [ "$driver_status" -ne 42 ]; then
    echo "benchmark failure status = $driver_status, want 42" >&2
    cat "$WORK/driver.log" >&2
    exit 1
fi
jq -e '.passed == false and (.failures | length > 0)' \
    "$WORK/driver-artifact/uffd-performance-gate.json" >/dev/null
grep -q '^Result: FAIL$' "$WORK/driver-artifact/uffd-performance-gate.md"
grep -q 'synthetic benchmark failure' "$WORK/driver-artifact/uffd-benchmark.txt"

echo "uffd_performance_gate_test: PASS"
