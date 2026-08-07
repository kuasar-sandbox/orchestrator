#!/usr/bin/env bash
#
# sandbox-perf.sh — track sandbox-ctl cold-start performance over time.
#
# Runs e2e_sandbox_cold (file://) and e2e_sandbox_cold_manifest (manifest://
# via cache-ctl tiered) N iterations each. Median wallclock + UFFD
# source/urgent/tail shape + lazy-load ratio + Go runtime metrics aggregated into a
# single comparable table so regressions surface immediately.
#
# Critical: binaries are cached into a Linux-native FS (/tmp/perf-bin)
# before each run. Slow-exec filesystems (e.g. WSL2 drvfs) add ~500ms
# to every exec, dominating cold-start measurement.
#
# Env:
#   PERF_ITERS=N          iterations per scenario (default 5)
#   PERF_BIN_CACHE=path   /tmp-side binary cache (default /tmp/perf-bin)
#   PERF_OUT=path         output report path (default test/results/sandbox-perf.txt)
#   PERF_SCENARIOS=...    space-separated subset: "cold cold-manifest" (default both)

set -uo pipefail

ITERS="${PERF_ITERS:-5}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC_BIN="${BIN:-$REPO_ROOT/bin}"
BIN_CACHE="${PERF_BIN_CACHE:-/tmp/perf-bin-$(uname -m)}"
OUT="${PERF_OUT:-$REPO_ROOT/test/results/sandbox-perf.txt}"
SCENARIOS="${PERF_SCENARIOS:-cold cold-manifest}"

mkdir -p "$(dirname "$OUT")"
mkdir -p "$BIN_CACHE"

# Refresh binary cache on mtime. cp -u skips up-to-date files.
echo "==> caching binaries to $BIN_CACHE (drvfs→tmpfs to skip ~500ms WSL2 exec overhead)"
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl cache-ctl mkfs.erofs vmlinux; do
    if [ -e "$SRC_BIN/$b" ]; then
        cp -u "$SRC_BIN/$b" "$BIN_CACHE/$b"
    fi
done

run_one() {
    local script="$1"
    local tag="$2"
    local i="$3"
    local stats_dir
    stats_dir=$(mktemp -d /tmp/perf-stats-XXXXXX)
    local log="$stats_dir/run.log"
    local stats="$stats_dir/stats.json"

    # PERF_STATS_JSON instructs e2e to forward --stats-json to sandbox-ctl.
    BIN="$BIN_CACHE" PERF_STATS_JSON="$stats" bash "$script" >"$log" 2>&1 || true

    local wall_ms
    wall_ms=$(grep "T0 → sandbox-ctl exit:" "$log" | awk '{print $(NF-1)}' | head -1)

    if [ ! -s "$stats" ]; then
        echo "[$tag] iter $i FAILED — no stats.json (see $log)"
        rm -rf "$stats_dir"
        return 1
    fi

    # One JSON line per iter so the aggregator can see all metrics.
    python3 - "$stats" "$tag" "$i" "$wall_ms" <<'PY'
import json, sys
path, tag, i, wall = sys.argv[1:]
with open(path) as f:
    r = json.load(f)
u = r.get("uffd") or {}
rt = r.get("runtime") or {}
backs = {b["name"]: b for b in r.get("backends", [])}
b0 = backs.get("blk0", {})
read = b0.get("read") or {}
out = {
    "tag": tag,
    "iter": int(i),
    "wall_ms": float(wall) if wall else 0,
    "internal_ms": (r.get("wallclock") or {}).get("duration_ms"),
    "uffd_faults": u.get("faults_absent"),
    "uffd_errors": u.get("errors"),
    "uffd_queue_p95_us": (u.get("fault_queue_wait_p95") or 0) / 1000.0,
    "uffd_queue_p99_us": (u.get("fault_queue_wait_p99") or 0) / 1000.0,
    "uffd_source_calls": u.get("source_read_calls"),
    "uffd_source_bytes": u.get("source_read_bytes"),
    "uffd_source_ms": (u.get("source_read_ns") or 0) / 1e6,
    "uffd_urgent_copy_calls": u.get("urgent_copy_calls"),
    "uffd_urgent_copy_ms": (u.get("urgent_copy_ns") or 0) / 1e6,
    "uffd_urgent_zero_calls": u.get("urgent_zero_calls"),
    "uffd_urgent_zero_ms": (u.get("urgent_zero_ns") or 0) / 1e6,
    "uffd_tail_submitted": u.get("tail_submitted"),
    "uffd_tail_dropped": u.get("tail_dropped_busy"),
    "uffd_tail_buffered": u.get("tail_buffered_data"),
    "uffd_tail_deferred": u.get("tail_deferred_data"),
    "uffd_tail_zero": u.get("tail_zero"),
    "uffd_tail_completed": u.get("tail_pages_completed"),
    "uffd_tail_conflicts": u.get("tail_conflicts"),
    "uffd_tail_partial": u.get("tail_partial"),
    "uffd_pages_zeroed": u.get("pages_zeroed"),
    "uffd_pages_copied": u.get("pages_copied"),
    "uffd_total_pages": u.get("total_pages"),
    "lazy_load_ratio": u.get("lazy_load_ratio"),
    "blk0_reqs": read.get("count"),
    "blk0_p50_us": (read.get("p50_ns") or 0) / 1000.0,
    "blk0_p99_us": (read.get("p99_ns") or 0) / 1000.0,
    "blk0_load_pct": 100.0 * (b0.get("loaded_blocks") or 0) / max(1, b0.get("total_blocks") or 1),
    "go_num_gc": rt.get("num_gc"),
    "go_gc_pause_ms": (rt.get("gc_pause_total_ns") or 0) / 1e6,
    "go_heap_alloc_kb": (rt.get("heap_alloc_bytes") or 0) / 1024,
    "go_total_alloc_mb": (rt.get("total_alloc_bytes") or 0) / 1024 / 1024,
    "go_mallocs": rt.get("mallocs"),
    "go_goroutines": rt.get("num_goroutine"),
}
print(json.dumps(out))
PY
    rm -rf "$stats_dir"
}

aggregate() {
    local tag="$1"
    shift
    python3 - "$tag" "$@" <<'PY'
import json, statistics, sys
tag = sys.argv[1]
rows = [json.loads(l) for l in sys.argv[2:]]
if not rows:
    print(f"[{tag}] no data")
    sys.exit(0)

def med(key, default=0):
    vs = [r.get(key) for r in rows if r.get(key) is not None]
    return statistics.median(vs) if vs else default

def fmt(v, unit=""):
    if v is None:
        return "n/a"
    if isinstance(v, float):
        return f"{v:.1f}{unit}"
    return f"{v}{unit}"

print(f"\n[{tag}] N={len(rows)}")
print(f"  wallclock T0→exit:    median={med('wall_ms'):.0f}ms  min={min(r['wall_ms'] for r in rows):.0f}ms  max={max(r['wall_ms'] for r in rows):.0f}ms")
internal = [r['internal_ms'] for r in rows if r.get('internal_ms') is not None]
if internal:
    print(f"  sandbox-ctl internal: median={statistics.median(internal):.0f}ms (excl. bash + exec)")
print(f"  uffd faults/errors:   faults={int(med('uffd_faults'))} errors={int(med('uffd_errors'))} queue_p95={med('uffd_queue_p95_us'):.1f}µs queue_p99={med('uffd_queue_p99_us'):.1f}µs")
print(f"  uffd source:          calls={int(med('uffd_source_calls'))} bytes={int(med('uffd_source_bytes'))} time={med('uffd_source_ms'):.1f}ms")
print(f"  uffd urgent:          copy={int(med('uffd_urgent_copy_calls'))}/{med('uffd_urgent_copy_ms'):.1f}ms zero={int(med('uffd_urgent_zero_calls'))}/{med('uffd_urgent_zero_ms'):.1f}ms")
print(f"  uffd tail:            submitted={int(med('uffd_tail_submitted'))} dropped={int(med('uffd_tail_dropped'))} buffered/deferred/zero={int(med('uffd_tail_buffered'))}/{int(med('uffd_tail_deferred'))}/{int(med('uffd_tail_zero'))} completed={int(med('uffd_tail_completed'))} conflicts={int(med('uffd_tail_conflicts'))} partial={int(med('uffd_tail_partial'))}")
zeroed = int(med('uffd_pages_zeroed'))
copied = int(med('uffd_pages_copied'))
total = int(med('uffd_total_pages')) or 1
ratio = med('lazy_load_ratio') * 100
print(f"  uffd lazy-load:       resident={zeroed+copied}/{total} pages = {ratio:.2f}%  (zeroed={zeroed} copied={copied})")
print(f"  blk0 read:            reqs={int(med('blk0_reqs'))} p50={med('blk0_p50_us'):.1f}µs p99={med('blk0_p99_us'):.1f}µs load_coverage={med('blk0_load_pct'):.2f}%")
print(f"  go runtime:           goroutines={int(med('go_goroutines'))} numGC={int(med('go_num_gc'))} pause_total={med('go_gc_pause_ms'):.2f}ms heap_alloc={med('go_heap_alloc_kb'):.0f}KiB total_alloc={med('go_total_alloc_mb'):.1f}MiB mallocs={int(med('go_mallocs'))}")
PY
}

run_scenario() {
    local script="$1"
    local tag="$2"
    local rows=()
    for i in $(seq 1 "$ITERS"); do
        echo "==> [$tag] iter $i/$ITERS"
        local row
        row=$(run_one "$script" "$tag" "$i") || { echo "  iter $i failed; skipping"; continue; }
        rows+=("$row")
        # Brief one-line per iter for live visibility.
        echo "$row" | python3 -c "
import json, sys
r = json.loads(sys.stdin.read())
print('    wall={:.0f}ms internal={}ms faults={} errors={} tail={}/{} drop={} lazy={:.1f}% blk0_p50={:.1f}us gc={} alloc={:.1f}MiB'.format(
    r['wall_ms'], r.get('internal_ms',0), r['uffd_faults'], r.get('uffd_errors',0),
    r.get('uffd_tail_completed',0), r.get('uffd_tail_submitted',0), r.get('uffd_tail_dropped',0),
    r['lazy_load_ratio']*100, r['blk0_p50_us'], r['go_num_gc'], r['go_total_alloc_mb']))
"
    done
    aggregate "$tag" "${rows[@]}"
}

{
    echo "# sandbox cold-start perf — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# repo: $REPO_ROOT"
    echo "# bin cache: $BIN_CACHE"
    echo "# iters: $ITERS"
    echo
    for s in $SCENARIOS; do
        case "$s" in
            cold)
                run_scenario "$REPO_ROOT/test/e2e/e2e_sandbox_cold.sh" "file://"
                ;;
            cold-manifest)
                run_scenario "$REPO_ROOT/test/e2e/e2e_sandbox_cold_manifest.sh" "manifest://"
                ;;
            *)
                echo "[$s] unknown scenario; skipping"
                ;;
        esac
    done
} | tee "$OUT"

echo
echo "==> report saved to $OUT"
