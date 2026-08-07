#!/usr/bin/env bash
#
# sandbox-perf-manifest.sh — comprehensive perf matrix for the manifest://
# data path: cold-start + snapshot --upload + restore manifest:// in
# both cold-L1 and hot-L1 cache states.
#
# Daemon model:
#   store-ctl + cache-ctl tiered live for the entire run (one per
#   process). Cache "cold" vs "hot" is controlled by clearing the
#   rocksdb path between iterations:
#     cold-L1:  rm -rf cache-rocks; restart cache-ctl
#     hot-L1:   keep cache-ctl alive; rocks retains chunks across iters
#
# Scenarios driven (each runs N iterations, median + min/max reported):
#   1. cold-start manifest://  (cold L1)   — first VM ever
#   2. cold-start manifest://  (hot L1)    — chunks cached from prior runs
#   3. snapshot --upload       (no dedup)  — first upload of a fresh sandbox
#   4. snapshot --upload       (dedup)     — same sandbox, same content, second upload
#   5. restore manifest://     (cold L1)   — first restore after upload (cache cold)
#   6. restore manifest://     (hot L1)    — second+ restore (cache warm)
#
# Output:
#   stats per iter via sandbox-ctl --stats-json (UFFD source/urgent/tail shape, blk0
#   load, Go memstats, lazy-load ratio, dedup chunks)
#   final report aggregates median into a single table
#
# Env:
#   PERF_ITERS=N            iterations per scenario (default 5)
#   PERF_BIN_CACHE=path     /tmp-side binary cache (default /tmp/perf-bin)
#   PERF_OUT=path           report path (default test/results/sandbox-perf-manifest.txt)
#   IMAGE=docker-tag        guest base image (default python:3.12-slim)
#   TAP_NAME=name           pre-existing TAP (default sb-tap0)
#   VMLINUX=path            kernel path (default $BIN_CACHE/vmlinux)

set -uo pipefail

ITERS="${PERF_ITERS:-5}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC_BIN="${BIN:-$REPO_ROOT/bin}"
BIN_CACHE="${PERF_BIN_CACHE:-/tmp/perf-bin-$(uname -m)}"
OUT="${PERF_OUT:-$REPO_ROOT/test/results/sandbox-perf-manifest.txt}"
IMAGE="${IMAGE:-python:3.12-slim}"
# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"

mkdir -p "$(dirname "$OUT")"
mkdir -p "$BIN_CACHE"

# Defensive pre-clean: a prior failed run may have left long-running daemons
# (cache-ctl / store-ctl) or CH zombies still executing from $BIN_CACHE. The
# cp below would then fail with "Text file busy" and silently leave a stale
# cached binary. Kill anything from $BIN_CACHE before copying.
echo "==> pre-clean stale perf daemons (cache-ctl / store-ctl / cloud-hypervisor)" >&2
pkill -9 -f "$BIN_CACHE/cloud-hypervisor" 2>/dev/null || true
pkill -9 -f "$BIN_CACHE/cache-ctl"        2>/dev/null || true
pkill -9 -f "$BIN_CACHE/store-ctl"        2>/dev/null || true
sleep 0.5

# Fast-path binaries to /tmp to skip WSL2 drvfs exec overhead.
echo "==> caching binaries to $BIN_CACHE" >&2
for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl cache-ctl mkfs.erofs vmlinux; do
    if [ -e "$SRC_BIN/$b" ]; then
        cp -u "$SRC_BIN/$b" "$BIN_CACHE/$b" \
          || { echo "FATAL: cp $b → $BIN_CACHE failed (still busy?)" >&2; exit 1; }
    fi
done
BIN="$BIN_CACHE"
VMLINUX="${VMLINUX:-$BIN/vmlinux}"

require() {
    [ -e "$1" ] || { echo "MISSING: $1" >&2; exit 1; }
}
require "$BIN/cloud-hypervisor"
require "$BIN/sandbox-ctl"
require "$BIN/sandbox-runtime.bundle"
require "$BIN/sandbox-init"
require "$BIN/store-ctl"
require "$BIN/cache-ctl"
require "$BIN/manifest-ctl"
require "$BIN/flatten-ctl"
require "$VMLINUX"

[ "$(id -u)" -eq 0 ] || { echo "must run as root" >&2; exit 1; }
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    [ "$(id -u)" -eq 0 ] || { echo "$0: must run as root to create $TAP_NAME" >&2; exit 1; }
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

# CH zombies still attached to the TAP from a prior failed run would
# wedge subsequent tap_open(). The daemon pre-clean above already killed
# anything from $BIN_CACHE; this is a belt-and-braces re-sweep after the
# require() gate, in case the user pointed BIN at a non-$BIN_CACHE path.
echo "==> defensive re-sweep of stale cloud-hypervisor processes" >&2
pkill -9 -f "$BIN/cloud-hypervisor" 2>/dev/null || true
sleep 0.5

# ---- workspace + daemons ------------------------------------------------

WORK="$(mktemp -d /tmp/perf-manifest-XXXXXX)"
PIDS=()
cleanup() {
    set +e
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    if [ -n "${PERF_KEEP:-}" ]; then echo "kept: $WORK" >&2; else rm -rf "$WORK"; fi
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null || true
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# Wait for a PID to fully exit (kill + poll). Required because
# `wait $pid` only works on direct children; long-sandbox is started
# inside a subshell ($()) so by the time the parent shell calls
# wait, the bg process has been re-parented to init and `wait`
# returns immediately. Poll-on-kill0 is the portable equivalent.
#
# After the PID is gone, sleep briefly so the kernel finishes
# releasing the TAP attachment held by CH (TAP teardown is async on
# CH process exit; the next iter would otherwise EBUSY on tap_open).
kill_and_wait() {
    # Graceful sandbox-ctl shutdown. sandbox-ctl owns the CH lifecycle
    # (forwards SIGTERM to CH, waits chShutdownGrace=5s, then SIGKILLs
    # CH + tears down tap/run dir).
    #
    # Project policy: NEVER strong-kill sandbox-ctl. SIGKILL orphans CH
    # (leaked tap/run/cgroup) and silently masks sandbox-ctl bugs. If
    # sandbox-ctl doesn't exit within the timeout, leave it for human
    # investigation — print a loud WARN with the exact cleanup commands.
    #
    # Timeout 60s: under heavy oversubscribe the Go scheduler can delay
    # shutdown goroutines (observed 5s expanding to ~11s wallclock).
    local pid="$1" waited=0 timeout=60
    [ -n "$pid" ] || return 0
    kill -TERM "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null; return 0; }
    while [ "$waited" -lt "$timeout" ]; do
        kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null; break; }
        sleep 1
        waited=$((waited+1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "WARN: sandbox-ctl pid=$pid did not exit within ${timeout}s of SIGTERM" >&2
        echo "WARN: leaving it running per project policy (no script-level SIGKILL of sandbox-ctl)" >&2
        echo "WARN: manual cleanup if needed: sudo kill -KILL $pid" >&2
        # Skip TAP bounce — without sandbox-ctl exit the TAP may still be
        # held; let the next iter's tap_open surface the error rather
        # than masking it here.
        return 1
    fi
    # TAP link bounce to force kernel cleanup of any TAP attachment.
    # sandbox-ctl's CH teardown already releases the TAP, but the kernel's
    # deferred unbind can briefly hold the fd, EBUSY'ing the next iter's
    # tap_open. A down/up bounce is cheap insurance.
    ip link set "$TAP_NAME" down 2>/dev/null || true
    ip link set "$TAP_NAME" up 2>/dev/null || true
    sleep 0.3
}

KEY=$(openssl rand -hex 32)
STORE_PORT=$(free_port)
CACHE_PORT=$(free_port)
CACHE_HEALTH_PORT=$(free_port)
CACHE_ROCKS="$WORK/cache-rocks"

cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store-data
  verify_content_key: true
EOF
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
      path: $CACHE_ROCKS
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
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

start_store_ctl() {
    "$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" >"$WORK/store.log" 2>&1 &
    STORE_PID=$!
    PIDS+=("$STORE_PID")
    for _ in $(seq 1 50); do
        (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null && return
        sleep 0.1
    done
    echo "store-ctl did not come up" >&2
    return 1
}

start_cache_ctl() {
    "$BIN/cache-ctl" serve --config "$WORK/cache-ctl.yaml" >"$WORK/cache.log" 2>&1 &
    CACHE_PID=$!
    PIDS+=("$CACHE_PID")
    for _ in $(seq 1 50); do
        "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null | grep -q SERVING && return
        sleep 0.1
    done
    echo "cache-ctl did not come up" >&2
    return 1
}

restart_cache_ctl_clean() {
    if [ -n "${CACHE_PID:-}" ]; then
        kill "$CACHE_PID" 2>/dev/null || true
        for _ in $(seq 1 100); do
            kill -0 "$CACHE_PID" 2>/dev/null || break
            sleep 0.05
        done
        if kill -0 "$CACHE_PID" 2>/dev/null; then
            echo "cache-ctl pid=$CACHE_PID did not stop before cold reset" >&2
            return 1
        fi
        wait "$CACHE_PID" 2>/dev/null || true
    fi
    rm -rf "$CACHE_ROCKS"
    start_cache_ctl
}

echo "==> spin up store-ctl + cache-ctl tiered" >&2
"$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1 >>"$WORK/store.log" 2>&1
start_store_ctl || exit 1
start_cache_ctl || exit 1

# ---- prepare blk0 manifest ----------------------------------------------

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> docker pull $IMAGE" >&2
    docker pull "$IMAGE" >/dev/null
fi
BLK0_EROFS="$WORK/blk0.img"
echo "==> docker save | flatten-ctl export > $BLK0_EROFS" >&2
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_EROFS" --no-progress
[ -s "$BLK0_EROFS" ] || { echo "FATAL: blk0 erofs empty (docker save | flatten-ctl failed)" >&2; exit 1; }

echo "==> manifest-ctl store $BLK0_EROFS" >&2
MKEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$BLK0_EROFS") \
  || { echo "FATAL: manifest-ctl store failed" >&2; exit 1; }
[ -n "$MKEY" ] || { echo "FATAL: manifest-ctl returned empty key" >&2; exit 1; }
echo "    blk0 manifest key: $MKEY" >&2

# ---- per-scenario sandbox.yaml templates --------------------------------

write_sandbox_yaml() {
    local out="$1"; local diff="$2"; local args_json="$3"
    cat > "$out" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: perf
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: manifest://$MKEY
    overlay:
      diff: file://$diff
      size: 1GiB
launch:
  args: $args_json
  restart: never
EOF
}

write_host_yaml() {
    local out="$1"; local diff="$2"
    cat > "$out" <<EOF
resources:
  capacity:    { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: perf-restored
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: manifest://$MKEY
    overlay:
      diff: file://$diff
      size: 1GiB
launch:
  args: ["-c", "import time; time.sleep(60)"]
  restart: never
EOF
}

# ---- helpers: stats extraction ------------------------------------------

stats_to_json() {
    # Args: stats-json path, log path, tag, iter, t0_ns
    local path="$1" log="$2" tag="$3" i="$4" t0="$5"
    [ -s "$path" ] || { echo "MISSING $path" >&2; return 1; }
    python3 - "$path" "$log" "$tag" "$i" "$t0" <<'PY'
import json, re, sys, os
path, logp, tag, i, t0 = sys.argv[1:]
with open(path) as f:
    r = json.load(f)
u = r.get("uffd") or {}
rt = r.get("runtime") or {}
backs = {b["name"]: b for b in r.get("backends", [])}
b0 = backs.get("blk0", {})
read = b0.get("read") or {}

# Wallclock from log (T0 → exit / T0 → app first stdout).
t_app_ms = None
t_exit_ms = None
with open(logp) as lf:
    body = lf.read()
m = re.search(r"T0 → app first stdout \(PYBOOT-OK\):\s+([0-9.]+) ms", body)
if m: t_app_ms = float(m.group(1))
m = re.search(r"T0 → sandbox-ctl exit:\s+([0-9.]+) ms", body)
if m: t_exit_ms = float(m.group(1))

out = {
    "tag": tag, "iter": int(i),
    "wall_app_ms": t_app_ms,
    "wall_exit_ms": t_exit_ms,
    "internal_ms": (r.get("wallclock") or {}).get("duration_ms"),
    "uffd_faults": u.get("faults_absent"),
    "uffd_errors": u.get("errors"),
    "uffd_queue_p95_us": (u.get("fault_queue_wait_p95") or 0) / 1000.0,
    "uffd_queue_p99_us": (u.get("fault_queue_wait_p99") or 0) / 1000.0,
    "uffd_source_calls": u.get("source_read_calls"),
    "uffd_source_bytes": u.get("source_read_bytes"),
    "uffd_source_ms": (u.get("source_read_ns") or 0) / 1e6,
    "uffd_urgent_copy_calls": u.get("urgent_copy_calls"),
    "uffd_urgent_zero_calls": u.get("urgent_zero_calls"),
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
    "go_total_alloc_mb": (rt.get("total_alloc_bytes") or 0) / 1024 / 1024,
    "go_mallocs": rt.get("mallocs"),
}
print(json.dumps(out))
PY
}

# ---- scenario runners ----------------------------------------------------
#
# Each runner emits one JSON line per iteration so the aggregator can
# sort / median later.

run_cold_iter() {
    local tag="$1"; local i="$2"
    local d="$WORK/run-${tag}-$i"
    mkdir -p "$d/runtime"
    local diff="$d/runtime/blk1.diff"
    truncate -s 1G "$diff"
    mkfs.ext4 -q -F "$diff"
    write_sandbox_yaml "$d/sandbox.yaml" "$diff" \
        '["-c", "import sys; print('"'"'PYBOOT-OK'"'"', sys.version_info.major*100+sys.version_info.minor)"]'

    local stats="$d/stats.json"
    local log="$d/run.log"
    local t0
    local t_end
    local exit=99
    # TAP busy retry — same pattern as start_long_sandbox.
    for try in 1 2 3; do
        rm -f "$log"
        t0=$(date +%s%N)
        timeout 90 "$BIN/sandbox-ctl" run \
            --config "$d/sandbox.yaml" \
            --manifest-config "$WORK/accelerator.yaml" \
            --ch-binary "$BIN/cloud-hypervisor" \
            --run-root "$d/runtime" \
            --sandbox-id "perf-c-$i" \
            --stats-json "$stats" \
            > "$log" 2>&1
        exit=$?
        t_end=$(date +%s%N)
        if [ "$exit" -eq 0 ] && grep -q "PYBOOT-OK" "$log" 2>/dev/null; then break; fi
        if grep -q "Device or resource busy" "$log" 2>/dev/null; then
            echo "  cold iter $i: TAP busy on try $try, retrying" >&2
            sleep 1
            continue
        fi
        # other failure → no retry
        break
    done

    # Reconstruct app and exit times from log timestamps so this script
    # is self-contained (no dependency on e2e_sandbox_cold's own log
    # format).
    local wall_app="" wall_exit=""
    wall_exit=$(awk "BEGIN{printf \"%.1f\", ($t_end - $t0) / 1000000.0}")
    if grep -q "PYBOOT-OK" "$log"; then
        # Approximate app time = exit time - exit-after-app-tail (very small).
        wall_app="$wall_exit"
    fi
    # Embed into log so stats_to_json sees the canonical lines.
    {
        echo "T0 → app first stdout (PYBOOT-OK):  $wall_app ms"
        echo "T0 → sandbox-ctl exit:               $wall_exit ms"
    } >> "$log"
    if [ "$exit" -ne 0 ]; then
        echo "  iter $i: sandbox-ctl exit=$exit (last log lines below)" >&2
        tail -10 "$log" >&2
    fi
    stats_to_json "$stats" "$log" "$tag" "$i" "$t0"
}

# Run a long-living sandbox (TICK counter) for snapshot/restore phases.
# Returns SBPID + LOG path via stdout (parsed by caller).
start_long_sandbox() {
    local sid="$1"
    local d="$WORK/long-$sid"
    mkdir -p "$d/runtime"
    local diff="$d/runtime/blk1.diff"
    truncate -s 1G "$diff"
    mkfs.ext4 -q -F "$diff"
    # Tight counter (no time.sleep) so the FIRST TICK after restore
    # appears immediately — measurement of restore latency would
    # otherwise be polluted by mid-sleep snapshot capture.
    write_sandbox_yaml "$d/sandbox.yaml" "$diff" \
        '["-c", "import sys,time\nprint('"'"'PYBOOT-OK'"'"', flush=True)\ni=0\nwhile True:\n    print('"'"'TICK'"'"', i, flush=True)\n    i+=1\n    if i%100==0: time.sleep(0.05)"]'

    local log="$d/run.log"
    # Retry on transient TAP busy: if CH fails to open the TAP because
    # the kernel hasn't released it yet, sleep and retry up to 3 times.
    local sbpid=0
    for try in 1 2 3; do
        rm -f "$log"
        "$BIN/sandbox-ctl" run \
            --config "$d/sandbox.yaml" \
            --manifest-config "$WORK/accelerator.yaml" \
            --ch-binary "$BIN/cloud-hypervisor" \
            --run-root "$d/runtime" \
            --sandbox-id "$sid" \
            > "$log" 2>&1 &
        sbpid=$!
        # Quick probe for early TAP-busy failure; if it fails we'll retry.
        for _ in $(seq 1 60); do
            if grep -qE "^TICK 5[[:space:]]*$" "$log" 2>/dev/null; then
                # Success.
                echo "$sbpid|$log|$d"
                return 0
            fi
            if ! kill -0 "$sbpid" 2>/dev/null; then
                if grep -q "Device or resource busy" "$log" 2>/dev/null; then
                    echo "long-sandbox $sid: TAP busy on try $try, retrying" >&2
                    sleep 1
                    break
                fi
                echo "long-sandbox $sid exited early on try $try" >&2
                tail -10 "$log" >&2
                return 1
            fi
            sleep 0.05
        done
        # If still alive but no TICK, keep waiting.
        for _ in $(seq 1 540); do
            if grep -qE "^TICK 5[[:space:]]*$" "$log" 2>/dev/null; then
                echo "$sbpid|$log|$d"
                return 0
            fi
            if ! kill -0 "$sbpid" 2>/dev/null; then break; fi
            sleep 0.05
        done
    done
    echo "long-sandbox $sid did not start after 3 retries" >&2
    return 1
}

run_upload_iter() {
    local tag="$1"; local i="$2"; local sbpid="$3"; local d="$4"
    local snap_log="$d/snap-$tag-$i.log"
    local t0
    t0=$(date +%s%N)
    local key
    if ! key=$("$BIN/sandbox-ctl" snapshot \
        --sandbox-id "$(basename "$d" | sed 's/long-//')" \
        --upload \
        --run-root "$d/runtime" \
        --resume=true 2>"$snap_log"); then
        echo "snapshot upload $tag iter $i failed" >&2
        sed -n '1,120p' "$snap_log" >&2
        return 1
    fi
    if [[ ! "$key" =~ ^[0-9a-f]{64}$ ]]; then
        echo "snapshot upload $tag iter $i returned invalid manifest key: $key" >&2
        sed -n '1,120p' "$snap_log" >&2
        return 1
    fi
    local t_end
    t_end=$(date +%s%N)
    local wall_ms
    wall_ms=$(awk "BEGIN{printf \"%.1f\", ($t_end - $t0) / 1000000.0}")
    local upload_line snapshot_line
    upload_line=$(grep -m1 'upload OK; overlay stored=' "$snap_log") || {
        echo "snapshot upload $tag iter $i did not report chunk statistics" >&2
        sed -n '1,120p' "$snap_log" >&2
        return 1
    }
    if [[ ! "$upload_line" =~ overlay[[:space:]]stored=([0-9]+)[[:space:]]dedup=([0-9]+),[[:space:]]snapshot[[:space:]]stored=([0-9]+)[[:space:]]dedup=([0-9]+) ]]; then
        echo "snapshot upload $tag iter $i returned malformed chunk statistics: $upload_line" >&2
        return 1
    fi
    local disk_stored="${BASH_REMATCH[1]}"
    local disk_dedup="${BASH_REMATCH[2]}"
    local snap_stored="${BASH_REMATCH[3]}"
    local snap_dedup="${BASH_REMATCH[4]}"
    local disk_total snap_total
    disk_total=$((disk_stored + disk_dedup))
    snap_total=$((snap_stored + snap_dedup))

    snapshot_line=$(grep -m1 '^snapshot upload done:' "$snap_log") || {
        echo "snapshot upload $tag iter $i did not report memory statistics" >&2
        return 1
    }
    if [[ ! "$snapshot_line" =~ resident=([0-9]+) ]]; then
        echo "snapshot upload $tag iter $i returned malformed memory statistics: $snapshot_line" >&2
        return 1
    fi
    local mem_resident="${BASH_REMATCH[1]}"
    python3 - <<PY
import json
print(json.dumps({
    "tag": "$tag",
    "iter": $i,
    "wall_ms": float("$wall_ms"),
    "snap_key": "$key",
    "snap_total": int("${snap_total:-0}"),
    "snap_dedup": int("${snap_dedup:-0}"),
    "disk_total": int("${disk_total:-0}"),
    "disk_dedup": int("${disk_dedup:-0}"),
    "memory_resident_bytes": int("${mem_resident:-0}"),
}))
PY
}

run_restore_iter() {
    local tag="$1"; local i="$2"; local snap_key="$3"
    local d="$WORK/restore-$tag-$i"
    mkdir -p "$d/runtime"
    local diff="$d/runtime/blk1.diff"
    truncate -s 1G "$diff"
    write_host_yaml "$d/host.yaml" "$diff"

    local log="$d/run.log"
    local stats="$d/stats.json"
    local t0
    local sbpid=0
    local t_first_tick=""
    # TAP busy retry.
    for try in 1 2 3; do
        rm -f "$log"
        t0=$(date +%s%N)
        "$BIN/sandbox-ctl" run \
            --restore "manifest://$snap_key" \
            --config "$d/host.yaml" \
            --manifest-config "$WORK/accelerator.yaml" \
            --ch-binary "$BIN/cloud-hypervisor" \
            --run-root "$d/runtime" \
            --sandbox-id "perf-r-$tag-$i" \
            --stats-json "$stats" \
            > "$log" 2>&1 &
        sbpid=$!
        # Wait for first TICK or early failure. 60s ceiling — cold-L1
        # restore over cache-ctl + store origin can take up to ~10s
        # for 70+ chunks + uffd page faults + vCPU resume; subsequent
        # iters with hot L1 finish in 1-2s.
        local got=0
        for _ in $(seq 1 3000); do
            if grep -qE "^TICK [0-9]+$" "$log" 2>/dev/null; then
                t_first_tick=$(date +%s%N)
                got=1
                break
            fi
            if ! kill -0 "$sbpid" 2>/dev/null; then break; fi
            sleep 0.02
        done
        if [ "$got" -eq 1 ]; then break; fi
        # No TICK observed AND sandbox still alive → kill it; retry only
        # on TAP busy.
        kill_and_wait "$sbpid"
        if grep -q "Device or resource busy" "$log" 2>/dev/null; then
            echo "  restore $tag iter $i: TAP busy on try $try, retrying" >&2
            sleep 1
            continue
        fi
        echo "restore $tag iter $i: sandbox did not produce TICK" >&2
        tail -10 "$log" >&2
        return 1
    done
    if [ -z "$t_first_tick" ]; then
        echo "restore $tag iter $i: never saw TICK" >&2
        kill_and_wait "$sbpid"
        return 1
    fi

    # Tear down restored sandbox so cache state for next iter starts
    # at "memory hit, cache hit" (the chunks just got pulled).
    kill_and_wait "$sbpid"

    local first_tick_ms=""
    if [ -n "$t_first_tick" ]; then
        first_tick_ms=$(awk "BEGIN{printf \"%.1f\", ($t_first_tick - $t0) / 1000000.0}")
    fi
    {
        echo "T0 → app first stdout (PYBOOT-OK):  $first_tick_ms ms"
        echo "T0 → sandbox-ctl exit:               $first_tick_ms ms"
    } >> "$log"
    stats_to_json "$stats" "$log" "$tag" "$i" "$t0"
}

# ---- aggregator ----------------------------------------------------------

aggregate_kv() {
    local title="$1"
    shift
    python3 - "$title" "$@" <<'PY'
import json, statistics, sys
title = sys.argv[1]
rows = [json.loads(l) for l in sys.argv[2:]]
print(f"\n[{title}] N={len(rows)}")
if not rows:
    sys.exit(0)
def med(k):
    vs = [r.get(k) for r in rows if r.get(k) is not None]
    return statistics.median(vs) if vs else None
def fmt(v, suffix=""):
    if v is None: return "n/a"
    if isinstance(v, float): return f"{v:.1f}{suffix}"
    return f"{v}{suffix}"

# Cold-start / restore rows have wall_app/wall_exit/lazy_load_ratio etc.
keys = rows[0].keys()
if "wall_exit_ms" in keys:
    print(f"  wall T0→app:          median={fmt(med('wall_app_ms'),'ms')}  min={min(r.get('wall_app_ms',0) for r in rows):.0f}ms  max={max(r.get('wall_app_ms',0) for r in rows):.0f}ms")
    print(f"  wall T0→exit:         median={fmt(med('wall_exit_ms'),'ms')}")
    print(f"  internal sandbox-ctl: median={fmt(med('internal_ms'),'ms')}")
    fa = med('uffd_faults') or 0
    z = med('uffd_pages_zeroed') or 0
    c = med('uffd_pages_copied') or 0
    t = med('uffd_total_pages') or 1
    rl = (med('lazy_load_ratio') or 0) * 100
    print(f"  uffd faults/errors:   faults={int(fa)} errors={int(med('uffd_errors') or 0)} queue_p95={fmt(med('uffd_queue_p95_us'),'µs')} queue_p99={fmt(med('uffd_queue_p99_us'),'µs')}")
    print(f"  uffd source:          calls={int(med('uffd_source_calls') or 0)} bytes={int(med('uffd_source_bytes') or 0)} time={fmt(med('uffd_source_ms'),'ms')}")
    print(f"  uffd urgent:          copy={int(med('uffd_urgent_copy_calls') or 0)} zero={int(med('uffd_urgent_zero_calls') or 0)}")
    print(f"  uffd tail:            submitted={int(med('uffd_tail_submitted') or 0)} dropped={int(med('uffd_tail_dropped') or 0)} buffered/deferred/zero={int(med('uffd_tail_buffered') or 0)}/{int(med('uffd_tail_deferred') or 0)}/{int(med('uffd_tail_zero') or 0)} completed={int(med('uffd_tail_completed') or 0)} conflicts={int(med('uffd_tail_conflicts') or 0)} partial={int(med('uffd_tail_partial') or 0)}")
    print(f"  uffd lazy-load:       resident={int(z)+int(c)}/{int(t)} pages = {rl:.2f}%")
    print(f"  blk0 read:            reqs={int(med('blk0_reqs') or 0)} p50={fmt(med('blk0_p50_us'),'µs')} p99={fmt(med('blk0_p99_us'),'µs')} load_coverage={fmt(med('blk0_load_pct'),'%')}")
    print(f"  go runtime:           numGC={int(med('go_num_gc') or 0)} pause={fmt(med('go_gc_pause_ms'),'ms')} total_alloc={fmt(med('go_total_alloc_mb'),'MiB')}")
elif "snap_total" in keys:
    print(f"  upload wallclock:     median={fmt(med('wall_ms'),'ms')}  min={min(r.get('wall_ms',0) for r in rows):.0f}ms  max={max(r.get('wall_ms',0) for r in rows):.0f}ms")
    print(f"  snapshot chunks:      total={int(med('snap_total') or 0)} dedup={int(med('snap_dedup') or 0)}")
    print(f"  disk chunks:          total={int(med('disk_total') or 0)} dedup={int(med('disk_dedup') or 0)}")
    print(f"  memory resident:      median={(med('memory_resident_bytes') or 0)/1024/1024:.1f} MiB")
PY
}

run_iters() {
    local fn="$1"; local tag="$2"; shift 2
    local rows=()
    local extra=("$@")
    for i in $(seq 1 "$ITERS"); do
        echo "==> [$tag] iter $i/$ITERS" >&2
        local row
        row=$($fn "$tag" "$i" "${extra[@]}") || { echo "  iter $i failed" >&2; sleep 0.5; continue; }
        rows+=("$row")
        # Sleep between iters so TAP / kernel state settles.
        sleep 0.5
        echo "$row" | python3 -c '
import json, sys
r = json.loads(sys.stdin.read())
if "wall_exit_ms" in r:
    print("    wall_exit={}ms faults={} lazy={:.1f}% blk0_p50={}us alloc={:.1f}MiB".format(
        r.get("wall_exit_ms",0), r.get("uffd_faults",0),
        (r.get("lazy_load_ratio") or 0)*100, r.get("blk0_p50_us",0),
        r.get("go_total_alloc_mb",0)))
elif "snap_total" in r:
    print("    upload={:.0f}ms total={} dedup={} resident={}MiB".format(
        r["wall_ms"], r["snap_total"], r["snap_dedup"], r["memory_resident_bytes"]//1024//1024))
' >&2
    done
    aggregate_kv "$tag" "${rows[@]}"
}

# ---- run the matrix ------------------------------------------------------

run_matrix() {
    echo "# sandbox manifest:// perf matrix"
    echo "# date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# iters:   $ITERS"
    echo "# image:   $IMAGE"
    echo "# bin:     $BIN"
    echo "# vmlinux: $VMLINUX"
    echo "# manifest key (blk0): $MKEY"
    echo

    # Scenario 1: cold-start manifest:// — cold L1 (one iteration with empty cache).
    echo "==> scenario 1: cold-start manifest:// (cold L1, single iter)" >&2
    restart_cache_ctl_clean || return 1
    rows=()
    row=$(run_cold_iter "cold-start-cold-L1" 1) && rows+=("$row")
    aggregate_kv "cold-start manifest:// (cold L1, single iter)" "${rows[@]}"

    # Scenario 2: cold-start manifest:// — hot L1 (N iters, cache retained).
    echo "==> scenario 2: cold-start manifest:// (hot L1, N=$ITERS)" >&2
    sleep 1   # settle TAP from scenario 1
    run_iters run_cold_iter "cold-start-hot-L1"

    # Scenario 3: snapshot --upload — first/no-dedup.
    echo "==> scenario 3: snapshot --upload (no dedup, fresh sandboxes)" >&2
    sleep 1
    rows=()
    for i in $(seq 1 "$ITERS"); do
        echo "==> [upload-no-dedup] iter $i/$ITERS" >&2
        info=$(start_long_sandbox "snap-nodedup-$i") || continue
        sbpid="${info%%|*}"
        d="${info##*|}"
        row=$(run_upload_iter "upload-no-dedup" "$i" "$sbpid" "$d") || true
        # Tear down sandbox so the next iter starts fresh. kill_and_wait
        # polls until the PID actually exits — `wait` doesn't work here
        # because the process was started inside a $() subshell and is
        # no longer a direct child of this shell.
        kill_and_wait "$sbpid"
        rm -rf "$d"
        if [ -n "$row" ]; then
            rows+=("$row")
            echo "$row" | python3 -c '
import json, sys
r = json.loads(sys.stdin.read())
print("    upload={}ms total={} dedup={}".format(int(r["wall_ms"]), r["snap_total"], r["snap_dedup"]))
' >&2
        fi
    done
    aggregate_kv "snapshot --upload (no dedup baseline)" "${rows[@]}"

    # Scenario 4: snapshot --upload — same sandbox, repeat for dedup measurement.
    echo "==> scenario 4: snapshot --upload (max dedup, same sandbox repeat)" >&2
    sleep 1   # let TAP / kernel state settle from prior scenario teardown
    info=$(start_long_sandbox "snap-dedup")
    sbpid="${info%%|*}"
    d="${info##*|}"
    rows=()
    SNAP_KEY=""
    for i in $(seq 1 "$ITERS"); do
        echo "==> [upload-max-dedup] iter $i/$ITERS" >&2
        row=$(run_upload_iter "upload-max-dedup" "$i" "$sbpid" "$d") || continue
        rows+=("$row")
        echo "$row" | python3 -c '
import json, sys
r = json.loads(sys.stdin.read())
print("    upload={}ms total={} dedup={}".format(int(r["wall_ms"]), r["snap_total"], r["snap_dedup"]))
' >&2
        SNAP_KEY=$(echo "$row" | python3 -c 'import json,sys;print(json.loads(sys.stdin.read()).get("snap_key",""))')
    done
    aggregate_kv "snapshot --upload (max dedup, same sandbox)" "${rows[@]}"
    # Keep the snapshot key for restore; tear down sandbox.
    kill_and_wait "$sbpid"

    if [ -z "$SNAP_KEY" ]; then
        echo "WARN: no snapshot key captured; skipping restore scenarios" >&2
    else
        # Scenario 5: restore manifest:// — cold L1 (clear cache).
        echo "==> scenario 5: restore manifest:// (cold L1, single iter)" >&2
        sleep 1
        restart_cache_ctl_clean || return 1
        rows=()
        row=$(run_restore_iter "restore-cold-L1" 1 "$SNAP_KEY") && rows+=("$row")
        aggregate_kv "restore manifest:// (cold L1, single iter)" "${rows[@]}"

        # Scenario 6: restore manifest:// — hot L1 (N iters, cache retained).
        echo "==> scenario 6: restore manifest:// (hot L1, N=$ITERS)" >&2
        sleep 1
        run_iters run_restore_iter "restore-hot-L1" "$SNAP_KEY"
    fi

    echo
    echo "==> done"
}

matrix_status=0
run_matrix >"$OUT" || matrix_status=$?
cat "$OUT"
if [ "$matrix_status" -ne 0 ]; then
    echo "FATAL: manifest perf matrix aborted with status $matrix_status — see $OUT" >&2
    exit "$matrix_status"
fi

# Sanity: any scenario with zero successful iterations means the run is broken
# (build/env/regression). The body's `|| continue` swallows per-iter failures
# by design, so this is the only gate that turns systemic breakage into a
# non-zero exit. `grep -c` exits 1 on zero matches; the wrapper makes that ok.
zero=$(grep -cE '^\[.*\] N=0' "$OUT" || true)
if [ "$zero" -gt 0 ]; then
    echo "FATAL: $zero scenario(s) produced N=0 samples — see $OUT" >&2
    exit 1
fi

echo "==> report saved to $OUT" >&2
