#!/bin/bash
set -euo pipefail

# Cache performance smoke benchmark.
#
# Spins up cache-ctl daemon(s) pinned to a fixed set of CPU cores, runs a
# GET/PUT concurrency sweep from a client pinned to a disjoint core set,
# and prints a human-readable report of throughput + latency percentiles.
#
# Scenarios (BENCH_SCENARIO env var, default "local"):
#
#   local            1 local cache-ctl (today's default behaviour)
#   tiered-l1        1 local (store role) + 1 tiered (L1 embedded rocks,
#                    origin: upstream → local)
#   tiered-shard-l2  1 local (store role) + 5 shard (L2 EC cluster) +
#                    1 tiered (no L1, tiers=[ec], origin: upstream → local)
#
# External mode (skip spin-up, bench a pre-started instance):
#
#   BENCH_EXTERNAL_ENDPOINT=ip:port            [required]
#   BENCH_EXTERNAL_PREFILL_ENDPOINT=ip:port    [optional; default=external endpoint]
#
# Usage:
#   bash test/scripts/bench_cache.sh
#   BENCH_SCENARIO=tiered-l1 bash test/scripts/bench_cache.sh
#   BENCH_SCENARIO=tiered-shard-l2 bash test/scripts/bench_cache.sh
#   BENCH_EXTERNAL_ENDPOINT=127.0.0.1:7700 bash test/scripts/bench_cache.sh
#
#   SERVER_CORES=0   CLIENT_CORES=1       bash test/scripts/bench_cache.sh
#   SERVER_CORES=0-1 CLIENT_CORES=2-5     bash test/scripts/bench_cache.sh
#   VALUE_SIZE=262144 DURATION=5s         bash test/scripts/bench_cache.sh
#   GET_CONCS="1 4 16" PUT_CONCS="1 2"    bash test/scripts/bench_cache.sh
#   GET_CONCS="" PUT_CONCS="1 4 8"        bash test/scripts/bench_cache.sh  # PUT only

BENCH_SCENARIO="${BENCH_SCENARIO:-local}"
BENCH_EXTERNAL_ENDPOINT="${BENCH_EXTERNAL_ENDPOINT:-}"
BENCH_EXTERNAL_PREFILL_ENDPOINT="${BENCH_EXTERNAL_PREFILL_ENDPOINT:-}"

SERVER_CORES="${SERVER_CORES:-0-1}"
CLIENT_CORES="${CLIENT_CORES:-2-3}"
VALUE_SIZE="${VALUE_SIZE:-524288}"
DURATION="${DURATION:-10s}"
PREFILL="${PREFILL:-2000}"
# Use ${VAR-default} (no colon) so users can pass empty string to skip a phase,
# e.g. GET_CONCS="" to run PUT only.
GET_CONCS="${CONCS:-${GET_CONCS-1 2 4 8}}"
PUT_CONCS="${CONCS:-${PUT_CONCS-1}}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$PROJECT_ROOT/bin}"
WORKDIR=$(mktemp -d /tmp/acc-bench-XXXXXX)
RAW="$WORKDIR/raw.tsv"
: > "$RAW"

# Endpoints resolved during setup. BENCH_ENDPOINT is the bench target
# (where reads go); PREFILL_ENDPOINT is where writes land. For the
# local scenario both are the same; for tiered scenarios they differ.
BENCH_ENDPOINT=""
PREFILL_ENDPOINT=""
HEALTH_ENDPOINT=""

# ALL_PIDS holds every daemon PID we spawn so cleanup can reap them on
# exit. External mode leaves this array empty and the trap is a noop.
# ALL_LOGS holds the path to each daemon's captured stdout+stderr log;
# cleanup prints tail -50 of each log on non-zero exit so daemon
# crashes stay diagnosable without re-running under `tee`.
ALL_PIDS=()
ALL_LOGS=()

cleanup() {
    local rc=$?
    for pid in "${ALL_PIDS[@]:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [ "$rc" -ne 0 ]; then
        for log in "${ALL_LOGS[@]:-}"; do
            if [ -n "$log" ] && [ -s "$log" ]; then
                echo "===== $log =====" >&2
                tail -50 "$log" >&2
            fi
        done
    fi
    if [ -z "${KEEP_WORKDIR:-}" ]; then
        rm -rf "$WORKDIR"
    else
        echo "KEEP_WORKDIR=1; logs + pprof files at $WORKDIR" >&2
    fi
}
trap cleanup EXIT

# ── Preflight ─────────────────────────────────────────────────────────────
if ! command -v taskset >/dev/null 2>&1; then
    echo "WARN: taskset not found, running unbound" >&2
    SERVER_CORES=""
    CLIENT_CORES=""
fi

NCPU=$(nproc 2>/dev/null || echo 1)
if [ -n "$SERVER_CORES" ] && [ "$NCPU" -lt 4 ]; then
    echo "WARN: only $NCPU cores available, degrading to SERVER=0 CLIENT=1" >&2
    SERVER_CORES="0"
    CLIENT_CORES="1"
fi

# Count CPUs in a taskset-style spec: "0" → 1, "0-1" → 2, "0,2,4" → 3.
count_cores() {
    local spec=$1
    [ -z "$spec" ] && { echo 0; return; }
    local total=0 part
    local IFS=','
    for part in $spec; do
        if [[ $part == *-* ]]; then
            total=$((total + ${part##*-} - ${part%%-*} + 1))
        else
            total=$((total + 1))
        fi
    done
    echo "$total"
}

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# spawn_daemon <config-path> <health-port> <label>
# Spawns cache-ctl serve with the given config, pinned to SERVER_CORES,
# and waits for the health endpoint to report SERVING. Daemon stdout+
# stderr is captured to $WORKDIR/$label.log so the cleanup trap can
# dump it on failure — never route daemon output to /dev/null, it hides
# the only signal available when startup fails. Appends the new PID to
# ALL_PIDS and the log path to ALL_LOGS. Fatal on timeout.
spawn_daemon() {
    local config=$1
    local health=$2
    local label=$3
    local log="$WORKDIR/$label.log"

    if [ -n "$SERVER_CORES" ]; then
        taskset -c "$SERVER_CORES" env CACHE_CTL_TIMING="${CACHE_CTL_TIMING:-}" "$BIN/cache-ctl" serve --config "$config" >"$log" 2>&1 &
    else
        env CACHE_CTL_TIMING="${CACHE_CTL_TIMING:-}" "$BIN/cache-ctl" serve --config "$config" >"$log" 2>&1 &
    fi
    local pid=$!
    ALL_PIDS+=("$pid")
    ALL_LOGS+=("$log")

    for _ in $(seq 1 100); do
        if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$health" 2>/dev/null | grep -q SERVING; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: cache-ctl daemon '$label' did not become ready" >&2
    echo "--- tail of $log ---" >&2
    tail -30 "$log" >&2 || true
    return 1
}

# ── Scenario helpers ──────────────────────────────────────────────────────

start_local_only() {
    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/local-rocks"
    config="$WORKDIR/local.yaml"
    cat > "$config" <<EOF
mode: local
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $rocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
    spawn_daemon "$config" "$health" "local"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="127.0.0.1:$data"
    HEALTH_ENDPOINT="127.0.0.1:$health"
}

# start_local_as_origin spawns a local cache-ctl intended to act as the
# upstream origin for a downstream tiered instance. The frequency-based
# compaction filter is disabled (freq.disable_eviction: true) — an
# origin daemon must never evict once-touched keys, otherwise prefill
# data vanishes before the bench rounds start. Returns the data endpoint
# via the global LOCAL_ORIGIN_EP, health via LOCAL_ORIGIN_HP.
start_local_as_origin() {
    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/origin-rocks"
    config="$WORKDIR/origin.yaml"
    cat > "$config" <<EOF
mode: local
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
rocks:
  path: $rocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
    spawn_daemon "$config" "$health" "origin-local"
    LOCAL_ORIGIN_EP="127.0.0.1:$data"
    LOCAL_ORIGIN_HP="127.0.0.1:$health"
}

start_tiered_l1() {
    start_local_as_origin

    local data health rocks config
    data=$(free_port)
    health=$(free_port)
    rocks="$WORKDIR/tiered-l1-rocks"
    config="$WORKDIR/tiered-l1.yaml"
    cat > "$config" <<EOF
mode: tiered
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $rocks
      disk_bytes: 4GiB
      mem_ratio: 0.1
      direct_reads: false
      bloom_bits: 10
origin:
  type: upstream
  upstream:
    endpoint: $LOCAL_ORIGIN_EP
    pool: 4
    timeout: 10s
  max_inflight: 16
EOF
    spawn_daemon "$config" "$health" "tiered-l1"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="$LOCAL_ORIGIN_EP"
    HEALTH_ENDPOINT="127.0.0.1:$health"
}

start_tiered_shard_l2() {
    start_local_as_origin

    # Five shard-mode daemons forming the L2 EC cluster.
    local shard_peers=()
    local shard_eps=()
    for i in 1 2 3 4 5; do
        local sdata shealth srocks sconfig
        sdata=$(free_port)
        shealth=$(free_port)
        srocks="$WORKDIR/shard-$i-rocks"
        sconfig="$WORKDIR/shard-$i.yaml"
        cat > "$sconfig" <<EOF
mode: shard
listen: 127.0.0.1:$sdata
health_listen: 127.0.0.1:$shealth
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $srocks
  disk_bytes: 4GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
        spawn_daemon "$sconfig" "$shealth" "shard-$i"
        shard_peers+=("  - id: s$i")
        shard_peers+=("    endpoint: 127.0.0.1:$sdata")
        shard_eps+=("127.0.0.1:$sdata")
    done

    local data health pprof config
    data=$(free_port)
    health=$(free_port)
    pprof=$(free_port)
    config="$WORKDIR/tiered-shard-l2.yaml"
    {
        cat <<EOF
mode: tiered
listen: 127.0.0.1:$data
health_listen: 127.0.0.1:$health
pprof_listen: 127.0.0.1:$pprof
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      pool: 2
      timeout: 10s
      peers:
EOF
        for line in "${shard_peers[@]}"; do
            printf "    %s\n" "$line"
        done
        cat <<EOF
origin:
  type: upstream
  upstream:
    endpoint: $LOCAL_ORIGIN_EP
    pool: 4
    timeout: 10s
  max_inflight: 16
EOF
    } > "$config"
    spawn_daemon "$config" "$health" "tiered-shard-l2"
    BENCH_ENDPOINT="127.0.0.1:$data"
    PREFILL_ENDPOINT="$LOCAL_ORIGIN_EP"
    HEALTH_ENDPOINT="127.0.0.1:$health"
    PPROF_ENDPOINT="127.0.0.1:$pprof"
}

# ── Resolve endpoints ─────────────────────────────────────────────────────
if [ -n "$BENCH_EXTERNAL_ENDPOINT" ]; then
    BENCH_ENDPOINT="$BENCH_EXTERNAL_ENDPOINT"
    PREFILL_ENDPOINT="${BENCH_EXTERNAL_PREFILL_ENDPOINT:-$BENCH_EXTERNAL_ENDPOINT}"
    echo "External mode: bench=$BENCH_ENDPOINT prefill=$PREFILL_ENDPOINT" >&2
else
    case "$BENCH_SCENARIO" in
        local)           start_local_only ;;
        tiered-l1)       start_tiered_l1 ;;
        tiered-shard-l2) start_tiered_shard_l2 ;;
        *)
            echo "ERROR: unknown BENCH_SCENARIO $BENCH_SCENARIO" >&2
            exit 1
            ;;
    esac
fi

# ── Bench driver ──────────────────────────────────────────────────────────
# run_bench <mode> <conc> [extra-flags...]
# Runs cache-ctl bench and appends raw numbers to $RAW as a TSV row.
# Format: mode \t conc \t ops \t thrpt \t bw \t p50_us \t p99_us \t p999_us
run_bench() {
    local mode=$1
    local conc=$2
    shift 2

    local out="$WORKDIR/bench-$mode-$conc.out"
    printf "  running %s c=%s\n" "$mode" "$conc" >&2

    # Always pass --prefill-endpoint; when BENCH_ENDPOINT == PREFILL_ENDPOINT
    # the bench tool short-circuits to the same conn pool.
    local prefill_flag=()
    if [ "$PREFILL_ENDPOINT" != "$BENCH_ENDPOINT" ]; then
        prefill_flag=(--prefill-endpoint "$PREFILL_ENDPOINT")
    fi

    # Thread the info endpoint (bench target's HealthListen port) through
    # so cache-ctl bench can snapshot daemon counters before/after the
    # bench window and print hit rates that exclude prefill noise.
    local info_flag=()
    if [ -n "$HEALTH_ENDPOINT" ]; then
        info_flag=(--info-endpoint "$HEALTH_ENDPOINT")
    fi

    # Run the bench; capture stdout+stderr so the awk parser below
    # can scrape latency rows. Non-zero exit dumps the captured file
    # to stderr — never let a bench failure silently rm the only
    # evidence via the cleanup trap.
    # Always capture pprof during the bench window. Files land in
    # $WORKDIR and are dumped alongside daemon logs on non-zero exit.
    # Set KEEP_WORKDIR=1 to retain them for post-mortem analysis.
    local profile_flags=()
    profile_flags+=(--cpu-profile "$WORKDIR/bench-$mode-$conc.cpu.pprof")
    profile_flags+=(--heap-profile "$WORKDIR/bench-$mode-$conc.heap.pprof")
    # runtime/trace is optional — enabled when PROFILE_TRACE=1.
    if [ -n "${PROFILE_TRACE:-}" ]; then
        profile_flags+=(--trace "$WORKDIR/bench-$mode-$conc.trace")
    fi

    # Capture bench-TARGET CPU profile in the background via its
    # pprof_listen endpoint. Duration matches DURATION so the window
    # aligns with the bench measurement window. Delayed start (prefill
    # + warm + WaitFills usually take a few seconds — pprof pulling
    # for DURATION will cover the bench window plus a bit of tail).
    local target_pprof_pid=""
    if [ -n "${PPROF_ENDPOINT:-}" ]; then
        local secs=${DURATION%s}
        ( sleep 2 && curl -sS -o "$WORKDIR/target-$mode-$conc.cpu.pprof" \
            "http://$PPROF_ENDPOINT/debug/pprof/profile?seconds=$secs" ) &
        target_pprof_pid=$!
    fi

    local rc=0
    if [ -n "$CLIENT_CORES" ]; then
        taskset -c "$CLIENT_CORES" "$BIN/cache-ctl" bench \
            --endpoint "$BENCH_ENDPOINT" \
            "${prefill_flag[@]}" \
            "${info_flag[@]}" \
            "${profile_flags[@]}" \
            --concurrency "$conc" --duration "$DURATION" \
            --value-size "$VALUE_SIZE" --mode "$mode" "$@" >"$out" 2>&1 || rc=$?
    else
        "$BIN/cache-ctl" bench \
            --endpoint "$BENCH_ENDPOINT" \
            "${prefill_flag[@]}" \
            "${info_flag[@]}" \
            "${profile_flags[@]}" \
            --concurrency "$conc" --duration "$DURATION" \
            --value-size "$VALUE_SIZE" --mode "$mode" "$@" >"$out" 2>&1 || rc=$?
    fi
    if [ -n "$target_pprof_pid" ]; then
        wait "$target_pprof_pid" 2>/dev/null || true
        # Also grab a heap snapshot immediately after the bench finishes.
        curl -sS -o "$WORKDIR/target-$mode-$conc.heap.pprof" \
            "http://$PPROF_ENDPOINT/debug/pprof/heap" 2>/dev/null || true
    fi

    if [ "$rc" -ne 0 ]; then
        echo "ERROR: cache-ctl bench mode=$mode c=$conc exited $rc" >&2
        echo "--- $out ---" >&2
        cat "$out" >&2
        return "$rc"
    fi

    awk -v mode="$mode" -v conc="$conc" '
        /^  ops:/        { ops=$2 }
        /^  throughput:/ { thrpt=$2 }
        /^  bandwidth:/  { bw=$2 }
        /^    p50:/      { p50=$2 }
        /^    p99:/      { p99=$2 }
        /^    p99\.9:/   { p999=$2 }
        END {
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                   mode, conc, ops, thrpt, bw, p50, p99, p999
        }' "$out" >> "$RAW"

    # Surface the bench-window counter section printed by cache-ctl bench
    # (only present when --info-endpoint was threaded through).
    awk '/^  ── bench-window counters ──$/,0' "$out" >&2
}

# ── Runs ──────────────────────────────────────────────────────────────────
# Every GET round passes --prefill $PREFILL so the key space is identical
# across rounds; omitting it would make later rounds fall back to the Go
# default (1000) and silently change the workload model.
#
# In non-local scenarios we always run GET (the bench target may be a
# tiered instance that rejects writes). PUT rounds are skipped unless
# the scenario is local or explicitly single-endpoint.
if [ "$PREFILL_ENDPOINT" != "$BENCH_ENDPOINT" ]; then
    # Split prefill/bench — only GET makes sense.
    for c in $GET_CONCS; do
        run_bench get "$c" --prefill "$PREFILL"
    done
else
    for c in $GET_CONCS; do
        run_bench get "$c" --prefill "$PREFILL"
    done
    for c in $PUT_CONCS; do
        run_bench put "$c"
    done
fi

# ── Report ────────────────────────────────────────────────────────────────
# Daemon counter hit rates are printed per-round by cache-ctl bench itself
# (see run_bench above) so they exclude prefill/warm noise. No post-loop
# snapshot here.
SCOUNT=$(count_cores "$SERVER_CORES")
CCOUNT=$(count_cores "$CLIENT_CORES")

awk -v scores="${SERVER_CORES:-unbound}" -v ccores="${CLIENT_CORES:-unbound}" \
    -v scount="$SCOUNT" -v ccount="$CCOUNT" \
    -v valsz="$VALUE_SIZE" -v duration="$DURATION" \
    -v scenario="$BENCH_SCENARIO" -v bench_ep="$BENCH_ENDPOINT" -v prefill_ep="$PREFILL_ENDPOINT" '
function commify(n,   s, out, len) {
    s = sprintf("%d", n)
    len = length(s)
    out = ""
    while (len > 3) {
        out = "," substr(s, len-2, 3) out
        len -= 3
    }
    return substr(s, 1, len) out
}
function fmt_lat(us,   ms) {
    if (us + 0 < 1000) return sprintf("%d us", us)
    ms = us / 1000.0
    if (ms < 10)  return sprintf("%.2f ms", ms)
    if (ms < 100) return sprintf("%.1f ms", ms)
    return sprintf("%.0f ms", ms)
}
function fmt_size(b) {
    if (b >= 1048576 && b % 1048576 == 0) return (b/1048576) " MiB"
    if (b >= 1024    && b % 1024    == 0) return (b/1024)    " KiB"
    return b " B"
}
function fmt_cores(spec, n) {
    if (spec == "unbound") return "unbound"
    if (n == 1) return spec " (1 core)"
    return spec " (" n " cores)"
}
function emit_section(label, n,   i, HDR, ROW) {
    HDR = "  %4s   %13s   %12s   %9s   %9s   %9s\n"
    ROW = "  %4d   %8s op/s   %6d MiB/s   %9s   %9s   %9s\n"
    printf "  ── %s ──\n", label
    printf HDR, "conc", "throughput", "bandwidth", "p50", "p99", "p99.9"
    printf HDR, "----", "-------------", "------------", "---------", "---------", "---------"
    for (i = 0; i < n; i++) {
        if (label == "GET") {
            printf ROW, g_conc[i], commify(g_thrpt[i]), g_bw[i]+0,
                   fmt_lat(g_p50[i]), fmt_lat(g_p99[i]), fmt_lat(g_p999[i])
        } else {
            printf ROW, p_conc[i], commify(p_thrpt[i]), p_bw[i]+0,
                   fmt_lat(p_p50[i]), fmt_lat(p_p99[i]), fmt_lat(p_p999[i])
        }
    }
    print ""
}
BEGIN {
    FS = "\t"
    ng = 0; np = 0
}
{
    mode = $1; conc = $2; ops = $3; thrpt = $4; bw = $5
    p50 = $6; p99 = $7; p999 = $8
    if (mode == "get") {
        g_conc[ng] = conc; g_thrpt[ng] = thrpt; g_bw[ng] = bw
        g_p50[ng] = p50; g_p99[ng] = p99; g_p999[ng] = p999
        ng++
    } else if (mode == "put") {
        p_conc[np] = conc; p_thrpt[np] = thrpt; p_bw[np] = bw
        p_p50[np] = p50; p_p99[np] = p99; p_p999[np] = p999
        np++
    }
}
END {
    print ""
    print "═══════════════════════════════════════════════════════════════════════════"
    print "                          Cache Bench Report"
    print "═══════════════════════════════════════════════════════════════════════════"
    printf "  Scenario     : %s\n", scenario
    printf "  Bench EP     : %s\n", bench_ep
    if (prefill_ep != bench_ep) {
        printf "  Prefill EP   : %s\n", prefill_ep
    }
    printf "  Server cores : %s\n", fmt_cores(scores, scount)
    printf "  Client cores : %s\n", fmt_cores(ccores, ccount)
    printf "  Value size   : %s\n", fmt_size(valsz+0)
    printf "  Duration     : %s per run\n", duration
    print ""
    if (ng > 0) emit_section("GET", ng)
    if (np > 0) emit_section("PUT", np)
}
' "$RAW"
