#!/bin/bash
set -euo pipefail

# Remote multi-host cache-ctl bench driver.
#
# Deploys a statically-linked cache-ctl binary + generated YAML configs
# to a set of remote hosts, brings up a full topology (N shard peers +
# 1 local origin + 1 tiered client), runs a concurrency sweep from the
# bench host, pulls bench logs and pprof files back to $RESULTS_DIR,
# and renders a markdown report.
#
# Prerequisites on the operator's workstation:
#   - Passwordless SSH (pubkey in ~/.ssh/authorized_keys on every host)
#   - bin/cache-ctl built via `make build-cgo` (statically linked C/C++)
#
# Prerequisites on each remote host:
#   - glibc >= build machine's glibc (binary is dynamic-glibc only)
#   - ~/cache-bench writeable by the login user
#
# Topology (override via env):
#   SHARDS           space-separated shard host IPs (required for bench)
#   ORIGIN_HOST      host running local-mode origin (default: first SHARDS host)
#   TIERED_HOST      host running tiered-mode cache (default: ORIGIN_HOST)
#   BENCH_HOST       host running bench client (default: TIERED_HOST)
#
# Knobs:
#   BINARY           local path to cache-ctl (default: bin/cache-ctl)
#   REMOTE_DIR       remote install path (default: cache-bench, relative to $HOME)
#   RESULTS_DIR      local output dir (default: build/test-results)
#   VALUE_SIZE       bench value size bytes (default: 524288)
#   PREFILL          number of keys (default: 500)
#   DURATION         bench duration per concurrency (default: 30s)
#   CONCS            space-separated concurrencies (default: 1 2 4 8)
#   EC_DATA          data shards (default: 4)
#   EC_PARITY        parity shards (default: 1; total = EC_DATA+EC_PARITY)
#   SSH_OPTS         extra ssh flags
#
# Subcommands:
#   deploy           scp binary + YAML to all hosts
#   start shards     launch shard daemons on $SHARDS
#   start origin     launch local daemon on $ORIGIN_HOST
#   start tiered     launch tiered daemon on $TIERED_HOST
#   start all        = shards + origin + tiered
#   health           ping every daemon's health endpoint
#   bench            run CONCS sweep from $BENCH_HOST, pull results
#   report           render $RESULTS_DIR/README.md from pulled bench logs
#   stop             pkill cache-ctl serve on all hosts
#   clean            stop + rm ~/$REMOTE_DIR/rocks-* + rm $RESULTS_DIR/*
#   all              clean + deploy + start all + health + bench + report
#                    (DESTRUCTIVE — wipes remote rocksdb dirs each run.
#                     Use the individual subcommands for incremental work.
#                     'clean' is required because (a) prior cache-ctl
#                     processes hold the deployed binary → scp would
#                     fail "Text file busy" on redeploy, and (b) the
#                     on-disk shard format is tied to the binary
#                     version; mixing old data with a new binary risks
#                     undefined behaviour.)
#
# Example:
#   SHARDS="192.168.1.129 192.168.1.35 192.168.1.11 192.168.1.120 192.168.1.139" \
#   ORIGIN_HOST=192.168.1.60 TIERED_HOST=192.168.1.60 BENCH_HOST=192.168.1.60 \
#   bash test/scripts/bench_cache_remote.sh all

# ── Config ────────────────────────────────────────────────────────────────

SHARDS="${SHARDS:-}"
ORIGIN_HOST="${ORIGIN_HOST:-}"
TIERED_HOST="${TIERED_HOST:-}"
BENCH_HOST="${BENCH_HOST:-}"

BINARY="${BINARY:-bin/cache-ctl}"
REMOTE_DIR="${REMOTE_DIR:-cache-bench}"
RESULTS_DIR="${RESULTS_DIR:-build/test-results}"

VALUE_SIZE="${VALUE_SIZE:-524288}"
PREFILL="${PREFILL:-500}"
DURATION="${DURATION:-30s}"
CONCS="${CONCS:-1 2 4 8}"

EC_DATA="${EC_DATA:-4}"
EC_PARITY="${EC_PARITY:-1}"

SSH_OPTS="${SSH_OPTS:--o ConnectTimeout=30 -o StrictHostKeyChecking=no -o BatchMode=yes}"

# Fixed port plan (single-topology-per-host assumption):
SHARD_PORT=17070
SHARD_HEALTH=17071
SHARD_PPROF=17072
LOCAL_PORT=17080
LOCAL_HEALTH=17081
LOCAL_PPROF=17082
TIERED_PORT=17090
TIERED_HEALTH=17091
TIERED_PPROF=17092

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TMPDIR_LOCAL="$(mktemp -d /tmp/acc-remote-XXXXXX)"
trap 'rm -rf "$TMPDIR_LOCAL"' EXIT

# ── Helpers ───────────────────────────────────────────────────────────────

log()  { echo "[$(date +%H:%M:%S)] $*" >&2; }
die()  { echo "error: $*" >&2; exit 1; }

# all_hosts prints the deduped union of shard hosts and origin/tiered/bench
all_hosts() {
    {
        for h in $SHARDS; do echo "$h"; done
        [ -n "$ORIGIN_HOST" ] && echo "$ORIGIN_HOST"
        [ -n "$TIERED_HOST" ] && echo "$TIERED_HOST"
        [ -n "$BENCH_HOST" ]  && echo "$BENCH_HOST"
    } | awk 'NF' | sort -u
}

# shard_peer_id maps a shard IP to its stable peer id (s1..sN), indexed by
# position in $SHARDS. The id is used in the EC cluster YAML.
shard_peer_id() {
    local target="$1" i=0
    for ip in $SHARDS; do
        i=$((i + 1))
        if [ "$ip" = "$target" ]; then
            echo "s$i"
            return
        fi
    done
    die "host $target not in SHARDS"
}

# ssh_run h cmd — execute cmd on host h, blocks for output
ssh_run() {
    local h="$1"; shift
    ssh $SSH_OPTS "$h" "$@"
}

# ssh_launch h cmd — start cmd on host h in the background (detached),
# returns immediately.
#
# Uses -n -f to detach the ssh client after authentication. Crucially,
# stdout+stderr are redirected to /dev/null — otherwise -f's forked
# child inherits whatever FDs the parent shell has (e.g. a pipe to
# `tee` or `tail`), keeping that pipe alive until the remote session
# closes, which blocks the caller's pipeline indefinitely.
ssh_launch() {
    local h="$1"; shift
    ssh -n -f $SSH_OPTS "$h" "$@" >/dev/null 2>&1
}

# scp_to h src dst — copy local src to h:dst
scp_to() {
    local h="$1" src="$2" dst="$3"
    scp $SSH_OPTS "$src" "${h}:${dst}" >/dev/null
}

# scp_from h src dst — copy h:src to local dst
scp_from() {
    local h="$1" src="$2" dst="$3"
    scp $SSH_OPTS "${h}:${src}" "$dst" >/dev/null
}

require() {
    local name="$1" value="$2"
    [ -n "$value" ] || die "$name is required (set via env var)"
}

# ── YAML generators ───────────────────────────────────────────────────────

gen_shard_yaml() {
    cat <<EOF
mode: shard
listen: 0.0.0.0:${SHARD_PORT}
health_listen: 0.0.0.0:${SHARD_HEALTH}
pprof_listen: 0.0.0.0:${SHARD_PPROF}
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: \$HOME/${REMOTE_DIR}/rocks-shard
  disk_bytes: 8GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
}

gen_local_yaml() {
    cat <<EOF
mode: local
listen: 0.0.0.0:${LOCAL_PORT}
health_listen: 0.0.0.0:${LOCAL_HEALTH}
pprof_listen: 0.0.0.0:${LOCAL_PPROF}
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
  disable_eviction: true
rocks:
  path: \$HOME/${REMOTE_DIR}/rocks-origin
  disk_bytes: 16GiB
  mem_ratio: 0.1
  direct_reads: false
  bloom_bits: 10
EOF
}

gen_tiered_yaml() {
    local origin_endpoint="${ORIGIN_HOST}:${LOCAL_PORT}"
    # Same-host optimisation: if origin and tiered share a host, dial
    # origin over loopback. Saves one NIC hop on every origin miss.
    if [ "$ORIGIN_HOST" = "$TIERED_HOST" ]; then
        origin_endpoint="127.0.0.1:${LOCAL_PORT}"
    fi
    cat <<EOF
mode: tiered
listen: 0.0.0.0:${TIERED_PORT}
health_listen: 0.0.0.0:${TIERED_HEALTH}
pprof_listen: 0.0.0.0:${TIERED_PPROF}
rpc_timeout: 10s
tiers:
  - type: ec
    cluster:
      data_shards: ${EC_DATA}
      parity_shards: ${EC_PARITY}
      peers:
EOF
    local i=0
    for ip in $SHARDS; do
        i=$((i + 1))
        cat <<EOF
        - id: s${i}
          endpoint: ${ip}:${SHARD_PORT}
EOF
    done
    cat <<EOF
      pool: 2
      timeout: 10s
origin:
  type: upstream
  upstream:
    endpoint: ${origin_endpoint}
    pool: 4
    timeout: 10s
  max_inflight: 16
EOF
}

# Pre-expand \$HOME with a given user's home. The YAML contains literal
# $HOME strings so we can ship it and resolve on the target — but
# runtime loads YAML before env expansion, so we substitute here before
# scp. We resolve once per host via ssh echo $HOME.
resolve_home() {
    local h="$1" src="$2" dst="$3"
    local remote_home
    remote_home=$(ssh_run "$h" 'echo $HOME')
    [ -n "$remote_home" ] || die "failed to resolve \$HOME on $h"
    sed "s|\$HOME|$remote_home|g" "$src" > "$dst"
}

# ── Subcommands ───────────────────────────────────────────────────────────

cmd_deploy() {
    require SHARDS "$SHARDS"
    require ORIGIN_HOST "$ORIGIN_HOST"
    require TIERED_HOST "$TIERED_HOST"

    [ -x "$BINARY" ] || die "binary $BINARY not found or not executable; run 'make build-cgo'"

    # Generate YAMLs into a local tmp dir first, then per-host resolve-and-scp
    gen_shard_yaml  > "$TMPDIR_LOCAL/shard.yaml.tmpl"
    gen_local_yaml  > "$TMPDIR_LOCAL/local.yaml.tmpl"
    gen_tiered_yaml > "$TMPDIR_LOCAL/tiered.yaml.tmpl"

    for h in $(all_hosts); do
        log "deploy → $h"
        ssh_run "$h" "mkdir -p ~/${REMOTE_DIR}"
        scp_to "$h" "$BINARY" "${REMOTE_DIR}/cache-ctl"
        ssh_run "$h" "chmod +x ~/${REMOTE_DIR}/cache-ctl"
    done

    for ip in $SHARDS; do
        log "ship shard.yaml → $ip"
        resolve_home "$ip" "$TMPDIR_LOCAL/shard.yaml.tmpl" "$TMPDIR_LOCAL/shard.$ip.yaml"
        scp_to "$ip" "$TMPDIR_LOCAL/shard.$ip.yaml" "${REMOTE_DIR}/shard.yaml"
    done

    log "ship local.yaml → $ORIGIN_HOST"
    resolve_home "$ORIGIN_HOST" "$TMPDIR_LOCAL/local.yaml.tmpl" "$TMPDIR_LOCAL/local.yaml"
    scp_to "$ORIGIN_HOST" "$TMPDIR_LOCAL/local.yaml" "${REMOTE_DIR}/local.yaml"

    log "ship tiered.yaml → $TIERED_HOST"
    resolve_home "$TIERED_HOST" "$TMPDIR_LOCAL/tiered.yaml.tmpl" "$TMPDIR_LOCAL/tiered.yaml"
    scp_to "$TIERED_HOST" "$TMPDIR_LOCAL/tiered.yaml" "${REMOTE_DIR}/tiered.yaml"
}

# launch_daemon h config logname
# Uses ssh -n -f so the remote process detaches and the ssh client exits.
# nohup + disown ensures the process survives session close.
launch_daemon() {
    local h="$1" config="$2" logname="$3"
    ssh_launch "$h" "cd ~/${REMOTE_DIR} && nohup ./cache-ctl serve --config ${config} > ${logname} 2>&1 & disown"
}

# wait_healthy host:port — ping via cache-ctl on $BENCH_HOST's local binary,
# accommodating cross-network topologies where the operator's machine cannot
# reach shard IPs directly.
wait_healthy() {
    local endpoint="$1" retries="${2:-20}"
    require BENCH_HOST "$BENCH_HOST"
    local i=0
    while [ $i -lt $retries ]; do
        if ssh_run "$BENCH_HOST" "~/${REMOTE_DIR}/cache-ctl ping --endpoint ${endpoint}" 2>/dev/null | grep -q SERVING; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.3
    done
    return 1
}

cmd_start_shards() {
    require SHARDS "$SHARDS"
    for ip in $SHARDS; do
        log "start shard → $ip"
        launch_daemon "$ip" shard.yaml shard.log
    done
    sleep 2
    for ip in $SHARDS; do
        if wait_healthy "${ip}:${SHARD_HEALTH}"; then
            log "  $ip: SERVING"
        else
            die "shard $ip failed to come up; check ssh $ip 'cat ~/${REMOTE_DIR}/shard.log'"
        fi
    done
}

cmd_start_origin() {
    require ORIGIN_HOST "$ORIGIN_HOST"
    log "start origin (local) → $ORIGIN_HOST"
    launch_daemon "$ORIGIN_HOST" local.yaml local.log
    sleep 2
    if wait_healthy "127.0.0.1:${LOCAL_HEALTH}"; then
        # If BENCH_HOST == ORIGIN_HOST, loopback ping above verified it.
        # Otherwise we still verify via the cross-host health port.
        [ "$BENCH_HOST" = "$ORIGIN_HOST" ] || wait_healthy "${ORIGIN_HOST}:${LOCAL_HEALTH}" || \
            die "origin on $ORIGIN_HOST not reachable from $BENCH_HOST"
        log "  $ORIGIN_HOST: SERVING (local)"
    else
        die "origin on $ORIGIN_HOST failed to come up"
    fi
}

cmd_start_tiered() {
    require TIERED_HOST "$TIERED_HOST"
    log "start tiered → $TIERED_HOST"
    launch_daemon "$TIERED_HOST" tiered.yaml tiered.log
    sleep 2
    if wait_healthy "127.0.0.1:${TIERED_HEALTH}"; then
        [ "$BENCH_HOST" = "$TIERED_HOST" ] || wait_healthy "${TIERED_HOST}:${TIERED_HEALTH}" || \
            die "tiered on $TIERED_HOST not reachable from $BENCH_HOST"
        log "  $TIERED_HOST: SERVING (tiered)"
    else
        die "tiered on $TIERED_HOST failed to come up"
    fi
}

cmd_start() {
    local what="${1:-all}"
    case "$what" in
        shards) cmd_start_shards ;;
        origin) cmd_start_origin ;;
        tiered) cmd_start_tiered ;;
        all)    cmd_start_shards; cmd_start_origin; cmd_start_tiered ;;
        *)      die "start: unknown target '$what' (want: shards|origin|tiered|all)" ;;
    esac
}

cmd_health() {
    require SHARDS "$SHARDS"
    require BENCH_HOST "$BENCH_HOST"
    local failed=0
    for ip in $SHARDS; do
        if wait_healthy "${ip}:${SHARD_HEALTH}" 1; then
            log "  shard $ip: SERVING"
        else
            log "  shard $ip: DOWN"
            failed=$((failed + 1))
        fi
    done
    if [ -n "$ORIGIN_HOST" ]; then
        local ep="${ORIGIN_HOST}:${LOCAL_HEALTH}"
        [ "$BENCH_HOST" = "$ORIGIN_HOST" ] && ep="127.0.0.1:${LOCAL_HEALTH}"
        if wait_healthy "$ep" 1; then log "  origin $ORIGIN_HOST: SERVING"; else log "  origin $ORIGIN_HOST: DOWN"; failed=$((failed + 1)); fi
    fi
    if [ -n "$TIERED_HOST" ]; then
        local ep="${TIERED_HOST}:${TIERED_HEALTH}"
        [ "$BENCH_HOST" = "$TIERED_HOST" ] && ep="127.0.0.1:${TIERED_HEALTH}"
        if wait_healthy "$ep" 1; then log "  tiered $TIERED_HOST: SERVING"; else log "  tiered $TIERED_HOST: DOWN"; failed=$((failed + 1)); fi
    fi
    [ $failed -eq 0 ] || die "$failed host(s) unhealthy"
}

cmd_bench() {
    require BENCH_HOST "$BENCH_HOST"
    require TIERED_HOST "$TIERED_HOST"
    require ORIGIN_HOST "$ORIGIN_HOST"

    # Bench always reads from tiered and prefills to origin. Endpoint
    # choice: if bench host == tiered/origin host, use loopback (saves
    # NIC); otherwise route across the network.
    local tiered_ep="${TIERED_HOST}:${TIERED_PORT}"
    local tiered_info_ep="${TIERED_HOST}:${TIERED_HEALTH}"
    local origin_ep="${ORIGIN_HOST}:${LOCAL_PORT}"
    if [ "$BENCH_HOST" = "$TIERED_HOST" ]; then
        tiered_ep="127.0.0.1:${TIERED_PORT}"
        tiered_info_ep="127.0.0.1:${TIERED_HEALTH}"
    fi
    if [ "$BENCH_HOST" = "$ORIGIN_HOST" ]; then
        origin_ep="127.0.0.1:${LOCAL_PORT}"
    fi

    mkdir -p "$RESULTS_DIR"
    ssh_run "$BENCH_HOST" "mkdir -p ~/${REMOTE_DIR}/results && rm -f ~/${REMOTE_DIR}/results/bench-c*.*"

    for C in $CONCS; do
        log "bench concurrency=$C duration=$DURATION value=$VALUE_SIZE prefill=$PREFILL"
        ssh_run "$BENCH_HOST" "cd ~/${REMOTE_DIR} && ./cache-ctl bench \
            --endpoint $tiered_ep \
            --prefill-endpoint $origin_ep \
            --info-endpoint $tiered_info_ep \
            --concurrency $C \
            --duration $DURATION \
            --value-size $VALUE_SIZE \
            --mode get \
            --prefill $PREFILL \
            --cpu-profile results/bench-c${C}.cpu.pprof \
            --heap-profile results/bench-c${C}.heap.pprof \
            2>&1 | tee results/bench-c${C}.log"
    done

    log "pulling results → $RESULTS_DIR"
    for C in $CONCS; do
        scp_from "$BENCH_HOST" "${REMOTE_DIR}/results/bench-c${C}.log"        "$RESULTS_DIR/bench-c${C}.log"
        scp_from "$BENCH_HOST" "${REMOTE_DIR}/results/bench-c${C}.cpu.pprof"  "$RESULTS_DIR/bench-c${C}.cpu.pprof" || true
        scp_from "$BENCH_HOST" "${REMOTE_DIR}/results/bench-c${C}.heap.pprof" "$RESULTS_DIR/bench-c${C}.heap.pprof" || true
    done
}

# cmd_report parses bench-c*.log files in $RESULTS_DIR and emits
# $RESULTS_DIR/README.md. Grep-based parsing; adapt if bench output
# format changes.
cmd_report() {
    [ -d "$RESULTS_DIR" ] || die "$RESULTS_DIR does not exist (run 'bench' first)"
    local out="$RESULTS_DIR/README.md"
    local timestamp; timestamp=$(date -u +'%Y-%m-%d %H:%M UTC')

    # Collect per-concurrency rows
    local rows="" peer_rows=""
    for C in $CONCS; do
        local f="$RESULTS_DIR/bench-c${C}.log"
        [ -f "$f" ] || { log "WARN: $f missing, skipping"; continue; }

        local ops thr bw p50 p99 p999 errs apo bpo
        ops=$(grep -E '^\s*ops:'         "$f" | awk '{print $2}')
        thr=$(grep -E '^\s*throughput:'  "$f" | awk '{print $2}')
        bw=$( grep -E '^\s*bandwidth:'   "$f" | awk '{print $2}')
        p50=$( grep -E '^\s*p50:'        "$f" | awk '{print $2}')
        p99=$( grep -E '^\s*p99:'        "$f" | awk '{print $2" "$3}')
        p999=$(grep -E '^\s*p99\.9:'     "$f" | awk '{print $2" "$3}')
        errs=$(grep -E '^\s*errors:'     "$f" | awk '{print $2}')
        apo=$( grep -E '^\s*allocs/op:'  "$f" | awk '{print $2}')
        bpo=$( grep -E '^\s*bytes/op:'   "$f" | awk '{print $2}')
        rows+="| $C | $ops | $thr ops/s | $bw MiB/s | $p50 us | $p99 | $p999 | ${errs:-0} | ${apo:-?} | ${bpo:-?} |"$'\n'

        # Extract peer rows (5 lines after 'ec peers:')
        while IFS= read -r line; do
            peer_rows+="| $C | $line |"$'\n'
        done < <(awk '/ec peers:/{flag=1;next} flag && /^    s[0-9]/{print} flag && !/^    s[0-9]/{flag=0}' "$f" \
                 | sed 's/^[[:space:]]\+//')
    done

    # Topology diagram (simple, informative)
    local shard_count; shard_count=$(echo $SHARDS | wc -w)

    {
        cat <<EOF
# Cache cluster bench report

- 生成时间：$timestamp
- 拓扑：$shard_count-host EC (${EC_DATA}+${EC_PARITY}) + 1 origin (local) + 1 tiered client
- 负载：value=$VALUE_SIZE B, prefill=$PREFILL, duration=$DURATION, concurrencies=($CONCS)

## 拓扑

| 角色 | 主机 | 端口（data / health / pprof） |
|---|---|---|
EOF
        local i=0
        for ip in $SHARDS; do
            i=$((i + 1))
            echo "| shard s$i | $ip | $SHARD_PORT / $SHARD_HEALTH / $SHARD_PPROF |"
        done
        cat <<EOF
| origin (local) | $ORIGIN_HOST | $LOCAL_PORT / $LOCAL_HEALTH / $LOCAL_PPROF |
| tiered | $TIERED_HOST | $TIERED_PORT / $TIERED_HEALTH / $TIERED_PPROF |
| bench client | $BENCH_HOST | (runs on-demand) |

## 端到端结果

| conc | ops | throughput | bandwidth | p50 | p99 | p99.9 | errors | allocs/op | bytes/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
$rows

## Per-peer 行为

| conc | peer 统计（hits/misses/errors/cancelled/fills） |
|---:|---|
$peer_rows

## 工件

- \`bench-c{$(echo $CONCS | tr ' ' ',')}.log\` — 完整 bench 输出
- \`bench-c{...}.cpu.pprof\` — bench 客户端 CPU profile
- \`bench-c{...}.heap.pprof\` — bench 客户端 heap profile

查看：
\`\`\`bash
go tool pprof -top -cum $RESULTS_DIR/bench-c${CONCS%% *}.cpu.pprof
go tool pprof -alloc_space -top $RESULTS_DIR/bench-c${CONCS%% *}.heap.pprof
\`\`\`
EOF
    } > "$out"
    log "report → $out"
}

cmd_stop() {
    # ssh exits 255 when remote sshd is killed as a side-effect of
    # pkill (e.g. via a matching parent). Swallow that locally so one
    # flaky host doesn't short-circuit the rest of the cleanup.
    for h in $(all_hosts); do
        log "stop → $h"
        ssh_run "$h" "pkill -f 'cache-ctl serve' >/dev/null 2>&1; exit 0" || true
    done
}

cmd_clean() {
    cmd_stop
    sleep 1
    for h in $(all_hosts); do
        log "clean → $h"
        ssh_run "$h" "rm -rf ~/${REMOTE_DIR}/rocks-* ~/${REMOTE_DIR}/results ~/${REMOTE_DIR}/*.log; exit 0" || true
    done
    if [ -d "$RESULTS_DIR" ]; then
        log "clean local $RESULTS_DIR/*.log *.pprof"
        rm -f "$RESULTS_DIR"/bench-c*.log "$RESULTS_DIR"/bench-c*.pprof
    fi
}

cmd_all() {
    # Clean first: stops any leftover daemons (which would otherwise
    # cause `scp: Text file busy` because they hold the deployed
    # binary open) and wipes remote rocksdb dirs (which would
    # otherwise mix data from a prior binary version with the freshly
    # deployed one).
    cmd_clean
    cmd_deploy
    cmd_start all
    cmd_health
    cmd_bench
    cmd_report
}

# ── Entrypoint ────────────────────────────────────────────────────────────

# Fill in defaults that depend on each other, after env read.
if [ -z "$ORIGIN_HOST" ] && [ -n "$SHARDS" ]; then
    ORIGIN_HOST=$(echo $SHARDS | awk '{print $1}')
fi
[ -z "$TIERED_HOST" ] && TIERED_HOST="$ORIGIN_HOST"
[ -z "$BENCH_HOST" ]  && BENCH_HOST="$TIERED_HOST"

CMD="${1:-}"
shift || true

case "$CMD" in
    deploy) cmd_deploy ;;
    start)  cmd_start "${1:-all}" ;;
    health) cmd_health ;;
    bench)  cmd_bench ;;
    report) cmd_report ;;
    stop)   cmd_stop ;;
    clean)  cmd_clean ;;
    all)    cmd_all ;;
    ""|-h|--help|help)
        sed -n '3,60p' "$0"
        ;;
    *)
        die "unknown command '$CMD'; run with --help"
        ;;
esac
