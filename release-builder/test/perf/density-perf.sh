#!/usr/bin/env bash
#
# density-perf.sh — sandbox resource-control density harness, human-readable.
#
# Spins up node-ctl + N sandboxes, runs the shared test/perf/workload.py
# inside each guest, and prints a live progress line every 5s plus a
# multi-section report at the end. Output is for humans (terminal + a
# .txt mirror under test/results/); no JSON.
#
# Guest workload (test/perf/workload.py — pulled verbatim into each sandbox):
#
#   MODE=cycles  default. Deterministic grow/rest: each guest grows RSS
#                from WL_RMIN_MIB to WL_RMAX_MIB then releases, WL_CYCLES
#                times across WL_DURATION. Reproducible signal — best for
#                regression tracking. (Default 4 cycles in 30s, 64→192 MiB.)
#   MODE=pareto  Closer to real agent traffic: Poisson(λ=WL_LAMBDA) arrivals
#                with Pareto(α=WL_ALPHA, xmin=WL_XMIN) active durations.
#                Needs longer windows (≥5 min) for stable percentiles.
#   MODE=idle    Boot then sleep — measures the per-sandbox baseline cost
#                (kernel + sandbox-init + idle CH) without workload pressure.
#
# Pages are dirtied with 0xff in 4 MiB chunks so anonymous RSS actually
# faults in on the host UFFD path (otherwise the kernel would defer faults
# and the harness would under-count true memory cost).
#
# Verdict (printed at end):
#   PASS      all N admitted, 0 rejects, 0 OOMs
#   DEGRADED  any rejects but 0 OOMs (admission backpressure worked)
#   FAIL      any OOM kill (controller didn't reclaim in time)
#
# Knobs (env-driven; only the bracketed group of node-ctl tuning is
# normally left at defaults — set them only when probing the controller).
#
#   Sandbox shape
#     N              concurrent sandboxes (default 8)
#     MEM_MIB        per-sandbox memory zone (default 256)
#     FLOOR_MIB      per-sandbox allocatable floor (default 64)
#     BURST_MIB      per-sandbox startup memory request (default = FLOOR_MIB)
#                    NOTE: yaml field is `resources.startup.memory` (was startup_burst)
#     CAP_MIB        per-sandbox capacity (default = MEM_MIB)
#
#   Workload
#     MODE           cycles | pareto | idle (default cycles)
#     WL_DURATION    workload run seconds inside the guest (default 30)
#     WL_RMIN_MIB    active phase rss min (default 64)
#     WL_RMAX_MIB    active phase rss max (default 192)
#     WL_CYCLES      (cycles) cycles per duration (default 4)
#     WL_LAMBDA      (pareto) Poisson events/s (default 0.5)
#     WL_ALPHA       (pareto) Pareto shape (default 1.5)
#     WL_XMIN        (pareto) Pareto x_min seconds (default 1.0)
#
#   Test windows
#     ADMIT_DEADLINE seconds an admit may sit in the queue before being
#                    canceled; also the host-side observation window when
#                    larger than WL_DURATION. Default = WL_DURATION (current
#                    behavior). Raise for N>>startup_pool/burst so queued
#                    admits get drained before TTL. Drives node-ctl
#                    admission.queue_ttl in the emitted yaml.
#
#   Launch + workspace
#     STAGGER_S      seconds between consecutive sandbox launches (default 0)
#     IMAGE          docker image for guest rootfs (default python:3.12-slim)
#     WORK           workdir for daemon/sandbox logs (default /tmp/density-perf-XXX)
#     PERF_KEEP=1    don't delete WORK on exit (default: delete)
#
#   node-ctl controller [advanced]
#     PHYS_MEM, HOST_RES_MEM, PHYS_CPU, HOST_RES_CPU,
#     HIGH_FACTOR, LOW_FACTOR, EMERG_FACTOR, STARTUP_FACTOR,
#     GRANT_PER_SEC_FACTOR, RECOVER_DUR,
#     ADM_RATE, ADM_BURST   # admission token bucket; node.md defaults 4/16
#
# Report sink:
#   test/results/density-perf-N{N}.txt (PERF_OUT overrides) — final report
#   verbatim mirror; live PROGRESS lines go to stderr only.
#
# Skips on missing kvm/root/docker/binaries; REQUIRE_KVM=1 turns skip into
# hard failure (CI gate).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$REPO_ROOT/test/lib/tarstream.sh"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

skip() {
    echo
    echo "==> density-perf: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible"
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

command -v docker   >/dev/null 2>&1 || skip "docker not available"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
command -v python3  >/dev/null 2>&1 || skip "python3 not on PATH"

for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl cloud-hypervisor; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run make vmlinux)"

# ---- knobs ----
N="${N:-8}"
MEM_MIB="${MEM_MIB:-256}"
FLOOR_MIB="${FLOOR_MIB:-64}"
BURST_MIB="${BURST_MIB:-$FLOOR_MIB}"
CAP_MIB="${CAP_MIB:-$MEM_MIB}"
MODE="${MODE:-cycles}"
WL_DURATION="${WL_DURATION:-30}"
WL_CYCLES="${WL_CYCLES:-4}"
WL_RMIN_MIB="${WL_RMIN_MIB:-64}"
WL_RMAX_MIB="${WL_RMAX_MIB:-192}"
WL_LAMBDA="${WL_LAMBDA:-0.5}"
WL_ALPHA="${WL_ALPHA:-1.5}"
WL_XMIN="${WL_XMIN:-1.0}"
ADMIT_DEADLINE="${ADMIT_DEADLINE:-$WL_DURATION}"
# Host-side observation runs for whichever is longer: workload duration or
# the admit deadline. Default (ADMIT_DEADLINE unset) matches WL_DURATION
# exactly so historical N=8 results stay comparable.
OBSERVE_DURATION=$(( ADMIT_DEADLINE > WL_DURATION ? ADMIT_DEADLINE : WL_DURATION ))

host_total_kib=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
PHYS_MEM_DEFAULT="$((host_total_kib/1024))MiB"
PHYS_MEM="${PHYS_MEM:-$PHYS_MEM_DEFAULT}"
HOST_RES_MEM="${HOST_RES_MEM:-1GiB}"
PHYS_CPU="${PHYS_CPU:-$(nproc)}"
HOST_RES_CPU="${HOST_RES_CPU:-1}"
HIGH_FACTOR="${HIGH_FACTOR:-0.85}"
STARTUP_FACTOR="${STARTUP_FACTOR:-0.50}"
ADM_RATE="${ADM_RATE:-4}"
ADM_BURST="${ADM_BURST:-16}"
LOW_FACTOR="${LOW_FACTOR:-0.70}"
EMERG_FACTOR="${EMERG_FACTOR:-0.05}"
GRANT_PER_SEC_FACTOR="${GRANT_PER_SEC_FACTOR:-0.20}"
RECOVER_DUR="${RECOVER_DUR:-30s}"
STAGGER_S="${STAGGER_S:-0}"
TICK_S=5

WORK="${WORK:-$(mktemp -d /tmp/density-perf-XXXXXX)}"
mkdir -p "$WORK" "$WORK/run" "$WORK/lib"
OUT="${PERF_OUT:-$REPO_ROOT/test/results/density-perf-N${N}.txt}"
mkdir -p "$(dirname "$OUT")"

DAEMON_PID=""
declare -a SB_PIDS=()
declare -a SB_SIDS=()

# Graceful sandbox-ctl shutdown. sandbox-ctl owns the CH lifecycle: on
# SIGTERM it forwards SIGTERM to CH, waits chShutdownGrace=5s, then
# SIGKILLs CH and tears down tap / run dir / cgroup.
#
# Project policy: NEVER strong-kill sandbox-ctl from the script. SIGKILL
# orphans CH (leaked tap/run/cgroup) and silently masks sandbox-ctl bugs.
# If sandbox-ctl doesn't exit within the timeout, leave it for human
# investigation — print a loud WARN with the exact cleanup commands.
#
# Timeout 60s: under heavy oversubscription (e.g. MEM_MIB=8192 N=8 on
# 8GiB host), the host page-fault path saturates uffd handlers and the
# Go scheduler delays sandbox-ctl's shutdown goroutine — observed 5s
# chShutdownGrace expanding to ~11s wallclock. 60s = generous slack.
stop_sandbox_ctl() {
    local pid="$1" sid="${2:-?}" waited=0 timeout=120
    # Reap already-exited children first. A sandbox-ctl that exited
    # naturally (e.g. its admit was queue-canceled and Run() returned
    # an error long ago) is gone from /proc OR sits as a zombie in
    # bash's job table. kill -0 is unreliable because zombies look
    # "alive" — read /proc/$pid/stat directly.
    if [ ! -d "/proc/$pid" ] || \
       [ "$(awk '{print $3}' /proc/$pid/stat 2>/dev/null)" = "Z" ]; then
        wait "$pid" 2>/dev/null || true   # process is gone; its exit code is not a teardown failure
        return 0
    fi
    kill -TERM "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
    while [ "$waited" -lt "$timeout" ]; do
        # Same as the entry check — kill -0 alone misses zombies.
        if [ ! -d "/proc/$pid" ] || \
           [ "$(awk '{print $3}' /proc/$pid/stat 2>/dev/null)" = "Z" ]; then
            wait "$pid" 2>/dev/null || true   # process is gone; its exit code is not a teardown failure
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    # 60s SIGTERM timeout — record the hang for the verdict.
    # HANG_FILE is set by the main script before spawning stop_sandbox_ctl
    # subshells. We can't rely on this subshell's exit code making it
    # back via `wait`: bash auto-reaps background children in script mode
    # (set +m), so the parent's later `wait $pid` returns 127 "no child"
    # for any subshell that completed before the wait was called.
    [ -n "${HANG_FILE:-}" ] && echo "$sid" >> "$HANG_FILE"
    echo "WARN: sandbox-ctl pid=$pid sid=$sid did not exit within ${timeout}s of SIGTERM" >&2
    echo "WARN: leaving it running per project policy (no script-level SIGKILL of sandbox-ctl)" >&2
    echo "WARN: this is a sandbox-ctl bug (likely scheduler starvation under host oversubscribe)" >&2
    echo "WARN: manual cleanup if needed:" >&2
    echo "WARN:   sudo kill -KILL $pid" >&2
    echo "WARN:   sudo pkill -KILL -f 'cloud-hypervisor.*--net tap=${sid}-tap'" >&2
    echo "WARN:   sudo ip link delete ${sid}-tap; sudo rmdir /sys/fs/cgroup/sandboxes/${sid}" >&2
    return 1
}

cleanup_all() {
    set +e
    # Graceful stop, in parallel — each sandbox-ctl runs its own CH teardown.
    # Track the helper subshell PIDs so we can `wait` on just those (bare
    # `wait` would also block on $DAEMON_PID which is still alive at this
    # point — that's the next step's job).
    local stop_pids=()
    for i in "${!SB_PIDS[@]}"; do
        stop_sandbox_ctl "${SB_PIDS[$i]}" "${SB_SIDS[$i]}" &
        stop_pids+=($!)
    done
    for p in "${stop_pids[@]}"; do wait "$p" 2>/dev/null; done
    for sid in "${SB_SIDS[@]}"; do
        rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null
        ip link delete "${sid}-tap" 2>/dev/null
        rm -rf "/run/sandbox/$sid" 2>/dev/null
    done
    if [ -n "$DAEMON_PID" ]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    if [ -z "${PERF_KEEP:-}" ]; then
        rm -rf "$WORK"
    else
        echo "kept work dir: $WORK" >&2
    fi
    set -e
}
trap cleanup_all EXIT

# ---- workload description (used in both SETUP block and the report) ----
case "$MODE" in
    cycles) WORKLOAD_DESC="'cycles' — each guest grows RSS ${WL_RMIN_MIB}→${WL_RMAX_MIB} MiB then releases, ${WL_CYCLES}× over ${WL_DURATION}s (deterministic)";;
    pareto) WORKLOAD_DESC="'pareto' — Poisson(λ=${WL_LAMBDA}/s) arrivals, Pareto(α=${WL_ALPHA}, xmin=${WL_XMIN}s) active durations, RSS ${WL_RMIN_MIB}-${WL_RMAX_MIB} MiB";;
    idle)   WORKLOAD_DESC="'idle' — boot then sleep ${WL_DURATION}s (per-sandbox baseline cost, no workload)";;
    *)      WORKLOAD_DESC="'$MODE' — see test/perf/workload.py";;
esac

# ---- prepare blk0 ----
BLK0="$WORK/blk0.img"
if ! [ -f "$BLK0" ]; then
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE"
    fi
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0" --no-progress >/dev/null
fi
BLK0_REF="$(plaintext_tarstream_ref "$BLK0")"

[ -d /sys/fs/cgroup/sandboxes ] || mkdir /sys/fs/cgroup/sandboxes
echo "+memory +cpu" > /sys/fs/cgroup/sandboxes/cgroup.subtree_control 2>/dev/null || true

WORKLOAD_PY="$(cat "$REPO_ROOT/test/perf/workload.py")"

emit_yaml() {
    local sid="$1"
    {
        cat <<EOF
resources:
  capacity:
    cpu: 1
    memory: ${CAP_MIB}MiB
  allocatable:
    cpu: 1
    memory: ${FLOOR_MIB}MiB
    deflate_on_oom: true
  control:
    cgroup_path: /sys/fs/cgroup/sandboxes/${sid}
    controller: $WORK/sandbox-resource.sock
    sensor:
      psi_some_stall_us: ${PSI_STALL_US:-10000}
      psi_some_window_us: ${PSI_WINDOW_US:-1000000}
      min_interval_ms: ${PSI_MIN_INT_MS:-100}
  startup:
    memory: ${BURST_MIB}MiB
network:
  tap: ${sid}-tap
boot:
  kernel: file://${VMLINUX}
  runtime: file://${BIN}/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root:
    base: ${BLK0_REF}
    overlay:
      diff: file://${WORK}/${sid}.diff
launch:
  exec: /usr/local/bin/python3
  env:
    WL_MODE: "${MODE}"
    WL_DURATION: "${WL_DURATION}"
    WL_CYCLES: "${WL_CYCLES}"
    WL_LAMBDA: "${WL_LAMBDA}"
    WL_ALPHA: "${WL_ALPHA}"
    WL_XMIN: "${WL_XMIN}"
    WL_RMIN_MIB: "${WL_RMIN_MIB}"
    WL_RMAX_MIB: "${WL_RMAX_MIB}"
    PYTHONUNBUFFERED: "1"
  restart: never
  args:
    - "-c"
    - |
EOF
        printf '%s\n' "$WORKLOAD_PY" | sed 's/^/      /'
    } > "$WORK/$sid.yaml"
}

# ---- node-ctl conductor serve config (only the resource_listen controller is live) ----
# install_units=false + api on a throwaway port keep serve from touching host
# systemd / real ports; sandboxes are still launched directly by sandbox-ctl
# against resource_listen.socket.
cat > "$WORK/node-ctl.yaml" <<EOF
api: { domain: density.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { mode: internal, auth: enforce }
sandbox:
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
paths:
  run_root: $WORK/run
  base_root: $WORK/lib
  config_socket: $WORK/node-ctl.socket
  db_path: $WORK/node-ctl.db
units: { dir: $WORK/units, install: false }
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  state_path: $WORK/state.json
  audit_path: $WORK/audit.log
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandboxes
  resources:
    physical_memory: $PHYS_MEM
    physical_cpu: $PHYS_CPU
    host_reserved:
      memory: $HOST_RES_MEM
      cpu: $HOST_RES_CPU
  watermarks:
    operational_margin_factor: 0.10
    high_factor: $HIGH_FACTOR
    low_factor: $LOW_FACTOR
    emergency_factor: $EMERG_FACTOR
    startup_factor: $STARTUP_FACTOR
  rate_limits:
    memory_grant_per_sec_factor: $GRANT_PER_SEC_FACTOR
  admission:
    rate: $ADM_RATE
    burst: $ADM_BURST
    startup_ttl: 30s
    queue_ttl: ${ADMIT_DEADLINE}s
    queue_max_depth: 256
  dampening:
    recover_duration: $RECOVER_DUR
    cooldown_periods: 5
  log_level: info
EOF

# ---- helpers: snapshot host avail / controller counters / cgroup events ----
read_avail() { awk '/MemAvailable:/ {printf "%d", $2/1024}' /proc/meminfo; }

# Counters: admits / rejects from audit + daemon log; reclaims from audit;
# grants / settled from daemon. We use `grep | wc -l` (not `grep -c`)
# because `grep -c` outputs "0" on zero matches but ALSO exits 1, which
# would chain into `|| echo 0` and emit a SECOND "0" — the caller's word-
# splitting would then take 0-match fields as TWO array entries and shift
# every subsequent counter by one position. wc -l always succeeds, emits
# exactly one number per invocation; safe under set -e.
count() { grep -E "$1" "$2" 2>/dev/null | wc -l; }

# queue_depth derives "currently queued admits" from audit events: a sid
# enters the queue on `admit_queued sid=X` and leaves it on either
# `admit token=... sid=X` (granted) or `admit_queue_canceled sid=X`
# (TTL / EOF). Awk maintains a per-sid set; END prints the residual.
queue_depth() {
    [ -f "$WORK/audit.log" ] || { echo 0; return; }
    awk '
/admit_queued/             { for(i=2;i<=NF;i++) if($i ~ /^sid=/) { q[substr($i,5)]=1; break } }
/admit token=/             { for(i=2;i<=NF;i++) if($i ~ /^sid=/) { delete q[substr($i,5)];   break } }
/admit_queue_canceled/     { for(i=2;i<=NF;i++) if($i ~ /^sid=/) { delete q[substr($i,5)];   break } }
END                        { n=0; for(k in q) n++; print n }' "$WORK/audit.log"
}

snapshot_counters() {
    # `admit token=` matches only the canonical success-admit event from
    # buildAdmitOK (server.go); excludes admit_queued / admit_queue_*
    # bookkeeping events that share the 'admit' substring.
    #
    # `admit_queue_canceled` (audit.log) counts queued admits that were
    # not admitted because of queue TTL expiry or client disconnect. It
    # is distinct from `rejected` (controller-side refusal) — we sum
    # them as "not admitted" in the verdict path.
    local admits reclaims grants settled rejects canceled queued
    admits=$(  count 'admit token='          "$WORK/audit.log" )
    reclaims=$(count '^[^ ]* reclaim'        "$WORK/audit.log" )
    grants=$(  count ' grant '               "$WORK/daemon.log")
    settled=$( count ' settled '             "$WORK/daemon.log")
    rejects=$( count 'rejected'              "$WORK/daemon.log")
    canceled=$(count 'admit_queue_canceled ' "$WORK/audit.log" )
    queued=$(queue_depth)
    echo "$admits $reclaims $grants $settled $rejects $canceled $queued"
}
snapshot_cgroup() {
    local total_high=0 total_oom=0 h o sid f
    for sid in "${SB_SIDS[@]}"; do
        f="/sys/fs/cgroup/sandboxes/$sid/memory.events.local"
        [ -f "$f" ] || continue
        h=$(awk '$1=="high" {print $2}' "$f"); h=${h:-0}
        o=$(awk '$1=="oom"  {print $2}' "$f"); o=${o:-0}
        total_high=$((total_high+h))
        total_oom=$((total_oom+o))
    done
    echo "$total_high $total_oom"
}

# ---- helper: print the BANNER/SETUP/BOOT sections (only once, into log+tee) ----
say()  { printf '%s\n' "$*"      | tee -a "$OUT"; }
sayf() { printf "$@"             | tee -a "$OUT"; }

: > "$OUT"   # truncate

say "============================================================"
say " density-perf:  N=$N sandboxes,  workload=$MODE,  dur=${WL_DURATION}s"
say "============================================================"
say ""
say " SETUP"
say "   per-sandbox:  zone=${MEM_MIB} MiB  floor=${FLOOR_MIB} MiB  burst=${BURST_MIB} MiB  cap=${CAP_MIB} MiB"
say "   node pool:    ${PHYS_MEM} (host_reserved=${HOST_RES_MEM}, host_cpu=${PHYS_CPU}/${HOST_RES_CPU} reserved)"
say "   watermarks:   high=${HIGH_FACTOR}  low=${LOW_FACTOR}  emergency=${EMERG_FACTOR}"
say "   workload:     $WORKLOAD_DESC"
say "   timing:       workload=${WL_DURATION}s  admit_deadline=${ADMIT_DEADLINE}s  observe=${OBSERVE_DURATION}s  (queue_ttl=${ADMIT_DEADLINE}s)"
say ""

# ---- spin up the resource controller (node-ctl conductor serve, resource_listen) ----
"$BIN/node-ctl" conductor serve --config "$WORK/node-ctl.yaml" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 60); do
    [ -S "$WORK/sandbox-resource.sock" ] && break
    kill -0 "$DAEMON_PID" 2>/dev/null || break
    sleep 0.25
done
[ -S "$WORK/sandbox-resource.sock" ] || { say " FATAL: node-ctl conductor serve did not bind the resource socket"; cat "$WORK/daemon.log" >&2; exit 1; }

# ---- baseline host memory (taken AFTER daemon up, BEFORE first sandbox) ----
host_baseline_avail=$(read_avail)

# ---- launch sandboxes ----
say " BOOT"
launch_t0=$(date +%s.%N)
for i in $(seq 1 "$N"); do
    sid="sb-perf-$i"
    SB_SIDS+=("$sid")
    mkdir -p "/sys/fs/cgroup/sandboxes/$sid"
    ip tuntap add "${sid}-tap" mode tap 2>/dev/null || true
    ip link set "${sid}-tap" up
    truncate -s 1G "$WORK/${sid}.diff"
    mkfs.ext4 -q -F "$WORK/${sid}.diff"
    emit_yaml "$sid"
    "$BIN/sandbox-ctl" run \
        --config "$WORK/$sid.yaml" \
        --sandbox-id "$sid" \
        --ch-binary "$BIN/cloud-hypervisor" \
        >"$WORK/$sid.log" 2>&1 &
    SB_PIDS+=("$!")
    [ "$STAGGER_S" != "0" ] && sleep "$STAGGER_S"
done
launch_t1=$(date +%s.%N)
launch_wall=$(awk -v a="$launch_t0" -v b="$launch_t1" 'BEGIN{printf "%.3f", b-a}')

# Give the daemon a moment to log admit events for the just-launched sandboxes.
sleep 1
boot_counters=( $(snapshot_counters) )
sayf "   launched %d sandboxes in %.2fs (stagger=%ss)\n" "$N" "$launch_wall" "$STAGGER_S"
sayf "   admitted by node-ctl: %d of %d  (rejected: %d)\n" \
    "${boot_counters[0]}" "$N" "${boot_counters[4]}"
say ""

# ---- monitor loop (each tick prints to stderr only — keeps OUT clean) ----
# The primary table is cumulative-only (count snapshots per tick); deltas
# and anomalies (admit / grant / reclaim / reject / oom / high spikes)
# are inlined into a variable-width "notes" column at row end. Pure ASCII
# in header strings to keep printf %s byte-counts == visual columns.
echo " PROGRESS (every ${TICK_S}s — host MemAvailable + node-ctl counters; deltas in notes)" >&2
# Columns:
#   adm/N  cumulative admits / target
#   set    cumulative Settled events (admit→Settled transition)
#   q      current queue depth (admits awaiting startup-pool / token slot)
#   cnl    cumulative queue-canceled (TTL or client EOF)
#   grnt   cumulative burst grants
#   rclm   cumulative controller reclaims
#   rej    cumulative rejects (long-term refusal)
#   oom    cumulative cgroup OOM kills
ROW_FMT="  %-5s   %5s   %5s    %-5s   %3s   %3s   %3s   %4s   %4s   %3s   %3s    %s\n"
printf "$ROW_FMT" "time" "avail" "dMiB" "adm/N" "set" "q" "cnl" "grnt" "rclm" "rej" "oom" "notes" >&2

prev_avail=$host_baseline_avail
prev_counters=( $(snapshot_counters) )
prev_cg=( $(snapshot_cgroup) )
min_avail=$host_baseline_avail        # tracks the worst (lowest) host avail
                                       # — RESULT uses it for peak per-sandbox cost
                                       # rather than the misleading end-of-run value
                                       # (page cache rebounds after workload drains)

elapsed=0
while [ "$elapsed" -lt "$OBSERVE_DURATION" ]; do
    sleep "$TICK_S"
    elapsed=$((elapsed + TICK_S))

    avail=$(read_avail)
    counters=( $(snapshot_counters) )   # admits reclaims grants settled rejects canceled queued
    cg=( $(snapshot_cgroup) )           # high oom

    [ "$avail" -lt "$min_avail" ] && min_avail=$avail

    d_avail=$((avail - prev_avail))
    d_admits=$((   counters[0] - prev_counters[0] ))
    d_reclaims=$(( counters[1] - prev_counters[1] ))
    d_grants=$((   counters[2] - prev_counters[2] ))
    d_settled=$((  counters[3] - prev_counters[3] ))
    d_rejects=$((  counters[4] - prev_counters[4] ))
    d_canceled=$(( counters[5] - prev_counters[5] ))
    d_high=$((     cg[0]       - prev_cg[0] ))
    d_oom=$((      cg[1]       - prev_cg[1] ))

    # Assemble inline notes. Only deltas that fired this tick (and OOM/REJ
    # / high spikes) show up — quiet ticks have an empty notes column.
    notes=""
    [ "$d_admits"   -gt 0 ]    && notes="$notes +${d_admits}adm"
    [ "$d_settled"  -gt 0 ]    && notes="$notes +${d_settled}set"
    [ "$d_canceled" -gt 0 ]    && notes="$notes +${d_canceled}cnl"
    [ "$d_grants"   -gt 0 ]    && notes="$notes +${d_grants}grnt"
    [ "$d_reclaims" -gt 0 ]    && notes="$notes +${d_reclaims}rclm"
    [ "$d_rejects"  -gt 0 ]    && notes="$notes +${d_rejects}REJ"
    [ "$d_oom"      -gt 0 ]    && notes="$notes +${d_oom}OOM!"
    [ "$d_high"     -gt 1000 ] && notes="$notes +${d_high}high"
    notes="${notes# }"

    adm_s="${counters[0]}/${N}"
    printf "$ROW_FMT" \
        "t+${elapsed}s" "$avail" "$d_avail" \
        "$adm_s" "${counters[3]}" "${counters[6]}" "${counters[5]}" \
        "${counters[2]}" "${counters[1]}" "${counters[4]}" "${cg[1]}" \
        "$notes" >&2

    prev_avail=$avail
    prev_counters=( "${counters[@]}" )
    prev_cg=( "${cg[@]}" )
done

# ---- shut down sandboxes via sandbox-ctl graceful path (parallel) ----
# workloads may have already self-exited at WL_DURATION — stop_sandbox_ctl
# handles the no-op case (kill -TERM of a dead PID returns silently).
# Track stop subshell PIDs so we only wait on those, not on $DAEMON_PID
# (still alive — cleanup_all takes it down later).
# Note: `|| true` is required even with `2>/dev/null` because the latter
# only masks stderr, not the exit code. wait returns the subshell's exit
# (1 if stop_sandbox_ctl timed out; 127 if already reaped) and set -e
# would otherwise abort the script before RESULT renders.
stop_pids=()
# stop_sandbox_ctl writes one line per hang to HANG_FILE (60s SIGTERM
# timeout cases). We can't read the subshell's exit code via wait — in
# script mode (set +m) bash auto-reaps completed background children,
# so a later wait returns 127 "no such child" indistinguishably from
# a real exit-1 hang.
HANG_FILE=$(mktemp -t density-perf-hangs.XXXXXX)
export HANG_FILE
for i in "${!SB_PIDS[@]}"; do
    stop_sandbox_ctl "${SB_PIDS[$i]}" "${SB_SIDS[$i]}" &
    stop_pids+=($!)
done
for p in "${stop_pids[@]}"; do wait "$p" 2>/dev/null || true; done
hangs=$(wc -l < "$HANG_FILE" 2>/dev/null || echo 0)
rm -f "$HANG_FILE"
unset HANG_FILE

# ---- final aggregate (after the workload window closes) ----
final_counters=( $(snapshot_counters) )
final_cg=( $(snapshot_cgroup) )

final_admits=${final_counters[0]}
final_reclaims=${final_counters[1]}
final_grants=${final_counters[2]}
final_settled=${final_counters[3]}
final_rejects=${final_counters[4]}
final_canceled=${final_counters[5]}
final_queued=${final_counters[6]}
final_high=${final_cg[0]}
final_oom=${final_cg[1]}
final_avail=$(read_avail)
peak_drop=$((host_baseline_avail - min_avail))
post_drop=$((host_baseline_avail - final_avail))
peak_per_sb=$(awk -v d="$peak_drop" -v n="$N" 'BEGIN{printf "%.1f", d/n}')
post_per_sb=$(awk -v d="$post_drop" -v n="$N" 'BEGIN{printf "%.1f", d/n}')
projected=$(awk -v a="$min_avail" -v p="$peak_per_sb" 'BEGIN{
    if (p > 0.5) printf "%d", a/p; else print "n/a (per-sandbox cost too small to project)"
}')

# Verdict. Order: OOM (FAIL) > hangs (DEGRADED) > rejections/cancellations
# (DEGRADED) > admit gap (DEGRADED) > PASS. Hangs degrade because a
# sandbox-ctl that ignores SIGTERM for 60s indicates a real bug, even
# when admission itself worked.
if [ "$final_oom" -gt 0 ]; then
    verdict="FAIL"
    verdict_reason="$final_oom cgroup OOM kill(s) — controller did not reclaim in time"
elif [ "$hangs" -gt 0 ]; then
    verdict="DEGRADED"
    verdict_reason="$hangs sandbox-ctl(s) did not exit within 60s of SIGTERM (see WARN above)"
elif [ "$final_rejects" -gt 0 ] || [ "$final_canceled" -gt 0 ]; then
    verdict="DEGRADED"
    verdict_reason="$final_rejects rejection(s) + $final_canceled queue-cancellation(s); $final_admits of $N admitted"
elif [ "$final_admits" -lt "$N" ]; then
    verdict="DEGRADED"
    if [ "$final_queued" -gt 0 ]; then
        verdict_reason="only $final_admits of $N admitted; $final_queued still queued at observe end (raise ADMIT_DEADLINE, lower BURST_MIB, or pool/startup capacity exhausted by settled tenants)"
    else
        verdict_reason="only $final_admits of $N admitted (no rejections / cancellations / queue residue — check daemon.log + audit.log)"
    fi
else
    verdict="PASS"
    verdict_reason="all $N admitted, 0 rejects, 0 cancels, 0 OOMs, 0 hangs"
fi

# ---- RESULT section ----
say ""
say " RESULT       verdict: $verdict"
say "              ($verdict_reason)"
say ""
say "   density"
sayf "     peak per-sandbox cost:           ~%s MiB MemAvailable\n" "$peak_per_sb"
sayf "       (at peak pressure: %d MiB drop across %d sandboxes; baseline=%d → min=%d)\n" \
    "$peak_drop" "$N" "$host_baseline_avail" "$min_avail"
sayf "     after-release per-sandbox:       ~%s MiB MemAvailable\n" "$post_per_sb"
sayf "       (end of window: %d MiB drop; workload drained + page reclaim active)\n" "$post_drop"
sayf "     projected ceiling (peak basis):  ~%s sandboxes\n" "$projected"
say ""
say "   admission"
sayf "     %d admitted / %d rejected / %d queue-canceled / %d still queued\n" \
    "$final_admits" "$final_rejects" "$final_canceled" "$final_queued"
sayf "     %d settled at shutdown / %d burst grants total / %d sandbox-ctl hang(s)\n" \
    "$final_settled" "$final_grants" "$hangs"
say ""
say "   pressure"
sayf "     cgroup memory.high events: %s across %d sandboxes\n" "$final_high" "$N"
if [ "$MODE" = "cycles" ] && [ "$final_high" -gt 0 ]; then
    say  "       (expected for 'cycles' workload that hovers near the floor;"
    say  "        non-zero ≠ failure — the only true failure signal is OOM)"
fi
sayf "     cgroup OOM kills:          %d\n" "$final_oom"
sayf "     controller reclaims:       %d (burst-grant pullbacks)\n" "$final_reclaims"
say ""
say "   notes"
say "     host 'used' is NOT reported — page cache movement makes it noisy and"
say "     not attributable to sandboxes. MemAvailable is the right density metric."
say ""
say " ARTIFACTS    $WORK/"
say "   daemon.log  audit.log  sb-perf-{1..$N}.{log,cgroup-events}"
say ""
say "============================================================"
echo "  full report: $OUT" >&2
