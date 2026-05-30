#!/usr/bin/env bash
#
# density-perf.sh — sandbox-resource-control perf harness.
#
# Spins up a node-ctl daemon + N sandboxes running the shared
# test/perf/workload.py model, samples controller events and host memory,
# emits JSON for downstream analysis (test/results/density-perf-N{N}.json
# by default; PERF_OUT overrides).
#
# Knobs (all env-driven so the same script profiles different setups):
#
#   N                 number of concurrent sandboxes (default 8)
#   MEM_MIB           per-sandbox memory-zone size MiB (default 256)
#   FLOOR_MIB         per-sandbox allocatable floor MiB (default 64)
#   BURST_MIB         per-sandbox startup_burst MiB (default = FLOOR_MIB)
#   CAP_MIB           per-sandbox capacity MiB (default = MEM_MIB)
#   MODE              workload mode: cycles | pareto | idle (default cycles)
#   WL_DURATION       seconds (default 30)
#   WL_CYCLES         (cycles mode) cycles per duration (default 4)
#   WL_RMIN_MIB       active phase rss min (default 64)
#   WL_RMAX_MIB       active phase rss max (default 192)
#   WL_LAMBDA         (pareto) Poisson events/s (default 0.5)
#   WL_ALPHA          (pareto) shape (default 1.5)
#   WL_XMIN           (pareto) x_min seconds (default 1.0)
#   PHYS_MEM          node-ctl physical_memory (default auto-from-host)
#   HOST_RES_MEM      node-ctl host_reserved.memory (default 1GiB)
#   PHYS_CPU          node-ctl physical_cpu (default 8)
#   HOST_RES_CPU      node-ctl host_reserved.cpu (default 1)
#   HIGH_FACTOR       node-ctl high_factor (default 0.85)
#   LOW_FACTOR        node-ctl low_factor (default 0.70)
#   EMERG_FACTOR      node-ctl emergency_factor (default 0.05)
#   GRANT_PER_SEC_FACTOR  node-ctl rate_limits factor (default 0.20)
#   RECOVER_DUR       node-ctl dampening recover_duration (default 30s)
#   STAGGER_S         seconds between consecutive sandbox launches (default 0)
#   IMAGE             docker image (default python:3.12-slim)
#   WORK              workdir for transient logs (default /tmp/density-perf-XXXXXX)
#   PERF_OUT          aggregated JSON output path
#                     (default $REPO_ROOT/test/results/density-perf-N{N}.json)
#
# Transient outputs in $WORK:
#   daemon.log                    controller daemon stdout
#   audit.log                     controller audit (admit/release/reclaim)
#   sb-perf-N.log per sandbox     sandbox-ctl stdout
#   sb-perf-N.cgroup-events       memory.events.local snapshot at end
#
# Persistent output (under test/results/, matches sandbox-perf{,-manifest}.sh):
#   density-perf-N{N}.json        aggregated metrics
#
# Skips on missing kvm/root/docker/binaries (REQUIRE_KVM=1 turns skip
# into failure).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
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
# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

command -v docker >/dev/null 2>&1 || skip "docker not available"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"

for b in sandbox-ctl node-ctl sandbox-init sandbox-runtime.erofs flatten-ctl cloud-hypervisor; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build cloud-hypervisor'"
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

host_total_kib=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
PHYS_MEM_DEFAULT="$((host_total_kib/1024))MiB"
PHYS_MEM="${PHYS_MEM:-$PHYS_MEM_DEFAULT}"
HOST_RES_MEM="${HOST_RES_MEM:-1GiB}"
PHYS_CPU="${PHYS_CPU:-$(nproc)}"
HOST_RES_CPU="${HOST_RES_CPU:-1}"
HIGH_FACTOR="${HIGH_FACTOR:-0.85}"
LOW_FACTOR="${LOW_FACTOR:-0.70}"
EMERG_FACTOR="${EMERG_FACTOR:-0.05}"
GRANT_PER_SEC_FACTOR="${GRANT_PER_SEC_FACTOR:-0.20}"
RECOVER_DUR="${RECOVER_DUR:-30s}"
STAGGER_S="${STAGGER_S:-0}"

WORK="${WORK:-$(mktemp -d /tmp/density-perf-XXXXXX)}"
mkdir -p "$WORK"
DAEMON_PID=""
declare -a SB_PIDS=()
declare -a SB_SIDS=()

cleanup_all() {
    set +e
    for p in "${SB_PIDS[@]}"; do
        kill -TERM "$p" 2>/dev/null
    done
    sleep 2
    for p in "${SB_PIDS[@]}"; do
        kill -KILL "$p" 2>/dev/null
        wait "$p" 2>/dev/null
    done
    for sid in "${SB_SIDS[@]}"; do
        pkill -KILL -f "cloud-hypervisor.*--api-socket /run/$sid/" 2>/dev/null
        rmdir "/sys/fs/cgroup/sandboxes/$sid" 2>/dev/null
        ip link delete "${sid}-tap" 2>/dev/null
        rm -rf "/run/$sid" 2>/dev/null
    done
    if [ -n "$DAEMON_PID" ]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    if [ -z "${PERF_KEEP:-}" ]; then
        rm -rf "$WORK"
    else
        echo "kept work dir: $WORK"
    fi
    set -e
}
trap cleanup_all EXIT

echo "==> density-perf: N=$N MEM=${MEM_MIB}MiB floor=${FLOOR_MIB}MiB mode=$MODE dur=${WL_DURATION}s"
echo "    pool=$PHYS_MEM (host_reserved=$HOST_RES_MEM) high=$HIGH_FACTOR low=$LOW_FACTOR emerg=$EMERG_FACTOR"

# ---- prepare blk0 ----
BLK0="$WORK/blk0.erofs"
if ! [ -f "$BLK0" ]; then
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        docker pull "$IMAGE"
    fi
    docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0" --no-progress >/dev/null
fi

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
  startup_burst:
    memory: ${BURST_MIB}MiB
network:
  tap: ${sid}-tap
boot:
  kernel: file://${VMLINUX}
  runtime: file://${BIN}/sandbox-runtime.erofs
  cmdline: "console=hvc0"
  root:
    base: file://${BLK0}
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

# ---- daemon config ----
cat > "$WORK/node-ctl.yaml" <<EOF
listen: $WORK/sandbox-resource.sock
state_path: $WORK/state.json
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
rate_limits:
  memory_grant_per_sec_factor: $GRANT_PER_SEC_FACTOR
admission:
  rate: 100
  burst: 100
  max_concurrent_creating: 100
  startup_ttl: 120s
  queue_ttl: 30s
dampening:
  recover_duration: $RECOVER_DUR
  cooldown_periods: 5
logging:
  level: info
  audit_path: $WORK/audit.log
EOF

"$BIN/node-ctl" daemon --config "$WORK/node-ctl.yaml" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do
    [ -S "$WORK/sandbox-resource.sock" ] && break
    sleep 0.2
done
[ -S "$WORK/sandbox-resource.sock" ] || { echo "daemon did not bind socket" >&2; exit 1; }

# ---- baseline host memory ----
host_baseline_used=$(free -m | awk '/^Mem:/ {print $3}')
host_baseline_avail=$(awk '/MemAvailable:/ {printf "%d", $2/1024}' /proc/meminfo)

# ---- launch sandboxes ----
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
    if [ "$STAGGER_S" != "0" ]; then
        sleep "$STAGGER_S"
    fi
done
launch_t1=$(date +%s.%N)
launch_wall=$(awk -v a="$launch_t0" -v b="$launch_t1" 'BEGIN{printf "%.3f", b-a}')
echo "    launched $N in ${launch_wall}s"

# ---- wait workload + watch host memory ----
echo "    waiting ${WL_DURATION}s for workloads"
sample_t=()
sample_avail=()
sample_used=()
elapsed=0
while [ "$elapsed" -lt "$WL_DURATION" ]; do
    avail=$(awk '/MemAvailable:/ {printf "%d", $2/1024}' /proc/meminfo)
    used=$(free -m | awk '/^Mem:/ {print $3}')
    sample_t+=("$elapsed")
    sample_avail+=("$avail")
    sample_used+=("$used")
    sleep 5
    elapsed=$((elapsed+5))
done

# ---- collect cgroup events ----
for sid in "${SB_SIDS[@]}"; do
    if [ -f "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" ]; then
        cp "/sys/fs/cgroup/sandboxes/$sid/memory.events.local" "$WORK/$sid.cgroup-events"
    fi
done

# ---- shut down ----
shutdown_t0=$(date +%s.%N)
# Best-effort shutdown: workloads may have already self-exited at dur, so
# their sandbox-ctl PIDs (and CH processes) can be gone — kill/pkill of an
# absent target must not abort the harness under `set -e`.
set +e
for p in "${SB_PIDS[@]}"; do
    kill -TERM "$p" 2>/dev/null
done
sleep 5
for p in "${SB_PIDS[@]}"; do
    kill -KILL "$p" 2>/dev/null
done
for sid in "${SB_SIDS[@]}"; do
    pkill -KILL -f "cloud-hypervisor.*--api-socket /run/$sid/" 2>/dev/null
done
set -e
shutdown_t1=$(date +%s.%N)

# ---- aggregate ----
audits=$(grep -c admit "$WORK/audit.log" 2>/dev/null) || audits=0
reclaims=$(grep -c "^[^ ]* reclaim" "$WORK/audit.log" 2>/dev/null) || reclaims=0
grants=$(grep -c " grant " "$WORK/daemon.log" 2>/dev/null) || grants=0
settled=$(grep -c " settled " "$WORK/daemon.log" 2>/dev/null) || settled=0
rejects=$(grep -c "rejected" "$WORK/daemon.log" 2>/dev/null) || rejects=0

# Per-sandbox cgroup events.
total_high=0
total_oom=0
for sid in "${SB_SIDS[@]}"; do
    f="$WORK/$sid.cgroup-events"
    [ -f "$f" ] || continue
    h=$(awk '$1=="high" {print $2}' "$f"); h=${h:-0}
    o=$(awk '$1=="oom" {print $2}' "$f"); o=${o:-0}
    total_high=$((total_high+h))
    total_oom=$((total_oom+o))
done

# Output JSON. Default lands in test/results/ so it survives $WORK cleanup
# and is consistent with sandbox-perf{,-manifest}.sh; PERF_OUT overrides.
OUT="${PERF_OUT:-$REPO_ROOT/test/results/density-perf-N${N}.json}"
mkdir -p "$(dirname "$OUT")"
out="$OUT"
{
    echo "{"
    echo "  \"n\": $N,"
    echo "  \"mode\": \"$MODE\","
    echo "  \"mem_zone_mib\": $MEM_MIB,"
    echo "  \"floor_mib\": $FLOOR_MIB,"
    echo "  \"burst_mib\": $BURST_MIB,"
    echo "  \"cap_mib\": $CAP_MIB,"
    echo "  \"workload\": {"
    echo "    \"duration_s\": $WL_DURATION,"
    echo "    \"cycles\": $WL_CYCLES,"
    echo "    \"rmin_mib\": $WL_RMIN_MIB,"
    echo "    \"rmax_mib\": $WL_RMAX_MIB,"
    echo "    \"lambda\": $WL_LAMBDA,"
    echo "    \"alpha\": $WL_ALPHA,"
    echo "    \"xmin\": $WL_XMIN"
    echo "  },"
    echo "  \"controller\": {"
    echo "    \"physical_memory\": \"$PHYS_MEM\","
    echo "    \"host_reserved_memory\": \"$HOST_RES_MEM\","
    echo "    \"high_factor\": $HIGH_FACTOR,"
    echo "    \"low_factor\": $LOW_FACTOR,"
    echo "    \"emergency_factor\": $EMERG_FACTOR,"
    echo "    \"grant_per_sec_factor\": $GRANT_PER_SEC_FACTOR,"
    echo "    \"recover_duration\": \"$RECOVER_DUR\""
    echo "  },"
    echo "  \"results\": {"
    echo "    \"launch_wall_s\": $launch_wall,"
    echo "    \"audit_admits\": $audits,"
    echo "    \"audit_reclaims\": $reclaims,"
    echo "    \"daemon_grants\": $grants,"
    echo "    \"daemon_settled\": $settled,"
    echo "    \"daemon_rejects\": $rejects,"
    echo "    \"cgroup_high_total\": $total_high,"
    echo "    \"cgroup_oom_total\": $total_oom,"
    echo "    \"host_baseline_used_mib\": $host_baseline_used,"
    echo "    \"host_baseline_avail_mib\": $host_baseline_avail,"
    echo "    \"host_samples\": ["
    for j in "${!sample_t[@]}"; do
        comma=","
        [ "$j" = "$((${#sample_t[@]}-1))" ] && comma=""
        echo "      {\"t_s\": ${sample_t[$j]}, \"avail_mib\": ${sample_avail[$j]}, \"used_mib\": ${sample_used[$j]}}$comma"
    done
    echo "    ]"
    echo "  }"
    echo "}"
} > "$out"

echo
echo "==> density-perf summary"
python3 -c "
import json
d = json.load(open('$out'))
r = d['results']
print(f'  N={d[\"n\"]} mode={d[\"mode\"]} mem_zone={d[\"mem_zone_mib\"]}MiB floor={d[\"floor_mib\"]}MiB burst={d[\"burst_mib\"]}MiB')
print(f'  launch_wall={r[\"launch_wall_s\"]}s')
print(f'  controller: admits={r[\"audit_admits\"]} settled={r[\"daemon_settled\"]} grants={r[\"daemon_grants\"]} reclaims={r[\"audit_reclaims\"]} rejects={r[\"daemon_rejects\"]}')
print(f'  cgroup totals: high={r[\"cgroup_high_total\"]} oom={r[\"cgroup_oom_total\"]}')
samples = r['host_samples']
if samples:
    avail0, availN = samples[0]['avail_mib'], samples[-1]['avail_mib']
    used0, usedN = samples[0]['used_mib'], samples[-1]['used_mib']
    print(f'  host avail: {avail0} → {availN} MiB (Δ={availN-avail0:+d})')
    print(f'  host used:  {used0} → {usedN} MiB (Δ={usedN-used0:+d})')
    base = d['results']['host_baseline_avail_mib']
    per_sb = (base - availN) / d['n'] if d['n'] else 0
    print(f'  Δ MemAvailable / sandbox = {per_sb:.1f} MiB')
"
echo "  full json: $out"
