#!/usr/bin/env bash
#
# e2e_sandbox_cold_manifest.sh — cold-start a sandbox VM whose blk0 base
# is fetched on demand from store-ctl + cache-ctl via a manifest:// URI.
#
# Pipeline:
#   1. Spin up store-ctl and cache-ctl daemons.
#   2. flatten-ctl python:3.12-slim → erofs (with appended config.json).
#   3. manifest-ctl store → ingest erofs into store, emit
#      a 32-byte hex manifest content key.
#   4. Write sandbox.yaml with boot.root.base: manifest://<hex>.
#   5. sandbox-ctl run pulls the manifest blob, decodes, builds a Fetcher,
#      serves blk0 reads via vhost-user-blk on demand.
#   6. Verify python prints PYBOOT-OK from inside the guest.
#
# Mirrors e2e_sandbox_cold.sh's prereq checks + skip semantics.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-python:3.12-slim}"

# ---- prerequisite checks --------------------------------------------------

skip() {
    echo
    echo "==> e2e_sandbox_cold_manifest: skipping ($*)"
    if [ "${REQUIRE_KVM:-0}" = "1" ]; then
        echo "REQUIRE_KVM=1 set; failing instead of skipping" >&2
        exit 1
    fi
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not accessible to current user"

for b in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl cache-ctl; do
    [ -e "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done

VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX (run 'make vmlinux' or set VMLINUX env var)"

# Self-elevate: tap creation, cgroup writes, vsock all need root. Done
# here (after prereq checks) so /dev/kvm-missing and missing-binary cases
# still fast-fail without prompting for sudo.
if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

TAP_NAME="${TAP_NAME:-sb-tap0}"
TAP_CREATED_BY_TEST=0
if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    [ "$(id -u)" -eq 0 ] || { echo "$0: must run as root to create $TAP_NAME" >&2; exit 1; }
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED_BY_TEST=1
fi

command -v docker >/dev/null 2>&1 || skip "docker not available"
command -v mkfs.ext4 >/dev/null 2>&1 || skip "mkfs.ext4 not on PATH"

# ---- workspace -----------------------------------------------------------

WORK="$(mktemp -d /tmp/e2e-sandbox-manifest-XXXXXX)"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [ -n "${E2E_KEEP:-}" ]; then
        echo "kept work dir: $WORK"
    else
        rm -rf "$WORK"
    fi
    [ "$TAP_CREATED_BY_TEST" = "1" ] && ip link del "$TAP_NAME" 2>/dev/null || true
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

KEY=$(openssl rand -hex 32)

# ---- daemons -------------------------------------------------------------

echo "==> spin up store-ctl"
STORE_PORT=$(free_port)
cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store-data
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" \
    >"$WORK/store.log" 2>&1 &
PIDS+=($!)
for _ in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then break; fi
    sleep 0.1
done

echo "==> spin up cache-ctl (tiered: embedded rocksdb L1 + store-ctl origin)"
CACHE_PORT=$(free_port)
CACHE_HEALTH_PORT=$(free_port)
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
      path: $WORK/cache-rocks
      disk_bytes: 1GiB
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
"$BIN/cache-ctl" serve --config "$WORK/cache-ctl.yaml" \
    >"$WORK/cache.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do
    if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null | grep -q SERVING; then
        break
    fi
    sleep 0.1
done

echo "==> store=127.0.0.1:$STORE_PORT cache=127.0.0.1:$CACHE_PORT (tiered: L1 rocksdb + store origin)"

# accelerator.yaml drives both manifest-ctl ingest (writes to store
# directly via store.endpoint) and sandbox-ctl resolve (reads via
# cache.endpoint → cache-ctl tiered → L1 rocksdb hit OR origin pull
# from store-ctl on miss).
cat > "$WORK/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 10s
cache:
  endpoint: 127.0.0.1:$CACHE_PORT
  pool: 4
  timeout: 5s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

# ---- prepare blk0: image → erofs → store → manifest key ------------------

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> docker pull $IMAGE"
    docker pull "$IMAGE" >/dev/null
fi

BLK0_EROFS="$WORK/blk0.img"
echo "==> docker save $IMAGE | flatten-ctl > $BLK0_EROFS"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$BLK0_EROFS" --no-progress

# Cache-ctl runs in-band with store-ctl as the read tier here; ingest
# always writes through the store directly (Filler path). The first
# sandbox-ctl read populates the cache.
echo "==> manifest-ctl store"
MKEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$BLK0_EROFS")
echo "    blk0 manifest key: $MKEY"
[ ${#MKEY} -eq 64 ] || { echo "FAIL: manifest key length ${#MKEY} != 64 hex chars"; exit 1; }

# ---- prepare sandbox.yaml -------------------------------------------------

mkdir -p "$WORK/runtime"
DIFF_FILE="$WORK/runtime/blk1.diff"
truncate -s 1G "$DIFF_FILE"
mkfs.ext4 -q -F "$DIFF_FILE"

cat > "$WORK/sandbox.yaml" <<EOF
resources:
  capacity:
    cpu: 1
    memory: 512MiB
  allocatable:
    cpu: 1
    memory: 512MiB
network:
  tap: $TAP_NAME
  interface: eth0
  ip: 169.254.1.1/31
  hostname: e2e-cold-mfst
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: manifest://$MKEY
    overlay:
      diff: file://$DIFF_FILE
      size: 1GiB
launch:
  # No exec: rely on image config extraction (Cmd=[python3] from the
  # appended ZIP). manifest:// goes through the same LoadImageConfigFrom
  # path as file:// — the test below verifies the "image config:"
  # log line appears.
  args: ["-c", "import sys; print('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor)"]
  restart: never
EOF

echo "==> sandbox.yaml:"
sed 's/^/    /' "$WORK/sandbox.yaml"

# ---- run sandbox-ctl ------------------------------------------------------

echo "==> launching sandbox-ctl run (timeout 90s; manifest path is cold on first run)"
LOG="$WORK/run.log"
T0_NS=$(date +%s%N)
set +e
STATS_JSON="${PERF_STATS_JSON:-$WORK/stats.json}"
timeout -k 10s 90 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --manifest-config "$WORK/accelerator.yaml" \
    --ch-binary "${CH_BINARY:-$BIN/cloud-hypervisor}" \
    --run-root "$WORK/runtime" \
    --stats-json "$STATS_JSON" \
    > "$LOG" 2>&1 &
SBPID=$!

T_APP_NS=""
while kill -0 "$SBPID" 2>/dev/null; do
    if grep -qE "^PYBOOT-OK [0-9]+$" "$LOG" 2>/dev/null; then
        T_APP_NS=$(date +%s%N)
        break
    fi
    sleep 0.005
done
wait "$SBPID"
EXIT=$?
T_END_NS=$(date +%s%N)
set -e

if [ -z "$T_APP_NS" ] && grep -q "PYBOOT-OK" "$LOG" 2>/dev/null; then
    T_APP_NS=$T_END_NS
fi

ms_delta() { awk "BEGIN{printf \"%.1f\", ($2 - $1) / 1000000.0}"; }

echo "==> sandbox-ctl exit code: $EXIT"
echo "==> last 60 lines of log:"
tail -60 "$LOG"

echo
echo "==> cold-start timing (wallclock):"
if [ -n "$T_APP_NS" ]; then
    APP_MS=$(ms_delta "$T0_NS" "$T_APP_NS")
    echo "    T0 → app first stdout (PYBOOT-OK):  ${APP_MS} ms"
fi
END_MS=$(ms_delta "$T0_NS" "$T_END_NS")
echo "    T0 → sandbox-ctl exit:               ${END_MS} ms"

# Confirm manifest:// path actually engaged: sandbox-ctl logs
# "manifest fetcher: store=... cache=... crypto=..." once it dialled
# the daemons.
if grep -q "manifest fetcher: store=" "$LOG"; then
    echo "==> PASS: manifest fetcher engaged (manifest:// path verified)"
else
    echo "==> FAIL: manifest fetcher log line missing — manifest:// path may not have been used"
    exit 1
fi

if [ "$EXIT" = "124" ] && ! grep -q "PYBOOT-OK" "$LOG"; then
    echo "==> FAIL: sandbox run timed out at 90s — guest didn't reach app. Inspect $LOG"
    exit 1
fi

if grep -qE "PYBOOT-OK [0-9]+" "$LOG"; then
    marker=$(grep -oE "PYBOOT-OK [0-9]+" "$LOG" | head -1)
    echo "==> PASS: python actually executed in sandbox: $marker"
else
    echo "==> FAIL: sandbox-ctl exit=$EXIT and PYBOOT-OK marker not seen"
    exit 1
fi

if grep -q "image config: cmd=\[python3\]" "$LOG"; then
    echo "==> PASS: image-config auto-extraction engaged for manifest:// (cmd=[python3] from manifest-backed ZIP)"
else
    echo "==> FAIL: image-config extraction did not engage on manifest:// path"
    exit 1
fi

echo "==> e2e_sandbox_cold_manifest: OK"
