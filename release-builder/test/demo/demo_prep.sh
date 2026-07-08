#!/usr/bin/env bash
#
# demo_prep.sh — persistent prerequisites for demo_e2b.sh, started ONCE and left
# running so repeated demo runs reuse them (no per-run startup; image + chunk caching):
#
#   • content store  (store-ctl)  on a UNIX socket  $STORE_SOCK
#   • local L1 cache (cache-ctl, tiered rocksdb L1 → store origin) on  $CACHE_SOCK
#   • an image registry — either a third-party REGISTRY you point at, or a persistent
#     local zot this script spins up — seeded once with the e2b base image.
#   • versitygw (S3 gateway) on 127.0.0.1:$VGW_PORT, backing COPY build contexts
#     (builder.files_storage) — optional; absent → the demo build omits COPY.
#
# Storage/cache bind UNIX sockets (no TCP port); their data lives under DEMO_DATA_DIR
# (default ~/.cache/kuasar-demo), NOT a temp dir, so content survives across runs.
#
#   REGISTRY=registry.example.com REGISTRY_USER=u REGISTRY_PASS=p bash demo_prep.sh
#   REGISTRY=127.0.0.1:5000 REGISTRY_INSECURE=1            bash demo_prep.sh
#   bash demo_prep.sh                 # no REGISTRY → spin up a persistent local zot
#   bash demo_prep.sh stop            # stop the persistent services (keep data)
#   bash demo_prep.sh reset           # stop + wipe DEMO_DATA_DIR
#
# On success it writes a sourceable env file ($DEMO_DATA_DIR/prep.env) that demo_e2b.sh
# reads (REGISTRY / REGISTRY_INSECURE / REGISTRY_USER/PASS / STORE_SOCK / CACHE_SOCK).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DEMO_DATA_DIR="${DEMO_DATA_DIR:-$HOME/.cache/kuasar-demo}"
RUN_DIR="${RUN_DIR:-/run/sandbox}"
STORE_SOCK="${STORE_SOCK:-$RUN_DIR/store.sock}"
CACHE_SOCK="${CACHE_SOCK:-$RUN_DIR/cache.sock}"
CACHE_HEALTH_SOCK="${CACHE_HEALTH_SOCK:-$RUN_DIR/cache-health.sock}"
ENV_FILE="$DEMO_DATA_DIR/prep.env"
PID_DIR="$DEMO_DATA_DIR/pids"
LOG_DIR="$DEMO_DATA_DIR/logs"
E2E_IMAGE="${E2E_IMAGE:-e2bdev/code-interpreter:latest}"
REGISTRY_NS="${REGISTRY_NS:-e2b}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
# versitygw (S3 gateway) backs COPY build contexts (builder.files_storage).
# Use VGW_BIN or a versitygw already on PATH. Absent -> COPY is disabled and
# demo_e2b.sh's build omits the COPY step (the rest still runs).
if [ -z "${VGW_BIN:-}" ]; then
    VGW_BIN="$(command -v versitygw 2>/dev/null || true)"
fi
VGW_PORT="${VGW_PORT:-5050}"
VGW_BUCKET="${VGW_BUCKET:-build-files}"
VGW_ACCESS_KEY="${VGW_ACCESS_KEY:-demoaccesskey}"
VGW_SECRET_KEY="${VGW_SECRET_KEY:-demosecretkey0123456}"
VGW_ENDPOINT=""

c_ok=$'\e[1;32m'; c_dim=$'\e[2m'; c_off=$'\e[0m'; [ -t 1 ] || { c_ok=; c_dim=; c_off=; }
say() { echo "${c_dim}  · $*${c_off}"; }
ok()  { echo "${c_ok}  ✓ $*${c_off}"; }
die() { echo $'\e[1;31m'"  ✗ $*"$'\e[0m' >&2; exit 1; }

alive() { [ -f "$PID_DIR/$1.pid" ] && kill -0 "$(cat "$PID_DIR/$1.pid" 2>/dev/null)" 2>/dev/null; }
start() { # start <name> <cmd...>  — detached (setsid + new session) so it outlives this script
    local name="$1"; shift
    if alive "$name"; then say "$name already running (pid $(cat "$PID_DIR/$name.pid"))"; return 0; fi
    setsid "$@" >"$LOG_DIR/$name.log" 2>&1 </dev/null & echo $! > "$PID_DIR/$name.pid"
    ok "$name started (pid $(cat "$PID_DIR/$name.pid"))"
}
stop_all() {
    for p in "$PID_DIR"/*.pid; do [ -e "$p" ] || continue; kill "$(cat "$p")" 2>/dev/null || true; rm -f "$p"; done
    rm -f "$STORE_SOCK" "$CACHE_SOCK" "$CACHE_HEALTH_SOCK" "$ENV_FILE"
    ok "stopped persistent services"
}

case "${1:-up}" in
  stop)  stop_all; exit 0;;
  reset) stop_all; rm -rf "$DEMO_DATA_DIR"; ok "wiped $DEMO_DATA_DIR"; exit 0;;
  up)    ;;
  *)     die "usage: demo_prep.sh [up|stop|reset]";;
esac

# ---- prerequisites --------------------------------------------------------
for b in store-ctl cache-ctl; do [ -x "$BIN/$b" ] || die "missing $BIN/$b — run 'make build' (cache-ctl needs CGO/rocksdb)"; done
command -v docker >/dev/null 2>&1 || die "docker not on PATH (needed once to seed the base image into the registry)"
[ "$(id -u)" -eq 0 ] || say "note: $RUN_DIR usually needs root; re-run under sudo if socket creation fails"
mkdir -p "$DEMO_DATA_DIR" "$PID_DIR" "$LOG_DIR" "$DEMO_DATA_DIR/store" "$DEMO_DATA_DIR/cache-rocks" "$DEMO_DATA_DIR/zot" "$DEMO_DATA_DIR/vgw" "$RUN_DIR"

# ---- registry: third-party (REGISTRY set) or a persistent local zot -------
REGISTRY_INSECURE="${REGISTRY_INSECURE:-}"
if [ -n "${REGISTRY:-}" ]; then
    say "using configured registry: $REGISTRY"
    case "$REGISTRY" in 127.0.0.1*|localhost*) REGISTRY_INSECURE="${REGISTRY_INSECURE:-1}";; esac
    if [ -n "${REGISTRY_USER:-}" ]; then
        echo "${REGISTRY_PASS:-}" | docker login "$REGISTRY" -u "$REGISTRY_USER" --password-stdin >/dev/null 2>&1 \
            || die "docker login $REGISTRY failed"
        ok "docker login $REGISTRY"
    fi
else
    [ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || die "no REGISTRY set and zot not found — set REGISTRY=<host:port>, set ZOT_BIN, or install zot on PATH"
    ZOT_PORT="${ZOT_PORT:-5000}"
    REGISTRY="127.0.0.1:$ZOT_PORT"; REGISTRY_INSECURE=1
    # 0.0.0.0: builds pull IN-GUEST — the build sandbox reaches this zot via the
    # vswitch mgmt VIP (169.254.169.254), which a loopback bind would refuse.
    # The host still pushes/pulls via 127.0.0.1.
    cat > "$DEMO_DATA_DIR/zot.json" <<EOF
{ "storage": { "rootDirectory": "$DEMO_DATA_DIR/zot", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn" } }
EOF
    start zot "$ZOT_BIN" serve "$DEMO_DATA_DIR/zot.json"
    for _ in $(seq 1 40); do (exec 3<>/dev/tcp/127.0.0.1/$ZOT_PORT) 2>/dev/null && { exec 3>&- 3<&-; break; }; sleep 0.25; done
    ok "persistent local zot on $REGISTRY (data $DEMO_DATA_DIR/zot)"
fi
BASE_REF="$REGISTRY/$REGISTRY_NS/base:v1"

# ---- store-ctl on UDS (persistent) ----------------------------------------
cat > "$DEMO_DATA_DIR/store.yaml" <<EOF
listen: unix://$STORE_SOCK
backend: fs
fs: { root: $DEMO_DATA_DIR/store, verify_content_key: true }
EOF
if ! alive store; then
    "$BIN/store-ctl" init --config "$DEMO_DATA_DIR/store.yaml" --generation G1 >"$LOG_DIR/store-init.log" 2>&1 || true
    start store "$BIN/store-ctl" serve --config "$DEMO_DATA_DIR/store.yaml"
    for _ in $(seq 1 40); do [ -S "$STORE_SOCK" ] && break; sleep 0.25; done
fi
[ -S "$STORE_SOCK" ] || die "store-ctl did not bind $STORE_SOCK (see $LOG_DIR/store.log)"
ok "store-ctl on $STORE_SOCK (data $DEMO_DATA_DIR/store)"

# ---- cache-ctl: local L1 (tiered rocksdb → store origin) on UDS ------------
cat > "$DEMO_DATA_DIR/cache.yaml" <<EOF
mode: tiered
listen: unix://$CACHE_SOCK
health_listen: unix://$CACHE_HEALTH_SOCK
rpc_timeout: 5s
freq: { counters: 1M, reset_after: 100K }
tiers:
  - type: embedded
    rocks: { path: $DEMO_DATA_DIR/cache-rocks, disk_bytes: 4GiB, mem_ratio: 0.1, direct_reads: true, bloom_bits: 10 }
origin:
  type: store
  store: { endpoint: unix://$STORE_SOCK, pool: 4, timeout: 5s }
  max_inflight: 32
EOF
if ! alive cache; then
    start cache "$BIN/cache-ctl" serve --config "$DEMO_DATA_DIR/cache.yaml"
    for _ in $(seq 1 40); do [ -S "$CACHE_SOCK" ] && break; sleep 0.25; done
fi
[ -S "$CACHE_SOCK" ] || die "cache-ctl did not bind $CACHE_SOCK (see $LOG_DIR/cache.log)"
ok "cache-ctl (L1 tiered) on $CACHE_SOCK (rocks $DEMO_DATA_DIR/cache-rocks)"

# ---- versitygw: S3 gateway for COPY build contexts (persistent; posix backend) --
# The e2b SDK direct-uploads a COPY context here (presigned PUT) and the build
# fetches it (presigned GET) — both from the host (127.0.0.1), so unlike zot it
# needs NO guest reachability (run-builder fetches host-side, streams into guest).
if [ -n "${VGW_BIN:-}" ]; then
    if ! alive vgw; then
        ROOT_ACCESS_KEY="$VGW_ACCESS_KEY" ROOT_SECRET_KEY="$VGW_SECRET_KEY" \
            start vgw "$VGW_BIN" --port "127.0.0.1:$VGW_PORT" posix "$DEMO_DATA_DIR/vgw"
        for _ in $(seq 1 40); do (exec 3<>"/dev/tcp/127.0.0.1/$VGW_PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }; sleep 0.25; done
    fi
    mkdir -p "$DEMO_DATA_DIR/vgw/$VGW_BUCKET"   # posix backend: a bucket is a top-level dir
    (exec 3<>"/dev/tcp/127.0.0.1/$VGW_PORT") 2>/dev/null && { exec 3>&- 3<&-; VGW_ENDPOINT="http://127.0.0.1:$VGW_PORT"; } \
        || die "versitygw did not bind 127.0.0.1:$VGW_PORT (see $LOG_DIR/vgw.log)"
    ok "versitygw (S3, COPY contexts) on $VGW_ENDPOINT (bucket $VGW_BUCKET, data $DEMO_DATA_DIR/vgw)"
else
    say "versitygw not found — COPY build contexts disabled (set VGW_BIN or install versitygw on PATH); the demo build will omit COPY"
fi

# ---- seed the base image into the registry (once; cached on reruns) --------
if curl -s -o /dev/null -w '%{http_code}' ${REGISTRY_INSECURE:+} "http${REGISTRY_INSECURE:+}://$REGISTRY/v2/$REGISTRY_NS/base/manifests/v1" 2>/dev/null | grep -q '^200$'; then
    say "base image already in registry: $BASE_REF (cached)"
else
    say "seeding base image into the registry (one-time; the node pulls it via from_image):"
    docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >"$LOG_DIR/pull.log" 2>&1 || die "docker pull $E2E_IMAGE"
    docker tag "$E2E_IMAGE" "$BASE_REF"
    docker push "$BASE_REF" >"$LOG_DIR/push.log" 2>&1 || { cat "$LOG_DIR/push.log"; die "docker push $BASE_REF"; }
    ok "seeded $E2E_IMAGE → $BASE_REF"
fi

# ---- hand off to demo_e2b.sh ----------------------------------------------
cat > "$ENV_FILE" <<EOF
export REGISTRY='$REGISTRY'
export REGISTRY_NS='$REGISTRY_NS'
export REGISTRY_INSECURE='${REGISTRY_INSECURE:-}'
export REGISTRY_USER='${REGISTRY_USER:-}'
export REGISTRY_PASS='${REGISTRY_PASS:-}'
export STORE_SOCK='$STORE_SOCK'
export CACHE_SOCK='$CACHE_SOCK'
export BASE_REF='$BASE_REF'
export VGW_ENDPOINT='$VGW_ENDPOINT'
export VGW_BUCKET='$VGW_BUCKET'
export VGW_REGION='us-east-1'
export VGW_ACCESS_KEY='$VGW_ACCESS_KEY'
export VGW_SECRET_KEY='$VGW_SECRET_KEY'
EOF
echo
ok "prep ready — run the demo (it reads $ENV_FILE automatically):"
echo "${c_dim}    sudo bash $(dirname "$0")/demo_e2b.sh${c_off}"
echo "${c_dim}    (stop services: bash demo_prep.sh stop   ·   wipe data: bash demo_prep.sh reset)${c_off}"
