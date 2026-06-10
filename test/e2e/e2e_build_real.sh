#!/usr/bin/env bash
#
# e2e_build_real.sh — a REAL orchestrator image build, end to end, against a live
# store-ctl + a local zot (OCI 1.1) registry seeded from a cached docker image.
# Unlike e2e_orchestrator.sh (which only asserts the build *API lifecycle* and
# tolerates status=error with no backend), this drives the build to a successful
# flatten: register → trigger → poll until status=ready with a persist templateID.
#
# What it exercises (the orchestrator's build backend, no e2b CLI yet):
#   store-ctl (fs backend) ← flatten-ctl upload      (content store)
#   zot (insecure 127.0.0.1) ← seeded base image     (registry pull)
#   sandbox-builder@<bid>.service → run-builder → flatten-ctl export --upload
#                                                    (systemd unit + config-socket)
#   flatten-ctl stdout (manifest key) → <bid>.result → status=ready + persist id
#
# Requires systemd as PID1 + root (drives units over D-Bus), docker (seeds the
# image), zot + store-ctl + mkfs.erofs on PATH/bin. No /dev/kvm needed: an image
# build (no start command) only flattens; the VM path is Phase 2 (execute).
# Missing prerequisites → exit 0 ("skipped") unless REQUIRE_BUILD=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
E2E_IMAGE="${E2E_IMAGE:-test-app-a:latest}"   # small cached image; override as needed
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"

skip() {
    echo
    echo "==> e2e_build_real: skipping ($*)"
    [ "${REQUIRE_BUILD:-0}" = "1" ] && { echo "REQUIRE_BUILD=1 set; failing instead" >&2; exit 1; }
    exit 0
}
fail() { echo "==> FAIL: $*" >&2; exit 1; }

# ---- prerequisite checks --------------------------------------------------
for b in orchestrator-ctl flatten-ctl store-ctl e2b-key-ctl; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 || skip "docker not on PATH"
docker info >/dev/null 2>&1 || skip "docker daemon not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not on PATH"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
[ -d /run/systemd/system ] || skip "systemd is not PID1 (orchestrator drives units over D-Bus)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || skip "seed image $E2E_IMAGE not cached (set E2E_IMAGE)"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

# mkfs.erofs from bin if not on PATH (flatten-ctl needs it on PATH)
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-build-XXXXXX)"
# Units must live in a real systemd load path for `systemctl start` to find them.
# /run/systemd/system is ephemeral (cleared on reboot) and a valid search dir.
# We only remove units we created (guard against an existing real deployment).
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u already exists (real deployment?); refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"
declare -a PIDS=()
declare -a TAGS=()
cleanup() {
    set +e
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    systemctl stop 'sandbox-builder@*.service' 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { # host port name
    for _ in $(seq 1 60); do
        (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
        sleep 0.5
    done
    fail "$3 did not open $1:$2"
}

# ---- store-ctl (fs backend, generation G1) --------------------------------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 \
    || { cat "$WORK/store-init.log"; fail "store-ctl init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$STORE_PORT" store-ctl
echo "==> store-ctl up (127.0.0.1:$STORE_PORT, fs backend, G1)"

# ---- zot (anonymous, insecure) + seed -------------------------------------
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{
  "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "127.0.0.1", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" }
}
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$ZOT_PORT" zot
REF="127.0.0.1:$ZOT_PORT/e2e/app:v1"
docker tag "$E2E_IMAGE" "$REF"; TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push $REF"; }
echo "==> zot up (127.0.0.1:$ZOT_PORT); seeded $E2E_IMAGE -> $REF"

# ---- shared manifest config (store endpoint + crypto; key via env) --------
cat > "$WORK/manifest.yaml" <<EOF
manifest:
  key: ""
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 30s
cache:
  endpoint: ""
chunker:
  mode: cdc
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
crypto:
  chunk: aes
  manifest: aes
EOF

# ---- tenant credentials + orchestrator config -----------------------------
MK="$("$BIN/e2b-key-ctl" gen-key)"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"
ENC="$("$BIN/e2b-key-ctl" gen-key)"

cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/orchestrator.socket }
units: { dir: $UNIT_DIR }
sandbox:
  network: { switch: sw0 }
  boot: { kernel: $BIN/vmlinux, runtime_e2b: $BIN/sandbox-runtime-e2b.erofs, runtime_base: $BIN/sandbox-runtime.erofs }
builder: { insecure_registry: true }
checkpoint: { mode: remote }
EOF

"$BIN/orchestrator-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; skip "orchestrator serve exited"; }
    sleep 0.5
done
echo "==> orchestrator-ctl serve up (dev http :$PORT, insecure registry)"
"$BIN/orchestrator-ctl" manifest-key add --socket "$WORK/orchestrator.socket" "$MK" >/dev/null || fail "manifest-key add"

req() { # method path key [body]
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method"
                -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

# ---- register → trigger (real fromImage) → poll until ready ----------------
code=$(req POST /v3/templates "$AK" '{"name":"e2e-real","tags":["e2e"]}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register = $code (want 202)"; }
TID=$(grep -o '"templateID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
BID=$(grep -o '"buildID":"[^"]*"'    "$WORK/resp.body" | head -1 | cut -d'"' -f4)
echo "==> register: templateID=$TID buildID=$BID"

code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger = $code (want 202)"; }
echo "==> trigger: fromImage=$REF"

status=""; persist=""
for _ in $(seq 1 120); do
    code=$(req GET "/templates/$TID/builds/$BID/status" "$AK")
    [ "$code" = "200" ] || fail "status = $code (want 200)"
    status=$(grep -o '"status":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
    case "$status" in
        # the persist id (e2b-img-<64hex>) rides in names/aliases once ready
        ready) persist=$(grep -o 'e2b-img-[0-9a-f]\{64\}' "$WORK/resp.body" | head -1); break;;
        error)
            echo "    build error; orchestrator log tail + builder result:" >&2
            sed 's/^/    orch| /' "$WORK/orch.log" >&2
            cat "$WORK/run/$BID/$BID.result" 2>/dev/null | sed 's/^/    result| /' >&2
            fail "build status=error (reason in response): $(cat "$WORK/resp.body")";;
    esac
    sleep 1
done
[ "$status" = "ready" ] || fail "build did not reach ready (last status=$status)"
echo "==> PASS: build reached READY"
echo "    persist templateID: $persist"

# the persist id must be a real e2b img template id (e2b-img-<64hex>)
[ -n "$persist" ] || fail "no persist templateID (e2b-img-…) in ready status: $(cat "$WORK/resp.body")"
keyhex="${persist##*-}"
[[ "$keyhex" =~ ^[0-9a-f]{64}$ ]] || fail "persist key is not 64-hex: $keyhex"
echo "==> PASS: persist templateID is a valid e2b img template ($persist)"

# the manifest key landed in the store (G1 objects present)
objs=$(find "$WORK/store" -type f | wc -l)
[ "$objs" -gt 0 ] || fail "store has no objects after build (expected chunks+manifest)"
echo "==> PASS: store holds $objs object(s) after flatten upload"

echo
echo "==> e2e_build_real: OK   (TEMPLATE_ID=$persist for Phase-2 execute)"
