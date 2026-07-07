#!/usr/bin/env bash
#
# e2e_obs.sh — store-ctl(obs backend) ↔ real S3-compatible OBS round-trip.
#
# Gated on OBS_E2E=1; credentials and endpoint default to ~/.obsconfig
# auto-discovery (so dev workstations with obsutil already configured
# need only set OBS_BUCKET).
#
# Required env (when OBS_E2E=1):
#   OBS_E2E=1
#   OBS_BUCKET    target bucket; the prefix below is created and
#                 cleaned within it (ListObjects + DeleteObject).
#                 Re-using a shared bucket is safe.
#
# Optional overrides (default = take from ~/.obsconfig / SDK chain):
#   OBS_ENDPOINT  e.g. https://obs.cn-north-4.example.com
#   OBS_REGION    e.g. cn-north-4
#   OBS_AK / OBS_SK  static credentials
#   OBS_PREFIX    in-bucket prefix; default a unique per-run path
#                 like "store-ctl-e2e/<rand>/" so concurrent runs
#                 don't collide.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"

skip() {
    echo
    if [ "${OBS_E2E:-0}" = "1" ]; then
        echo "==> e2e_obs: FAIL ($*)" >&2
        exit 1
    fi
    echo "==> e2e_obs: skipping ($*)"
    exit 0
}

if [ "${OBS_E2E:-0}" != "1" ]; then
    skip "OBS_E2E=1 not set"
fi
if [ -z "${OBS_BUCKET:-}" ]; then
    skip "OBS_BUCKET env var missing"
fi
# When OBS_AK/SK aren't set, expect ~/.obsconfig to provide them via
# store-ctl's auto-discovery layer. Skip if neither is available.
if [ -z "${OBS_AK:-}" ] && [ ! -f "$HOME/.obsconfig" ]; then
    skip "no OBS_AK env and ~/.obsconfig missing"
fi

for b in store-ctl manifest-ctl flatten-ctl; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done

WORK="$(mktemp -d /tmp/e2e-obs-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"' EXIT

# Per-run unique prefix avoids collisions when multiple e2e runs
# share the same bucket. The trailing slash is normalised away by
# the obs backend.
OBS_PREFIX="${OBS_PREFIX:-store-ctl-e2e/$(date +%s)-$$/}"

# Allocate a free TCP port for store-ctl to listen on.
free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}
STORE_PORT=$(free_port)

# Random key so each run produces fresh content (tests both Put-new
# and dedup paths within the same run).
KEY=$(openssl rand -hex 32)

# ─── store-ctl yaml (obs backend) ──────────────────────────────────────
# yaml only carries fields the test-runner overrides; everything else
# (endpoint, region, AK/SK) defaults via ~/.obsconfig auto-discovery
# inside store-ctl itself. Test-time overrides (OBS_AK / OBS_SK as env
# in the make wrapper) still win because store-ctl expands ${VAR}
# before falling through to ~/.obsconfig.
cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:${STORE_PORT}
backend: obs
obs:
  bucket: ${OBS_BUCKET}
  prefix: ${OBS_PREFIX}
EOF
# Optional explicit overrides — only emitted when the env var is set,
# so the yaml exercises the auto-discovery path by default.
if [ -n "${OBS_ENDPOINT:-}" ]; then echo "  endpoint: ${OBS_ENDPOINT}" >> "$WORK/store-ctl.yaml"; fi
if [ -n "${OBS_REGION:-}"   ]; then echo "  region: ${OBS_REGION}"     >> "$WORK/store-ctl.yaml"; fi
if [ -n "${OBS_AK:-}"       ]; then echo "  access_key: \${OBS_AK}"    >> "$WORK/store-ctl.yaml"; fi
if [ -n "${OBS_SK:-}"       ]; then echo "  secret_key: \${OBS_SK}"    >> "$WORK/store-ctl.yaml"; fi
echo "  verify_content_key: true" >> "$WORK/store-ctl.yaml"
echo "  op_timeout: 15s"          >> "$WORK/store-ctl.yaml"

# ─── init the store at the per-run prefix ──────────────────────────────
# Run init explicitly before serve; on a fresh prefix this writes the
# meta object, on a re-run it would fail with ErrAlreadyInitialised
# (the per-run unique prefix above prevents that in practice).
echo "==> store-ctl init --generation G1"
OBS_AK="${OBS_AK:-}" OBS_SK="${OBS_SK:-}" \
    "$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1

cat > "$WORK/accelerator.yaml" <<EOF
manifest:
  key: "${KEY}"
store:
  endpoint: 127.0.0.1:${STORE_PORT}
  pool: 2
  timeout: 30s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

# ─── spawn store-ctl ───────────────────────────────────────────────────
echo "==> store-ctl: backend=obs bucket=${OBS_BUCKET} prefix=${OBS_PREFIX} (creds: yaml${OBS_AK:+ + env} or ~/.obsconfig)"
OBS_AK="${OBS_AK:-}" OBS_SK="${OBS_SK:-}" \
    "$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" \
    >"$WORK/store.log" 2>&1 &
STORE_PID=$!
trap "kill $STORE_PID 2>/dev/null || true; rm -rf $WORK" EXIT

for _ in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/${STORE_PORT}) 2>/dev/null; then
        break
    fi
    sleep 0.3
done

if ! kill -0 $STORE_PID 2>/dev/null; then
    echo "==> store-ctl exited prematurely:"
    cat "$WORK/store.log"
    exit 1
fi

# ─── small payload round-trip ──────────────────────────────────────────
dd if=/dev/urandom of="$WORK/payload.bin" bs=4096 count=64 status=none
ORIG_HASH=$(sha256sum "$WORK/payload.bin" | awk '{print $1}')

# store consumes tarstream artifacts (ff88f5f); wrap the raw payload first
# (flatten-ctl tar stream, pure Go), then read the payload back out of the
# loaded artifact with tar extract --dense before comparing hashes.
"$BIN/flatten-ctl" tar stream -f "$WORK/payload.tar" "image:$WORK/payload.bin"

echo "==> manifest-ctl store"
MKEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$WORK/payload.tar")
[ ${#MKEY} -eq 64 ] || { echo "FAIL: bad manifest key length ${#MKEY}"; exit 1; }
echo "    manifest key: $MKEY"

echo "==> manifest-ctl load (round-trip via OBS)"
"$BIN/manifest-ctl" load --manifest-config "$WORK/accelerator.yaml" \
    --output "$WORK/restored.tar" --no-progress "$MKEY"
"$BIN/flatten-ctl" tar extract -f "$WORK/restored.tar" --dense "image:$WORK/restored.bin"
RESTORED_HASH=$(sha256sum "$WORK/restored.bin" | awk '{print $1}')

if [ "$ORIG_HASH" != "$RESTORED_HASH" ]; then
    echo "FAIL: restored hash mismatch"
    echo "  orig:     $ORIG_HASH"
    echo "  restored: $RESTORED_HASH"
    exit 1
fi
echo "    PASS: round-trip hash matches"

# ─── dedup verification ────────────────────────────────────────────────
echo "==> dedup: store same payload again, expect 0 new chunks"
OUT=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$WORK/payload.tar" 2>&1)
STORED=$(echo "$OUT" | grep "chunks:" | grep -oP 'stored=\K[0-9]+' || echo "?")
if [ "$STORED" = "0" ]; then
    echo "    PASS: dedup hit (0 new chunks on second store)"
else
    echo "    FAIL: expected 0 stored chunks on dedup, got '$STORED'"
    exit 1
fi

# ─── manifest verify (re-decrypt + re-hash every chunk) ────────────────
echo "==> manifest verify (round-trip every chunk via OBS)"
"$BIN/manifest-ctl" verify --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$MKEY"

# ─── self-cleanup ──────────────────────────────────────────────────────
# Wipe the per-run prefix so concurrent / repeat runs don't accumulate
# objects in ops-dev. Failure to clean is logged but doesn't fail the
# test — a residual ${OBS_PREFIX} is recoverable via the same
# `store-ctl purge --all` command, just less convenient.
#
# Stop store-ctl first so its mid-flight reads don't race with the
# bucket sweep (Drop/Wipe issues parallel List+Delete).
kill "$STORE_PID" 2>/dev/null || true
wait "$STORE_PID" 2>/dev/null || true
echo "==> cleanup: store-ctl purge --all --confirm"
if OBS_AK="${OBS_AK:-}" OBS_SK="${OBS_SK:-}" \
        "$BIN/store-ctl" purge --config "$WORK/store-ctl.yaml" \
        --all --confirm 2>&1; then
    echo "    PASS: per-run prefix purged"
else
    echo "    WARN: cleanup failed; manually run: store-ctl purge --config ... --all --confirm"
fi

echo
echo "==> e2e_obs: OK (bucket=$OBS_BUCKET prefix=$OBS_PREFIX)"
