#!/usr/bin/env bash
#
# e2e_build_cli.sh — drive a REAL `e2b template build` (the unmodified e2b CLI)
# against orchestrator-ctl, end to end, to a ready template.
#
# How the real CLI build maps onto our flatten path (discovered from CLI 2.10.3):
#   1. POST /v3/templates {name,tags,cpuCount,memoryMB} -> templateID,buildID
#   2. client-side `docker build` of e2b.Dockerfile
#   3. docker push the built image to <E2B_IMAGE_URI_MASK with {templateID}/{buildID}>
#      — setting E2B_IMAGE_URI_MASK makes the CLI SKIP the docker.<domain> token
#        broker (the login is gated on the mask being unset), so it pushes straight
#        to our local zot (127.0.0.1 = insecure to docker by default).
#   4. POST /v2/templates/{tid}/builds/{bid} {dockerfile,start_cmd,ready_cmd,...}
#      — no image ref; the orchestrator derives fromImage from builder_image_uri_mask
#        (same template) and runs the unchanged flatten-ctl export --upload <img>.
#   5. GET /templates/{tid}/builds/{bid}/status (poll until ready/error).
#
# The CLI reaches the control plane via E2B_API_URL=http://api.<domain>:<port>
# (the orchestrator routes by Host: api.*), so we add a transient /etc/hosts entry.
#
# Needs systemd+root, docker, zot, store-ctl, mkfs.erofs, and the e2b CLI on PATH.
# No /dev/kvm (image build only; the VM/execute path is Phase 2). Missing prereqs
# -> exit 0 ("skipped") unless REQUIRE_BUILD=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
E2E_IMAGE="${E2E_IMAGE:-test-app-a:latest}"   # base for the e2b.Dockerfile (cached locally)
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"
E2B_BIN="${E2B_BIN:-$(command -v e2b || true)}"

skip() { echo; echo "==> e2e_build_cli: skipping ($*)"; [ "${REQUIRE_BUILD:-0}" = "1" ] && { echo "REQUIRE_BUILD=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in orchestrator-ctl flatten-ctl store-ctl e2b-key-ctl; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
command -v curl  >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 || skip "docker not on PATH"
docker info >/dev/null 2>&1 || skip "docker daemon not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not on PATH"
[ -n "$E2B_BIN" ] && [ -x "$E2B_BIN" ] || skip "e2b CLI not on PATH"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || skip "base image $E2E_IMAGE not cached (set E2E_IMAGE)"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-cli-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data" "$WORK/build" "$WORK/home"
declare -a PIDS=()
declare -a TAGS=()
HOSTS_LINE=""
cleanup() {
    set +e
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    systemctl stop 'sandbox-builder@*.service' 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "$HOSTS_LINE" ] && sed -i "\#$HOSTS_LINE#d" /etc/hosts 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }

# ---- store-ctl + zot + creds ----------------------------------------------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl
echo "==> store-ctl up (127.0.0.1:$STORE_PORT)"

ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "127.0.0.1", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
echo "==> zot up (127.0.0.1:$ZOT_PORT)"

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
  cdc: { min: 128KiB, avg: 512KiB, max: 1MiB }
crypto:
  chunk: aes
  manifest: aes
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"
ENC="$("$BIN/e2b-key-ctl" gen-key)"

# image-uri mask the CLI pushes to and the orchestrator flattens from (must match)
MASK="127.0.0.1:$ZOT_PORT/e2b/{templateID}:{buildID}"

cat > "$WORK/config.yaml" <<EOF
domain: $DOMAIN
listen: ":$PORT"
encryption_key: "$ENC"
run_root: $WORK/run
base_root: $WORK/lib
manifest_config: $WORK/manifest.yaml
runtime_e2b_erofs: $BIN/sandbox-runtime-e2b.erofs
runtime_erofs: $BIN/sandbox-runtime.erofs
kernel: $BIN/vmlinux
config_socket: $WORK/orchestrator.socket
switch: sw0
unit_dir: $UNIT_DIR
exec_dir: $BIN
builder_insecure_registry: true
builder_image_uri_mask: "$MASK"
EOF

"$BIN/orchestrator-ctl" manifest-key add --config "$WORK/config.yaml" "$MK" >/dev/null || fail "manifest-key add"

# route the CLI's api.<domain> to localhost
HOSTS_LINE="127.0.0.1 api.$DOMAIN # e2e_build_cli $$"
grep -q "api.$DOMAIN" /etc/hosts || echo "$HOSTS_LINE" >> /etc/hosts

"$BIN/orchestrator-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; skip "orchestrator exited"; }
    sleep 0.5
done
echo "==> orchestrator-ctl up (dev http :$PORT, mask=$MASK)"

# ---- the e2b build context ------------------------------------------------
# The CLI runs `docker build --pull`, which forces a registry pull of the FROM
# base — so the base must live in a registry docker can reach. Seed it into our
# local zot (127.0.0.1 = insecure to docker, no TLS) and FROM that. docker build
# re-tags it, the CLI pushes the result to the mask, the orchestrator flattens it.
# (RUN steps would also work — run here by docker — but we keep it minimal/KVM-free.)
BASE_REF="127.0.0.1:$ZOT_PORT/base/app:v1"
docker tag "$E2E_IMAGE" "$BASE_REF"; TAGS+=("$BASE_REF")
docker push "$BASE_REF" >"$WORK/base-push.log" 2>&1 || { cat "$WORK/base-push.log"; fail "seed base $BASE_REF"; }
echo "==> seeded base $E2E_IMAGE -> $BASE_REF"
cat > "$WORK/build/e2b.Dockerfile" <<EOF
FROM $BASE_REF
EOF

# ---- run the real e2b CLI -------------------------------------------------
echo "==> e2b template build (real CLI: $("$E2B_BIN" --version 2>/dev/null | head -1))"
set +e
( cd "$WORK/build" && \
  env -i \
    PATH="$PATH" HOME="$WORK/home" \
    NO_PROXY='*' no_proxy='*' \
    E2B_API_URL="http://api.$DOMAIN:$PORT" \
    E2B_DOMAIN="$DOMAIN" \
    E2B_API_KEY="$AK" \
    E2B_ACCESS_TOKEN="sk_e2e_dummy_access_token" \
    E2B_TEAM_ID="e2e-team" \
    E2B_IMAGE_URI_MASK="$MASK" \
    E2B_DEBUG="${E2B_DEBUG:-}" \
    "$E2B_BIN" template build --name e2e-cli --dockerfile e2b.Dockerfile ) >"$WORK/cli.out" 2>&1
cli_rc=$?
set -e
echo "---- e2b CLI output ----"; sed 's/^/    cli| /' "$WORK/cli.out"; echo "---- (rc=$cli_rc) ----"

if [ "$cli_rc" -ne 0 ]; then
    echo "==> orchestrator log:" >&2; sed 's/^/    orch| /' "$WORK/orch.log" >&2
    fail "e2b template build exited $cli_rc (see CLI output above; adapt orchestrator/env)"
fi

# ---- verify the build actually finished -----------------------------------
# The CLI's wait loop blocks while status=="building" and prints a "finished"
# banner only on status=="ready" — so rc=0 + that banner means a real flatten.
TID=$(grep -oE 'transient-[0-9a-f-]{36}' "$WORK/cli.out" | head -1)
echo "==> CLI-reported templateID: ${TID:-<none parsed>}"
grep -qiE 'finished|✅' "$WORK/cli.out" || { echo "==> orch log:"; sed 's/^/  orch| /' "$WORK/orch.log" >&2; fail "no 'finished' banner — build did not reach ready (CLI bailed early?)"; }
echo "==> PASS: CLI reported the template build finished (status=ready)"

# store must hold the flatten output (more than just the G1 generation marker)
objs=$(find "$WORK/store" -type f | wc -l)
[ "$objs" -gt 1 ] || fail "store holds only $objs object(s) — flatten did not upload chunks"
echo "==> PASS: store holds $objs object(s) (chunks + manifest) after the CLI build"

# and our own API must show the build ready with a persist template id
if [ -n "$TID" ]; then
    BID=$(grep -oE 'build ID: [0-9a-f-]{36}' "$WORK/cli.out" | head -1 | awk '{print $3}')
    if [ -n "$BID" ]; then
        body=$(curl -sS --noproxy '*' -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
               "http://127.0.0.1:$PORT/templates/$TID/builds/$BID/status")
        echo "    final status body: $body"
        echo "$body" | grep -q '"status":"ready"' || fail "orchestrator status not ready: $body"
        echo "$body" | grep -oE 'e2b-img-[0-9a-f]{64}' | head -1 | grep -q . || fail "no persist e2b-img-… template in status"
        echo "==> PASS: orchestrator reports ready + persist template ($(echo "$body" | grep -oE 'e2b-img-[0-9a-f]{64}' | head -1))"
    fi
fi
echo
echo "==> e2e_build_cli: OK"
