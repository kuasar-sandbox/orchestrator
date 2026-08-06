#!/usr/bin/env bash
#
# e2e_orchestrator.sh — bring up node-ctl (the e2b-compatible single-node
# ingress) and exercise it end to end with plain curl (the same surface the e2b
# SDK/CLI use):
#
#   1. unit auto-install   serve generates+installs sandbox-runner@ / sandbox-builder@
#                          (+ slices) into a transient unit_dir and daemon-reloads.
#   2. control plane        GET /health, X-API-KEY auth (401 paths).
#   3. build API (e2b v3)   POST /v3/templates (register, transient id) →
#                          POST /v2/templates/{tid}/builds/{bid} (trigger) →
#                          GET …/status (poll). Cross-key access → 404 (ownership).
#   4. optional data plane  if TEMPLATE_ID is explicitly provided:
#                          POST /sandboxes (bare) → GET /v2/sandboxes → DELETE.
#                          The self-contained data-plane e2e lives in
#                          e2e_execute.sh / e2e_orchestrator_proxy.sh.
#
# node-ctl needs systemd (it drives units over D-Bus), so this test
# requires systemd as PID1 and root. Missing prerequisites → exit 0 ("skipped on
# this host") unless REQUIRE_ORCH=1. The control-plane tests need only systemd +
# root.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
SWITCH="${SWITCH:-sw0}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
# Tenant credentials are derived from manifest keys via e2b-key-ctl, below.

skip() {
    echo
    echo "==> e2e_orchestrator: skipping ($*)"
    [ "${REQUIRE_ORCH:-0}" = "1" ] && { echo "REQUIRE_ORCH=1 set; failing instead" >&2; exit 1; }
    exit 0
}

# ---- prerequisite checks --------------------------------------------------
for b in node-ctl sandbox-ctl e2b-key-ctl; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
[ -d /run/systemd/system ] || skip "systemd is not PID1 (node-ctl drives units over D-Bus)"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

WORK="$(mktemp -d /tmp/e2e-orch-XXXXXX)"
UNIT_DIR="$WORK/units"           # transient unit_dir (not /etc) so the test is self-contained
mkdir -p "$UNIT_DIR"
declare -a PIDS=()
cleanup() {
    set +e
    for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
    # best-effort teardown of any unit the run left behind
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "==> FAIL: $*" >&2; exit 1; }

# Derive tenant credential pairs. ManifestKey protects content; the fixed KDF
# yields APISecret, which alone mints API keys. MK_OTHER remains unallowlisted.
MK="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
MK_OTHER="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET_OTHER="$("$BIN/e2b-key-ctl" derive-api-secret "$MK_OTHER")"
AK_OTHER="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET_OTHER")"
ENC="$("$BIN/e2b-key-ctl" gen-key)"

# curl helper: $1=method $2=path $3=api-key $4=body(optional). Prints "<code>\n<body>".
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method"
                -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
wait_running() { # sid
    local sid="$1" code state=""
    for _ in $(seq 1 120); do
        code=$(req GET "/sandboxes/$sid" "$AK" || true)
        if [ "$code" = "200" ]; then
            state=$(json_field "$WORK/resp.body" state)
            [ "$state" = "running" ] && return 0
            [ "$state" = "dead" ] && break
        fi
        sleep 0.5
    done
    echo "sandbox $sid state=$state, want running" >&2
    return 1
}

# The control-plane / unit-install / build-API / ownership tests below do NOT need
# a running vswitch — node-ctl only dials connector-ctl vswitch on sandbox *create*
# (the gated data-plane step at the end). So no `connector-ctl vswitch serve` here.

# ---- orchestrator config (dev http; transient paths) ----------------------
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle }
checkpoint: { mode: remote }
EOF
mkdir -p "$WORK/run" "$WORK/lib"
# minimal shared manifest config (endpoints; key empty) so serve can read it
cat > "$WORK/manifest.yaml" <<EOF
manifest:
  key: ""
EOF

# ---- 2. start node-ctl conductor serve --------------------------------------
echo "==> node-ctl conductor serve (dev http :$PORT, unit_dir=$UNIT_DIR)"
"$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for i in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; skip "node-ctl conductor serve exited (see log)"; }
    sleep 0.5
done

# Allowlist MK so it may create/build — now via serve's admin plane on the control
# socket (the daemon owns the manifest_keys credential-pair table), so it runs AFTER serve is up.
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add failed"

# ---- 1. assert unit auto-install ------------------------------------------
for u in sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice; do
    [ -f "$UNIT_DIR/$u" ] || fail "unit $u was not generated into $UNIT_DIR"
done
grep -q "run-sandbox .*--run-id=%i" "$UNIT_DIR/sandbox-runner@.service" || fail "runner unit ExecStart is not run-id based"
grep -q "run-builder .*--run-id=%i" "$UNIT_DIR/sandbox-builder@.service" || fail "builder unit ExecStart is not run-id based"
grep -q "ExecStopPost=/bin/rm -f .*runs/%i.pid" "$UNIT_DIR/sandbox-runner@.service" || fail "runner unit does not clean its run pidfile"
grep -q "ExecStopPost=/bin/rm -f .*runs/%i.pid" "$UNIT_DIR/sandbox-builder@.service" || fail "builder unit does not clean its run pidfile"
grep -q "StandardOutput=file:" "$UNIT_DIR/sandbox-builder@.service" && fail "builder unit still uses obsolete result-file capture"
echo "==> PASS: unit auto-install (runner+builder+slices; run-id assignment + config-socket build result)"

# ---- 2. control plane: health + auth --------------------------------------
code=$(req GET /health "")
[ "$code" = "204" ] || fail "/health = $code (want 204)"
code=$(req POST /sandboxes "" '{"templateID":"x"}')
[ "$code" = "401" ] || fail "POST /sandboxes with no key = $code (want 401)"
code=$(req POST /sandboxes "e2b_deadbeef_not_a_real_token" '{"templateID":"x"}')
[ "$code" = "401" ] || fail "POST /sandboxes with malformed key = $code (want 401)"
echo "==> PASS: control plane up; auth rejects missing/malformed keys (401)"

# A valid api key whose APISecret+ManifestKey pair is NOT allowlisted: passes the format check
# (not 401) but create/register is refused with 403.
code=$(req POST /v3/templates "$AK_OTHER" '{"name":"denied"}')
[ "$code" = "403" ] || fail "register with non-allowlisted key = $code (want 403)"
echo "==> PASS: non-allowlisted credential pair refused (403)"

# ---- 3. build API (e2b v3) + ownership ------------------------------------
code=$(req POST /v3/templates "$AK" '{"name":"e2e-tmpl","tags":["e2e"]}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register = $code (want 202)"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
case "$TID" in transient-*) : ;; *) fail "register templateID=$TID (want transient-…)";; esac
echo "==> register: templateID=$TID buildID=$BID"

# ownership: another tenant cannot see this build
code=$(req GET "/templates/$TID/builds/$BID/status" "$AK_OTHER")
[ "$code" = "404" ] || fail "cross-tenant status = $code (want 404 — ownership leak!)"
echo "==> PASS: ownership — other tenant gets 404 on this build"

# trigger + poll (the build itself may end in error without a live store/registry;
# we assert the API lifecycle + status transitions, not a successful flatten).
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"${BUILD_IMAGE:-docker.io/library/alpine:3.19}\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger = $code (want 202)"; }
status=""
for i in $(seq 1 20); do
    code=$(req GET "/templates/$TID/builds/$BID/status" "$AK")
    [ "$code" = "200" ] || fail "status = $code (want 200)"
    status=$(json_field "$WORK/resp.body" status)
    echo "    build status: $status"
    case "$status" in ready|error) break;; esac
    sleep 1
done
case "$status" in
    waiting|building|ready|error) echo "==> PASS: build API lifecycle (register→trigger→status=$status)";;
    *) fail "unexpected build status '$status'";;
esac

# ---- 4. optional data-plane create for manual TEMPLATE_ID validation --------
if [ -n "${TEMPLATE_ID:-}" ]; then
    [ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || fail "TEMPLATE_ID was set but /dev/kvm is not available (rw)"
    echo "==> create bare sandbox from TEMPLATE_ID=$TEMPLATE_ID"
    code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE_ID\",\"timeout\":30}")
    [ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "create = $code (want 201)"; }
    SID=$(json_field "$WORK/resp.body" sandboxID)
    echo "    sandboxID=$SID"
    wait_running "$SID" || fail "created sandbox $SID did not reach running"
    code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list = $code"
    grep -q "$SID" "$WORK/resp.body" || fail "created sandbox $SID not in list"
    code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill = $code (want 204)"
    echo "==> PASS: sandbox create → list → kill"
fi

echo "==> e2e_orchestrator: OK"
