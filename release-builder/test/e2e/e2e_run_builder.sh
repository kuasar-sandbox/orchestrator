#!/usr/bin/env bash
#
# e2e_run_builder.sh — the orchestrator's three-phase build pipeline
# (run-builder), end to end on real microVMs. Builds run INSIDE build
# sandboxes: the base image is pulled + flattened in-guest over the tenant
# network (zot reached via the vswitch mgmt NIC), steps and startCmd/readyCmd
# run THROUGH ENVD (the e2b exec channel, /bin/bash -l -c), and the template
# snapshot is taken from a production-runtime VM with the start command left
# as an envd-managed process. One orchestrator, five builds + three creates:
#
#   B1  fromImage (in-guest pull + flatten)                → e2b-img template
#   B2  fromTemplate(B1, img) + steps + startCmd/readyCmd  → e2b-snp template
#       boots the steps VM from manifest://, applies RUN/ENV/WORKDIR, exports
#       (config merge), runs startCmd/readyCmd on the production runtime,
#       snapshots, then ONE upload-snapshot uploads bundle + image + overlay
#   B3  fromTemplate(B2, snp) + steps only                 → e2b-snp template
#       extracts the base image from B2's snapshot.cfg and INHERITS its
#       startCmd/readyCmd (reaching ready proves both ran)
#   B4  COPY build context (versitygw required)            → e2b-img template
#       files endpoint → presigned direct-to-bucket PUT → in-build extract via
#       flatten-ctl; a RUN step asserts content + default/--chown ownership
#   B5  profile=bare + fromImage                            → bare-img template
#       rejects start/ready, uses bare build network, and remains image-only
#   create from B3, B1 and B5 → 201 → wait running → kill  (snapshot + e2b/bare cold boot)
#
# Plus the negative surface: COPY without files_storage → 501; with it, a COPY
# missing its filesHash → 400 and an un-uploaded context → 400.
# Deep asserts via the artifact chain: B2's snapshot.cfg carries
# e2b.start_cmd metadata + a manifest:// base whose image config holds the
# merged ENV/WORKDIR; B3's RUN step only succeeds if B2's RUN persisted.
#
# Requires systemd as PID1 + root (units over D-Bus), /dev/kvm, docker (seeds
# the base image), zot, mkfs.ext4, and bin/: node-ctl sandbox-ctl
# e2b-key-ctl connector-ctl vswitch cloud-hypervisor flatten-ctl manifest-ctl store-ctl
# + vmlinux + sandbox-runtime{,-e2b,-builder}.erofs. Missing prerequisites →
# exit 0 ("skipped") unless REQUIRE_BUILDER=1.
# Build Register MMDS is also exercised by a real RUN guest, including Trigger
# immutability, terminal secret cleanup, and artifact/log plaintext checks.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
# Builds with steps/startCmd carry the e2b contract: envd runs them as
# `/bin/bash -l -c` — the image must have bash (python:3.12-slim does).
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
# versitygw (S3 gateway) backs COPY build contexts and is required by the full
# test-e2e suite. Use VGW_BIN or a versitygw already on PATH; source-tree
# `make e2e-tools` prepares VGW_BIN under build/e2e-tools/.
if [ -z "${VGW_BIN:-}" ]; then
    VGW_BIN="$(command -v versitygw 2>/dev/null || true)"
fi
VGW_BIN="${VGW_BIN:-}"
SWITCH="${SWITCH:-swbld}"; SW_NETNS="${SW_NETNS:-e2ebld_sw}"; SW_MGMT="${SW_MGMT:-swbldm0}"
MGMT_VIP="169.254.169.254"                   # host-side mgmt NIC IP; guests route 0/0 here

skip() {
    echo
    echo "==> e2e_run_builder: skipping ($*)"
    [ "${REQUIRE_BUILDER:-0}" = "1" ] && { echo "REQUIRE_BUILDER=1 set; failing instead" >&2; exit 1; }
    exit 0
}
fail() { echo "==> FAIL: $*" >&2; exit 1; }

# ---- prerequisite checks --------------------------------------------------
for b in node-ctl sandbox-ctl e2b-key-ctl connector-ctl cloud-hypervisor flatten-ctl manifest-ctl store-ctl; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b — run 'make build'"
done
for f in vmlinux sandbox-runtime.bundle; do
    [ -f "$BIN/$f" ] || skip "missing $BIN/$f — run 'make all'"
done
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v docker >/dev/null 2>&1 || skip "docker not on PATH"
docker info >/dev/null 2>&1 || skip "docker daemon not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.ext4 >/dev/null 2>&1 || [ -x /sbin/mkfs.ext4 ] || skip "mkfs.ext4 not found"
[ -d /run/systemd/system ] || skip "systemd is not PID1 (orchestrator drives units over D-Bus)"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "seed image $E2E_IMAGE not cached and pull failed (set E2E_IMAGE)"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

WORK="$(mktemp -d /tmp/e2e-builder-XXXXXX)"
TAPFD_SOCKET="$WORK/tapfd.sock"
# Units must live in a real systemd load path; we only remove what we created.
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u already exists (real deployment?); refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/saved" "$WORK/store" "$WORK/zot" "$WORK/vgw"
declare -a PIDS=() TAGS=()
cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
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

# ---- zot (anonymous, insecure) + seed --------------------------------------
# Bound to 0.0.0.0: the host pushes via 127.0.0.1, the BUILD SANDBOX pulls via
# the vswitch mgmt VIP ($MGMT_VIP) — guest loopback is not the host's.
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{
  "storage": { "rootDirectory": "$WORK/zot", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" }
}
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$ZOT_PORT" zot
PUSH_REF="127.0.0.1:$ZOT_PORT/e2e/base:v1"
PULL_REF="$MGMT_VIP:$ZOT_PORT/e2e/base:v1"
docker tag "$E2E_IMAGE" "$PUSH_REF"; TAGS+=("$PUSH_REF")
docker push "$PUSH_REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push $PUSH_REF"; }
echo "==> zot up (0.0.0.0:$ZOT_PORT); seeded $E2E_IMAGE → $PUSH_REF (guest pulls $PULL_REF)"

# ---- vswitch (guest network for the build sandboxes) -----------------------
MMDS_PORT="$(free_port)"
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns add "$SW_NETNS"
# --mgmt-extract puts $MGMT_VIP on host NIC $SW_MGMT and routes guest 0/0 to it;
# that is the only path the builds need (zot on the host). No NAT required.
"$BIN/connector-ctl" vswitch serve "$SWITCH" --netns="$SW_NETNS" --ports=16 --mac-addr=02:00:00:00:01:01 \
    --floating-ip-base=100.100.112.0 --mode=tap \
    --mgmt-extract=:$SW_MGMT:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$MGMT_VIP:$MMDS_PORT \
    --tapfd-listen="$TAPFD_SOCKET" --watch-interval=2s >"$WORK/vswitch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
    if [ -S "$TAPFD_SOCKET" ] && "$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1; then
        break
    fi
    kill -0 "${PIDS[-1]}" 2>/dev/null || { cat "$WORK/vswitch.log"; fail "vswitch serve exited"; }
    sleep 0.2
done
"$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1 \
    || { cat "$WORK/vswitch.log"; fail "vswitch not ready"; }
echo "==> vswitch up ($SWITCH; mgmt $SW_MGMT=$MGMT_VIP; tapfd_socket=$TAPFD_SOCKET)"

# ---- manifest config + diff templates ---------------------------------------
KEYLESS_CACHE=""   # no cache-ctl: empty endpoint falls back to store-as-cache
cat > "$WORK/manifest.yaml" <<EOF
manifest:
  key: ""
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 30s
cache:
  endpoint: "$KEYLESS_CACHE"
chunker:
  mode: cdc
  cdc: { min: 128KiB, avg: 512KiB, max: 1MiB }
crypto:
  chunk: aes
  manifest: aes
EOF
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
OVL="$WORK/overlay-1G.ext4"             # cold-boot overlay upper (template VMs)
truncate -s 1G "$OVL" && "$MKFS_EXT4" -F -q -b 4096 "$OVL"
BLDDIFF="$WORK/builder-2G.ext4"         # build VM writable disk (pull cache + steps delta + export scratch)
truncate -s 2G "$BLDDIFF" && "$MKFS_EXT4" -F -q -b 4096 "$BLDDIFF"

# ---- versitygw (S3 gateway for COPY build contexts) ------------------------
# Backs builder.files_storage: the client direct-uploads a COPY context here
# (presigned PUT) and the build sandbox fetches it (presigned GET). Bound to
# 127.0.0.1 — both the client (this script) and the build-side fetch
# (run-builder, host) reach it from the host. Full e2e requires it so the COPY
# chain is covered.
VGW_AK="e2eaccess"; VGW_SK="e2esecretkey0123"; VGW_BUCKET="build-files"
FILES_STORAGE_YAML=""
if [ -n "$VGW_BIN" ] && [ -x "$VGW_BIN" ]; then
    VGW_PORT="$(free_port)"
    mkdir -p "$WORK/vgw/$VGW_BUCKET"   # posix backend: a bucket is a top-level dir
    ROOT_ACCESS_KEY="$VGW_AK" ROOT_SECRET_KEY="$VGW_SK" \
        "$VGW_BIN" --port "127.0.0.1:$VGW_PORT" posix "$WORK/vgw" >"$WORK/vgw.log" 2>&1 &
    PIDS+=($!)
    wait_port 127.0.0.1 "$VGW_PORT" versitygw
    FILES_STORAGE_YAML=$(cat <<EOF
  files_storage:
    endpoint: http://127.0.0.1:$VGW_PORT
    region: us-east-1
    bucket: $VGW_BUCKET
    access_key: $VGW_AK
    secret_key: $VGW_SK
    force_path_style: true
EOF
)
    echo "==> versitygw up (127.0.0.1:$VGW_PORT, posix, bucket=$VGW_BUCKET) — COPY chain enabled"
else
    fail "versitygw missing; COPY chain is required by test-e2e (set VGW_BIN or install versitygw on PATH)"
fi

# ---- tenant credentials + orchestrator --------------------------------------
MK="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC="$("$BIN/e2b-key-ctl" gen-key)"
PORT="$(free_port)"

cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { mode: internal, auth: enforce }
mmds:
  enabled: true
  listen: "$MGMT_VIP:$MMDS_PORT"
  routes:
    enabled: true
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
    overlay_diff_template: $OVL
builder:
  insecure_registry: true
  diff_template: $BLDDIFF
  vcpu: 1
  memory: 1GiB
  pull_timeout_sec: 300
  step_timeout_sec: 180
  ready_timeout_sec: 60
  total_timeout_sec: 1200
$FILES_STORAGE_YAML
checkpoint: { mode: local, local_dir: $WORK/saved }
EOF

"$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; fail "orchestrator serve exited"; }
    sleep 0.5
done
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"
echo "==> orchestrator up (dev http :$PORT); tenant allowlisted"

req() { # method path key [body]
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method"
                -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "${REQ_MMDS_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
wait_running() { # sid
    local sid="$1" code state=""
    for _ in $(seq 1 180); do
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
persist_ref() {
    python3 - "$1" <<'PY'
import base64
import re
import sys

try:
    profile, kind, payload = sys.argv[1].split("-", 2)
    if profile not in {"e2b", "bare"} or kind not in {"img", "snp"}:
        raise ValueError
    raw = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    if base64.urlsafe_b64encode(raw).decode().rstrip("=") != payload:
        raise ValueError
    ref = raw.decode()
    if not re.fullmatch(r"manifest://[0-9a-f]{64}", ref):
        raise ValueError
except (ValueError, UnicodeDecodeError):
    raise SystemExit(1)
print(ref)
PY
}
valid_persist_id() { persist_ref "$1" >/dev/null; }
register() { # name [profile] → sets TID/BID
    local code body expected_profile got_profile
    expected_profile="${2:-e2b}"
    body="{\"name\":\"$1\"}"
    [ -z "${2:-}" ] || body="{\"name\":\"$1\",\"profile\":\"$2\"}"
    code=$(req POST /v3/templates "$AK" "$body")
    [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register $1 = $code (want 202)"; }
    TID=$(json_field "$WORK/resp.body" templateID)
    BID=$(json_field "$WORK/resp.body" buildID)
    got_profile=$(json_field "$WORK/resp.body" profile)
    [ "$got_profile" = "$expected_profile" ] \
        || fail "register $1 profile=$got_profile (want $expected_profile)"
}
diag() { # bid — failure diagnostics (workdir is reaped by the orchestrator)
    echo "---- orchestrator log (tail) ----"
    tail -40 "$WORK/orch.log" 2>/dev/null | sed 's/^/    /'
    echo "---- journal build $1 (tail) ----"
    journalctl KUASAR_BUILD_ID="$1" --no-pager -n 120 2>/dev/null | sed 's/^/    /'
}
wait_ready() { # tid bid label → sets PERSIST (<profile>-{img,snp}-<base64url(portable-ref)>)
    local tid="$1" bid="$2" label="$3" status="" code
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK")
        [ "$code" = "200" ] || fail "$label status = $code (want 200)"
        status=$(json_field "$WORK/resp.body" status)
        case "$status" in
            ready)
                PERSIST=$(json_field "$WORK/resp.body" templateID)
                valid_persist_id "$PERSIST" || fail "$label ready but invalid persist id: $(cat "$WORK/resp.body")"
                return 0;;
            error)
                echo "    $label error response: $(cat "$WORK/resp.body")"
                diag "$bid"; fail "$label build status=error";;
        esac
        sleep 2
    done
    diag "$bid"; fail "$label did not reach ready (last status=$status)"
}

# ---- negative surface: COPY gating depends on files_storage posture ---------
register neg
if [ -z "$FILES_STORAGE_YAML" ]; then
    # Unconfigured: COPY and the files endpoint are unsupported → 501 (loud).
    code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
        "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\"a\",\"b\"],\"filesHash\":\"x\"}]}")
    [ "$code" = "501" ] || fail "COPY trigger (no files_storage) = $code (want 501)"
    code=$(req GET "/templates/$TID/files/deadbeef" "$AK")
    [ "$code" = "501" ] || fail "files endpoint (no files_storage) = $code (want 501)"
    echo "==> PASS: without files_storage, COPY + files endpoint report 501"
else
    # Configured: a COPY with no filesHash is a malformed request → 400.
    code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
        "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\"a\",\"b\"]}]}")
    [ "$code" = "400" ] || fail "COPY trigger (no filesHash) = $code (want 400)"
    echo "==> PASS: COPY without a filesHash rejected (400); files-storage configured (B4 exercises it)"
fi

# ---- BM: Build Register MMDS in a real builder guest -----------------------
echo "==> BM: Build Register MMDS routes/initial secret, Trigger immutability, real guest GET"
MMDS_BUILD_SECRET=MMDS_BUILD_SECRET_GUEST_E2E
REQ_MMDS_HEADER='{"secrets":{"build_secret":"MMDS_BUILD_SECRET_GUEST_E2E"},"routes":[{"path":"/e2e/build-static","data":"MMDS_BUILD_STATIC_GUEST_E2E"},{"path":"/e2e/build-secret","type":"secret","secret":"build_secret"},{"path":"/e2e/build-unresolved","type":"secret","secret":"build_unresolved"}]}'
register e2e-mmds
BM_TID="$TID"; BM_BID="$BID"

# Both Trigger entry points are forbidden from replacing Register's MMDS.
REQ_MMDS_HEADER='{"routes":[]}'
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = 400 ] || { cat "$WORK/resp.body"; fail "BM Trigger MMDS header override=$code (want 400)"; }
unset REQ_MMDS_HEADER
BM_OVERRIDE_BODY=$(python3 - "$PULL_REF" <<'PY'
import json, sys
print(json.dumps({
    "fromImage": sys.argv[1],
    "metadata": {"kuasar-sandbox.mmds": json.dumps({"routes": []})},
}))
PY
)
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "$BM_OVERRIDE_BODY")
[ "$code" = 400 ] || { cat "$WORK/resp.body"; fail "BM Trigger MMDS metadata override=$code (want 400)"; }

BM_SECRET_HASH="$(printf '%s' "$MMDS_BUILD_SECRET" | sha256sum | cut -d' ' -f1)"
BM_RUN=$(python3 - "$BM_SECRET_HASH" <<'PY'
import base64, sys
expected_hash = sys.argv[1]
program = f'''
import hashlib, http.client

def request(method, path, token=None):
    conn = http.client.HTTPConnection("169.254.169.254", 80, timeout=10)
    headers = {{}}
    if method == "PUT":
        headers["X-metadata-token-ttl-seconds"] = "60"
    if token:
        headers["X-metadata-token"] = token
    conn.request(method, path, headers=headers)
    response = conn.getresponse()
    result = response.status, {{k.lower(): v for k, v in response.getheaders()}}, response.read()
    conn.close()
    return result

status, headers, body = request("PUT", "/latest/api/token")
assert status == 200, status
token = body.decode()
status, headers, body = request("GET", "/e2e/build-static", token)
assert status == 200 and body == b"MMDS_BUILD_STATIC_GUEST_E2E", (status, body)
assert headers.get("content-type") == "text/plain", headers
status, headers, body = request("GET", "/e2e/build-secret", token)
assert status == 200 and hashlib.sha256(body).hexdigest() == {expected_hash!r}, "build secret response mismatch"
assert headers.get("content-type") == "text/plain", headers
status, headers, body = request("GET", "/e2e/build-unresolved", token)
assert status == 404, (status, body)
print("MMDS_BUILD_GUEST_OK")
'''
encoded = base64.b64encode(program.encode()).decode()
print("python3 -c 'import base64; exec(base64.b64decode(\"%s\"))'" % encoded)
PY
)
BM_BODY=$(python3 - "$PULL_REF" "$BM_RUN" <<'PY'
import json, sys
print(json.dumps({"fromImage": sys.argv[1], "steps": [{"type": "RUN", "args": [sys.argv[2]]}]}))
PY
)
code=$(req POST "/v2/templates/$BM_TID/builds/$BM_BID" "$AK" "$BM_BODY")
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "BM trigger=$code (want 202)"; }
wait_ready "$BM_TID" "$BM_BID" BM
BM_PERSIST="$PERSIST"
case "$BM_PERSIST" in e2b-img-*) : ;; *) fail "BM persist=$BM_PERSIST (want e2b-img-…)";; esac

journalctl KUASAR_BUILD_ID="$BM_BID" --no-pager --output=cat >"$WORK/bm-mmds.journal" 2>/dev/null || true
grep -q 'MMDS_BUILD_GUEST_OK' "$WORK/bm-mmds.journal" \
    || { diag "$BM_BID"; fail "BM real builder guest did not report MMDS_BUILD_GUEST_OK"; }
python3 - "$WORK/lib/node-ctl.db" "$BM_BID" "$MMDS_BUILD_SECRET" <<'PY'
import json, pathlib, sqlite3, sys
db_path, build_id, secret = sys.argv[1:]
with sqlite3.connect(db_path, timeout=5) as db:
    row = db.execute("select metadata_json from builds where build_id=?", (build_id,)).fetchone()
    secret_rows = db.execute(
        "select count(*) from build_mmds_route_secret_values where build_id=?", (build_id,)
    ).fetchone()[0]
assert row is not None, "BM build row missing"
metadata = json.loads(row[0])
assert "kuasar-sandbox.mmds" not in metadata, "BM terminal build retained builder-only MMDS routes"
assert secret not in row[0], "BM secret leaked into build metadata"
assert secret_rows == 0, "BM terminal cleanup left a build secret row"
for path in pathlib.Path(db_path).parent.glob(pathlib.Path(db_path).name + "*"):
    assert secret.encode() not in path.read_bytes(), f"BM plaintext found in {path}"
PY
BM_REF=$(persist_ref "$BM_PERSIST") || fail "BM persistent id is invalid"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BM_REF" >"$WORK/bm-image.json" 2>"$WORK/bm-image.err" \
    || { cat "$WORK/bm-image.err"; fail "flatten-ctl info BM image"; }
for artifact in "$WORK/orch.log" "$WORK/bm-mmds.journal" "$WORK/bm-image.json" "$WORK/bm-image.err"; do
    grep -a -F -q -- "$MMDS_BUILD_SECRET" "$artifact" \
        && fail "Build Register MMDS secret plaintext appeared in $artifact"
done
grep -Fq 'kuasar-sandbox.mmds' "$WORK/bm-image.json" \
    && fail "Build Register MMDS routes leaked into final image config"
echo "==> PASS: BM real guest MMDS, Trigger immutability, terminal cleanup, and artifact/log secrecy"

# ---- B1: fromImage → e2b-img -----------------------------------------------
echo "==> B1: fromImage=$PULL_REF (in-guest pull + flatten)"
register e2e-img
B1_TID="$TID"; B1_BID="$BID"
code=$(req POST "/v2/templates/$B1_TID/builds/$B1_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B1 trigger = $code (want 202)"; }
wait_ready "$B1_TID" "$B1_BID" B1
B1_PERSIST="$PERSIST"
case "$B1_PERSIST" in e2b-img-*) : ;; *) fail "B1 persist=$B1_PERSIST (want e2b-img-…)";; esac
echo "==> PASS: B1 ready → $B1_PERSIST"

# ---- B2: fromTemplate(img) + steps + startCmd/readyCmd → e2b-snp ------------
echo "==> B2: fromTemplate=$B1_PERSIST + steps + startCmd/readyCmd"
register e2e-tpl
B2_TID="$TID"; B2_BID="$BID"
B2_BODY=$(cat <<EOF
{"fromTemplate":"$B1_PERSIST",
 "steps":[
   {"type":"RUN","args":["useradd -m -d /home/user user || adduser -D user"]},
   {"type":"RUN","args":["echo b2 > /etc/b2-marker"]},
   {"type":"ENV","args":["BUILT","yes"]},
   {"type":"WORKDIR","args":["/home/user"]}],
 "startCmd":"touch /home/user/started; exec sleep 86400",
 "readyCmd":"test -f /home/user/started"}
EOF
)
code=$(req POST "/v2/templates/$B2_TID/builds/$B2_BID" "$AK" "$B2_BODY")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B2 trigger = $code (want 202)"; }
wait_ready "$B2_TID" "$B2_BID" B2
B2_PERSIST="$PERSIST"
case "$B2_PERSIST" in e2b-snp-*) : ;; *) fail "B2 persist=$B2_PERSIST (want e2b-snp-…)";; esac
echo "==> PASS: B2 ready → $B2_PERSIST"

# Deep asserts through the artifact chain: the uploaded snapshot.cfg names a
# manifest:// base image and carries the e2b start/ready metadata; the base
# image's runtime config holds the merged ENV/WORKDIR from the steps.
B2_REF=$(persist_ref "$B2_PERSIST") \
    || fail "B2 persist id does not contain a valid portable ref: $B2_PERSIST"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$B2_REF" >"$WORK/b2.cfg.json" 2>"$WORK/b2.cfg.err" \
    || { cat "$WORK/b2.cfg.err"; fail "sandbox-ctl info $B2_REF"; }
grep -q '"e2b.start_cmd": *"touch /home/user/started' "$WORK/b2.cfg.json" \
    || fail "B2 snapshot.cfg missing e2b.start_cmd metadata: $(cat "$WORK/b2.cfg.json")"
grep -q '"e2b.ready_cmd": *"test -f /home/user/started"' "$WORK/b2.cfg.json" \
    || fail "B2 snapshot.cfg missing e2b.ready_cmd metadata"
B2_IMG_HEX=$(grep -o '"BaseRef": *"manifest://[0-9a-f]*"' "$WORK/b2.cfg.json" | grep -o '[0-9a-f]\{64\}' | head -1)
[ -n "$B2_IMG_HEX" ] || fail "B2 snapshot.cfg base is not manifest:// (upload-snapshot did not rewrite?): $(cat "$WORK/b2.cfg.json")"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "manifest://$B2_IMG_HEX" >"$WORK/b2.img.json" 2>"$WORK/b2.img.err" \
    || { cat "$WORK/b2.img.err"; fail "flatten-ctl info manifest://$B2_IMG_HEX"; }
grep -q '"BUILT=yes"' "$WORK/b2.img.json" || fail "B2 image config missing merged ENV BUILT=yes: $(cat "$WORK/b2.img.json")"
grep -q '"WorkingDir": *"/home/user"' "$WORK/b2.img.json" || fail "B2 image config missing merged WORKDIR"
echo "==> PASS: B2 artifacts — snapshot.cfg metadata + manifest:// base + merged ENV/WORKDIR"

# ---- B3: fromTemplate(snp) + steps only (start/ready inherited) -------------
echo "==> B3: fromTemplate=$B2_PERSIST + steps (inherits startCmd/readyCmd)"
register e2e-child
B3_TID="$TID"; B3_BID="$BID"
code=$(req POST "/v2/templates/$B3_TID/builds/$B3_BID" "$AK" \
    "{\"fromTemplate\":\"$B2_PERSIST\",\"steps\":[{\"type\":\"RUN\",\"args\":[\"test -f /etc/b2-marker\"]}]}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B3 trigger = $code (want 202)"; }
wait_ready "$B3_TID" "$B3_BID" B3
B3_PERSIST="$PERSIST"
case "$B3_PERSIST" in e2b-snp-*) : ;; *) fail "B3 persist=$B3_PERSIST (want e2b-snp-…)";; esac
# ready is only reachable if: the RUN saw B2's marker (base extraction worked)
# AND the inherited startCmd/readyCmd ran on the new template VM.
echo "==> PASS: B3 ready → $B3_PERSIST (base-image extraction + start/ready inheritance)"

# ---- B4: COPY build context via files endpoint + presigned direct upload ----
# Acts as the e2b client: GET the files endpoint (present=false) → PUT the
# gzipped context straight to the bucket → GET again (present=true), then build
# fromImage with COPY steps and assert (via a RUN step) that the files landed
# with the right ownership. Skipped without versitygw.
if [ -n "$FILES_STORAGE_YAML" ]; then
    echo "==> B4: COPY build context (files endpoint → presigned PUT → in-build extract)"
    register e2e-copy
    B4_TID="$TID"; B4_BID="$BID"

    # Build the COPY context: ./hello.txt + ./sub/nested.txt, gzipped tar with
    # arcnames relative to the context (the e2b SDK's layout).
    CTX="$WORK/ctx"; mkdir -p "$CTX/sub"
    echo "COPY-MARKER-$RANDOM" > "$CTX/hello.txt"; MARKER="$(cat "$CTX/hello.txt")"
    echo nested > "$CTX/sub/nested.txt"
    ( cd "$CTX" && tar czf "$WORK/ctx.tgz" . )
    HASH="$(sha256sum "$WORK/ctx.tgz" | cut -d' ' -f1)"

    # 1. files endpoint: not present yet, returns a presigned PUT url.
    code=$(req GET "/templates/$B4_TID/files/$HASH" "$AK")
    [ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "files GET = $code (want 201)"; }
    # Parse with a real JSON reader (not grep): the presigned url carries '&',
    # which stdlib json escapes to & — every real client (the e2b SDK)
    # decodes it; a sed extraction would not.
    present=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["present"])' "$WORK/resp.body")
    [ "$present" = "False" ] || fail "files: expected present=false on first GET: $(cat "$WORK/resp.body")"
    PUT_URL="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["url"])' "$WORK/resp.body")"
    [ -n "$PUT_URL" ] || fail "files: no presigned url"

    # 2. client uploads the context straight to the bucket (bytes skip the orchestrator).
    pcode=$(curl -sS --noproxy '*' -o /dev/null -w '%{http_code}' -X PUT --data-binary @"$WORK/ctx.tgz" "$PUT_URL")
    [ "$pcode" = "200" ] || fail "presigned PUT = $pcode (want 200)"

    # 3. now present (idempotency: client skips re-upload).
    code=$(req GET "/templates/$B4_TID/files/$HASH" "$AK")
    [ "$code" = "201" ] || fail "files GET#2 = $code"
    present=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["present"])' "$WORK/resp.body")
    [ "$present" = "True" ] || fail "files: expected present=true after upload: $(cat "$WORK/resp.body")"
    echo "==> PASS: files endpoint round-trip (present false→PUT→true; direct-to-bucket upload)"

    # Negative: a COPY referencing an un-uploaded context → 400 at trigger.
    code=$(req POST "/v2/templates/$B4_TID/builds/$B4_BID" "$AK" \
        "{\"fromImage\":\"$PULL_REF\",\"steps\":[{\"type\":\"COPY\",\"args\":[\".\",\"/opt/x\"],\"filesHash\":\"deadbeefdeadbeef\"}]}")
    [ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "COPY w/ unuploaded context = $code (want 400)"; }
    echo "==> PASS: COPY referencing an un-uploaded context rejected (400)"

    # Real build: COPY whole context to /opt/ct (default owner 0:0), the single
    # file to /opt/ct2/ with --chown 1000:1000, then RUN asserts presence+owner.
    # Reaching ready proves the extract + ownership are correct.
    B4_BODY=$(cat <<EOF
{"fromImage":"$PULL_REF",
 "steps":[
   {"type":"COPY","args":[".","/opt/ct"],"filesHash":"$HASH"},
   {"type":"COPY","args":["hello.txt","/opt/ct2/","1000:1000"],"filesHash":"$HASH"},
   {"type":"RUN","args":["test \"\$(cat /opt/ct/hello.txt)\" = \"$MARKER\" && test -f /opt/ct/sub/nested.txt && test \"\$(stat -c %u:%g /opt/ct/hello.txt)\" = 0:0 && test \"\$(stat -c %u:%g /opt/ct2/hello.txt)\" = 1000:1000"]}]}
EOF
)
    code=$(req POST "/v2/templates/$B4_TID/builds/$B4_BID" "$AK" "$B4_BODY")
    [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B4 trigger = $code (want 202)"; }
    wait_ready "$B4_TID" "$B4_BID" B4
    B4_PERSIST="$PERSIST"
    case "$B4_PERSIST" in e2b-img-*) : ;; *) fail "B4 persist=$B4_PERSIST (want e2b-img-…)";; esac
    echo "==> PASS: B4 ready → $B4_PERSIST (COPY extract + default/--chown ownership verified in-build)"
else
    fail "B4 COPY chain requires files_storage; versitygw was not configured"
fi

# ---- B5: bare profile fromImage → bare-img -------------------------------
echo "==> B5: profile=bare fromImage=$PULL_REF (image-only, bare network)"
register e2e-bare bare
B5_TID="$TID"; B5_BID="$BID"
code=$(req POST "/v2/templates/$B5_TID/builds/$B5_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"startCmd\":\"sleep 60\"}")
[ "$code" = "400" ] || { cat "$WORK/resp.body"; fail "B5 startCmd = $code (want 400)"; }
code=$(req POST "/v2/templates/$B5_TID/builds/$B5_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B5 trigger = $code (want 202)"; }
wait_ready "$B5_TID" "$B5_BID" B5
B5_PERSIST="$PERSIST"
case "$B5_PERSIST" in bare-img-*) : ;; *) fail "B5 persist=$B5_PERSIST (want bare-img-…)";; esac
echo "==> PASS: B5 ready → $B5_PERSIST (bare profile remained image-only)"

# ---- create a sandbox from the built template -------------------------------
echo "==> create sandbox from $B3_PERSIST (snapshot restore path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B3_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B3_BID"; fail "create = $code (want 201)"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$SID" ] || fail "create returned no sandboxID"
wait_running "$SID" || { diag "$B3_BID"; fail "snapshot-template sandbox did not reach running"; }
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list = $code (want 200)"
grep -q "$SID" "$WORK/resp.body" || fail "created sandbox $SID not in list"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill = $code (want 204)"
echo "==> PASS: sandbox create → list → kill from the built template"

echo "==> create e2b cold sandbox from $B1_PERSIST (image path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B1_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B1_BID"; fail "e2b cold create = $code (want 201)"; }
E2B_COLD_SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$E2B_COLD_SID" ] || fail "e2b cold create returned no sandboxID"
wait_running "$E2B_COLD_SID" || { diag "$B1_BID"; fail "e2b cold sandbox did not reach running"; }
code=$(req DELETE "/sandboxes/$E2B_COLD_SID" "$AK"); [ "$code" = "204" ] || fail "e2b cold kill = $code (want 204)"
echo "==> PASS: e2b image cold Create reached running and cleaned up"

echo "==> create bare sandbox from $B5_PERSIST (image cold-boot path)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$B5_PERSIST\",\"timeout\":60}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; diag "$B5_BID"; fail "bare create = $code (want 201)"; }
BARE_SID=$(json_field "$WORK/resp.body" sandboxID)
[ -n "$BARE_SID" ] || fail "bare create returned no sandboxID"
wait_running "$BARE_SID" || { diag "$B5_BID"; fail "bare cold sandbox did not reach running"; }
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "bare list = $code (want 200)"
grep -q "$BARE_SID" "$WORK/resp.body" || fail "created bare sandbox $BARE_SID not in list"
code=$(req DELETE "/sandboxes/$BARE_SID" "$AK"); [ "$code" = "204" ] || fail "bare kill = $code (want 204)"
echo "==> PASS: bare sandbox create → list → kill from bare-img build"

# store actually holds the uploaded chunks/manifests
objs=$(find "$WORK/store" -type f | wc -l)
[ "$objs" -gt 0 ] || fail "store has no objects after the builds"
echo "==> store holds $objs object(s)"

echo
echo "==> e2e_run_builder: OK   (B1=$B1_PERSIST B2=$B2_PERSIST B3=$B3_PERSIST${B4_PERSIST:+ B4=$B4_PERSIST} B5=$B5_PERSIST)"
