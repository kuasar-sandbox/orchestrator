#!/usr/bin/env bash
#
# e2e_run_builder.sh — the orchestrator's three-phase build pipeline
# (run-builder), end to end on real microVMs. Builds run INSIDE build
# sandboxes: the base image is pulled + flattened in-guest over the tenant
# network (zot reached via the vswitch mgmt NIC), steps and startCmd/readyCmd
# run THROUGH ENVD (the e2b exec channel, /bin/bash -l -c), and the template
# snapshot is taken from a production-runtime VM with the start command left
# as an envd-managed process. One orchestrator, five successful builds, one
# deterministic failed build, and three creates:
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
#   BF  fromTemplate(B1) + failing RUN                     → error
#       preserves journal logs while its failed systemd instance is collected
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
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

# Keep #205's finite CPUQuota assertions while avoiding an additional parent
# throttle around each independently limited phase VM. A Build receives the
# host's full CPU capacity; admission aggregate limits scale with max_builds.
BUILDER_CPU="$(nproc)"
BUILDER_CPU_MILLI=$((BUILDER_CPU * 1000))
BUILDER_EXECUTION_CPU=$((BUILDER_CPU * 2))
BUILDER_EXECUTION_CPU_MILLI=$((BUILDER_EXECUTION_CPU * 1000))
BUILDER_REGISTRATION_CPU=$((BUILDER_CPU * 16))
BUILDER_REGISTRATION_CPU_MILLI=$((BUILDER_REGISTRATION_CPU * 1000))

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
# The extraction CIDRs classify guest traffic; this test owns the management
# VIP address used to reach Zot and MMDS on the host. No NAT is required.
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
ip addr replace "$MGMT_VIP/32" dev "$SW_MGMT" \
    || fail "configure management VIP on $SW_MGMT"
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
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable: { cpu: 1, memory: 256MiB }
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
    overlay_diff_template: $OVL
builder:
  admission:
    registration:
      max_builds: 16
      resources: { cpu: $BUILDER_REGISTRATION_CPU, memory: 96GiB, storage: 128GiB }
    execution:
      max_builds: 2
      resources: { cpu: $BUILDER_EXECUTION_CPU, memory: 12GiB, storage: 16GiB }
  registration_ttl: 1h
  queue_ttl: 30m
  insecure_registry: true
  diff_template: $BLDDIFF
  pull_timeout_sec: 300
  step_timeout_sec: 180
  ready_timeout_sec: 60
  total_timeout_sec: 1200
$FILES_STORAGE_YAML
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  audit_path: $WORK/resource-audit.log
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 1GiB, cpu: 0.5 }
  admission: { rate: 50, burst: 50, startup_ttl: 180s, queue_ttl: 30s, queue_max_depth: 256 }
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
    [ -n "${REQ_BUILDER_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Builder: ${REQ_BUILDER_HEADER}")
    [ -n "${REQ_RESOURCE_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Resource: ${REQ_RESOURCE_HEADER}")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
failed_unit_count() { # $1=unit pattern; list-units does not load missing instances
    systemctl list-units --all --state=failed --type=service --no-legend --no-pager "$1" 2>/dev/null \
        | awk 'NF { count++ } END { print count + 0 }'
}
wait_unit_collected() { # $1=exact unit
    local unit="$1" loaded
    for _ in $(seq 1 100); do
        loaded=$(systemctl list-units --all --type=service --no-legend --no-pager "$unit" 2>/dev/null) \
            || return 1
        if ! grep -Fq -- "$unit" <<<"$loaded"; then
            return 0
        fi
        sleep 0.1
    done
    systemctl list-units --all --type=service --no-legend --no-pager "$unit" >&2 || true
    return 1
}
wait_unit_journal_contains() { # $1=unit, $2=fixed string, $3=output file
    local unit="$1" pattern="$2" output="$3"
    for _ in $(seq 1 100); do
        journalctl -u "$unit" --no-pager --output=cat >"$output" 2>/dev/null || true
        grep -Fq -- "$pattern" "$output" && return 0
        sleep 0.1
    done
    return 1
}
build_run_id() { # $1=build id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select run_id from builds where build_id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
}
build_trigger_signature() { # $1=build id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute(
        """select kind, from_image, from_template, start_cmd, ready_cmd,
                  steps_json, registry_auth_enc, metadata_json, builder_json,
                  status, reason, run_id
             from builds where build_id=?""",
        (sys.argv[2],),
    ).fetchone()
assert row is not None, "build row missing"
print(json.dumps(row, separators=(",", ":")))
PY
}
assert_trigger_conflict() { # $1=exact internal state
    python3 - "$WORK/resp.body" "$1" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
state = sys.argv[2]
assert body == {
    "message": f"build cannot be triggered from state {state}",
    "state": state,
}, body
PY
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
wait_resource_capacity() { # $1=sid, $2=expected memTotal bytes
    local sid="$1" expected="$2" code=""
    for _ in $(seq 1 240); do
        code=$(req GET "/sandboxes/$sid/stats/resource" "$AK" || true)
        if [ "$code" = "200" ] && python3 - "$WORK/resp.body" "$expected" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
assert stats.get("memTotal") == int(sys.argv[2]), stats
PY
        then
            return 0
        fi
        sleep 0.25
    done
    echo "resource stats sid=$sid status=$code expected memTotal=$expected body=$(cat "$WORK/resp.body" 2>/dev/null)" >&2
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
phase_sandbox_id() { # $1=phase, $2=opaque build id
    python3 - "$1" "$2" <<'PY'
import base64
import hashlib
import sys

digest = hashlib.sha256(sys.argv[2].encode()).digest()[:12]
encoded = base64.b32hexencode(digest).decode().rstrip("=").lower()
print(f"bp-{sys.argv[1]}-{encoded}")
PY
}
wait_phase_audit() { # $1=phase, $2=build id
    local sid
    sid=$(phase_sandbox_id "$1" "$2")
    for _ in $(seq 1 120); do
        if grep -Eq "admit .*sid=$sid( |$)" "$WORK/resource-audit.log" 2>/dev/null &&
            grep -Eq "release .*sid=$sid( |$)" "$WORK/resource-audit.log" 2>/dev/null; then
            return 0
        fi
        sleep 0.25
    done
    tail -100 "$WORK/resource-audit.log" >&2 2>/dev/null || true
    return 1
}
wait_phase_admit() { # $1=phase, $2=build id
    local sid
    sid=$(phase_sandbox_id "$1" "$2")
    for _ in $(seq 1 120); do
        if grep -Eq "admit .*sid=$sid( |$)" "$WORK/resource-audit.log" 2>/dev/null; then
            return 0
        fi
        sleep 0.25
    done
    tail -100 "$WORK/resource-audit.log" >&2 2>/dev/null || true
    return 1
}
wait_resource_reservations_empty() {
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" >"$WORK/phase-reservations-final.json" 2>/dev/null &&
            python3 - "$WORK/phase-reservations-final.json" 2>/dev/null <<'PY'
import json, sys
assert not json.load(open(sys.argv[1]))
PY
        then
            return 0
        fi
        sleep 0.25
    done
    cat "$WORK/phase-reservations-final.json" >&2 2>/dev/null || true
    return 1
}
register() { # name [profile] → sets TID/BID
    local code body expected_profile got_profile
    expected_profile="${2:-e2b}"
    # The finite, asserted Builder quota equals host capacity; the phase
    # Sandbox independently remains 2 vCPU.
    body="{\"name\":\"$1\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":6144}"
    [ -z "${2:-}" ] || body="{\"name\":\"$1\",\"profile\":\"$2\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":6144}"
    REQ_BUILDER_HEADER="{\"resources\":{\"cpu\":$BUILDER_CPU,\"memory\":\"6GiB\",\"storage\":\"4GiB\"}}"
    REQ_RESOURCE_HEADER='{"capacity":{"cpu":2,"memory":"3GiB"},"allocatable":{"cpu":1,"memory":"512MiB"},"startup":{"memory":"3GiB"}}'
    code=$(req POST /v3/templates "$AK" "$body")
    unset REQ_BUILDER_HEADER REQ_RESOURCE_HEADER
    [ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register $1 = $code (want 202)"; }
    TID=$(json_field "$WORK/resp.body" templateID)
    BID=$(json_field "$WORK/resp.body" buildID)
    got_profile=$(json_field "$WORK/resp.body" profile)
    [ "$got_profile" = "$expected_profile" ] \
        || fail "register $1 profile=$got_profile (want $expected_profile)"
}
assert_active_build_accounting() { # $1=tid, $2=bid
    local tid="$1" bid="$2" code="" phase="" sid="" run_id="" reservation_json=""
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK" || true)
        if [ "$code" = "200" ]; then
            phase="" sid="" run_id=""
            read -r phase sid run_id < <(python3 - "$WORK/resp.body" <<'PY'
import json, sys
status = json.load(open(sys.argv[1]))
phase = status.get("phase") or {}
if phase.get("name") and phase.get("sandboxID") and status.get("runID"):
    print(phase["name"], phase["sandboxID"], status["runID"])
PY
) || true
            [ -n "$phase" ] && break
        fi
        sleep 0.25
    done
    [ -n "$phase" ] || fail "build $bid never exposed an active phase"

    python3 - "$WORK/resp.body" "$BUILDER_CPU_MILLI" <<'PY' || fail "active Build status resources/enforcement"
import json, sys
status = json.load(open(sys.argv[1]))
assert status["resources"] == {
    "cpuMilli": int(sys.argv[2]),
    "memoryBytes": 6 << 30,
    "storageBytes": 4 << 30,
}, status
assert status["executionClaimed"] is True, status
assert status["systemdEnforcement"] == "cpu,memory", status
assert status["storageEnforcement"] == "admission-only", status
PY

    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" resource list --socket "$WORK/sandbox-resource.sock" >"$WORK/phase-reservations.json" 2>/dev/null &&
            SID="$sid" python3 - "$WORK/phase-reservations.json" 2>/dev/null <<'PY'
import json, os, sys
rows = json.load(open(sys.argv[1]))
assert len(rows) == 1, rows
row = rows[0]
assert row["sandbox_id"] == os.environ["SID"], row
assert row["capacity"] == {"memory_bytes": 3 << 30, "cpu_milli": 2000}, row
assert row["floor"] == {"memory_bytes": 512 << 20, "cpu_milli": 1000}, row
assert row.get("connected") is True, row
assert row.get("provisional", False) is False, row
assert row["stage"] == "settled", row
# The existing admin-list wire name carries the sandbox's absolute safe
# reservation baseline.  It is not the configured headroom (reported above as
# floor.memory_bytes), and may move by one controller step while this poll runs.
reservation = row["allocatable_memory"]
assert 512 << 20 <= reservation <= 3 << 30, row
assert reservation % (64 << 20) == 0, row
assert row["cgroup_path"].endswith("/vmm"), row
assert row["peer_pid"] > 0, row
PY
        then
            reservation_json=$(cat "$WORK/phase-reservations.json")
            break
        fi
        sleep 0.25
    done
    [ -n "$reservation_json" ] || fail "phase $phase/$sid did not become the sole precise nodectl reservation"

    "$BIN/node-ctl" builder status --socket "$WORK/node-ctl.socket" >"$WORK/builder-status.json" \
        || fail "node-ctl builder status"
    python3 - "$WORK/builder-status.json" "$BUILDER_CPU_MILLI" "$BUILDER_EXECUTION_CPU_MILLI" "$BUILDER_REGISTRATION_CPU_MILLI" <<'PY' || fail "durable Builder admission status"
import json, sys
status = json.load(open(sys.argv[1]))
build_cpu, execution_cpu, registration_cpu = map(int, sys.argv[2:])
assert status["registration"]["configured"] == {
    "max_builds": 16,
    "resources": {"cpu": registration_cpu, "memory": 96 << 30, "storage": 128 << 30},
}, status
assert status["execution"]["configured"] == {
    "max_builds": 2,
    "resources": {"cpu": execution_cpu, "memory": 12 << 30, "storage": 16 << 30},
}, status
# The deliberately untriggered negative-test registration and this active
# Build both consume registration admission. Only this Build consumes execution.
assert status["registration"]["used_builds"] == 2, status
assert status["registration"]["used_resources"] == {
    "cpu": 2 * build_cpu, "memory": 12 << 30, "storage": 8 << 30,
}, status
assert status["execution"]["used_builds"] == 1, status
assert status["execution"]["used_resources"] == {
    "cpu": build_cpu, "memory": 6 << 30, "storage": 4 << 30,
}, status
PY

    local build_pid sandbox_ctl_pid vmm_path ctl_path unit_path slice_path memory_high=""
    build_pid=$(cat "$WORK/run/runs/$run_id.pid")
    sandbox_ctl_pid=$(python3 - "$WORK/phase-reservations.json" "$sid" <<'PY'
import json, sys
for row in json.load(open(sys.argv[1])):
    if row["sandbox_id"] == sys.argv[2]:
        print(row["peer_pid"])
        break
PY
)
    vmm_path=$(python3 - "$WORK/phase-reservations.json" "$sid" <<'PY'
import json, sys
for row in json.load(open(sys.argv[1])):
    if row["sandbox_id"] == sys.argv[2]:
        print(row["cgroup_path"])
        break
PY
)
    ctl_path="$(dirname "$vmm_path")/ctl"
    unit_path=$(dirname "$vmm_path")
    slice_path=$(dirname "$unit_path")
    grep -Eq '/sandbox-builder.slice/.+/ctl$' "/proc/$build_pid/cgroup" \
        || fail "run-builder pid $build_pid is not in its ctl subgroup"
    grep -Eq '/sandbox-builder.slice/.+/ctl$' "/proc/$sandbox_ctl_pid/cgroup" \
        || fail "phase sandbox-ctl pid $sandbox_ctl_pid is not in its ctl subgroup"
    [ ! -s "$unit_path/cgroup.procs" ] || fail "builder service cgroup root has direct processes"
    grep -qw cpu "$unit_path/cgroup.subtree_control" || fail "builder unit did not enable cpu controller"
    grep -qw memory "$unit_path/cgroup.subtree_control" || fail "builder unit did not enable memory controller"
    [ "$(cat "$ctl_path/memory.high")" = "max" ] || fail "builder ctl subgroup inherited a low memory.high"
    # Cold start deliberately leaves VMM memory.high=max through launch ACK and
    # settled.  The first trusted guest report starts the initial shrink, and
    # high becomes finite only after balloon current converges.  The B2 phase
    # stays alive for 20 seconds specifically so active enforcement can be
    # inspected; wait inside that window instead of racing the report barrier.
    for _ in $(seq 1 60); do
        memory_high=$(cat "$vmm_path/memory.high" 2>/dev/null || true)
        [ "$memory_high" != "max" ] && [ -n "$memory_high" ] && break
        sleep 0.25
    done
    if [ "$memory_high" = "max" ] || [ -z "$memory_high" ]; then
        fail "phase VMM did not leave deferred memory.high after a trusted report (last=${memory_high:-missing})"
    fi
    python3 - "$unit_path" "$slice_path" "$vmm_path" "$BUILDER_CPU_MILLI" "$BUILDER_EXECUTION_CPU_MILLI" <<'PY' || fail "effective per-Build/aggregate/phase cgroup limits"
import pathlib, sys

def assert_cpu(path, milli):
    quota, period = (path / "cpu.max").read_text().split()
    assert quota != "max", (path, quota, period)
    assert int(quota) * 1000 == int(period) * milli, (path, quota, period, milli)

unit, pool, vmm = map(pathlib.Path, sys.argv[1:4])
build_cpu, execution_cpu = map(int, sys.argv[4:])
assert (unit / "memory.max").read_text().strip() == str(6 << 30), unit
assert (pool / "memory.max").read_text().strip() == str(12 << 30), pool
assert_cpu(unit, build_cpu)
assert_cpu(pool, execution_cpu)
assert_cpu(vmm, 2000)
PY
    echo "==> PASS: active phase $phase/$sid is the only nodectl reservation; Build limits and ctl/vmm isolation verified (memory.high=$memory_high)"
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
wait_error() { # tid bid label
    local tid="$1" bid="$2" label="$3" status="" code
    for _ in $(seq 1 240); do
        code=$(req GET "/templates/$tid/builds/$bid/status" "$AK")
        [ "$code" = "200" ] || fail "$label status = $code (want 200)"
        status=$(json_field "$WORK/resp.body" status)
        case "$status" in
            error) return 0 ;;
            ready) fail "$label unexpectedly reached ready" ;;
        esac
        sleep 0.5
    done
    diag "$bid"; fail "$label did not reach error (last status=$status)"
}

# Exercise the installed templates without going through orchestrator cleanup:
# an unknown run id reaches the real launcher, fails WaitAssignment, and exits
# non-zero. CollectMode must unload each failed instance while journald retains
# its diagnostics. Repetition proves the failed-unit set does not accumulate.
RUNNER_FAILED_BASE=$(failed_unit_count 'sandbox-runner@*.service')
BUILDER_FAILED_BASE=$(failed_unit_count 'sandbox-builder@*.service')
for kind in runner builder; do
    case "$kind" in
        runner) failed_before="$RUNNER_FAILED_BASE" ;;
        builder) failed_before="$BUILDER_FAILED_BASE" ;;
    esac
    for attempt in 1 2 3; do
        run_id="issue178-$kind-$RANDOM-$attempt"
        unit="sandbox-$kind@$run_id.service"
        systemctl start "$unit" >"$WORK/$run_id.start" 2>&1 || true
        wait_unit_journal_contains "$unit" "wait assignment" "$WORK/$run_id.journal" \
            || { cat "$WORK/$run_id.start" "$WORK/$run_id.journal" >&2; fail "$unit journal was not retained"; }
        wait_unit_collected "$unit" || fail "$unit remained loaded and failed"
    done
    failed_after=$(failed_unit_count "sandbox-$kind@*.service")
    [ "$failed_after" = "$failed_before" ] \
        || fail "failed $kind units grew from $failed_before to $failed_after"
done
echo "==> PASS: repeated runner/builder pre-assignment failures were collected; journals remained queryable"

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
B1_BEFORE_RETRY=$(build_trigger_signature "$B1_BID")
code=$(req POST "/v2/templates/$B1_TID/builds/$B1_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"force\":true}")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "B1 second trigger = $code (want 409)"; }
assert_trigger_conflict ready || fail "B1 second trigger did not report exact ready state"
[ "$(build_trigger_signature "$B1_BID")" = "$B1_BEFORE_RETRY" ] \
    || fail "B1 second trigger changed the terminal build row"
code=$(req GET /templates "$AK")
[ "$code" = "200" ] || fail "template list after B1 conflict = $code (want 200)"
python3 - "$WORK/resp.body" "$B1_BID" "$B1_PERSIST" <<'PY'
import json, sys
items = json.load(open(sys.argv[1]))
assert any(item.get("buildID") == sys.argv[2] and item.get("templateID") == sys.argv[3] for item in items), items
PY
echo "==> PASS: B1 ready → $B1_PERSIST"

# ---- BF: deterministic business failure → API error + collected unit -------
echo "==> BF: deterministic RUN failure keeps API/journal evidence without a failed unit"
register issue178-failed-build
BF_TID="$TID"; BF_BID="$BID"
BF_BODY=$(python3 - "$B1_PERSIST" <<'PY'
import json, sys
print(json.dumps({
    "fromTemplate": sys.argv[1],
    "steps": [{"type": "RUN", "args": ["echo ISSUE178_BUILDER_FAILURE >&2; exit 78"]}],
}))
PY
)
code=$(req POST "/v2/templates/$BF_TID/builds/$BF_BID" "$AK" "$BF_BODY")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "BF trigger = $code (want 202)"; }
wait_error "$BF_TID" "$BF_BID" BF
BF_RUN_ID=$(build_run_id "$BF_BID")
[ -n "$BF_RUN_ID" ] || fail "BF error record lost its run id"
BF_UNIT="sandbox-builder@$BF_RUN_ID.service"
wait_unit_journal_contains "$BF_UNIT" ISSUE178_BUILDER_FAILURE "$WORK/bf.journal" \
    || { diag "$BF_BID"; fail "BF journal was not retained after the business failure"; }
wait_unit_collected "$BF_UNIT" || fail "$BF_UNIT remained loaded and failed"
code=$(req GET "/templates/$BF_TID/builds/$BF_BID/status" "$AK")
[ "$code" = "200" ] && [ "$(json_field "$WORK/resp.body" status)" = "error" ] \
    || fail "BF API did not retain terminal error status"
BF_BEFORE_RETRY=$(build_trigger_signature "$BF_BID")
code=$(req POST "/v2/templates/$BF_TID/builds/$BF_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"force\":true}")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "BF second trigger = $code (want 409)"; }
assert_trigger_conflict error || fail "BF second trigger did not report exact error state"
[ "$(build_trigger_signature "$BF_BID")" = "$BF_BEFORE_RETRY" ] \
    || fail "BF second trigger changed the terminal build row"
[ "$(failed_unit_count 'sandbox-builder@*.service')" = "$BUILDER_FAILED_BASE" ] \
    || fail "BF increased the failed builder unit count"
echo "==> PASS: ready/error build ids reject force retries without mutation; ready remains listed"
echo "==> PASS: BF status=error, journal retained, sandbox-builder instance collected"

# ---- B2: fromTemplate(img) + steps + startCmd/readyCmd → e2b-snp ------------
echo "==> B2: fromTemplate=$B1_PERSIST + steps + startCmd/readyCmd"
register e2e-tpl
B2_TID="$TID"; B2_BID="$BID"
[ "$B2_BID" != "$B1_BID" ] || fail "new registration reused B1 build id"
B2_BODY=$(cat <<EOF
{"fromTemplate":"$B1_PERSIST",
 "steps":[
   {"type":"RUN","args":["sleep 20; useradd -m -d /home/user user || adduser -D user"]},
   {"type":"RUN","args":["grep -Eq '^0::/user(/|$)' /proc/self/cgroup && test ! -s /sys/fs/cgroup/cgroup.procs && for group in user ptys socats; do test -d /sys/fs/cgroup/\$group && test -e /sys/fs/cgroup/\$group/cpu.weight && test -e /sys/fs/cgroup/\$group/memory.max && test -e /sys/fs/cgroup/\$group/io.weight || exit 1; done"]},
   {"type":"RUN","args":["echo b2 > /etc/b2-marker"]},
   {"type":"ENV","args":["BUILT","yes"]},
   {"type":"WORKDIR","args":["/home/user"]}],
 "startCmd":"touch /home/user/started; exec sleep 86400",
 "readyCmd":"test -f /home/user/started"}
EOF
)
code=$(req POST "/v2/templates/$B2_TID/builds/$B2_BID" "$AK" "$B2_BODY")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "B2 trigger = $code (want 202)"; }
assert_active_build_accounting "$B2_TID" "$B2_BID"
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
python3 - "$WORK/b2.cfg.json" <<'PY' || fail "B2 snapshot.cfg lost fixed capacity, leaked node-local resource policy, or launch.cgroup_control=true"
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    cfg = json.load(source)
assert cfg["Launch"]["CgroupControl"] is True, cfg["Launch"]
resources = cfg["Resources"]
# Snapshot portability fixes capacity only. The restore node re-resolves
# allocatable, startup, overhead, and controller from its own resource policy.
assert resources == {"Capacity": {"CPU": 2, "Memory": "3GiB"}}, resources
PY
B2_IMG_HEX=$(grep -o '"BaseRef": *"manifest://[0-9a-f]*"' "$WORK/b2.cfg.json" | grep -o '[0-9a-f]\{64\}' | head -1)
[ -n "$B2_IMG_HEX" ] || fail "B2 snapshot.cfg base is not manifest:// (upload-snapshot did not rewrite?): $(cat "$WORK/b2.cfg.json")"
MANIFEST_KEY="$MK" "$BIN/flatten-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "manifest://$B2_IMG_HEX" >"$WORK/b2.img.json" 2>"$WORK/b2.img.err" \
    || { cat "$WORK/b2.img.err"; fail "flatten-ctl info manifest://$B2_IMG_HEX"; }
grep -q '"BUILT=yes"' "$WORK/b2.img.json" || fail "B2 image config missing merged ENV BUILT=yes: $(cat "$WORK/b2.img.json")"
grep -q '"WorkingDir": *"/home/user"' "$WORK/b2.img.json" || fail "B2 image config missing merged WORKDIR"
echo "==> PASS: B2 artifacts — cgroup_control + snapshot metadata + manifest:// base + merged ENV/WORKDIR"
wait_phase_audit a "$B1_BID" || fail "B1 phase A lacked ordinary nodectl Admit/Release"
wait_phase_admit b "$B2_BID" || fail "B2 phase B lacked ordinary nodectl Admit"
wait_phase_audit c "$B2_BID" || fail "B2 phase C lacked ordinary nodectl Admit/Release"
wait_resource_reservations_empty || fail "phase sandbox reservation remained after B2 completion"
B2_PHASE_B_SID=$(phase_sandbox_id b "$B2_BID")
if ! grep -Eq "release .*sid=$B2_PHASE_B_SID( |$)" "$WORK/resource-audit.log" 2>/dev/null; then
    echo "==> NOTE: long phase B drained without an explicit normal Release; sandboxer#115 tracks that lifecycle race"
fi
echo "==> PASS: A/B/C used ordinary nodectl Admit; phases serialized and left no reservation (A/C normal Release verified)"

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
B3_RUN_ID=$(build_run_id "$B3_BID")
[ -n "$B3_RUN_ID" ] || fail "B3 ready record lost its run id"
journalctl --no-pager -o cat -u "sandbox-builder@$B3_RUN_ID.service" >"$WORK/b3-builder.journal" 2>&1 || true
B3_ROOT_READS=$(grep -F -c 'build task snapshot prepared' "$WORK/b3-builder.journal" || true)
[ "$B3_ROOT_READS" = "1" ] \
    || { cat "$WORK/b3-builder.journal"; fail "B3 task root snapshot.cfg read count=$B3_ROOT_READS (want 1)"; }
grep -F -q 'task_snapshot_ref_count' "$WORK/b3-builder.journal" \
    || { cat "$WORK/b3-builder.journal"; fail "B3 task snapshot ref-count instrumentation missing"; }
# ready is only reachable if: the RUN saw B2's marker (base extraction worked)
# AND the inherited startCmd/readyCmd ran on the new template VM.
echo "==> PASS: B3 ready → $B3_PERSIST (one task-local root cfg read; base-image extraction + start/ready inheritance)"

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
wait_resource_capacity "$SID" "$((3 << 30))" \
    || fail "snapshot Create did not preserve the phase-C snapshot capacity"
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
wait_resource_capacity "$E2B_COLD_SID" "$((2 << 30))" \
    || fail "IMG Create inherited the build node's phase patch instead of target node policy"
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
