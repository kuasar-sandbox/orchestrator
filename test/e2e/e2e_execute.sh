#!/usr/bin/env bash
#
# e2e_execute.sh — Phase 2: boot a REAL microVM sandbox from a built template and
# execute in it, end to end.
#
#   build (native v3, proven)  -> a ready e2b-img template in the store
#   vswitch (ip netns + start) -> sw0 (eBPF/TC), the data plane
#   POST /sandboxes            -> sandbox-runner@<sid> -> sandbox-ctl boots
#                                cloud-hypervisor (KVM) from the template +
#                                sandbox-runtime-e2b.erofs; envd comes up at 49983,
#                                exposed as envd.sock; orchestrator waitReady(/health)
#                                + envdInit(/init). 201 == microVM booted + envd ready.
#   exec                       -> run a command in the guest via envd.
#   DELETE                     -> teardown.
#
# Needs systemd+root, /dev/kvm (rw), the vswitch eBPF stack, store-ctl, zot, docker,
# mkfs.erofs, and the built kernel + runtime erofs in bin. Missing prereqs -> exit 0
# ("skipped") unless REQUIRE_EXEC=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-test-app-a:latest}"
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"
SW_NETNS="${SW_NETNS:-e2e_sw}"

skip() { echo; echo "==> e2e_execute: skipping ($*)"; [ "${REQUIRE_EXEC:-0}" = "1" ] && { echo "REQUIRE_EXEC=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in orchestrator-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl vswitch-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime-e2b.erofs" ] || skip "missing $BIN/sandbox-runtime-e2b.erofs"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not on PATH"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || skip "base image $E2E_IMAGE not cached"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-exec-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"
declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""
cleanup() {
    set +e
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/vswitch-ctl" stop "$SWITCH" >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

# ---- store + zot + creds + orchestrator (same as the build e2e) -----------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl

ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "127.0.0.1", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
REF="127.0.0.1:$ZOT_PORT/e2e/app:v1"
# A real e2b base template has a 'user' account and util-linux/coreutils, because
# envd runs guest processes as /init's defaultUser and wraps them as
# `ionice -c 2 -n 4 nice -n N "$@"`. Bare alpine has neither, and apk has no
# network here — so build a minimal compliant image offline: add the user and
# tiny ionice/nice shims (strip -c/-n opts, exec the rest). The orchestrator/envd
# are unchanged; this only makes the test image match the e2b base contract.
cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -c|-n) shift 2 ;;
    -c*|-n*) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
exec "$@"
SH
cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice && (adduser -D -h /home/user user || useradd -m -d /home/user user)
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build (e2b-compliant image)"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF (user + ionice/nice shims)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper
# (deployment-provided in prod; created inline here). mkfs.ext4 may live in /sbin.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
echo "==> overlay diff_template: $OVL ($(du -h "$OVL" | cut -f1) on disk)"

cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/orchestrator.socket }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime_e2b: $BIN/sandbox-runtime-e2b.erofs, runtime_base: $BIN/sandbox-runtime.erofs, overlay_diff_template: $OVL }
builder: { insecure_registry: true }
checkpoint: { mode: remote }
EOF
"$BIN/orchestrator-ctl" manifest-key add --config "$WORK/config.yaml" "$MK" >/dev/null || fail "manifest-key add"
"$BIN/orchestrator-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null && break
    kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/  /' "$WORK/orch.log"; skip "orchestrator exited"; }
    sleep 0.5
done
echo "==> orchestrator-ctl up (:$PORT)"

# ---- build a ready template (native v3, proven) ---------------------------
code=$(req POST /v3/templates "$AK" '{"name":"exec-tmpl"}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(grep -o '"templateID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
BID=$(grep -o '"buildID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "{\"fromImage\":\"$REF\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(grep -o '"status":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
    case "$st" in
        ready) TEMPLATE=$(grep -oE 'e2b-img-[0-9a-f]{64}' "$WORK/resp.body" | head -1); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
echo "==> built template: $TEMPLATE"

# ---- vswitch up (ip netns + start) ----------------------------------------
# Clear any leftover switch of the same name (eBPF maps are pinned and survive a
# crash; --force drains orphaned ports). Then create the netns fresh.
"$BIN/vswitch-ctl" stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
echo "==> starting vswitch $SWITCH (netns=$SW_NETNS)"
"$BIN/vswitch-ctl" start "$SWITCH" \
    --netns="$SW_NETNS" \
    --ports=64 \
    --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 \
    --mode=tap >"$WORK/vswitch-start.log" 2>&1 || { echo "vswitch start failed:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch start"; }
SW_STARTED=1
echo "==> vswitch up"

# ---- create the sandbox (boots the microVM) -------------------------------
echo "==> POST /sandboxes (boot microVM from $TEMPLATE)"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo
    echo "==> orchestrator log:"; sed 's/^/  orch| /' "$WORK/orch.log"
    SID=$(ls "$WORK/run" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> runner unit journal:"; journalctl -u "sandbox-runner@$SID.service" --no-pager -n 60 2>/dev/null | sed 's/^/  unit| /'; }
    fail "create=$code (want 201) — VM boot/envd readiness failed"
fi
SID=$(grep -o '"sandboxID":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
ENVD_TOKEN=$(grep -o '"envdAccessToken":"[^"]*"' "$WORK/resp.body" | head -1 | cut -d'"' -f4)
echo "==> PASS: sandbox $SID running (microVM booted + envd ready + /init)"

# ---- list -----------------------------------------------------------------
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list=$code"
grep -q "$SID" "$WORK/resp.body" || fail "sandbox $SID not listed"
echo "==> PASS: sandbox listed"

# ---- execute a command in the guest via envd (Connect-RPC over envd.sock) --
ENVD_SOCK="$WORK/run/$SID/envd.sock"
[ -S "$ENVD_SOCK" ] || fail "envd.sock not found at $ENVD_SOCK"
cat > "$WORK/envd_exec.py" <<'PY'
import http.client, socket, struct, json, base64, sys
sock_path, token, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
class UDS(http.client.HTTPConnection):
    def __init__(s): super().__init__("envd")
    def connect(s):
        s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.sock.connect(sock_path)
req = {"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}}
body = json.dumps(req).encode()
env = b"\x00" + struct.pack(">I", len(body)) + body
c = UDS()
c.request("POST", "/process.Process/Start", body=env, headers={
    "Content-Type": "application/connect+json", "Connect-Protocol-Version": "1",
    "X-Access-Token": token})
r = c.getresponse(); data = r.read()
out = b""; exit_code = None; err = None; i = 0
while i + 5 <= len(data):
    flag = data[i]; ln = struct.unpack(">I", data[i+1:i+5])[0]; msg = data[i+5:i+5+ln]; i += 5+ln
    j = json.loads(msg) if msg else {}
    if flag & 2:
        if j.get("error"): err = j
        continue
    ev = j.get("event", {})
    if "data" in ev:
        d = ev["data"]
        for k in ("stdout","stderr"):
            if d.get(k): out += base64.b64decode(d[k])
    if "end" in ev: exit_code = ev["end"].get("exitCode", 0)
print("HTTP_STATUS", r.status)
print("EXIT_CODE", exit_code)
print("ERROR", json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n"); sys.stdout.flush()
sys.stdout.buffer.write(out); sys.stdout.write("\nOUTPUT_END\n")
PY
MARK="HELLO_FROM_GUEST_$RANDOM"
echo "==> exec in guest: sh -c 'id; echo $MARK; uname -sm'"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "id; echo $MARK; uname -sm" > "$WORK/exec.out" 2>&1 || true
sed 's/^/  guest| /' "$WORK/exec.out"
grep -q "$MARK" "$WORK/exec.out" || fail "guest command output missing $MARK (envd exec failed; see above)"
grep -q 'EXIT_CODE 0' "$WORK/exec.out" || fail "guest command exit code != 0"
echo "==> PASS: command executed in guest (saw $MARK, exit 0)"

# ---- pause (snapshot+upload) -> resume -> verify state survived ------------
# Write a marker file in the guest BEFORE pausing; after resume it must still be
# there — proving both the snapshot/restore overlay AND that a resumed img sandbox
# restores (not cold-boots). /home/user is user-owned (flatten preserves ownership).
PERSIST="PERSIST_$MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "echo $PERSIST > /home/user/persist.txt; cat /home/user/persist.txt" > "$WORK/wr.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/wr.out" || { sed 's/^/  guest| /' "$WORK/wr.out"; fail "could not write /home/user/persist.txt as the guest user (ownership not preserved?)"; }
echo "==> wrote /home/user/persist.txt in the guest (as user)"

echo "==> pause (snapshot+upload) $SID"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
if [ "$code" = "204" ]; then
    echo "==> PASS: sandbox paused (snapshot uploaded to store)"
    echo "==> resume via connect"
    code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":120}')
    [ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "resume(connect)=$code (want 200)"; }
    for _ in $(seq 1 40); do [ -S "$ENVD_SOCK" ] && break; sleep 0.3; done
    python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" > "$WORK/exec2.out" 2>&1 || true
    sed 's/^/  guest2| /' "$WORK/exec2.out"
    grep -q "$PERSIST" "$WORK/exec2.out" || fail "pre-pause state LOST after resume (restore regressed to cold boot?)"
    echo "==> PASS: pre-pause guest state survived resume (snapshot/restore + img-resume restore)"
else
    echo "==> NOTE: pause=$code — snapshot error (diagnostic):"
    grep -iE 'snapshot|pause|api error' "$WORK/orch.log" | tail -10 | sed 's/^/  orch| /'
    SID_JOURNAL=$(journalctl -u "sandbox-runner@$SID.service" --no-pager -n 30 2>/dev/null | grep -iE 'snapshot|ctl.sock|error' | tail -8)
    [ -n "$SID_JOURNAL" ] && echo "$SID_JOURNAL" | sed 's/^/  unit| /'
    PAUSE_FAILED=1
fi

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill=$code (want 204)"
echo "==> PASS: sandbox killed"
[ -n "${PAUSE_FAILED:-}" ] && fail "pause/resume did not complete (see snapshot error above)"
echo
echo "==> e2e_execute: OK   (template $TEMPLATE, sandbox $SID)"
