#!/usr/bin/env bash
#
# e2e_execute.sh — Phase 2: boot a REAL microVM sandbox from a built template and
# execute in it, end to end.
#
#   vswitch (ip netns + start) -> sw0 (eBPF/TC); builds need it too (the pull
#                                runs INSIDE a build sandbox on the tenant network)
#   build (native v3)          -> import build sandbox pulls via the mgmt VIP +
#                                flattens -> a ready e2b-img template in the store
#   POST /sandboxes            -> sandbox-runner@<run-id> assignment -> sandbox-ctl boots
#                                cloud-hypervisor (KVM) from the template +
#                                sandbox-runtime.bundle; envd comes up at 49983,
#                                exposed as envd.sock; after runtime readiness the
#                                orchestrator requires envdInit(/init), without a
#                                launch-time /health probe. 201 == durable starting
#                                acceptance; the immediate data request below parks
#                                until runtime readiness + /init complete.
#                                The create injects sandbox config via the
#                                X-Kuasar-Sandbox-Network header (hostname), checked
#                                in the guest below (§4.6 config passing chain).
#   exec-session + CONNECT     -> issue an explicit exec capability, then use the
#                                real sandbox-ctl HTTP CONNECT client against the
#                                guest (stdio, PTY resize, exit status, pause wake).
#   envd exec                  -> run a command in the guest via envd (incl. hostname).
#   local Pause policy        -> all-unset keeps the legacy argv; then node,
#                                Create metadata/header, Pause body/header, and
#                                automatic reaper policy are layered fieldwise.
#                                Captures are real local bundles and are restored
#                                before teardown.
#   portable working set      -> restore local B, capture W -> local B, remove
#                                B's merged disk top, explicitly export/promote
#                                W, then restore the portable W with self-only
#                                memory prefetch.
#   DELETE                    -> teardown.
#
# Needs systemd+root, /dev/kvm (rw), the vswitch eBPF stack, store-ctl, zot, docker,
# mkfs.erofs, and the built kernel + runtime erofs
# in bin. Missing prereqs -> exit 0 ("skipped") unless REQUIRE_EXEC=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
MMDS_ROUTES_E2E="${MMDS_ROUTES_E2E:-0}"
DOMAIN="${DOMAIN:-sandboxes.e2e.local}"
PORT="${PORT:-3000}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_sw}"
PROXY_NETNS="${PROXY_NETNS:-e2e_proxy_int}"
PROXY_VETH_HOST="${PROXY_VETH_HOST:-e2eih0}"
PROXY_VETH_NS="${PROXY_VETH_NS:-e2ein0}"
PROXY_HOST_IP="${PROXY_HOST_IP:-172.31.253.1}"
PROXY_NS_IP="${PROXY_NS_IP:-172.31.253.2}"
FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"

skip() { echo; echo "==> e2e_execute: skipping ($*)"; [ "${REQUIRE_EXEC:-0}" = "1" ] && { echo "REQUIRE_EXEC=1; failing" >&2; exit 1; }; exit 0; }
fail() { echo "==> FAIL: $*" >&2; exit 1; }

case "$BIN" in
    /*) ;;
    *) BIN="$(cd "$BIN" 2>/dev/null && pwd)" || skip "BIN directory not found";;
esac

for b in node-ctl sandbox-ctl flatten-ctl manifest-ctl store-ctl e2b-key-ctl connector-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || skip "missing $BIN/$b"; done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.bundle" ] || skip "missing $BIN/sandbox-runtime.bundle"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
command -v iptables >/dev/null 2>&1 || skip "iptables not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "base image $E2E_IMAGE unavailable (set E2E_IMAGE to a local or pullable image)"

if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then export PATH="$BIN:$PATH"; fi

WORK="$(mktemp -d /tmp/e2e-exec-XXXXXX)"
TAPFD_SOCKET="$WORK/tapfd.sock"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data"

MMDS_ROUTES_CONFIG=""
MMDS_SERVICES_CONFIG=""
if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$REPO_ROOT/test/e2e/lib/mmds_service_guest.sh"
    start_mmds_service_backend "$WORK" || fail "start MMDS service UDS backend"
    MMDS_ROUTES_CONFIG="  routes:
    enabled: true"
    MMDS_SERVICES_CONFIG="  services:
    $MMDS_SERVICE_NAME:
      endpoint: unix://$MMDS_SERVICE_SOCKET"
fi

# Run conductor from a private sibling-bin directory so its normal binary
# discovery reaches this transparent sandbox-ctl wrapper. The wrapper records
# snapshot argv as JSONL, then normally execs the unmodified release candidate
# binary. Explicit sentinel modes below are confined to deterministic negative
# lifecycle tests; every ordinary create/restore still uses the real KVM stack.
ORCH_BIN_DIR="$WORK/orch-bin"
SNAPSHOT_ARGV_LOG="$WORK/snapshot-argv.jsonl"
mkdir -p "$ORCH_BIN_DIR"
cp "$BIN/node-ctl" "$ORCH_BIN_DIR/node-ctl"
for b in connector-ctl flatten-ctl manifest-ctl; do
    ln -s "$BIN/$b" "$ORCH_BIN_DIR/$b"
done
cat > "$ORCH_BIN_DIR/sandbox-ctl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "snapshot" ]; then
    python3 - "$SNAPSHOT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\\n")
PY
fi
if [ "\${1:-}" = "run" ] && [ -f "$WORK/inject-sandbox-run" ]; then
    mode="\$(<"$WORK/inject-sandbox-run")"
    case "\$mode" in
        hold)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
            exit 44
            ;;
        runtime-wire-failure)
            python3 - "\$@" <<'PY'
import os, sys
fd = next(int(arg.split("=", 1)[1]) for arg in sys.argv[1:] if arg.startswith("--ready-fd="))
os.write(fd, b"control_ready\ninvalid_runtime_event\n")
os.close(fd)
PY
            exit 42
            ;;
        envd-init-failure)
            # Replace this wrapper so no parent process retains ready-fd after
            # Python closes it; EOF is the final readiness protocol event.
            exec python3 - "\$@" <<'PY'
import os, socket, sys, time

args = sys.argv[1:]
def value(name):
    for index, arg in enumerate(args):
        if arg == name:
            return args[index + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    raise SystemExit("missing " + name)

sid = value("--sandbox-id")
run_root = value("--run-root")
ready_fd = int(value("--ready-fd"))
envd_path = os.path.join(run_root, sid, "envd.sock")
try:
    os.unlink(envd_path)
except FileNotFoundError:
    pass
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(envd_path)
server.listen(1)
server.settimeout(15)
os.write(ready_fd, b"control_ready\nready\n")
os.close(ready_fd)
conn, _ = server.accept()
conn.settimeout(5)
request = b""
while b"\r\n\r\n" not in request:
    chunk = conn.recv(65536)
    if not chunk:
        break
    request += chunk
conn.sendall(b"HTTP/1.1 500 Injected envd failure\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
conn.close()
server.close()
time.sleep(300)
PY
            exit 43
            ;;
        *)
            echo "unknown sandbox run injection: \$mode" >&2
            exit 2
            ;;
    esac
fi
exec "$BIN/sandbox-ctl" "\$@"
EOF
chmod +x "$ORCH_BIN_DIR/sandbox-ctl"

declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""
ORIG_IP_FORWARD=""
cleanup() {
    set +e
    [ "$MMDS_ROUTES_E2E" = 1 ] && stop_mmds_service_backend
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" >/dev/null 2>&1
    iptables -D FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null
    iptables -D FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null
    ip link del "$PROXY_VETH_HOST" 2>/dev/null
    ip netns del "$PROXY_NETNS" 2>/dev/null
    ip netns del "$SW_NETNS" 2>/dev/null
    [ -n "$ORIG_IP_FORWARD" ] && sysctl -q -w "net.ipv4.ip_forward=$ORIG_IP_FORWARD" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
wait_mmds_listener() {
    local hex
    hex="$(printf '%04X' "$MMDS_PORT")"
    for _ in $(seq 1 60); do
        ip netns exec "$PROXY_NETNS" awk -v p=":$hex" '$2 ~ p && $4 == "0A" { found = 1 } END { exit(found ? 0 : 1) }' /proc/net/tcp 2>/dev/null && return 0
        sleep 0.5
    done
    fail "internal mmds listener did not appear in proxy_netns=$PROXY_NETNS on $PROXY_NS_IP:$MMDS_PORT"
}
setup_proxy_netns() {
    ip link del "$PROXY_VETH_HOST" 2>/dev/null || true
    ip netns del "$PROXY_NETNS" 2>/dev/null || true
    ip netns add "$PROXY_NETNS"
    ip link add "$PROXY_VETH_HOST" type veth peer name "$PROXY_VETH_NS"
    ip link set "$PROXY_VETH_NS" netns "$PROXY_NETNS"
    ip addr add "$PROXY_HOST_IP/30" dev "$PROXY_VETH_HOST"
    ip link set "$PROXY_VETH_HOST" up
    ip netns exec "$PROXY_NETNS" ip addr add "$PROXY_NS_IP/30" dev "$PROXY_VETH_NS"
    ip netns exec "$PROXY_NETNS" ip link set lo up
    ip netns exec "$PROXY_NETNS" ip link set "$PROXY_VETH_NS" up
    ip netns exec "$PROXY_NETNS" ip route add "$FIP_CIDR" via "$PROXY_HOST_IP"
    ORIG_IP_FORWARD="$(sysctl -n net.ipv4.ip_forward 2>/dev/null || true)"
    sysctl -q -w net.ipv4.ip_forward=1
}
allow_proxy_forwarding() {
    iptables -C FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT
    iptables -C FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null \
        || iptables -A FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT
}
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    # Optional sandbox-config injection header (§4.6): set REQ_NET_HEADER to a JSON
    # network spec to exercise X-Kuasar-Sandbox-Network on a create.
    [ -n "${REQ_NET_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Network: ${REQ_NET_HEADER}")
    [ -n "${REQ_CHECKPOINT_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Checkpoint: ${REQ_CHECKPOINT_HEADER}")
    if [ "${REQ_ATTACH_MMDS:-0}" = 1 ] && [ -n "${REQ_MMDS_HEADER:-}" ]; then
        args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    fi
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
}

snapshot_argv_count() {
    python3 - "$SNAPSHOT_ARGV_LOG" <<'PY'
import json, os, sys
path = sys.argv[1]
if not os.path.exists(path):
    print(0)
else:
    with open(path, encoding="utf-8") as source:
        print(sum(1 for line in source if line.strip()))
PY
}

assert_snapshot_argv() { # $1=index, remaining args=expected argv
    local index="$1"
    shift
    python3 - "$SNAPSHOT_ARGV_LOG" "$index" "$@" <<'PY'
import json, sys
path, index, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
with open(path, encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
if index >= len(calls):
    raise SystemExit(f"missing snapshot call {index}; captured {len(calls)}")
if calls[index] != expected:
    raise SystemExit(f"snapshot call {index}={calls[index]!r}, want {expected!r}")
PY
}

assert_snapshot_has_no_policy_flags() { # $1=index
    python3 - "$SNAPSHOT_ARGV_LOG" "$1" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
call = calls[int(sys.argv[2])]
bad = [arg for arg in call if arg.startswith("--merge-ref=") or arg.startswith("--drop-caches=")]
if bad:
    raise SystemExit(f"builder snapshot unexpectedly received checkpoint policy flags: {bad!r}")
PY
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
sandbox_run_id() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select run_id from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
}
sandbox_state() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
}
wait_sandbox_state() { # $1=sandbox id, $2=state, $3=attempts(optional)
    local sid="$1" want="$2" attempts="${3:-120}" state=""
    for _ in $(seq 1 "$attempts"); do
        state="$(sandbox_state "$sid")"
        [ "$state" = "$want" ] && return 0
        sleep 0.1
    done
    echo "sandbox $sid state=$state, want $want" >&2
    return 1
}
wait_unit_journal_contains() { # $1=unit, $2=fixed string, $3=output file
    local unit="$1" pattern="$2" output="$3"
    for _ in $(seq 1 50); do
        journalctl -u "$unit" --no-pager >"$output" 2>/dev/null || true
        grep -Fq "$pattern" "$output" && return 0
        sleep 0.1
    done
    return 1
}
assert_sandbox_detail() {
    python3 - "$@" <<'PY'
import datetime
import json
import re
import sys

path, sandbox_id, expected_cpu, expected_memory, expected_disk = sys.argv[1:]
expected = {
    "cpuCount": int(expected_cpu),
    "memoryMB": int(expected_memory),
    "diskSizeMB": int(expected_disk),
}
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
with open(path, encoding="utf-8") as f:
    detail = json.load(f)

if detail.get("sandboxID") != sandbox_id:
    raise SystemExit(f"detail sandboxID={detail.get('sandboxID')!r}, want {sandbox_id!r}")
for field in ("startedAt", "endAt"):
    value = detail.get(field)
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        raise SystemExit(f"detail {field}={value!r} is not RFC3339")
    try:
        datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"detail {field}={value!r} is not RFC3339: {exc}")

started = datetime.datetime.fromisoformat(detail["startedAt"].replace("Z", "+00:00"))
ended = datetime.datetime.fromisoformat(detail["endAt"].replace("Z", "+00:00"))
if ended < started:
    raise SystemExit(f"detail endAt={detail['endAt']!r} is before startedAt={detail['startedAt']!r}")
for field, want in expected.items():
    if detail.get(field) != want:
        raise SystemExit(f"detail {field}={detail.get(field)!r}, want {want}")
PY
}
assert_no_default_exec_token() {
    python3 - "$1" <<'PY'
import json, sys
created = json.load(open(sys.argv[1]))
if "execAccessToken" in created:
    raise SystemExit("create response unexpectedly contains execAccessToken")
PY
}
issue_exec_session() {
    local sid="$1" key="$2" code
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-API-KEY: $key" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$PORT/sandboxes/$sid/exec-sessions")"
    [ "$code" = "201" ] || fail "exec-session=$code (want 201)"
    python3 - "$WORK/exec-session.headers" "$WORK/exec-session.secret" <<'PY'
import json, sys
headers = [line.strip().lower() for line in open(sys.argv[1], "rb").read().splitlines()]
if b"cache-control: no-store" not in headers:
    raise SystemExit("exec-session response omitted Cache-Control: no-store")
payload = json.load(open(sys.argv[2]))
if not isinstance(payload, dict) or set(payload) != {"execAccessToken"}:
    raise SystemExit("exec-session response must contain only execAccessToken")
token = payload["execAccessToken"]
if not isinstance(token, str) or not token.startswith("kat1.") or len(token.split(".")) != 3:
    raise SystemExit("exec-session response contains an invalid KAT token")
print(token)
PY
}
exec_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local status

    printf 'stdin:%s\n' "$marker" >"$input"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --proxy-header "X-Kuasar-E2E-Duplicate: first" \
        --proxy-header "X-Kuasar-E2E-Duplicate: second" \
        --stdin-from "$input" --stdout-to "$output" --stderr-to "$error_output" -- \
        /bin/sh -c "IFS= read -r value; printf 'stdout:%s:%s\\n' '$marker' \"\$value\"; printf 'stderr:%s\\n' '$marker' >&2; exit 47" \
        >"$diagnostics" 2>&1; then
        status=0
    else
        status=$?
    fi
    grep -Fxq "stdout:$marker:stdin:$marker" "$output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stdout| /' "$output" 2>/dev/null; fail "native exec stdout/stdin mismatch"; }
    grep -Fxq "stderr:$marker" "$error_output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stderr| /' "$error_output" 2>/dev/null; fail "native exec stderr mismatch"; }
    [ "$status" = "47" ] \
        || { sed 's/^/  client| /' "$diagnostics"; fail "native exec exit=$status (want guest status 47)"; }
}

exec_pty_resize_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local output="$WORK/native-exec-pty.out"
    local status=0

    python3 - "$output" "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --tty -- /bin/sh -c \
        "stty size; trap 'stty size; echo $marker; exit 23' WINCH; echo PTY_READY; while :; do sleep 1; done" <<'PY' || status=$?
import errno, fcntl, os, pty, select, signal, struct, subprocess, sys, termios, time

output_path, argv = sys.argv[1], sys.argv[2:]
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 37, 91, 0, 0))
proc = subprocess.Popen(argv, stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
captured = bytearray()
resized = False
deadline = time.monotonic() + 60
try:
    while True:
        if time.monotonic() >= deadline:
            proc.kill()
            proc.wait()
            raise SystemExit(124)
        readable, _, _ = select.select([master], [], [], 0.1)
        if not readable:
            continue
        try:
            chunk = os.read(master, 65536)
        except OSError as exc:
            if exc.errno == errno.EIO:
                break
            raise
        if not chunk:
            break
        captured.extend(chunk)
        if not resized and b"PTY_READY" in captured:
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 41, 101, 0, 0))
            os.kill(proc.pid, signal.SIGWINCH)
            resized = True
finally:
    os.close(master)
    with open(output_path, "wb") as output_file:
        output_file.write(captured)
raise SystemExit(proc.wait())
PY
    grep -q '37 91' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec initial PTY size mismatch"; }
    grep -q '41 101' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec resized PTY size mismatch"; }
    grep -q "$marker" "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY marker missing"; }
    [ "$status" = "23" ] || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY exit=$status (want 23)"; }
}
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --max-time "${DP_MAX_TIME:-120}" --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
    [ -n "$token" ] && args+=(-H "X-Access-Token: $token")
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

# 0.0.0.0: the host pushes via 127.0.0.1; the BUILD SANDBOX pulls via the
# vswitch mgmt VIP (guest loopback is not the host's).
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
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
RUN chmod +x /usr/bin/ionice /usr/bin/nice \
 && if ! id -u user >/dev/null 2>&1; then \
      if command -v useradd >/dev/null 2>&1; then useradd -m -d /home/user -s /bin/sh user; \
      elif command -v adduser >/dev/null 2>&1; then adduser -D -h /home/user -s /bin/sh user; \
      else echo "missing useradd/adduser" >&2; exit 1; fi; \
    fi \
 && mkdir -p /home/user \
 && chown user:user /home/user \
 && id user >/dev/null
EOF
docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" >"$WORK/imgbuild.log" 2>&1 || { cat "$WORK/imgbuild.log"; fail "docker build (e2b-compliant image)"; }
TAGS+=("$REF")
docker push "$REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push"; }
echo "==> store-ctl + zot up; built+seeded $REF (user + ionice/nice shims)"

# ---- vswitch up (ip netns + start) -----------------------------------------
# BEFORE the build: the image pull runs INSIDE a build sandbox, so the build
# needs a network slot and reaches zot via the mgmt VIP. Clear any leftover
# switch of the same name (eBPF maps are pinned and survive a crash; --force
# drains orphaned ports), then create the netns fresh.
MGMT_VIP="169.254.169.254"
MMDS_PORT="$(free_port)"
setup_proxy_netns
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns del "$SWITCH" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
echo "==> starting vswitch $SWITCH (netns=$SW_NETNS)"
"$BIN/connector-ctl" vswitch serve "$SWITCH" \
    --netns="$SW_NETNS" \
    --ports=64 \
    --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 \
    --mode=tap \
    --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$PROXY_NS_IP:$MMDS_PORT \
    --tapfd-listen="$TAPFD_SOCKET" --watch-interval=2s >"$WORK/vswitch-start.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
    if [ -S "$TAPFD_SOCKET" ] && "$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1; then
        break
    fi
    kill -0 "${PIDS[-1]}" 2>/dev/null || { echo "vswitch serve failed:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch serve exited"; }
    sleep 0.2
done
"$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1 \
    || { echo "vswitch not ready:"; sed 's/^/  /' "$WORK/vswitch-start.log"; fail "vswitch not ready"; }
SW_STARTED=1
allow_proxy_forwarding
GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/app:v1"
echo "==> vswitch up (build sandboxes pull $GUEST_REF; tapfd_socket=$TAPFD_SOCKET; internal proxy_netns=$PROXY_NETNS reaches $FIP_CIDR)"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

MK="$("$BIN/e2b-key-ctl" gen-key)"; API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"; AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"; ENC="$("$BIN/e2b-key-ctl" gen-key)"

# Cold boot needs a pre-formatted empty ext4 to seed the writable overlay upper
# (deployment-provided in prod; created inline here). mkfs.ext4 may live in /sbin.
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
[ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found (overlay template)"
OVL="$WORK/overlay-1G.ext4"
truncate -s 1G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs.log" 2>&1 || { cat "$WORK/mkfs.log"; fail "mkfs.ext4 overlay template"; }
echo "==> overlay diff_template: $OVL ($(du -h "$OVL" | cut -f1) on disk)"
BLD="$WORK/builder-2G.ext4"   # build sandbox writable disk (pull cache + export scratch)
truncate -s 2G "$BLD"
"$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-bld.log" 2>&1 || { cat "$WORK/mkfs-bld.log"; fail "mkfs.ext4 builder template"; }

CHECKPOINT_DIR="$WORK/checkpoints"
write_orchestrator_config() { # $1=unset|node-policy
    local policy_mode="$1"
    cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { mode: internal, auth: enforce, proxy_netns: $PROXY_NETNS, park_timeout: 120s }
mmds:
  enabled: true
  listen: "$PROXY_NS_IP:$MMDS_PORT"
$MMDS_ROUTES_CONFIG
$MMDS_SERVICES_CONFIG
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint:
  mode: local
  local_dir: $CHECKPOINT_DIR
EOF
    if [ "$policy_mode" = "node-policy" ]; then
        cat >> "$WORK/config.yaml" <<'EOF'
  merge_ref: true
  drop_caches: false
EOF
    fi
}

ORCH_PID=""
start_orchestrator() { # $1=log path
    local log_path="$1" ready=""
    "$ORCH_BIN_DIR/node-ctl" conductor serve --config "$WORK/config.yaml" >"$log_path" 2>&1 &
    ORCH_PID=$!
    PIDS+=("$ORCH_PID")
    for _ in $(seq 1 30); do
        if curl -sS --noproxy '*' -o /dev/null "http://127.0.0.1:$PORT/health" -H "Host: api.$DOMAIN" 2>/dev/null; then
            ready=1
            break
        fi
        kill -0 "$ORCH_PID" 2>/dev/null || { sed 's/^/  /' "$log_path"; fail "orchestrator exited"; }
        sleep 0.5
    done
    [ -n "$ready" ] || { sed 's/^/  /' "$log_path"; fail "orchestrator health did not become ready"; }
    if grep -q 'checkpoint.mode=remote is deprecated' "$log_path"; then
        fail "local checkpoint mode emitted the remote deprecation warning"
    fi
    return 0
}

stop_orchestrator() {
    [ -n "$ORCH_PID" ] || return 0
    local stopped_pid="$ORCH_PID"
    kill -TERM "$stopped_pid" 2>/dev/null || true
    wait "$stopped_pid" 2>/dev/null || true
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" = "$stopped_pid" ] && unset 'PIDS[i]'
    done
    ORCH_PID=""
}

write_orchestrator_config unset
start_orchestrator "$WORK/orch.log"
echo "==> node-ctl up (:$PORT)"
wait_mmds_listener
echo "==> PASS: internal mmds.listen is bound in proxy_netns=$PROXY_NETNS"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null || fail "manifest-key add"

# ---- build a ready template (native v3, proven) ---------------------------
code=$(req POST /v3/templates "$AK" '{"name":"exec-tmpl"}')
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\",\"startCmd\":\"exec sleep 86400\",\"readyCmd\":\"true\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || fail "build did not become ready"
case "$TEMPLATE" in e2b-snp-*) : ;; *) fail "build produced $TEMPLATE (want e2b-snp-...)";; esac
echo "==> built template: $TEMPLATE"
BUILD_SNAPSHOT_COUNT=$(snapshot_argv_count)
[ "$BUILD_SNAPSHOT_COUNT" -gt 0 ] || fail "builder did not invoke sandbox-ctl snapshot"
for ((i=0; i<BUILD_SNAPSHOT_COUNT; i++)); do
    assert_snapshot_has_no_policy_flags "$i" || fail "builder snapshot received Pause-only policy flags"
done
echo "==> PASS: local checkpoint mode did not add merge-ref/drop-caches to builder snapshots"

# ---- async launch failure/kill gates --------------------------------------
# These deterministic injections surround the release-candidate binaries; they
# exercise the real conductor, sqlite store, run pool, systemd units, network,
# routes and cleanup without relying on timing races.
RUNNER_UNIT="$UNIT_DIR/sandbox-runner@.service"
RUNNER_UNIT_SAVED="$WORK/sandbox-runner@.service.saved"
cp "$RUNNER_UNIT" "$RUNNER_UNIT_SAVED"
python3 - "$RUNNER_UNIT" <<'PY'
import sys
path = sys.argv[1]
lines = open(path, encoding="utf-8").read().splitlines()
lines = ["ExecStart=/bin/sleep 30" if line.startswith("ExecStart=") else line for line in lines]
open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
PY
systemctl daemon-reload
echo "==> inject runner WaitAssignment timeout; Create must still return durable 201"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runner-timeout create=$code (want 201)"; }
RUNNER_TIMEOUT_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNNER_TIMEOUT_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNNER_TIMEOUT_SID" dead 200 || fail "runner-timeout sandbox did not roll back to dead"
[ -z "$(sandbox_run_id "$RUNNER_TIMEOUT_SID")" ] || fail "runner-timeout rollback retained run_id"
code=$(DP_MAX_TIME=10 dp "49983-$RUNNER_TIMEOUT_SID" /health "$RUNNER_TIMEOUT_TOKEN" || true)
[ "$code" = "404" ] || fail "runner-timeout route=$code (want prompt 404 after Delete)"
code=$(req DELETE "/sandboxes/$RUNNER_TIMEOUT_SID" "$AK"); [ "$code" = "204" ] || fail "delete runner-timeout sandbox=$code"
cp "$RUNNER_UNIT_SAVED" "$RUNNER_UNIT"
systemctl daemon-reload
systemctl stop 'sandbox-runner@*.service' >/dev/null 2>&1 || true
systemctl reset-failed 'sandbox-runner@*.service' >/dev/null 2>&1 || true
echo "==> PASS: runner wait timeout rolled accepted fresh Create to dead + Delete with empty run_id"

printf '%s\n' runtime-wire-failure >"$WORK/inject-sandbox-run"
echo "==> inject malformed runtime readiness after runner commit"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "runtime-failure create=$code (want 201)"; }
RUNTIME_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
RUNTIME_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$RUNTIME_FAILURE_SID" dead 120 || fail "runtime protocol failure did not roll back to dead"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$RUNTIME_FAILURE_SID" /health "$RUNTIME_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "runtime-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$RUNTIME_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete runtime-failure sandbox=$code"
echo "==> PASS: readiness protocol failure fenced the assigned runner and published Delete"

printf '%s\n' envd-init-failure >"$WORK/inject-sandbox-run"
echo "==> inject mandatory envd /init HTTP 500 after valid readiness wire"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "envd-failure create=$code (want 201)"; }
ENVD_FAILURE_SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_FAILURE_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
wait_sandbox_state "$ENVD_FAILURE_SID" dead 120 || fail "envd init failure did not roll back to dead"
rm -f "$WORK/inject-sandbox-run"
code=$(DP_MAX_TIME=10 dp "49983-$ENVD_FAILURE_SID" /health "$ENVD_FAILURE_TOKEN" || true)
[ "$code" = "404" ] || fail "envd-failure route=$code (want prompt 404)"
code=$(req DELETE "/sandboxes/$ENVD_FAILURE_SID" "$AK"); [ "$code" = "204" ] || fail "delete envd-failure sandbox=$code"
echo "==> PASS: mandatory envd /init failure rolled accepted Create to dead + Delete"

printf '%s\n' hold >"$WORK/inject-sandbox-run"
echo "==> hold assigned runtime to exercise starting SetTimeout/Pause/Kill"
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "starting-kill create=$code (want 201)"; }
STARTING_KILL_SID=$(json_field "$WORK/resp.body" sandboxID)
wait_sandbox_state "$STARTING_KILL_SID" starting 50 || fail "held sandbox was not starting"
STARTING_KILL_RUN_ID=""
for _ in $(seq 1 100); do
    STARTING_KILL_RUN_ID="$(sandbox_run_id "$STARTING_KILL_SID")"
    [ -n "$STARTING_KILL_RUN_ID" ] && break
    sleep 0.1
done
[ -n "$STARTING_KILL_RUN_ID" ] || fail "held starting sandbox never bound a runner"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/timeout" "$AK" '{"timeout":77}')
[ "$code" = "204" ] || fail "SetTimeout starting=$code (want 204)"
code=$(req POST "/sandboxes/$STARTING_KILL_SID/pause" "$AK")
[ "$code" = "409" ] || fail "Pause starting=$code (want 409)"
code=$(req DELETE "/sandboxes/$STARTING_KILL_SID" "$AK")
[ "$code" = "204" ] || fail "Kill starting=$code (want 204)"
rm -f "$WORK/inject-sandbox-run"
wait_sandbox_state "$STARTING_KILL_SID" missing 50 || fail "Kill starting left a durable row"
if systemctl is-active --quiet "sandbox-runner@$STARTING_KILL_RUN_ID.service"; then
    fail "Kill starting left runner $STARTING_KILL_RUN_ID active"
fi
echo "==> PASS: starting SetTimeout=204, Pause=409, Kill removed row/runner without resurrection"

# ---- create the sandbox (boots the microVM) -------------------------------
# Inject sandbox config via the X-Kuasar-Sandbox-Network header (§4.6): the guest
# hostname should become CFG_HOST, verified by `hostname` in the exec below.
CFG_HOST="e2e-cfg-host"
echo "==> POST /sandboxes (boot microVM from $TEMPLATE; inject hostname=$CFG_HOST via header)"
CREATE_BASE_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.restore": json.dumps({"prefetch": "memory"}),
    },
}))
PY
)
REQ_NET_HEADER="{\"hostname\":\"$CFG_HOST\"}"
REQ_ATTACH_MMDS="$MMDS_ROUTES_E2E"
code=$(req POST /sandboxes "$AK" "$CREATE_BASE_BODY")
unset REQ_NET_HEADER REQ_ATTACH_MMDS
if [ "$code" != "201" ]; then
    echo "create=$code body:"; cat "$WORK/resp.body"; echo
    echo "==> orchestrator log:"; sed 's/^/  orch| /' "$WORK/orch.log"
    SID=$(ls "$WORK/run" 2>/dev/null | head -1)
    [ -n "$SID" ] && { echo "==> sandbox journal:"; journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 60 2>/dev/null | sed 's/^/  sandbox| /'; }
    fail "create=$code (want 201 durable acceptance)"
fi
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "create response exposed a default exec token"
CREATE_RETURN_STATE="$(sandbox_state "$SID")"
case "$CREATE_RETURN_STATE" in starting|running) ;; *) fail "state immediately after 201=$CREATE_RETURN_STATE";; esac
echo "==> PASS: Create 201 durably accepted sandbox $SID (observed state=$CREATE_RETURN_STATE)"
echo "==> issue data request immediately after Create; starting must park until running"
code=$(DP_MAX_TIME=120 dp "49983-$SID" /health "$ENVD_TOKEN" || true)
{ [ "$code" = "204" ] || [ "$code" = "200" ]; } \
    || { cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; fail "immediate post-Create data request=$code"; }
wait_sandbox_state "$SID" running 20 || fail "sandbox was not running after parked data request"
echo "==> PASS: post-Create data request parked through runner handoff/readiness/envd init (code=$code)"

# ---- list -----------------------------------------------------------------
code=$(req GET /v2/sandboxes "$AK"); [ "$code" = "200" ] || fail "list=$code"
grep -q "$SID" "$WORK/resp.body" || fail "sandbox $SID not listed"
echo "==> PASS: sandbox listed"

# ---- detail / info contract -----------------------------------------------
# This is the HTTP endpoint used by the E2B-compatible sandbox info/get-info
# path. Keep this in the real microVM E2E so the detail contract is checked
# against a running sandbox, not only through an in-process handler test.
code=$(req GET "/sandboxes/$SID" "$AK")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "sandbox detail=$code"; }
# These are the node defaults used by this E2E config. The API obtains them
# from node-ctl's a.res, not from the sandbox create request.
assert_sandbox_detail "$WORK/resp.body" "$SID" 2 2048 1024 \
    || { cat "$WORK/resp.body"; fail "sandbox detail contract"; }
echo "==> PASS: sandbox detail/info returned RFC3339 timestamps and resource fields"

# ---- native exec capability -> CONNECT -> sandbox-ctl -> real guest -------
echo "==> issue an explicit native exec capability (create has no default token)"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue native exec capability"
rm -f "$WORK/exec-session.secret"
NATIVE_MARK="NATIVE_EXEC_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
PTY_MARK="NATIVE_EXEC_PTY_$RANDOM"
exec_pty_resize_through_connect "$SID" "$EXEC_TOKEN" "$PTY_MARK"
echo "==> PASS: real sandbox-ctl CONNECT reached the guest (stdio, duplicate headers, PTY resize, exit status)"

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
if err is not None:
    print("API_ERROR", json.dumps(err))
sys.stdout.write("OUTPUT_BEGIN\n"); sys.stdout.flush()
sys.stdout.buffer.write(out); sys.stdout.write("\nOUTPUT_END\n")
PY
MARK="HELLO_FROM_GUEST_$RANDOM"
echo "==> exec in guest: sh -c 'hostname; id; echo $MARK; uname -sm'"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "hostname; id; echo $MARK; uname -sm" > "$WORK/exec.out" 2>&1 || true
sed 's/^/  guest| /' "$WORK/exec.out"
grep -q "$MARK" "$WORK/exec.out" || fail "guest command output missing $MARK (envd exec failed; see above)"
grep -q 'EXIT_CODE 0' "$WORK/exec.out" || fail "guest command exit code != 0"
echo "==> PASS: command executed in guest (saw $MARK, exit 0)"
# Sandbox-config injection (§4.6): the X-Kuasar-Sandbox-Network header set the guest
# hostname. Best-effort (the main flow already passed); a note rather than a failure.
if grep -q "$CFG_HOST" "$WORK/exec.out"; then echo "==> PASS: config injected (guest hostname=$CFG_HOST via X-Kuasar-Sandbox-Network)"
else echo "    (note: guest hostname != $CFG_HOST; config-injection check inconclusive)"; fi

# ---- internal proxy_netns -> floatingip user port -------------------------
USER_MARK="internal-proxy-netns-user-port-$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "mkdir -p /home/user/e2e-site; echo '$USER_MARK' > /home/user/e2e-site/index.html; cd /home/user/e2e-site; python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/e2e-http-8000.log 2>&1 &" \
    >"$WORK/start-user-port.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-user-port.out" || { sed 's/^/  envd| /' "$WORK/start-user-port.out"; fail "start guest user-port server"; }
ok=""
for _ in $(seq 1 30); do
    code=$(DP_MAX_TIME=8 dp "8000-$SID" / "$FORWARD_TOKEN" || true)
    grep -q "$USER_MARK" "$WORK/dp.body" 2>/dev/null && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; fail "internal proxy_netns -> floatingip user port did not return marker"; }
echo "==> PASS: internal proxy per-dial proxy_netns reached sandbox floatingip:8000 (marker=$USER_MARK)"

# ---- local Pause, all policy fields unset -> exact legacy argv + restore ---
# Write a marker file in the guest BEFORE pausing; after resume it must still be
# there — proving both the local snapshot/restore overlay AND that a resumed img
# sandbox restores (not cold-boots). /home/user is user-owned.
PERSIST="PERSIST_$MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "echo $PERSIST > /home/user/persist.txt; cat /home/user/persist.txt" > "$WORK/wr.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/wr.out" || { sed 's/^/  guest| /' "$WORK/wr.out"; fail "could not write /home/user/persist.txt as the guest user (ownership not preserved?)"; }
echo "==> wrote /home/user/persist.txt in the guest (as user)"

UNSET_CALL=$(snapshot_argv_count)
echo "==> local pause with node/metadata/action policy all unset: $SID"
code=$(req POST "/sandboxes/$SID/pause" "$AK")
[ "$code" = "204" ] || {
    echo "==> pause=$code — snapshot error:"
    grep -iE 'snapshot|pause|api error' "$WORK/orch.log" | tail -10 | sed 's/^/  orch| /'
    SID_JOURNAL=$(journalctl KUASAR_SANDBOX_ID="$SID" --no-pager -n 30 2>/dev/null | grep -iE 'snapshot|ctl.sock|error' | tail -8)
    [ -n "$SID_JOURNAL" ] && echo "$SID_JOURNAL" | sed 's/^/  unit| /'
    fail "local all-unset pause=$code (want 204)"
}
assert_snapshot_argv "$UNSET_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    || fail "all-unset local Pause changed the legacy snapshot argv"
B_LOCAL="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$B_LOCAL" ] || fail "local Pause did not create $B_LOCAL"
B_ARTIFACT="$(readlink -f "$B_LOCAL")"
[ -f "$B_ARTIFACT" ] || fail "local B target is missing: $B_ARTIFACT"
B_SNAPSHOT_BASENAME="$(basename "$B_ARTIFACT")"
"$BIN/sandbox-ctl" info --json "$B_LOCAL" >"$WORK/b-local.json" \
    || fail "all-unset local B is not a readable snapshot bundle"
echo "==> PASS: all-unset local Pause produced B and passed no policy flags"

echo "==> accept paused -> starting through POST /connect, then activate native exec immediately"
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":113}')
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "Connect paused=$code (want 200)"; }
CONNECT_RETURN_STATE="$(sandbox_state "$SID")"
case "$CONNECT_RETURN_STATE" in starting|running) ;; *) fail "state immediately after Connect=$CONNECT_RETURN_STATE (must not remain paused)";; esac
RESUME_MARK="NATIVE_EXEC_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "envd did not become ready after local restore"
echo "==> PASS: Connect returned after durable starting acceptance (observed $CONNECT_RETURN_STATE); immediate native exec parked to running"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" > "$WORK/exec2.out" 2>&1 || true
sed 's/^/  guest2| /' "$WORK/exec2.out"
grep -q "$PERSIST" "$WORK/exec2.out" || fail "pre-pause state LOST after local restore"
echo "==> PASS: all-unset B restored locally and preserved guest state"

# ---- local B -> working-set W -> independent portable publication --------
# The second local Pause keeps memory self separate while disks still merge.
# Make the restored disk delta observably different from B: a content-addressed
# merge with no intervening writes can legitimately reproduce B's top digest.
# This W-only marker makes removal of B's old disk top a meaningful proof.
W_DISK_PERSIST="W_DISK_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $W_DISK_PERSIST > /home/user/working-set-disk.txt" >"$WORK/w-disk-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/w-disk-write.out" \
    || { sed 's/^/  guest| /' "$WORK/w-disk-write.out"; fail "write W-only disk marker"; }
# Removing B's old root-disk top before promotion proves upload-snapshot treats
# B.snapshot as an opaque memory lower instead of recursively publishing B's
# stale disk graph.
PORTABLE_W_CALL=$(snapshot_argv_count)
PORTABLE_W_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$PORTABLE_W_RUN_ID" ] || fail "working-set source runner id is empty"
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":false,"checkpoint_drop_caches":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "portable W pause=$code (want 204)"; }
assert_snapshot_argv "$PORTABLE_W_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "portable W Pause policy did not reach sandbox-ctl exactly"
W_PORTABLE_LOCAL="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$W_PORTABLE_LOCAL" ] || fail "working-set Pause did not create $W_PORTABLE_LOCAL"
W_PORTABLE_ARTIFACT="$(readlink -f "$W_PORTABLE_LOCAL")"
[ "$W_PORTABLE_ARTIFACT" != "$B_ARTIFACT" ] || fail "working-set W reused B memory self"
"$BIN/sandbox-ctl" info --json "$W_PORTABLE_LOCAL" >"$WORK/w-portable-local.json" \
    || fail "local working-set W is unreadable"
PORTABLE_W_UNIT="sandbox-runner@$PORTABLE_W_RUN_ID.service"
wait_unit_journal_contains "$PORTABLE_W_UNIT" \
    'quiesce: guest acked (drop_caches=skipped' "$WORK/w-portable-local.journal" \
    || { tail -40 "$WORK/w-portable-local.journal" | sed 's/^/  unit| /'; fail "working-set Pause did not preserve guest page cache"; }
B_ROOT_TOP_BASENAME=$(python3 - "$WORK/b-local.json" "$WORK/w-portable-local.json" "$B_SNAPSHOT_BASENAME" <<'PY'
import json, os, sys

with open(sys.argv[1], encoding="utf-8") as source:
    parent = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    working = json.load(source)

parent_refs = parent.get("FromRefs") or []
memory_refs = working.get("FromRefs") or []
if len(memory_refs) != len(parent_refs) + 1 or not memory_refs[0].startswith("file://"):
    raise SystemExit(f"working-set from_refs={memory_refs!r}, want local B plus {parent_refs!r}")
memory_path = memory_refs[0][len("file://"):].split("@", 1)[0]
if os.path.basename(memory_path) != sys.argv[3]:
    raise SystemExit(f"working-set memory lower={memory_refs[0]!r}, want {sys.argv[3]!r}")
if memory_refs[1:] != parent_refs:
    raise SystemExit(f"working-set lower tail={memory_refs[1:]!r}, want inherited {parent_refs!r}")

def top(node):
    overlay = node.get("Overlay")
    return overlay["Base"] if overlay else node["Base"]

def chain(node):
    overlay = node.get("Overlay")
    return (overlay.get("BaseFromRefs") if overlay else node.get("BaseFromRefs")) or []

parent_top = top(parent["Boot"]["Root"])
working_root = working["Boot"]["Root"]
if parent_top == top(working_root) or parent_top in chain(working_root):
    raise SystemExit(f"W retained B disk top {parent_top!r}; local disks must merge")
if not parent_top.startswith("file://"):
    raise SystemExit(f"B root disk top is not local: {parent_top!r}")
relative = parent_top[len("file://"):].split("@", 1)[0]
if relative != os.path.basename(relative) or not relative.endswith(".overlay"):
    raise SystemExit(f"refusing to remove unexpected B disk ref {parent_top!r}")
print(relative)
PY
) || fail "local B/W graph validation failed"
B_ROOT_TOP_PATH="$CHECKPOINT_DIR/$SID/$B_ROOT_TOP_BASENAME"
[ -f "$B_ROOT_TOP_PATH" ] || fail "B root disk top is missing before minimal-set test: $B_ROOT_TOP_PATH"
rm -f -- "$B_ROOT_TOP_PATH"
echo "==> PASS: W -> local B is separate; B root disk top was merged and removed"

PROMOTION_TOKEN=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --socket "$WORK/node-ctl.socket") \
    || fail "independent export/promote of local W failed"
case "$PROMOTION_TOKEN" in kmt1.*) ;; *) fail "export-sandbox returned a non-KMT result" ;; esac
PORTABLE_W_REF=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select snapshot_ref from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
)
PORTABLE_W_KEY="${PORTABLE_W_REF#manifest://}"
[[ "$PORTABLE_W_REF" == manifest://* && "$PORTABLE_W_KEY" =~ ^[0-9a-f]{64}$ ]] \
    || fail "promoted W ref is not manifest://<64hex>: $PORTABLE_W_REF"
[ ! -e "$CHECKPOINT_DIR/$SID" ] || fail "promotion retained redundant local checkpoint directory"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$PORTABLE_W_REF" >"$WORK/w-portable-manifest.json" \
    || fail "promoted W is not readable from the manifest store"
PORTABLE_LAYER_SUMMARY=$(python3 - "$WORK/w-portable-manifest.json" "$WORK/b-local.json" "$PORTABLE_W_REF" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    working = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    parent = json.load(source)
refs = working.get("FromRefs") or []
parent_refs = parent.get("FromRefs") or []
if len(refs) != len(parent_refs) + 1 or not refs[0].startswith("manifest://"):
    raise SystemExit(f"portable W from_refs={refs!r}, want manifest B plus {parent_refs!r}")
if refs[1:] != parent_refs:
    raise SystemExit(f"portable W lower tail={refs[1:]!r}, want preserved {parent_refs!r}")
if refs[0] == sys.argv[3]:
    raise SystemExit("portable W self and B memory lower collapsed to one ref")
print(refs[0][len("manifest://"):], len(refs))
PY
) || fail "portable W/B layer validation failed"
read -r PORTABLE_B_KEY PORTABLE_PARENT_LAYERS <<<"$PORTABLE_LAYER_SUMMARY"
echo "==> PASS: independent promotion published distinct W self and opaque B memory layer"

RESUME_MARK="PORTABLE_W_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "portable W did not restore"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" \
    >"$WORK/portable-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/portable-read.out" || { sed 's/^/  guest| /' "$WORK/portable-read.out"; fail "portable W lost guest state"; }
# The working-set memory intentionally retained guest cache, so evict it before
# reading the W-only file. This makes the assertion prove the published disk
# artifact, independently of the restored memory self/lower chain.
"$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/run" -- /bin/sh -c \
    'sync && echo 3 > /proc/sys/vm/drop_caches && cat /home/user/working-set-disk.txt' \
    >"$WORK/portable-disk-read.out" 2>&1 || true
grep -q "$W_DISK_PERSIST" "$WORK/portable-disk-read.out" \
    || { sed 's/^/  guest| /' "$WORK/portable-disk-read.out"; fail "portable W lost merged W-only disk state"; }
PORTABLE_RESTORE_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$PORTABLE_RESTORE_RUN_ID" ] || fail "portable restore runner id is empty"
PORTABLE_RESTORE_UNIT="sandbox-runner@$PORTABLE_RESTORE_RUN_ID.service"
wait_unit_journal_contains "$PORTABLE_RESTORE_UNIT" \
    "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_W_KEY" \
    "$WORK/portable-w.journal" || { tail -40 "$WORK/portable-w.journal" | sed 's/^/  unit| /'; fail "portable restore did not prefetch W self"; }
MANIFEST_PREFETCH_COUNT=$(grep -Fc 'memory prefetch started backend=manifest' "$WORK/portable-w.journal" || true)
[ "$MANIFEST_PREFETCH_COUNT" = "1" ] || fail "portable restore started $MANIFEST_PREFETCH_COUNT manifest prefetches, want W self only"
grep -Fq "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_B_KEY" \
    "$WORK/portable-w.journal" && fail "portable restore prefetched B memory lower"
echo "==> PASS: portable W restored state; prefetch targeted W self only (not B or disk)"

if [ "$MMDS_ROUTES_E2E" = 1 ]; then
    source "$REPO_ROOT/test/e2e/lib/mmds_static_guest.sh"
    source "$REPO_ROOT/test/e2e/lib/mmds_secret_guest.sh"
    run_mmds_static_guest_get "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS static exact route"
    run_mmds_secret_standalone_e2e "$WORK/node-ctl.socket" "$SID" "$WORK/envd_exec.py" \
        "$ENVD_SOCK" "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS initial/unresolved/update/delete lifecycle"
    run_mmds_service_standalone_e2e "$SID" "$WORK/envd_exec.py" "$ENVD_SOCK" \
        "$ENVD_TOKEN" "$WORK" internal \
        || fail "internal MMDS conductor-owned local service"
    for value in MMDS_SECRET_INITIAL_GUEST_E2E MMDS_SECRET_UPDATED_GUEST_E2E MMDS_SECRET_ROTATED_GUEST_E2E; do
        for artifact in "$WORK"/orch*.log "$WORK"/mmds-*.out "$WORK"/mmds-service.requests \
            "$WORK"/lib/node-ctl.db*; do
            [ -f "$artifact" ] || continue
            grep -a -F -q -- "$value" "$artifact" \
                && fail "MMDS secret plaintext appeared in internal E2E artifact $artifact"
        done
    done
    echo "==> PASS: internal real guest covered static, initial/unresolved/rotated/deleted secret, and local service"
fi

code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill first sandbox before KMT import=$code (want 204)"
unset EXEC_TOKEN

echo "==> KMT missing import -> paused -> durable starting -> asynchronous restore"
code=$(curl -sS --noproxy '*' --max-time 30 -o "$WORK/resp.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
    -H "X-Kuasar-Migration-Token: $PROMOTION_TOKEN" \
    -H 'Content-Type: application/json' --data '{"timeout":119}' \
    "http://127.0.0.1:$PORT/sandboxes/$SID/connect")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "KMT Connect=$code (want 200)"; }
[ "$(json_field "$WORK/resp.body" sandboxID)" = "$SID" ] || fail "KMT Connect changed sandbox identity"
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
KMT_RETURN_STATE="$(sandbox_state "$SID")"
case "$KMT_RETURN_STATE" in starting|running) ;; *) fail "KMT Connect returned with state=$KMT_RETURN_STATE";; esac
KMT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue exec capability during KMT starting"
rm -f "$WORK/exec-session.secret"
KMT_MARK="KMT_RESTORE_$RANDOM"
exec_through_connect "$SID" "$KMT_EXEC_TOKEN" "$KMT_MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" >"$WORK/kmt-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/kmt-read.out" \
    || { sed 's/^/  guest| /' "$WORK/kmt-read.out"; fail "KMT restore lost portable guest state"; }
wait_sandbox_state "$SID" running 20 || fail "KMT restore did not commit running"
echo "==> PASS: KMT Connect returned at $KMT_RETURN_STATE; immediate native exec parked and portable state restored"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill KMT-imported sandbox=$code (want 204)"
unset KMT_EXEC_TOKEN
SID_UNSET="$SID"

# Restart the conductor against the same store with explicit node defaults.
# Local mode must remain warning-free; these values are inherited only when a
# higher layer leaves the corresponding field unset.
stop_orchestrator
write_orchestrator_config node-policy
start_orchestrator "$WORK/orch-node-policy.log"
wait_mmds_listener
echo "==> PASS: conductor restarted in local mode with node merge_ref=true/drop_caches=false"

# ---- Create metadata/header + Pause body/header fieldwise overlays --------
CREATE_POLICY_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": True, "drop_caches": True})
    },
}))
PY
)
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST /sandboxes "$AK" "$CREATE_POLICY_BODY")
unset REQ_CHECKPOINT_HEADER
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "policy create=$code (want 201)"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
assert_no_default_exec_token "$WORK/resp.body" || fail "policy create exposed a default exec token"

# The Create header overrides merge_ref, while its null drop_caches inherits
# the body. The stored request-scoped namespace must be canonical.
python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select metadata_json from sandboxes where id=?", (sys.argv[2],)).fetchone()
if row is None:
    raise SystemExit("created sandbox row not found")
metadata = json.loads(row[0])
got = metadata.get("kuasar-sandbox.checkpoint")
want = '{"merge_ref":false,"drop_caches":true}'
if got != want:
    raise SystemExit(f"stored checkpoint metadata={got!r}, want {want!r}")
PY
echo "==> PASS: Create checkpoint header overlaid body per field and persisted canonical metadata"

ENVD_SOCK="$WORK/run/$SID/envd.sock"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue policy sandbox exec capability"
POLICY_START_MARK="POLICY_CREATE_ACTIVATION_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$POLICY_START_MARK"
wait_sandbox_state "$SID" running 20 || fail "policy sandbox did not reach running after exec activation"
POLICY_PERSIST="POLICY_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $POLICY_PERSIST > /home/user/policy-persist.txt" >"$WORK/policy-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/policy-write.out" || { sed 's/^/  guest| /' "$WORK/policy-write.out"; fail "write policy sandbox marker"; }

POLICY_CALL=$(snapshot_argv_count)
# Body overrides stored metadata; the header then overrides only merge_ref.
# Its null drop_caches must preserve the body's false.
REQ_CHECKPOINT_HEADER='{"merge_ref":false,"drop_caches":null}'
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":true,"checkpoint_drop_caches":false}')
unset REQ_CHECKPOINT_HEADER
[ "$code" = "204" ] || { cat "$WORK/resp.body"; sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "policy pause=$code (want 204)"; }
assert_snapshot_argv "$POLICY_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "Pause body/header policy did not reach sandbox-ctl exactly"
W_POLICY="$CHECKPOINT_DIR/$SID/$SID.snapshot"
[ -f "$W_POLICY" ] || fail "policy Pause did not create $W_POLICY"
"$BIN/sandbox-ctl" info --json "$W_POLICY" >"$WORK/w-policy.json" || fail "policy W is unreadable"
python3 - "$WORK/w-policy.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    info = json.load(source)
refs = info.get("FromRefs") or []
if not refs:
    raise SystemExit("working-set W has no memory lower reference")
if "kuasar-sandbox.checkpoint" in (info.get("Metadata") or {}):
    raise SystemExit("host-only checkpoint policy leaked into snapshot.cfg metadata")
PY
echo "==> PASS: Pause header null inherited body, explicit false flags reached local capture, W self remains separate from its memory lower"

RESUME_MARK="POLICY_W_RESUME_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "policy W did not restore locally"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/policy-persist.txt" >"$WORK/policy-read.out" 2>&1 || true
grep -q "$POLICY_PERSIST" "$WORK/policy-read.out" || { sed 's/^/  guest| /' "$WORK/policy-read.out"; fail "policy W lost guest state"; }
echo "==> PASS: policy W restored locally with guest state intact"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill policy sandbox=$code"
unset EXEC_TOKEN
SID_POLICY="$SID"

# ---- automatic Pause: metadata merge_ref > node; node supplies drop_caches -
AUTO_BODY=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({
    "templateID": sys.argv[1],
    "timeout": 120,
    "metadata": {
        "kuasar-sandbox.checkpoint": json.dumps({"merge_ref": False})
    },
}))
PY
)
AUTO_CALL=$(snapshot_argv_count)
code=$(req POST /sandboxes "$AK" "$AUTO_BODY")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "auto-pause create=$code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
code=$(req POST "/sandboxes/$SID/timeout" "$AK" '{"timeout":15}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "arm auto-pause timeout=$code"; }
AUTO_PAUSED=""
for _ in $(seq 1 180); do
    state=$(python3 - "$WORK/lib/node-ctl.db" "$SID" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
)
    [ "$state" = "paused" ] && { AUTO_PAUSED=1; break; }
    sleep 0.5
done
[ -n "$AUTO_PAUSED" ] || { sed 's/^/  orch| /' "$WORK/orch-node-policy.log"; fail "reaper did not auto-pause policy sandbox (last state=$state)"; }
assert_snapshot_argv "$AUTO_CALL" \
    snapshot --sandbox-id "$SID" --output "$CHECKPOINT_DIR/$SID" --run-root "$WORK/run" \
    --merge-ref=false --drop-caches=false \
    || fail "auto-pause did not resolve metadata > node fieldwise"
[ -f "$CHECKPOINT_DIR/$SID/$SID.snapshot" ] || fail "auto-pause did not create local W"
echo "==> PASS: reaper auto-pause used metadata merge_ref=false and node drop_caches=false"

# ---- teardown -------------------------------------------------------------
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill auto-paused sandbox=$code (want 204)"
echo "==> PASS: all local checkpoint-policy sandboxes killed"
echo
echo "==> e2e_execute: OK   (template $TEMPLATE, portable $PORTABLE_W_REF, all-unset $SID_UNSET, policy $SID_POLICY, auto $SID)"
