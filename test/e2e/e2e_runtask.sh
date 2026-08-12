#!/usr/bin/env bash
#
# e2e_runtask.sh — verify config/info CLI surfaces, then exercise the
# run-sandbox handoff in a real delegated transient systemd unit without KVM.
#
# The launcher case uses production cgroup preparation. It does not add an
# environment variable or command-line bypass for the required ctl/vmm topology.
#
# Missing binaries -> exit 0 ("skipped") unless REQUIRE_RUNTASK=1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
ORCH="$BIN/node-ctl"
SANDBOX="$BIN/sandbox-ctl"
FLATTEN="$BIN/flatten-ctl"

skip() {
    echo; echo "==> e2e_runtask: skipping ($*)"
    [ "${REQUIRE_RUNTASK:-0}" = "1" ] && { echo "REQUIRE_RUNTASK=1 set; failing instead" >&2; exit 1; }
    exit 0
}
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in "$ORCH" "$SANDBOX" "$FLATTEN"; do [ -x "$b" ] || skip "missing $b — run 'make build'"; done
# ---- CLI smokes -----------------------------------------------------------
WORK="$(mktemp -d /tmp/e2e-runtask-XXXXXX)"
SRV_PID=""
UNIT=""
DUP_UNIT=""
cleanup() {
    set +e
    [ -n "$UNIT" ] && systemctl stop "$UNIT.service" >/dev/null 2>&1
    [ -n "$UNIT" ] && systemctl reset-failed "$UNIT.service" >/dev/null 2>&1
    [ -n "$DUP_UNIT" ] && systemctl stop "$DUP_UNIT.service" >/dev/null 2>&1
    [ -n "$DUP_UNIT" ] && systemctl reset-failed "$DUP_UNIT.service" >/dev/null 2>&1
    [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null
    [ -n "${E2E_KEEP:-}" ] && echo "kept $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT
echo "==> CLI: config --template + round-trip (validate)"
"$ORCH" config conductor --template > "$WORK/orch.yaml"
grep -q "domain:" "$WORK/orch.yaml" || fail "node-ctl config conductor --template missing domain"
"$ORCH" config conductor --config "$WORK/orch.yaml" >/dev/null || fail "node-ctl config conductor --config did not validate the template"

"$FLATTEN" config --template > "$WORK/flatten.yaml"
grep -q "referer:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer"
grep -q "validity:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer.validity"
"$FLATTEN" config --config "$WORK/flatten.yaml" >/dev/null || fail "flatten-ctl config --config did not load"

"$SANDBOX" config --template > "$WORK/sb.yaml" 2>/dev/null || true
grep -q "resources:" "$WORK/sb.yaml" || fail "sandbox-ctl config --template missing resources"
echo "==> PASS: config --template + round-trip for node-ctl/flatten-ctl/sandbox-ctl"

echo "==> CLI: arg-validation (must reject)"
"$ORCH" run-sandbox >/dev/null 2>&1 && fail "run-sandbox with no args should fail" || true
"$ORCH" run-builder >/dev/null 2>&1 && fail "run-builder with no args should fail" || true
"$SANDBOX" info >/dev/null 2>&1 && fail "sandbox-ctl info with no arg should fail" || true
"$SANDBOX" info /dev/null >/dev/null 2>&1 && fail "sandbox-ctl info on a non-snapshot should fail" || true
echo "==> PASS: run-sandbox/run-builder + sandbox-ctl info reject bad invocation"

# ---- run-sandbox against a fake config socket ----------------------------
# The full suite already requires root/systemd. Standalone rootless invocation
# still keeps the CLI smokes useful and skips only this production-topology case.
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v systemd-run >/dev/null 2>&1 || skip "systemd-run not on PATH"
[ "$(id -u)" -eq 0 ] || skip "run-sandbox handoff requires a delegated systemd unit"
systemctl show-environment >/dev/null 2>&1 || skip "systemd manager is unavailable"

SOCK="$WORK/node-ctl.socket"
RUN_ID="sr-00000000-0000-7000-8000-000000000001"
RUN_ROOT="$WORK/runroot"
PIDFILE="$RUN_ROOT/runs/$RUN_ID.pid"
TASK_PIDFILE="$RUN_ROOT/probe/probe.pid"
OUTFILE="$WORK/marker.out"
READY_SOCK="$RUN_ROOT/probe/ready.sock"
READY_WIRE="$WORK/ready.wire"
ORDER_FILE="$WORK/order.log"
mkdir -p "$WORK/wd" "$RUN_ROOT/runs" "$RUN_ROOT/probe"

cat > "$WORK/marker.sh" <<EOF
#!/usr/bin/env bash
READY_FD=""
CGROUP_FD=""
for arg in "\$@"; do
  case "\$arg" in
    --ready-fd=*) READY_FD="\${arg#*=}";;
    --cgroup-path=fd=*) CGROUP_FD="\${arg#--cgroup-path=fd=}";;
  esac
done
[ -n "\$READY_FD" ] || { echo "missing --ready-fd" >&2; exit 2; }
[ -n "\$CGROUP_FD" ] || { echo "missing --cgroup-path=fd=N" >&2; exit 2; }
CGROUP_TARGET="\$(readlink "/proc/self/fd/\$CGROUP_FD")"
case "\$CGROUP_TARGET" in */vmm) ;; *) echo "invalid cgroup target: \$CGROUP_TARGET" >&2; exit 2;; esac
eval "printf 'control_ready\\nready\\n' >&\${READY_FD}"
eval "exec \${READY_FD}>&-"
{
  echo "args=[\$*]"
  echo "cwd=\$PWD"
  echo "secret=[\${SECRET:-}]"
  echo "task_pidfile=[\${TASK_PIDFILE:-}]"
  echo "task_run_id=[\${TASK_RUN_ID:-}]"
  echo "task_sandbox_id=[\${TASK_SANDBOX_ID:-}]"
  echo "cgroup_target=[\$CGROUP_TARGET]"
} > "$OUTFILE"
sleep 30
EOF
chmod +x "$WORK/marker.sh"

cat > "$WORK/server.py" <<EOF
import json, os, socket, socketserver, sys, threading
from http.server import BaseHTTPRequestHandler
spec = {"exec": "$WORK/marker.sh", "args": ["run", "A", "B"],
        "workdir": "$WORK/wd", "env": {"SECRET": "s3cr3t"}}
assignment = {"kind": "sandbox", "run_id": "$RUN_ID", "task_id": "probe"}
try: os.unlink("$READY_SOCK")
except FileNotFoundError: pass
ready_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
ready_listener.bind("$READY_SOCK")
os.chmod("$READY_SOCK", 0o600)
ready_listener.listen(1)
ready_connected = threading.Event()
def receive_readiness():
    conn, _ = ready_listener.accept()
    with open("$ORDER_FILE", "a") as f: f.write("connect\\n")
    ready_connected.set()
    data = bytearray()
    while True:
        chunk = conn.recv(4096)
        if not chunk: break
        data.extend(chunk)
    conn.close()
    with open("$READY_WIRE", "wb") as f: f.write(data)
threading.Thread(target=receive_readiness, daemon=True).start()
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        if n: self.rfile.read(n)
        if self.path == "/internal/run/assignment":
            body = json.dumps(assignment).encode()
        elif self.path == "/internal/task/launchspec":
            if not ready_connected.wait(2):
                body = json.dumps({"error": "ready.sock was not connected before FetchLaunchSpec"}).encode()
            else:
                with open("$ORDER_FILE", "a") as f: f.write("fetch\\n")
                body = json.dumps(spec).encode()
        else:
            body = json.dumps(spec).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
try: os.unlink("$SOCK")
except FileNotFoundError: pass
class Srv(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
srv = Srv("$SOCK", H)
sys.stderr.write("server-ready\\n"); sys.stderr.flush()
srv.serve_forever()
EOF
python3 "$WORK/server.py" 2>"$WORK/server.log" &
SRV_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { cat "$WORK/server.log"; fail "fake config socket did not come up"; }

UNIT="e2e-runtask@$RUN_ID"
echo "==> run-sandbox: launch in delegated transient unit $UNIT"
systemd-run --quiet --unit="$UNIT" --service-type=exec \
    --slice=sandbox-runner.slice \
    --property=Delegate=yes --property=KillMode=control-group \
    --setenv="TASK_PIDFILE=$PIDFILE" --setenv="TASK_CONFIG_SOCKET=$SOCK" \
    --setenv="TASK_RUN_ID=$RUN_ID" --setenv=TASK_SANDBOX_ID=legacy \
    "$ORCH" run-sandbox
for _ in $(seq 1 100); do [ -s "$OUTFILE" ] && break; sleep 0.1; done
[ -s "$OUTFILE" ] || {
    journalctl -u "$UNIT.service" --no-pager -n 80 >&2 || true
    fail "target did not run"
}
echo "--- marker output ---"; sed 's/^/    /' "$OUTFILE"

grep -Eq 'args=\[run --cgroup-path=fd=[0-9]+ --ready-fd=[0-9]+ A B\]' "$OUTFILE" \
    || fail "injected cgroup/readiness descriptors or spec args not delivered"
grep -q "cwd=$WORK/wd" "$OUTFILE" || fail "workdir not applied"
grep -q 'secret=\[s3cr3t\]' "$OUTFILE" || fail "spec env not injected"
grep -q 'task_pidfile=\[\]' "$OUTFILE" || fail "TASK_PIDFILE not stripped"
grep -q 'task_run_id=\[\]' "$OUTFILE" || fail "TASK_RUN_ID not stripped"
grep -q 'task_sandbox_id=\[\]' "$OUTFILE" || fail "TASK_SANDBOX_ID not stripped"
grep -Eq 'cgroup_target=\[.*/vmm\]' "$OUTFILE" || fail "inherited descriptor did not target vmm"

for _ in $(seq 1 50); do [ -f "$READY_WIRE" ] && break; sleep 0.1; done
[ -f "$READY_WIRE" ] || fail "readiness connection did not reach EOF"
printf 'control_ready\nready\n' > "$WORK/ready.expected"
cmp -s "$WORK/ready.expected" "$READY_WIRE" || fail "readiness wire was not exact"
[ "$(cat "$ORDER_FILE")" = $'connect\nfetch' ] || fail "LaunchSpec fetched before ready socket connect"

RT_PID="$(tr -d '[:space:]' < "$PIDFILE")"
TASK_PIDF="$(tr -d '[:space:]' < "$TASK_PIDFILE")"
[ "$TASK_PIDF" = "$RT_PID" ] || fail "sandbox pidfile did not preserve the launcher PID"
[ "$(systemctl show "$UNIT.service" -p MainPID --value)" = "$RT_PID" ] \
    || fail "exec replacement did not preserve the unit MainPID"
UNIT_CGROUP="$(systemctl show "$UNIT.service" -p ControlGroup --value)"
RUNNER_CGROUP="$(awk -F: '$1 == "0" { print $3 }' "/proc/$RT_PID/cgroup")"
[ "$RUNNER_CGROUP" = "$UNIT_CGROUP/ctl" ] \
    || fail "runner cgroup=$RUNNER_CGROUP, want $UNIT_CGROUP/ctl"
[ ! -s "/sys/fs/cgroup$UNIT_CGROUP/cgroup.procs" ] \
    || fail "runner unit root still contains a process"
kill -0 "$RT_PID" 2>/dev/null || fail "exec-replaced target is not alive"
echo "==> PASS: portable root-to-ctl placement, delegated ctl/vmm handoff, exact readiness wire, and PID inheritance"

DUP_UNIT="e2e-runtask-duplicate@$RUN_ID"
set +e
systemd-run --quiet --wait --pipe --unit="$DUP_UNIT" --service-type=exec \
    --slice=sandbox-runner.slice \
    --property=Delegate=yes --property=KillMode=control-group \
    --setenv="TASK_PIDFILE=$PIDFILE" --setenv="TASK_CONFIG_SOCKET=$SOCK" \
    --setenv="TASK_RUN_ID=$RUN_ID" "$ORCH" run-sandbox > "$WORK/dup.log" 2>&1
DUP_RC=$?
set -e
[ "$DUP_RC" -ne 0 ] || fail "second run-sandbox succeeded despite held pidfile lock"
grep -qi 'lock' "$WORK/dup.log" || { sed 's/^/    /' "$WORK/dup.log"; fail "double-start error omitted lock"; }
echo "==> PASS: double-start refused"

systemctl stop "$UNIT.service"
for _ in $(seq 1 50); do
    [ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] && break
    sleep 0.1
done
[ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] \
    || fail "runner cgroup remained after StopUnit: $UNIT_CGROUP"
systemctl reset-failed "$UNIT.service" >/dev/null 2>&1 || true
UNIT=""
echo "==> PASS: StopUnit removed the delegated ctl/vmm hierarchy"

echo "==> e2e_runtask: OK"
