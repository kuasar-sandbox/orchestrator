#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"
ORCH="$BIN/node-ctl"
[ -x "$ORCH" ] || { echo "missing prepared product: $ORCH" >&2; exit 1; }

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
}
trap cleanup EXIT
fail() { echo "FAIL orchestrator.runtask.sh: $*" >&2; exit 1; }

command -v python3 >/dev/null
command -v systemd-run >/dev/null
[ -d /run/systemd/system ] || { echo "systemd manager is required" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "root is required" >&2; exit 1; }
systemctl show-environment >/dev/null 2>&1 || { echo "systemd manager is unavailable" >&2; exit 1; }

SOCK="$WORK/node-ctl.socket"
RUN_ID="sr-00000000-0000-7000-8000-000000000001"
RUN_ROOT="$WORK/runroot"
PIDFILE="$RUN_ROOT/runners/$RUN_ID.pid"
TASK_PIDFILE="$RUN_ROOT/sandboxes/probe/probe.pid"
OUTFILE="$WORK/marker.out"
READY_SOCK="$RUN_ROOT/sandboxes/probe/ready.sock"
READY_WIRE="$WORK/ready.wire"
ORDER_FILE="$WORK/order.log"
mkdir -p "$WORK/wd" "$RUN_ROOT/runners" "$RUN_ROOT/sandboxes/probe"

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
  echo "manifest_key=[\${MANIFEST_KEY:-}]"
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
bootstrap = {"sandbox_id": "probe", "run_id": "$RUN_ID", "workdir": "$WORK/wd",
             "env": {"MANIFEST_KEY": "task-authoritative-key"}, "final": spec}
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
        elif self.path == "/internal/task/sandbox/bootstrap":
            if not ready_connected.wait(2):
                body = json.dumps({"error": "ready.sock was not connected before bootstrap"}).encode()
            else:
                with open("$ORDER_FILE", "a") as f: f.write("fetch\\n")
                body = json.dumps(bootstrap).encode()
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
    --setenv=MANIFEST_KEY=inherited-wrong-key \
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
grep -q 'manifest_key=\[task-authoritative-key\]' "$OUTFILE" \
    || fail "task bootstrap MANIFEST_KEY did not override inherited value"
grep -q 'task_pidfile=\[\]' "$OUTFILE" || fail "TASK_PIDFILE not stripped"
grep -q 'task_run_id=\[\]' "$OUTFILE" || fail "TASK_RUN_ID not stripped"
grep -q 'task_sandbox_id=\[\]' "$OUTFILE" || fail "TASK_SANDBOX_ID not stripped"
grep -Eq 'cgroup_target=\[.*/vmm\]' "$OUTFILE" || fail "inherited descriptor did not target vmm"

for _ in $(seq 1 50); do [ -f "$READY_WIRE" ] && break; sleep 0.1; done
[ -f "$READY_WIRE" ] || fail "readiness connection did not reach EOF"
printf 'control_ready\nready\n' > "$WORK/ready.expected"
cmp -s "$WORK/ready.expected" "$READY_WIRE" || fail "readiness wire was not exact"
[ "$(cat "$ORDER_FILE")" = $'connect\nfetch' ] || fail "bootstrap fetched before ready socket connect"

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

echo "PASS orchestrator.runtask.sh"
