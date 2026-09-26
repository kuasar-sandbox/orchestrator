#!/usr/bin/env bash
#
# e2e_runtask.sh — verify config/info CLI surfaces, then exercise the
# exact-run run-sandbox bootstrap handoff in a delegated transient systemd unit
# without KVM.
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

# shellcheck source=test/e2e/lib/runtask_privilege.sh
. "$SCRIPT_DIR/lib/runtask_privilege.sh"

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
cleanup_before_privilege_reexec() {
    # This is the first, unprivileged CLI-smoke workspace. It must never survive
    # re-entry, even when E2E_KEEP asks the new root invocation to retain its own
    # diagnostic workspace.
    rm -rf "$WORK"
    trap - EXIT
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
runtask_enter_privileged cleanup_before_privilege_reexec "$SCRIPT_DIR/e2e_runtask.sh" "$@"
systemctl show-environment >/dev/null 2>&1 || skip "systemd manager is unavailable"

SOCK="$WORK/node-ctl.socket"
RUN_ID="sr-00000000-0000-7000-8000-$(python3 -c 'import uuid; print(uuid.uuid4().hex[-12:])')"
RUN_ROOT="$WORK/runroot"
PIDFILE="$RUN_ROOT/runners/$RUN_ID.pid"
OUTFILE="$WORK/marker.json"
READY_SOCK="$RUN_ROOT/sandboxes/probe/ready.sock"
READY_WIRE="$WORK/ready.wire"
mkdir -p "$WORK/wd" "$RUN_ROOT/runners" "$RUN_ROOT/sandboxes/probe"

cat > "$WORK/marker.py" <<'CHILD'
#!/usr/bin/env python3
import fcntl, json, os, pathlib, sys, time
root = pathlib.Path(os.environ["PROBE_ROOT"])
args = sys.argv[1:]
ready = int(next(a.split("=", 1)[1] for a in args if a.startswith("--ready-fd=")))
vmm = int(next(a.split("fd=", 1)[1] for a in args if a.startswith("--cgroup-path=fd=")))
assert [a for a in args if not a.startswith(("--ready-fd=", "--cgroup-path="))] == ["run", "A", "B"]
assert os.readlink(f"/proc/self/fd/{vmm}").endswith("/vmm")
parent_pidfile = next((root / "runroot/runners").glob("*.pid"))
parent_identity = parent_pidfile.stat()
for name in os.listdir("/proc/self/fd"):
    fd = int(name)
    if fd <= 2:
        continue
    try:
        st = os.fstat(fd)
        target = os.readlink(f"/proc/self/fd/{fd}")
    except OSError:
        continue
    assert (st.st_dev, st.st_ino) != (parent_identity.st_dev, parent_identity.st_ino), "inherited parent PID descriptor"
    assert not target.startswith("socket:") or fd == ready, "inherited parent Run session descriptor"
assert not any(k.startswith("TASK_") for k in os.environ), "bootstrap environment escaped into child"
assert os.environ["MANIFEST_KEY"] == "task-authoritative-key"
assert os.environ["SECRET"] == "fixture-value"
assert pathlib.Path.cwd() == root / "wd"
# Simulate the runtime's separate identity, never the parent RunID lock.
runtime_pidfile = root / "runroot/sandboxes/probe/probe.pid"
identity_fd = os.open(runtime_pidfile, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
fcntl.lockf(identity_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.write(identity_fd, str(os.getpid()).encode())
# O_EXCL makes any duplicate child launch a test failure.
with (root / "marker.json").open("x") as stream:
    json.dump({"pid": os.getpid(), "ppid": os.getppid(), "parent_only_fds_absent": True}, stream)
os.write(ready, b"control_ready\nready\n")
os.close(ready)
deadline = time.monotonic() + 45
while not (root / "release-child").exists():
    assert time.monotonic() < deadline, "parent test did not release child"
    time.sleep(0.02)
os.close(identity_fd)
sys.exit(7)
CHILD
chmod +x "$WORK/marker.py"

cat > "$WORK/server.py" <<'SERVER'
import json, os, pathlib, select, socket, socketserver, struct, sys, threading, time
from http.server import BaseHTTPRequestHandler
root, run_id = pathlib.Path(sys.argv[1]), sys.argv[2]
pidfile = root / "runroot/runners" / (run_id + ".pid")
lock = threading.Lock()
sessions = set()
sequence = 0
counts = {"assignment": 0, "bootstrap": 0, "result": 0}
ready_connected = threading.Event()

def event(kind, **fields):
    with lock, (root / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(dict(event=kind, **fields)) + "\n")

ready_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
ready_listener.bind(str(root / "runroot/sandboxes/probe/ready.sock"))
ready_listener.listen(1)
def receive_readiness():
    conn, _ = ready_listener.accept()
    event("readiness-connect")
    ready_connected.set()
    data = bytearray()
    with conn:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data.extend(chunk)
    (root / "ready.wire").write_bytes(data)
    event("readiness-eof")
threading.Thread(target=receive_readiness, daemon=True).start()

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *args):
        pass
    def read_request(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
    def peer_pid(self):
        return struct.unpack("3i", self.connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]
    def authenticated(self):
        try:
            return self.peer_pid() == int(pidfile.read_text().strip())
        except (OSError, ValueError):
            return False
    def reply(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
    def do_PUT(self):
        global sequence
        req = self.read_request()
        if self.path != "/internal/run/session" or req != {"kind": "sandbox", "run_id": run_id} or not self.authenticated():
            self.reply({"error": "invalid session identity"}, 403)
            return
        with lock:
            sequence += 1
            generation = sequence
            sessions.add(generation)
        event("session", generation=generation, pid=self.peer_pid())
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Connection", "close")
        self.end_headers()
        body = (json.dumps({"kind": "sandbox", "run_id": run_id}) + "\n").encode()
        self.wfile.write(("%x\r\n" % len(body)).encode() + body + b"\r\n")
        self.wfile.flush()
        self.close_connection = True
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if generation == 1 and (root / "drop-first-session").exists():
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                    return
                readable, _, _ = select.select([self.connection], [], [], 0.02)
                if readable and not self.connection.recv(1, socket.MSG_PEEK):
                    return
        finally:
            with lock:
                sessions.discard(generation)
            event("session-closed", generation=generation)
    def do_POST(self):
        req = self.read_request()
        if not self.authenticated():
            self.reply({"error": "invalid parent PID"}, 403)
            return
        if self.path == "/internal/run/assignment":
            with lock:
                active = bool(sessions)
                counts["assignment"] += 1
            if not active or req != {"kind": "sandbox", "run_id": run_id}:
                self.reply({"error": "assignment without session"}, 409)
                return
            event("assignment")
            self.reply({"kind": "sandbox", "run_id": run_id, "task_id": "probe"})
        elif self.path == "/internal/task/sandbox/bootstrap":
            if not ready_connected.wait(2) or req.get("sandbox_id") != "probe" or req.get("run_id") != run_id:
                self.reply({"error": "bootstrap before readiness or wrong identity"}, 409)
                return
            with lock:
                counts["bootstrap"] += 1
            event("bootstrap")
            self.reply({"sandbox_id": "probe", "run_id": run_id, "workdir": str(root / "wd"),
                        "env": {"MANIFEST_KEY": "task-authoritative-key"},
                        "final": {"exec": str(root / "marker.py"), "args": ["run", "A", "B"],
                                  "workdir": str(root / "wd"), "env": {"SECRET": "fixture-value", "PROBE_ROOT": str(root)}}})
        elif self.path == "/internal/run/sandbox-result":
            result = req.get("result", {})
            if req.get("run_id") != run_id or req.get("sandbox_id") != "probe" or result.get("run_id") != run_id or result.get("sid") != "probe":
                self.reply({"error": "result identity mismatch"}, 409)
                return
            with lock:
                counts["result"] += 1
            event("result", result=result, pid=self.peer_pid())
            deadline = time.monotonic() + 10
            while not (root / "allow-result-ack").exists():
                if time.monotonic() >= deadline:
                    self.reply({"error": "test did not allow ACK"}, 500)
                    return
                time.sleep(0.02)
            self.reply({})
        else:
            self.reply({"error": "unexpected path"}, 404)
class Srv(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
srv = Srv(str(root / "node-ctl.socket"), H)
srv.serve_forever()
SERVER
python3 "$WORK/server.py" "$WORK" "$RUN_ID" 2>"$WORK/server.log" &
SRV_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { cat "$WORK/server.log"; fail "fake config socket did not come up"; }

UNIT="e2e-runtask@$RUN_ID"
echo "==> run-sandbox: resident parent in delegated transient unit $UNIT"
systemd-run --quiet --unit="$UNIT" --service-type=exec \
    --slice=sandbox-runner.slice \
    --property=Delegate=yes --property=KillMode=control-group \
    --setenv="TASK_PIDFILE=$PIDFILE" --setenv="TASK_CONFIG_SOCKET=$SOCK" \
    --setenv="TASK_RUN_ID=$RUN_ID" --setenv=TASK_SANDBOX_ID=legacy \
    --setenv=MANIFEST_KEY=inherited-wrong-key \
    "$ORCH" run-sandbox
for _ in $(seq 1 100); do [ -s "$OUTFILE" ] && [ -f "$READY_WIRE" ] && break; sleep 0.1; done
[ -s "$OUTFILE" ] && [ -f "$READY_WIRE" ] || {
    journalctl -u "$UNIT.service" --no-pager -n 80 >&2 || true
    cat "$WORK/server.log" >&2
    fail "direct child did not finish readiness"
}
printf 'control_ready\nready\n' > "$WORK/ready.expected"
cmp -s "$WORK/ready.expected" "$READY_WIRE" || fail "readiness wire was not exact"
PARENT_PID="$(tr -d '[:space:]' < "$PIDFILE")"
CHILD_PID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pid"])' "$OUTFILE")"
[ "$PARENT_PID" != "$CHILD_PID" ] || fail "runtime replaced the resident parent"
[ "$(systemctl show "$UNIT.service" -p MainPID --value)" = "$PARENT_PID" ] || fail "unit MainPID is not the parent"
[ "$(readlink "/proc/$PARENT_PID/exe")" = "$(readlink -f "$ORCH")" ] || fail "MainPID no longer executes node-ctl"
[ "$(cat "$RUN_ROOT/sandboxes/probe/probe.pid")" = "$CHILD_PID" ] || fail "runtime PID identity is not child-owned"
UNIT_CGROUP="$(systemctl show "$UNIT.service" -p ControlGroup --value)"
for pid in "$PARENT_PID" "$CHILD_PID"; do
    [ "$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup")" = "$UNIT_CGROUP/ctl" ] || fail "process $pid escaped delegated ctl"
done
[ ! -s "/sys/fs/cgroup$UNIT_CGROUP/cgroup.procs" ] || fail "unit root contains a process"
python3 - "$WORK" "$PARENT_PID" <<'CHECK'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
marker = json.loads((root / "marker.json").read_text())
assert marker["ppid"] == int(sys.argv[2])
assert marker["parent_only_fds_absent"]
events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
names = [e["event"] for e in events]
assert names.index("session") < names.index("assignment") < names.index("readiness-connect") < names.index("bootstrap") < names.index("readiness-eof"), names
assert "result" not in names, "result arrived while child still runs"
CHECK
echo "==> PASS: resident parent/direct child identity, FD isolation and readiness EOF before child exit"

# Reconnect must not ask for assignment or launch another child.
touch "$WORK/drop-first-session"
for _ in $(seq 1 50); do
    [ "$(grep -c '"event": "session"' "$WORK/events.jsonl")" -ge 2 ] && break
    sleep 0.1
done
[ "$(grep -c '"event": "session"' "$WORK/events.jsonl")" -ge 2 ] || fail "parent did not reconnect"
[ "$(grep -c '"event": "assignment"' "$WORK/events.jsonl")" = 1 ] || fail "session reconnect repeated assignment"
[ "$(grep -c '"event": "bootstrap"' "$WORK/events.jsonl")" = 1 ] || fail "session reconnect repeated bootstrap"
kill -0 "$CHILD_PID" || fail "session reconnect killed the child"
echo "==> PASS: same-run reconnect without duplicate assignment or child launch"

DUP_UNIT="e2e-runtask-duplicate@$RUN_ID"
set +e
systemd-run --quiet --wait --pipe --unit="$DUP_UNIT" --service-type=exec \
    --slice=sandbox-runner.slice \
    --property=Delegate=yes --property=KillMode=control-group \
    --setenv="TASK_PIDFILE=$PIDFILE" --setenv="TASK_CONFIG_SOCKET=$SOCK" \
    --setenv="TASK_RUN_ID=$RUN_ID" "$ORCH" run-sandbox > "$WORK/dup.log" 2>&1
DUP_RC=$?
set -e
[ "$DUP_RC" -ne 0 ] || fail "second parent succeeded despite held RunID PID lock"
grep -qi 'lock' "$WORK/dup.log" || { cat "$WORK/dup.log"; fail "double-start omitted lock error"; }
echo "==> PASS: duplicate parent refused"

# The child exits with a known failure. Hold the result ACK briefly and prove
# that the unit parent, not a detached reporter, remains responsible for it.
touch "$WORK/release-child"
for _ in $(seq 1 50); do grep -q '"event": "result"' "$WORK/events.jsonl" && break; sleep 0.1; done
grep -q '"event": "result"' "$WORK/events.jsonl" || fail "child exit was not reported"
kill -0 "$PARENT_PID" || fail "parent exited before its result ACK"
[ ! -e "/proc/$CHILD_PID" ] || fail "direct child was not reaped before reporting"
python3 - "$WORK/events.jsonl" "$PARENT_PID" "$RUN_ID" <<'RESULT'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
results = [e for e in events if e["event"] == "result"]
assert len(results) == 1, results
r = results[0]
assert r["pid"] == int(sys.argv[2])
assert r["result"]["stage"] == "run" and r["result"]["exit_code"] == 7, r
assert r["result"]["sid"] == "probe" and r["result"]["run_id"] == sys.argv[3]
RESULT
touch "$WORK/allow-result-ack"
for _ in $(seq 1 50); do [ ! -e "/proc/$PARENT_PID" ] && break; sleep 0.1; done
[ ! -e "/proc/$PARENT_PID" ] || fail "parent stayed alive after ACK"
[ "$(grep -c '"event": "result"' "$WORK/events.jsonl")" = 1 ] || fail "parent emitted duplicate/conflicting results"
echo "==> PASS: child exit reaped once and reported once by parent before exit"

systemctl stop "$UNIT.service"
for _ in $(seq 1 50); do [ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] && break; sleep 0.1; done
[ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] || fail "delegated hierarchy survived unit cleanup"
systemctl reset-failed "$UNIT.service" >/dev/null 2>&1 || true
UNIT=""
echo "==> PASS: unit cleanup removed delegated ctl/vmm hierarchy"
echo "==> e2e_runtask: OK"
