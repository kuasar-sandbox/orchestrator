#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"
. "${E2E_LIB:?}/orchestrator/case_workspace.sh"
. "$E2E_LIB/orchestrator/runtask_privilege.sh"
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
    if [ -n "$SRV_PID" ]; then
        kill "$SRV_PID" 2>/dev/null
        wait "$SRV_PID" 2>/dev/null || true
    fi
    case_workspace_cleanup
}
trap cleanup EXIT
fail() { echo "FAIL orchestrator.runtask.sh: $*" >&2; exit 1; }

command -v python3 >/dev/null
command -v systemd-run >/dev/null
[ -d /run/systemd/system ] || { echo "systemd manager is required" >&2; exit 1; }
runtask_enter_privileged case_workspace_cleanup "$0" "$@"
systemctl show-environment >/dev/null 2>&1 || { echo "systemd manager is unavailable" >&2; exit 1; }
case_workspace_init

SOCK="$WORK/node-ctl.socket"
RUN_ID="sr-00000000-0000-7000-8000-$(python3 -c 'import uuid; print(uuid.uuid4().hex[-12:])')"
RUN_ROOT="$WORK/runroot"
PIDFILE="$RUN_ROOT/runners/$RUN_ID.pid"
READY_SOCK="$RUN_ROOT/sandboxes/probe/ready.sock"
READY_WIRE="$WORK/ready.wire"
mkdir -p "$WORK/wd" "$RUN_ROOT/runners" "$RUN_ROOT/sandboxes/probe"

# Block SDK config loading on a task-owned FIFO. This exercises the resident
# owner/session/lock boundary without requiring a guest or spawning sandbox-ctl.
mkfifo "$WORK/sandbox.yaml"
cat > "$WORK/sandbox-ctl" <<'CHILD'
#!/bin/sh
touch "${PROBE_ROOT}/unexpected-child"
exit 7
CHILD
chmod +x "$WORK/sandbox-ctl"

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
                        "final": {"exec": str(root / "sandbox-ctl"), "args": [
                            "run", "--sandbox-id", "probe", "--path-id", "probe",
                            "--config", str(root / "sandbox.yaml"), "--manifest-config", "",
                            "--run-root", str(root / "runroot/sandboxes"), "--base-root", str(root / "base"),
                            "--log-to", "default", "--stdout-to", str(root / "stdout"),
                            "--stderr-to", str(root / "stderr"), "--console", "off"],
                                  "workdir": str(root / "wd"), "env": {"SECRET": "fixture-value", "PROBE_ROOT": str(root),
                                      "SANDBOX_CH_PATH": str(root / "unused-cloud-hypervisor")}}})
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

UNIT="e2e-runtask-${WORK##*/}@$RUN_ID"
echo "==> run-sandbox: resident parent in delegated transient unit $UNIT"
systemd-run --quiet --unit="$UNIT" --service-type=exec \
    --slice=sandbox-runner.slice \
    --property=Delegate=yes --property=KillMode=control-group \
    --setenv="TASK_PIDFILE=$PIDFILE" --setenv="TASK_CONFIG_SOCKET=$SOCK" \
    --setenv="TASK_RUN_ID=$RUN_ID" --setenv=TASK_SANDBOX_ID=legacy \
    --setenv=MANIFEST_KEY=inherited-wrong-key \
    "$ORCH" run-sandbox
for _ in $(seq 1 100); do
    [ -f "$WORK/events.jsonl" ] && grep -q '"event": "bootstrap"' "$WORK/events.jsonl" && break
    sleep 0.1
done
grep -q '"event": "bootstrap"' "$WORK/events.jsonl" || fail "SDK bootstrap not received"
PARENT_PID="$(tr -d '[:space:]' < "$PIDFILE")"
[ "$(systemctl show "$UNIT.service" -p MainPID --value)" = "$PARENT_PID" ] || fail "unit MainPID is not the SDK owner"
[ "$(readlink "/proc/$PARENT_PID/exe")" = "$(readlink -f "$ORCH")" ] || fail "MainPID no longer executes node-ctl"
UNIT_CGROUP="$(systemctl show "$UNIT.service" -p ControlGroup --value)"
[ "$(awk -F: '$1 == "0" { print $3 }' "/proc/$PARENT_PID/cgroup")" = "$UNIT_CGROUP/ctl" ] || fail "SDK owner escaped ctl"
[ "$(cat "/sys/fs/cgroup$UNIT_CGROUP/ctl/cgroup.procs")" = "$PARENT_PID" ] || fail "unexpected ctl child"
[ ! -s "/sys/fs/cgroup$UNIT_CGROUP/cgroup.procs" ] || fail "unit root contains a process"
[ ! -s "/sys/fs/cgroup$UNIT_CGROUP/vmm/cgroup.procs" ] || fail "VMM started before config load"
[ ! -e "$WORK/unexpected-child" ] || fail "sandbox-ctl executable was invoked"
python3 - "$WORK" <<'CHECK'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
names = [e["event"] for e in events]
assert names.index("session") < names.index("assignment") < names.index("readiness-connect") < names.index("bootstrap"), names
assert "result" not in names, "result arrived while SDK config loading is blocked"
CHECK
echo "==> PASS: SDK owner identity, assignment ordering and no sandbox-ctl process"

# Reconnect must not ask for assignment or execute the SDK twice.
touch "$WORK/drop-first-session"
for _ in $(seq 1 50); do
    [ "$(grep -c '"event": "session"' "$WORK/events.jsonl")" -ge 2 ] && break
    sleep 0.1
done
[ "$(grep -c '"event": "session"' "$WORK/events.jsonl")" -ge 2 ] || fail "parent did not reconnect"
[ "$(grep -c '"event": "assignment"' "$WORK/events.jsonl")" = 1 ] || fail "session reconnect repeated assignment"
[ "$(grep -c '"event": "bootstrap"' "$WORK/events.jsonl")" = 1 ] || fail "session reconnect repeated bootstrap"
kill -0 "$PARENT_PID" || fail "session reconnect killed the SDK owner"
echo "==> PASS: same-run reconnect without duplicate assignment or SDK execution"

DUP_UNIT="e2e-runtask-duplicate-${WORK##*/}@$RUN_ID"
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

# Release config loading with invalid YAML. The SDK fails before VM side
# effects; hold the result ACK to prove the owner reports after readiness EOF.
timeout 10 sh -c 'printf "invalid: [\n" > "$1"' sh "$WORK/sandbox.yaml"
for _ in $(seq 1 50); do grep -q '"event": "result"' "$WORK/events.jsonl" && [ -f "$READY_WIRE" ] && break; sleep 0.1; done
grep -q '"event": "result"' "$WORK/events.jsonl" || fail "SDK failure was not reported"
[ -f "$READY_WIRE" ] && [ ! -s "$READY_WIRE" ] || fail "SDK failure did not emit readiness EOF"
kill -0 "$PARENT_PID" || fail "SDK owner exited before its result ACK"
[ ! -e "$WORK/unexpected-child" ] || fail "sandbox-ctl executable was invoked"
python3 - "$WORK/events.jsonl" "$PARENT_PID" "$RUN_ID" <<'RESULT'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
results = [e for e in events if e["event"] == "result"]
assert len(results) == 1, results
r = results[0]
assert r["pid"] == int(sys.argv[2])
assert r["result"]["stage"] == "run" and r["result"]["exit_code"] == 1, r
assert r["result"]["sid"] == "probe" and r["result"]["run_id"] == sys.argv[3]
RESULT
touch "$WORK/allow-result-ack"
for _ in $(seq 1 50); do [ ! -e "/proc/$PARENT_PID" ] && break; sleep 0.1; done
[ ! -e "/proc/$PARENT_PID" ] || fail "parent stayed alive after ACK"
[ "$(grep -c '"event": "result"' "$WORK/events.jsonl")" = 1 ] || fail "parent emitted duplicate/conflicting results"
echo "==> PASS: SDK failure reported once after readiness EOF and before owner exit"

systemctl stop "$UNIT.service"
for _ in $(seq 1 50); do [ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] && break; sleep 0.1; done
[ ! -e "/sys/fs/cgroup$UNIT_CGROUP" ] || fail "delegated hierarchy survived unit cleanup"
systemctl reset-failed "$UNIT.service" >/dev/null 2>&1 || true
UNIT=""
echo "==> PASS: unit cleanup removed delegated ctl/vmm hierarchy"
echo "PASS orchestrator.runtask.sh"
