#!/usr/bin/env bash
#
# e2e_runtask.sh — verify the in-unit task launchers run-sandbox / run-builder and the
# new config/info CLI surfaces, WITHOUT needing root / systemd / KVM (pure userspace).
#
#   1. CLI smokes      orchestrator-ctl/flatten-ctl/sandbox-ctl `config` round-trip
#                      (--template emits, --config re-loads + validates); arg-validation
#                      for run-sandbox / run-builder and sandbox-ctl info.
#   2. run-sandbox     against a fake config-socket (python): verifies it locks+writes
#                      the pidfile, fetches the LaunchSpec over HTTP (h2c-capable UDS),
#                      exec-replaces into the target (PID inherited) with the spec's
#                      args/workdir/env, strips TASK_* from the child env, and that a
#                      second run-sandbox on the held pidfile is refused (double-start).
#
# Missing prerequisites (binaries / python3) → exit 0 ("skipped") unless REQUIRE_RUNTASK=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
ORCH="$BIN/orchestrator-ctl"
SANDBOX="$BIN/sandbox-ctl"
FLATTEN="$BIN/flatten-ctl"

skip() {
    echo; echo "==> e2e_runtask: skipping ($*)"
    [ "${REQUIRE_RUNTASK:-0}" = "1" ] && { echo "REQUIRE_RUNTASK=1 set; failing instead" >&2; exit 1; }
    exit 0
}
fail() { echo "==> FAIL: $*" >&2; exit 1; }

for b in "$ORCH" "$SANDBOX" "$FLATTEN"; do [ -x "$b" ] || skip "missing $b — run 'make build'"; done
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH (needed for the fake config-socket)"

WORK="$(mktemp -d /tmp/e2e-runtask-XXXXXX)"
trap '[ -n "${E2E_KEEP:-}" ] && echo "kept $WORK" || rm -rf "$WORK"; [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; [ -n "${RT_PID:-}" ] && kill "$RT_PID" 2>/dev/null; true' EXIT

# ---- 1. CLI smokes --------------------------------------------------------
echo "==> CLI: config --template + round-trip (validate)"
"$ORCH" config --template > "$WORK/orch.yaml"
grep -q "domain:" "$WORK/orch.yaml" || fail "orchestrator-ctl config --template missing domain"
"$ORCH" config --config "$WORK/orch.yaml" >/dev/null || fail "orchestrator-ctl config --config did not validate the template"

"$FLATTEN" config --template > "$WORK/flatten.yaml"
grep -q "referer:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer"
grep -q "enabled:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer.enabled"
"$FLATTEN" config --config "$WORK/flatten.yaml" >/dev/null || fail "flatten-ctl config --config did not load"

"$SANDBOX" config --template > "$WORK/sb.yaml" 2>/dev/null || true
grep -q "resources:" "$WORK/sb.yaml" || fail "sandbox-ctl config --template missing resources"
echo "==> PASS: config --template + round-trip for orchestrator-ctl/flatten-ctl/sandbox-ctl"

echo "==> CLI: arg-validation (must reject)"
"$ORCH" run-sandbox >/dev/null 2>&1 && fail "run-sandbox with no args should fail" || true
"$ORCH" run-builder >/dev/null 2>&1 && fail "run-builder with no args should fail" || true
"$SANDBOX" info >/dev/null 2>&1 && fail "sandbox-ctl info with no arg should fail" || true
"$SANDBOX" info /dev/null >/dev/null 2>&1 && fail "sandbox-ctl info on a non-snapshot should fail" || true
echo "==> PASS: run-sandbox/run-builder + sandbox-ctl info reject bad invocation"

# ---- 2. run-sandbox against a fake config-socket --------------------------
SOCK="$WORK/orchestrator.socket"
PIDFILE="$WORK/task.pid"
OUTFILE="$WORK/marker.out"
mkdir -p "$WORK/wd"

# Target: record argv / cwd / injected secret / (stripped) TASK_* env, then sleep
# so the pidfile lock stays held while we probe double-start.
cat > "$WORK/marker.sh" <<EOF
#!/usr/bin/env bash
{
  echo "args=[\$*]"
  echo "cwd=\$PWD"
  echo "secret=[\${SECRET:-}]"
  echo "task_pidfile=[\${TASK_PIDFILE:-}]"
  echo "task_sandbox_id=[\${TASK_SANDBOX_ID:-}]"
} > "$OUTFILE"
sleep 30
EOF
chmod +x "$WORK/marker.sh"

# Fake config-socket: an HTTP server over the UDS answering POST /internal/task/
# launchspec with a LaunchSpec that exec's the marker (the run-sandbox client speaks
# HTTP/h2c). The request body (config_id=sandbox:probe) is ignored — always same spec.
cat > "$WORK/server.py" <<EOF
import json, os, socketserver, sys
from http.server import BaseHTTPRequestHandler
spec = {"exec": "$WORK/marker.sh", "args": ["A", "B"],
        "workdir": "$WORK/wd", "env": {"SECRET": "s3cr3t"}}
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        if n: self.rfile.read(n)            # request JSON (ignored: we always serve the same spec)
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
sys.stderr.write("server-ready\n"); sys.stderr.flush()
srv.serve_forever()
EOF
python3 "$WORK/server.py" 2>"$WORK/server.log" &
SRV_PID=$!
for i in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { cat "$WORK/server.log"; fail "fake config-socket did not come up"; }

echo "==> run-sandbox: launch via TASK_* env (env-default + stripping)"
TASK_PIDFILE="$PIDFILE" TASK_CONFIG_SOCKET="$SOCK" TASK_SANDBOX_ID="probe" \
    "$ORCH" run-sandbox &
RT_PID=$!
for i in $(seq 1 50); do [ -s "$OUTFILE" ] && break; sleep 0.1; done
[ -s "$OUTFILE" ] || { cat "$WORK/server.log"; fail "target did not run (run-sandbox never exec'd)"; }
echo "--- marker output ---"; sed 's/^/    /' "$OUTFILE"

grep -q "args=\[A B\]"           "$OUTFILE" || fail "args not delivered (want 'A B')"
grep -q "cwd=$WORK/wd"           "$OUTFILE" || fail "workdir not applied"
grep -q "secret=\[s3cr3t\]"      "$OUTFILE" || fail "spec env (SECRET) not injected"
grep -q "task_pidfile=\[\]"      "$OUTFILE" || fail "TASK_PIDFILE not stripped from child env"
grep -q "task_sandbox_id=\[\]"   "$OUTFILE" || fail "TASK_SANDBOX_ID not stripped from child env"
echo "==> PASS: run-sandbox exec-replaced target with args/workdir/env; TASK_* stripped"

# PID inheritance: run-sandbox exec'd the marker, so RT_PID == pidfile contents.
PIDF="$(tr -d '[:space:]' < "$PIDFILE" 2>/dev/null || true)"
[ "$PIDF" = "$RT_PID" ] || fail "pidfile=$PIDF != run-sandbox pid=$RT_PID (exec did not inherit PID)"
kill -0 "$RT_PID" 2>/dev/null || fail "target process (inherited PID) is not alive"
echo "==> PASS: target inherited run-sandbox's PID ($RT_PID); pidfile matches"

echo "==> run-sandbox: second launch on the held pidfile must be refused"
set +e
TASK_PIDFILE="$PIDFILE" TASK_CONFIG_SOCKET="$SOCK" TASK_SANDBOX_ID="dup" \
    "$ORCH" run-sandbox > "$WORK/dup.log" 2>&1
DUP_RC=$?
set -e
[ "$DUP_RC" -ne 0 ] || fail "second run-sandbox succeeded despite held pidfile lock (double-start!)"
grep -qi "lock" "$WORK/dup.log" || { sed 's/^/    /' "$WORK/dup.log"; fail "double-start error did not mention the lock"; }
echo "==> PASS: double-start refused ($(grep -oi 'already running[^)]*' "$WORK/dup.log" | head -1 || echo 'pidfile locked'))"

echo "==> e2e_runtask: OK"
