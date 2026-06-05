#!/usr/bin/env bash
#
# e2e_runtask.sh — verify the run-task universal launcher and the new config/info
# CLI surfaces, WITHOUT needing root / systemd / KVM (pure userspace).
#
#   1. CLI smokes      orchestrator-ctl/flatten-ctl/sandbox-ctl `config` round-trip
#                      (--template emits, --config re-loads + validates); arg-validation
#                      for run-task and sandbox-ctl info.
#   2. run-task        against a fake config-socket (python): verifies it locks+writes
#                      the pidfile, fetches the LaunchSpec over the 4-byte-LE framing,
#                      exec-replaces into the target (PID inherited) with the spec's
#                      args/workdir/env, strips TASK_* from the child env, and that a
#                      second run-task on the held pidfile is refused (double-start guard).
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
grep -q "^domain:" "$WORK/orch.yaml" || fail "orchestrator-ctl config --template missing domain"
"$ORCH" config --config "$WORK/orch.yaml" >/dev/null || fail "orchestrator-ctl config --config did not validate the template"

"$FLATTEN" config --template > "$WORK/flatten.yaml"
grep -q "referer:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer"
grep -q "enabled:" "$WORK/flatten.yaml" || fail "flatten-ctl config --template missing referer.enabled"
"$FLATTEN" config --config "$WORK/flatten.yaml" >/dev/null || fail "flatten-ctl config --config did not load"

"$SANDBOX" config --template > "$WORK/sb.yaml" 2>/dev/null || true
grep -q "resources:" "$WORK/sb.yaml" || fail "sandbox-ctl config --template missing resources"
echo "==> PASS: config --template + round-trip for orchestrator-ctl/flatten-ctl/sandbox-ctl"

echo "==> CLI: arg-validation (must reject)"
"$ORCH" run-task >/dev/null 2>&1 && fail "run-task with no args should fail" || true
"$SANDBOX" info >/dev/null 2>&1 && fail "sandbox-ctl info with no arg should fail" || true
"$SANDBOX" info /dev/null >/dev/null 2>&1 && fail "sandbox-ctl info on a non-snapshot should fail" || true
echo "==> PASS: run-task + sandbox-ctl info reject bad invocation"

# ---- 2. run-task against a fake config-socket -----------------------------
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
  echo "task_config_id=[\${TASK_CONFIG_ID:-}]"
} > "$OUTFILE"
sleep 30
EOF
chmod +x "$WORK/marker.sh"

# Fake config-socket: speak the [4-byte LE len][JSON] framing; reply with a
# LaunchSpec that exec's the marker with args/workdir/env.
cat > "$WORK/server.py" <<EOF
import json, os, socket, struct, sys
spec = {"exec": "$WORK/marker.sh", "args": ["A", "B"],
        "workdir": "$WORK/wd", "env": {"SECRET": "s3cr3t"}}
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
try: os.unlink("$SOCK")
except FileNotFoundError: pass
s.bind("$SOCK"); s.listen(4)
sys.stderr.write("server-ready\n"); sys.stderr.flush()
while True:
    c, _ = s.accept()
    hdr = c.recv(4)
    if len(hdr) < 4: c.close(); continue
    n = struct.unpack("<I", hdr)[0]
    _ = c.recv(n)                       # request JSON (ignored: we always serve the same spec)
    body = json.dumps(spec).encode()
    c.sendall(struct.pack("<I", len(body)) + body)
    c.close()
EOF
python3 "$WORK/server.py" 2>"$WORK/server.log" &
SRV_PID=$!
for i in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { cat "$WORK/server.log"; fail "fake config-socket did not come up"; }

echo "==> run-task: launch via TASK_* env (env-default + stripping)"
TASK_PIDFILE="$PIDFILE" TASK_CONFIG_SOCKET="$SOCK" TASK_CONFIG_ID="sandbox:probe" \
    "$ORCH" run-task &
RT_PID=$!
for i in $(seq 1 50); do [ -s "$OUTFILE" ] && break; sleep 0.1; done
[ -s "$OUTFILE" ] || { cat "$WORK/server.log"; fail "target did not run (run-task never exec'd)"; }
echo "--- marker output ---"; sed 's/^/    /' "$OUTFILE"

grep -q "args=\[A B\]"           "$OUTFILE" || fail "args not delivered (want 'A B')"
grep -q "cwd=$WORK/wd"           "$OUTFILE" || fail "workdir not applied"
grep -q "secret=\[s3cr3t\]"      "$OUTFILE" || fail "spec env (SECRET) not injected"
grep -q "task_pidfile=\[\]"      "$OUTFILE" || fail "TASK_PIDFILE not stripped from child env"
grep -q "task_config_id=\[\]"    "$OUTFILE" || fail "TASK_CONFIG_ID not stripped from child env"
echo "==> PASS: run-task exec-replaced target with args/workdir/env; TASK_* stripped"

# PID inheritance: run-task exec'd the marker, so RT_PID == pidfile contents.
PIDF="$(tr -d '[:space:]' < "$PIDFILE" 2>/dev/null || true)"
[ "$PIDF" = "$RT_PID" ] || fail "pidfile=$PIDF != run-task pid=$RT_PID (exec did not inherit PID)"
kill -0 "$RT_PID" 2>/dev/null || fail "target process (inherited PID) is not alive"
echo "==> PASS: target inherited run-task's PID ($RT_PID); pidfile matches"

echo "==> run-task: second launch on the held pidfile must be refused"
set +e
TASK_PIDFILE="$PIDFILE" TASK_CONFIG_SOCKET="$SOCK" TASK_CONFIG_ID="sandbox:dup" \
    "$ORCH" run-task > "$WORK/dup.log" 2>&1
DUP_RC=$?
set -e
[ "$DUP_RC" -ne 0 ] || fail "second run-task succeeded despite held pidfile lock (double-start!)"
grep -qi "lock" "$WORK/dup.log" || { sed 's/^/    /' "$WORK/dup.log"; fail "double-start error did not mention the lock"; }
echo "==> PASS: double-start refused ($(grep -oi 'already running[^)]*' "$WORK/dup.log" | head -1 || echo 'pidfile locked'))"

echo "==> e2e_runtask: OK"
