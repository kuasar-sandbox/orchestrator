#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
ORCH_BIN_DIR="$WORK/orch-bin"
SNAPSHOT_ARGV_LOG="$WORK/snapshot-argv.jsonl"
EXPORT_ARGV_LOG="$WORK/export-argv.jsonl"
RUN_ARGV_LOG="$WORK/run-argv.jsonl"
PAUSE_BARRIER_TARGET="$WORK/pause-barrier-target"
PAUSE_BARRIER_REACHED="$WORK/pause-barrier-reached"
PAUSE_BARRIER_RELEASE="$WORK/pause-barrier-release"
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
path_id=""
if [ "\${1:-}" = "snapshot" ] && [ -f "$PAUSE_BARRIER_TARGET" ]; then
    previous=""
    for arg in "\$@"; do
        if [ "\$previous" = "--path-id" ]; then path_id="\$arg"; break; fi
        previous="\$arg"
    done
    if [ "\$path_id" = "\$(cat "$PAUSE_BARRIER_TARGET")" ]; then
        : > "$PAUSE_BARRIER_REACHED"
        while [ ! -e "$PAUSE_BARRIER_RELEASE" ]; do sleep 0.02; done
    fi
fi
if [ "\${1:-}" = "export" ]; then
    python3 - "$EXPORT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\n")
PY
fi
if [ "\${1:-}" = "run" ]; then
    python3 - "$RUN_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\n")
PY
fi
if [ "\${1:-}" = "run" ] && [ -f "$WORK/inject-sandbox-run" ]; then
    mode="\$(<"$WORK/inject-sandbox-run")"
    case "\$mode" in
        hold)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
            exit 44
            ;;
        park)
            while [ -f "$WORK/inject-sandbox-run" ]; do sleep 0.05; done
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
path_id = value("--path-id")
run_root = value("--run-root")
ready_fd = int(value("--ready-fd"))
envd_path = os.path.join(run_root, path_id, "envd.sock")
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
